"""Probe real AWS service latency — refreshes ``rules.LATENCY_PRESETS``.

REAL AWS ONLY. This script makes authenticated, read-only calls against
live AWS endpoints using your ambient credentials (env vars, ~/.aws, or
instance role). It never writes to AWS — every probe is a list/describe/
get call — but it does spend real (free-tier) API requests.

    python tools/latency_probe.py --region us-east-1 \
        --out /tmp/latency_probe.json

It times ``SAMPLES`` calls per service, prints min/p50/mean/sd/max in ms,
and writes the raw samples + stats as JSON. To update the presets, take
p50 minus your network floor (the minimum ms observed across ALL probes
is a decent floor estimate — it's mostly host→region RTT) and encode the
residual as the gaussian ``mean``; ``stddev`` is a modeling assumption
(``max(5.0, 0.35 * mean)``) since service-side variance isn't recoverable
through network noise. See the LATENCY_PRESETS comment in rules.py.

Last full run: 2026-02, us-east-1, n=8, floor ≈ 155ms.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

import boto3

SAMPLES = 8

# (service label, boto3 client name, operation, kwargs) — all read-only.
PROBES: list[tuple[str, str, str, dict]] = [
    ("dynamodb",       "dynamodb",       "list_tables",             {}),
    ("s3",             "s3",             "list_buckets",            {}),
    ("sqs",            "sqs",            "list_queues",             {}),
    ("sns",            "sns",            "list_topics",             {}),
    ("lambda",         "lambda",         "list_functions",          {}),
    ("kinesis",        "kinesis",        "list_streams",            {}),
    ("iam",            "iam",            "list_roles",              {}),
    ("ec2",            "ec2",            "describe_regions",        {}),
    ("cloudformation", "cloudformation", "list_stacks",             {}),
    ("ssm",            "ssm",            "describe_parameters",     {}),
    ("secretsmanager", "secretsmanager", "list_secrets",            {}),
    ("sts",            "sts",            "get_caller_identity",     {}),
    ("logs",           "logs",           "describe_log_groups",     {}),
    ("firehose",       "firehose",       "list_delivery_streams",   {}),
    ("events",         "events",         "list_rules",              {}),
    ("stepfunctions",  "stepfunctions",  "list_state_machines",     {}),
    ("kms",            "kms",            "list_keys",               {}),
    ("athena",         "athena",         "list_work_groups",        {}),
    ("route53",        "route53",        "list_hosted_zones",       {}),
    ("cloudfront",     "cloudfront",     "list_distributions",      {}),
    ("glacier",        "glacier",        "list_vaults",             {}),
    ("wafv2",          "wafv2",          "list_web_acls",
     {"Scope": "REGIONAL"}),
    ("elbv2",          "elbv2",          "describe_load_balancers", {}),
    ("apigateway",     "apigateway",     "get_rest_apis",           {}),
    ("pinpoint",       "pinpoint",       "get_apps",                {}),
]


def probe(client, method: str, kwargs: dict) -> list[float]:
    fn = getattr(client, method)
    samples = []
    for _ in range(SAMPLES):
        t0 = time.perf_counter()
        fn(**kwargs)
        samples.append((time.perf_counter() - t0) * 1000)
    return samples


def stats(samples: list[float]) -> dict:
    return {
        "n": len(samples),
        "min_ms": round(min(samples), 1),
        "p50_ms": round(statistics.median(samples), 1),
        "mean_ms": round(statistics.fmean(samples), 1),
        "sd_ms": round(statistics.stdev(samples), 1) if len(samples) > 1 else 0.0,
        "max_ms": round(max(samples), 1),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--out", type=Path, default=None,
                    help="write results JSON here")
    args = ap.parse_args()

    results = {}
    for label, client_name, method, kwargs in PROBES:
        client = boto3.client(client_name, region_name=args.region)
        try:
            samples = probe(client, method, kwargs)
        except Exception as e:  # noqa: BLE001 — report, keep probing
            print(f"{label:16} {method:24} FAILED: {e}")
            results[label] = {"op": method, "error": str(e)}
            continue
        s = stats(samples)
        s["op"] = method
        s["samples_ms"] = [round(v, 1) for v in samples]
        results[label] = s
        print(f"{label:16} {method:24} "
              f"p50={s['p50_ms']:7.1f}  mean={s['mean_ms']:7.1f}  "
              f"sd={s['sd_ms']:6.1f}  min={s['min_ms']:7.1f}  "
              f"max={s['max_ms']:7.1f}")

    ok = [r["min_ms"] for r in results.values() if "min_ms" in r]
    if ok:
        floor = min(ok)
        print(f"\nnetwork floor (min observed): {floor:.1f}ms — "
              f"subtract from p50 to get the service-side residual "
              f"for LATENCY_PRESETS")

    if args.out:
        payload = {
            "region": args.region,
            "samples_per_service": SAMPLES,
            "network_floor_ms": floor if ok else None,
            "results": results,
        }
        args.out.write_text(json.dumps(payload, indent=2) + "\n")
        print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
