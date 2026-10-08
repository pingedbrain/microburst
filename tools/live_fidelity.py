"""Live-AWS fidelity harness — capture real error responses and diff them
against microburst's renderer.

Captures raw wire responses (status, headers, body) by wrapping botocore's
urllib3 send — real TLS, real AWS bytes, no proxy plumbing. Then diffs each
capture against ``render_error()``: status, Content-Type, body top-level
fields, request-id placement, and — what matters most — the ``Error.Code``
the SDK parser recovers from each.

Credentials come from ``AWS_PROFILE``/env; every probe targets a
nonexistent resource (read-only — nothing is created or mutated).

    python tools/live_fidelity.py capture [--services dynamodb,s3]
    python tools/live_fidelity.py report

Artifacts land in ``fidelity/`` — captures (raw) + REPORT.md (diff).
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any

NONEXIST = "microburst-fidelity-doesnotexist-7f3a9"
CAPTURE_DIR = Path(__file__).resolve().parent.parent / "fidelity" / "captures"
REPORT = CAPTURE_DIR.parent / "REPORT.md"

# operation -> kwargs that deterministically produce a modeled error
# against AWS with zero side effects (all reference nonexistent resources)
PROBES: list[tuple[str, str, dict[str, Any], str]] = [
    # (service, boto3 method, kwargs, protocol family)
    ("dynamodb", "describe_table", {"TableName": NONEXIST}, "json"),
    ("kms", "describe_key", {"KeyId": NONEXIST}, "json"),
    ("secretsmanager", "describe_secret", {"SecretId": NONEXIST}, "json"),
    ("logs", "get_log_events",
     {"logGroupName": NONEXIST, "logStreamName": "x",
      "startTime": 0, "endTime": 1}, "json"),
    ("sqs", "get_queue_url", {"QueueName": NONEXIST}, "query"),
    ("sns", "get_topic_attributes",
     {"TopicArn": f"arn:aws:sns:us-east-1:000000000000:{NONEXIST}"}, "query"),
    ("iam", "get_user", {"UserName": NONEXIST}, "query"),
    ("ec2", "describe_instances",
     {"InstanceIds": ["i-0000000000000000x"]}, "ec2"),
    ("s3", "head_bucket", {"Bucket": NONEXIST}, "rest-xml"),
    ("s3", "head_object", {"Bucket": NONEXIST, "Key": "x"}, "rest-xml"),
    ("route53", "get_hosted_zone",
     {"Id": f"/hostedzone/Z{NONEXIST[:14].upper()}"}, "rest-xml"),
    ("lambda", "get_function", {"FunctionName": NONEXIST}, "rest-json"),
    ("apigateway", "get_rest_api", {"restApiId": NONEXIST}, "rest-json"),
    # — second sweep: more families —
    ("kinesis", "describe_stream", {"StreamName": NONEXIST}, "json"),
    ("stepfunctions", "describe_state_machine",
     {"stateMachineArn":
      f"arn:aws:states:us-east-1:000000000000:stateMachine:{NONEXIST}"}, "json"),
    ("cognito-idp", "describe_user_pool",
     {"UserPoolId": "us-east-1_Xxxxx"}, "json"),
    ("athena", "get_work_group", {"WorkGroup": NONEXIST}, "json"),
    ("route53resolver", "get_resolver_endpoint",
     {"ResolverEndpointId": NONEXIST}, "json"),
    ("wafv2", "get_web_acl",
     {"Name": NONEXIST, "Scope": "REGIONAL", "Id": NONEXIST}, "json"),
    # query-compat JSON on the wire even though the model says rpc-v2-cbor
    ("cloudwatch", "get_dashboard",
     {"DashboardName": NONEXIST}, "query-compat"),
    ("events", "describe_event_bus", {"Name": NONEXIST}, "query-compat"),
    ("glacier", "describe_vault",
     {"accountId": "-", "vaultName": NONEXIST}, "rest-json"),
    ("sesv2", "get_email_identity", {"EmailIdentity": NONEXIST}, "rest-json"),
    ("pinpoint", "get_app", {"ApplicationId": NONEXIST}, "rest-json"),
    ("appsync", "get_graphql_api", {"apiId": NONEXIST}, "rest-json"),
    ("elbv2", "describe_load_balancers",
     {"Names": [NONEXIST]}, "query"),
    ("rds", "describe_db_instances",
     {"DBInstanceIdentifier": NONEXIST}, "query"),
    ("cloudformation", "describe_stacks",
     {"StackName": NONEXIST}, "query"),
]


def capture(services: list[str] | None) -> int:
    """Run probes against real AWS, dumping raw wire responses."""
    import boto3
    import botocore.httpsession
    from botocore.exceptions import ClientError, EndpointConnectionError

    captures: dict[str, dict] = {}

    def wrap_send(orig):
        def send(self, request):
            resp = orig(self, request)
            captures["last"] = {
                "status": resp.status_code,
                "headers": dict(resp.headers),
                "method": request.method,
                "url": request.url,
                "body_b64": None,
                "body": None,
            }
            content = resp.content
            try:
                captures["last"]["body"] = content.decode("utf-8")
            except UnicodeDecodeError:
                import base64
                captures["last"]["body_b64"] = base64.b64encode(content).decode()
            return resp
        return send

    botocore.httpsession.URLLib3Session.send = wrap_send(
        botocore.httpsession.URLLib3Session.send
    )

    # Redact the caller account id so captures are safe to commit.
    try:
        account_id = boto3.client("sts").get_caller_identity()["Account"]
    except Exception:  # noqa: BLE001
        account_id = None

    CAPTURE_DIR.mkdir(parents=True, exist_ok=True)
    wanted = {s for s in services} if services else None
    n_ok = 0
    for service, method, kwargs, family in PROBES:
        if wanted and service not in wanted:
            continue
        client = boto3.client(service, region_name="us-east-1")
        try:
            getattr(client, method)(**kwargs)
            print(f"  {service}.{method}: no error?? probe returned success")
            continue
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "?")
        except EndpointConnectionError as e:
            print(f"  {service}.{method}: unreachable ({e})")
            continue
        except Exception as e:  # noqa: BLE001 — report and continue
            print(f"  {service}.{method}: {type(e).__name__}: {e}")
            continue
        cap = captures.pop("last", None)
        if cap is None:
            print(f"  {service}.{method}: error but no capture")
            continue
        if account_id and cap.get("body"):
            cap["body"] = cap["body"].replace(account_id, "000000000000")
        cap.update(
            service=service, operation=method, family=family,
            model_service=client.meta.service_model.service_name,
            sdk_error_code=code, ts=time.time(),
        )
        fname = CAPTURE_DIR / f"{service}_{method}.json"
        fname.write_text(json.dumps(cap, indent=2, sort_keys=True))
        print(f"  {service}.{method} → {code} ({cap['status']}) captured")
        n_ok += 1
    print(f"{n_ok} captures → {CAPTURE_DIR}")
    return 0 if n_ok else 1


def _top_keys(body: str | None) -> list[str]:
    if not body:
        return []
    try:
        obj = json.loads(body)
        return sorted(obj) if isinstance(obj, dict) else ["<non-dict>"]
    except ValueError:
        return ["<non-json>"]


def report() -> int:
    """Diff every capture against what microburst would have rendered."""
    from botocore.parsers import create_parser
    from botocore.session import Session

    from microburst.protocols import render_error

    session = Session()
    rows = []
    for path in sorted(CAPTURE_DIR.glob("*.json")):
        cap = json.loads(path.read_text())
        svc, op = cap["service"], cap["operation"]
        code = cap["sdk_error_code"]
        real_status, real_ct = cap["status"], cap["headers"].get("Content-Type", "")
        real_body = cap.get("body") or ""

        model = session.get_service_model(cap.get("model_service") or svc)
        protocol = model.protocol
        # migrated services speak query-compat JSON on the wire even when the
        # model says cbor — use the wire evidence, same as detection does.
        if "x-amzn-query-error" in cap["headers"] or (
            real_ct.startswith("application/x-amz-json") and protocol == "smithy-rpc-v2-cbor"
        ):
            protocol = "json"

        ours_status, ours_headers, ours_body = render_error(
            cap.get("model_service") or svc, code, "(fidelity)",
            protocol=protocol,
        )
        if cap.get("method") == "HEAD":
            ours_body = b""
        ours_ct = ours_headers.get("Content-Type", "")

        parser = create_parser(protocol)
        # lowercased header keys — botocore's dict lookup is case-sensitive;
        # real transports hand it a case-insensitive mapping.
        ours_parsed = parser.parse(
            {"status_code": ours_status,
             "headers": {k.lower(): v for k, v in ours_headers.items()},
             "body": ours_body}, None)
        real_parsed = parser.parse(
            {"status_code": real_status,
             "headers": {k.lower(): v for k, v in cap["headers"].items()},
             "body": real_body.encode() if real_body else b""}, None)
        ours_code = (ours_parsed.get("Error") or {}).get("Code")
        real_code = (real_parsed.get("Error") or {}).get("Code")

        status_ok = ours_status == real_status
        code_ok = ours_code == real_code
        ct_ok = ours_ct.split(";")[0] == real_ct.split(";")[0]
        verdict = "✅" if (status_ok and code_ok and ct_ok) else "❌"
        rows.append((
            svc, op, code, real_status, ours_status,
            real_ct or "—", ours_ct, verdict,
            status_ok, code_ok, ct_ok,
            {"real_keys": _top_keys(real_body),
             "ours_keys": _top_keys(ours_body.decode()),
             "real_rid_headers": sorted(
                 h for h in cap["headers"] if "request" in h.lower() or "id" in h.lower()),
             "ours_rid_headers": sorted(
                 h for h in ours_headers if "request" in h.lower() or "id" in h.lower()),
             "real_body": real_body[:400],
             "ours_body": ours_body.decode()[:400]},
        ))

    lines = [
        "# Live-AWS fidelity report", "",
        (
            f"Captured {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())} "
            "against real AWS (us-east-1). Each probe targets a nonexistent "
            "resource; captures are raw wire bytes via botocore's transport."
        ),
        "",
        "| service | operation | error code | AWS status | ours | AWS CT | ours | verdict |",
        "|---|---|---|---|---|---|---|---|",
    ]
    n_pass = 0
    for svc, op, code, rs, os_, rct, oct_, verdict, s_ok, c_ok, ct_ok, detail in rows:
        if s_ok and c_ok:
            n_pass += 1
        lines.append(
            f"| {svc} | {op} | `{code}` | {rs} | {os_} | `{rct}` | `{oct_}` | {verdict} |"
        )
    lines += ["", f"**{n_pass}/{len(rows)} probes: status + parsed Error.Code match.**", ""]

    # field-level detail for anything that isn't a clean match
    interesting = [r for r in rows if not (r[8] and r[9] and r[10])]
    if interesting:
        lines += ["## Diffs", ""]
        for svc, op, code, rs, os_, rct, oct_, v, s_ok, c_ok, ct_ok, d in interesting:
            lines.append(f"### {svc}.{op} (`{code}`)")
            if not s_ok:
                lines.append(f"- status: AWS {rs} vs ours {os_}")
            if not c_ok:
                lines.append("- parsed Error.Code differs")
            if not ct_ok:
                lines.append(f"- content-type: AWS `{rct}` vs ours `{oct_}`")
            lines.append(f"- real body keys: `{d['real_keys']}` vs ours `{d['ours_keys']}`")
            lines.append(f"- real rid headers: `{d['real_rid_headers']}` vs ours `{d['ours_rid_headers']}`")
            lines.append(f"- real body: `{d['real_body'][:200]}`")
            lines.append("")
    REPORT.write_text("\n".join(lines))
    print(f"report → {REPORT}  ({n_pass}/{len(rows)} pass)")
    return 0


if __name__ == "__main__":
    args = sys.argv[1:]
    services = None
    if "--services" in args:
        i = args.index("--services")
        services = args[i + 1].split(",")
        args = args[:i] + args[i + 2:]
    cmd = args[0] if args else "report"
    if cmd == "capture":
        sys.exit(capture(services))
    if cmd == "report":
        sys.exit(report())
    print(__doc__)
    sys.exit(2)
