"""Live-AWS fidelity harness — capture real error responses and diff them
against microburst's renderer.

Captures raw wire responses (status, headers, body) by wrapping botocore's
urllib3 send — real TLS, real AWS bytes, no proxy plumbing. Then diffs each
capture against ``render_error()``: status, Content-Type, body top-level
fields, request-id placement, and — what matters most — the ``Error.Code``
the SDK parser recovers from each.

Credentials come from ``AWS_PROFILE``/env; every probe targets a
nonexistent resource (read-only — nothing is created or mutated).

    microburst fidelity capture [--services dynamodb,s3] [--out DIR]
    microburst fidelity report [--dir DIR]

Artifacts land in ``--dir`` (default ``./fidelity``) — captures (raw) +
REPORT.md (diff). ``capture`` needs boto3 installed.
"""

from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from typing import Any

NONEXIST = "microburst-fidelity-doesnotexist-7f3a9"
DEFAULT_DIR = Path("fidelity")

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


def capture(
    services: list[str] | None,
    out_dir: Path,
    endpoint_url: str | None = None,
) -> int:
    """Run probes against real AWS — or any emulator endpoint when
    ``endpoint_url`` is given — dumping raw wire responses."""
    try:
        import boto3
        import botocore.httpsession
        from botocore.exceptions import ClientError, EndpointConnectionError
    except ImportError:
        print("fidelity capture needs boto3 — pip install boto3")
        return 1

    capture_dir = out_dir / "captures"
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

    # Emulators don't validate credentials — give boto3 dummies so it
    # still signs requests the way it would against real AWS.
    client_kwargs: dict = {}
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url

    capture_dir.mkdir(parents=True, exist_ok=True)
    wanted = {s for s in services} if services else None
    n_ok = 0
    for service, method, kwargs, family in PROBES:
        if wanted and service not in wanted:
            continue
        client = boto3.client(
            service, region_name="us-east-1", **client_kwargs
        )
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
            provenance=_provenance(cap["headers"]),
        )
        fname = capture_dir / f"{service}_{method}.json"
        fname.write_text(json.dumps(cap, indent=2, sort_keys=True))
        print(f"  {service}.{method} → {code} ({cap['status']}) captured")
        n_ok += 1
    print(f"{n_ok} captures → {capture_dir}")
    return 0 if n_ok else 1


# Headers AWS stamps on real responses — the bits that let a reviewer
# sanity-check a capture is genuine wire evidence.
_REQUEST_ID_HEADERS = (
    "x-amzn-requestid", "x-amzn-request-id",
    "x-amz-request-id", "x-amz-id-2", "x-amz-requestid",
)


def _provenance(headers: dict) -> dict:
    """Evidence block: which response ids AWS stamped + capture context.

    No account identifiers — enough for a reviewer to see this came off
    real AWS wire, not a stub."""
    import botocore

    lower = {k.lower(): v for k, v in headers.items()}
    return {
        "request_ids": {
            k: lower[k] for k in _REQUEST_ID_HEADERS if k in lower
        },
        "server": lower.get("server"),
        "region": "us-east-1",
        "botocore": botocore.__version__,
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def backfill_provenance(out_dir: Path) -> int:
    """Add ``provenance`` blocks to captures taken before the field
    existed — request ids are already in the stored headers."""
    n = 0
    for path in sorted((out_dir / "captures").glob("*.json")):
        cap = json.loads(path.read_text())
        prov = _provenance(cap.get("headers", {}))
        prov["botocore"] = None          # unknown — predates provenance
        prov["backfilled"] = True
        cap["provenance"] = prov
        path.write_text(json.dumps(cap, indent=2, sort_keys=True) + "\n")
        n += 1
    print(f"provenance backfilled on {n} captures")
    return 0


_META_KEYS = (
    "protocol", "protocols", "jsonVersion", "endpointPrefix",
    "targetPrefix", "serviceId", "signingName", "signatureVersion",
    "awsQueryCompatible", "serviceAbbreviation",
)


def model_snapshot(out_dir: Path) -> int:
    """Digest the fidelity-relevant surface of every botocore model.

    Per-service sha256 over: metadata (protocol/prefixes/query-compat),
    each operation's http route + error refs, and every error shape's
    ``error`` trait (code/httpStatusCode) + member names. Documentation
    and unrelated shapes are excluded so the digest only moves when the
    *error wire contract* moves.

    Pair with the model-drift workflow: regenerate against the latest
    botocore release and ``git diff`` — drift means it's time to
    re-capture. No AWS credentials needed.
    """
    import hashlib

    from botocore.session import Session

    session = Session()
    loader = session.get_component("data_loader")
    digests: dict[str, str] = {}
    for svc in sorted(session.get_available_services()):
        try:
            model = loader.load_service_model(svc, "service-2")
        except Exception as e:  # noqa: BLE001 — partial/absent model
            print(f"  {svc}: model load failed ({e})")
            continue
        subset = {
            "meta": {
                k: model.get("metadata", {}).get(k)
                for k in _META_KEYS
                if k in model.get("metadata", {})
            },
            "ops": {
                name: {
                    "http": op.get("http"),
                    "errors": [ref.get("shape") for ref in op.get("errors", [])],
                }
                for name, op in sorted(model.get("operations", {}).items())
            },
            "error_shapes": {
                name: {
                    "error": sh.get("error"),
                    "members": sorted(sh.get("members", {})),
                }
                for name, sh in sorted(model.get("shapes", {}).items())
                if sh.get("exception") or sh.get("error")
            },
        }
        digests[svc] = hashlib.sha256(
            json.dumps(subset, sort_keys=True, default=str).encode()
        ).hexdigest()

    import botocore

    snap = {"botocore": botocore.__version__, "services": digests}
    path = out_dir / "models_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snap, indent=2, sort_keys=True) + "\n")
    print(f"{len(digests)} service digests → {path}")
    return 0


def _top_keys(body: str | None) -> list[str]:
    if not body:
        return []
    try:
        obj = json.loads(body)
        return sorted(obj) if isinstance(obj, dict) else ["<non-dict>"]
    except ValueError:
        return ["<non-json>"]


def report(out_dir: Path) -> int:
    """Diff every capture against what microburst would have rendered."""
    from botocore.parsers import create_parser
    from botocore.session import Session

    from microburst.protocols import render_error

    capture_dir = out_dir / "captures"
    report_path = out_dir / "REPORT.md"
    session = Session()
    rows = []
    for path in sorted(capture_dir.glob("*.json")):
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
            real_ct.startswith("application/x-amz-json")
            and protocol == "smithy-rpc-v2-cbor"
        ):
            protocol = "json"

        ours_status, ours_headers, ours_body = render_error(
            cap.get("model_service") or svc, code, "(fidelity)",
            protocol=protocol if isinstance(protocol, str) else None,
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
                 h for h in cap["headers"]
                 if "request" in h.lower() or "id" in h.lower()),
             "ours_rid_headers": sorted(
                 h for h in ours_headers
                 if "request" in h.lower() or "id" in h.lower()),
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
        ("| service | operation | error code | AWS status | ours | "
         "AWS CT | ours | verdict |"),
        "|---|---|---|---|---|---|---|---|",
    ]
    n_pass = 0
    for svc, op, code, rs, os_, rct, oct_, verdict, s_ok, c_ok, ct_ok, d in rows:
        if s_ok and c_ok and ct_ok:
            n_pass += 1
        lines.append(
            f"| {svc} | {op} | `{code}` | {rs} | {os_} | `{rct}` | `{oct_}` | {verdict} |"
        )
    lines += [
        "",
        (f"**{n_pass}/{len(rows)} probes: status + parsed Error.Code "
         "+ Content-Type match.**"),
        "",
    ]

    # field-level detail for anything that isn't a clean match
    interesting = [r for r in rows if not (r[8] and r[9] and r[10])]
    if interesting:
        lines += ["## Diffs", ""]
        for svc, op, code, rs, os_, rct, oct_, v, s_ok, c_ok, ct_ok, d \
                in interesting:
            lines.append(f"### {svc}.{op} (`{code}`)")
            if not s_ok:
                lines.append(f"- status: AWS {rs} vs ours {os_}")
            if not c_ok:
                lines.append("- parsed Error.Code differs")
            if not ct_ok:
                lines.append(f"- content-type: AWS `{rct}` vs ours `{oct_}`")
            lines.append(
                f"- real body keys: `{d['real_keys']}` vs ours "
                f"`{d['ours_keys']}`")
            lines.append(
                f"- real rid headers: `{d['real_rid_headers']}` vs ours "
                f"`{d['ours_rid_headers']}`")
            lines.append(f"- real body: `{d['real_body'][:200]}`")
            lines.append("")
    report_path.write_text("\n".join(lines))
    print(f"report → {report_path}  ({n_pass}/{len(rows)} pass)")
    return 0 if n_pass == len(rows) and rows else 1


def _parse_capture(cap: dict, session) -> tuple[Any, Any]:
    """(parsed_response, protocol) for a raw capture — real AWS bytes or
    emulator bytes, treated identically. Wire evidence decides protocol:
    query-compat headers win over the model's declared protocol."""
    from botocore.parsers import create_parser

    svc = cap.get("model_service") or cap["service"]
    protocol = session.get_service_model(svc).protocol
    ct = cap["headers"].get("Content-Type", "")
    if "x-amzn-query-error" in cap["headers"] or (
        ct.startswith("application/x-amz-json")
        and protocol == "smithy-rpc-v2-cbor"
    ):
        protocol = "json"
    parser = create_parser(protocol)
    body = cap.get("body") or ""
    parsed = parser.parse(
        {
            "status_code": cap["status"],
            "headers": {
                k.lower(): v for k, v in cap["headers"].items()
            },
            "body": body.encode() if body else b"",
        },
        None,
    )
    return parsed, protocol


def conform(aws_dir: Path, emu_dir: Path) -> int:
    """Diff an emulator's captures against the real-AWS goldens.

    For every probe present in both trees we compare what an SDK would
    see: HTTP status, Content-Type, and the parsed ``Error.Code`` — the
    trio that drives retry classification. Body top-level keys and
    request-id header presence are reported as secondary signals.
    Writes ``CONFORM.md`` next to the emulator captures.
    """
    from botocore.session import Session

    session = Session()
    aws_caps = {
        p.name: json.loads(p.read_text())
        for p in sorted((aws_dir / "captures").glob("*.json"))
    }
    emu_caps = {
        p.name: json.loads(p.read_text())
        for p in sorted((emu_dir / "captures").glob("*.json"))
    }
    rows = []
    for name, aws in aws_caps.items():
        emu = emu_caps.get(name)
        if emu is None:
            rows.append((name, "—", "missing", "—", "—", "—", "⬜"))
            continue
        aws_parsed, _ = _parse_capture(aws, session)
        emu_parsed, _ = _parse_capture(emu, session)
        aws_code = (aws_parsed.get("Error") or {}).get("Code")
        emu_code = (emu_parsed.get("Error") or {}).get("Code")
        aws_ct = aws["headers"].get("Content-Type", "").split(";")[0]
        emu_ct = emu["headers"].get("Content-Type", "").split(";")[0]
        ok = (
            aws["status"] == emu["status"]
            and aws_code == emu_code
            and aws_ct == emu_ct
        )
        rows.append((
            name, aws["status"], emu["status"], aws_code, emu_code,
            f"{aws_ct} → {emu_ct}" if aws_ct != emu_ct else emu_ct,
            "✅" if ok else "❌",
        ))

    n_pass = sum(1 for r in rows if r[-1] == "✅")
    lines = [
        "# Conformance report — emulator vs real AWS", "",
        ("Diffs each probe's emulator response against the committed "
         "real-AWS capture on the fields an SDK actually reads: HTTP "
         "status, parsed `Error.Code`, Content-Type."),
        "",
        "| probe | AWS status | emu status | AWS code | emu code | CT | |",
        "|---|---|---|---|---|---|---|",
    ]
    for name, rs, es, rc, ec, ct, v in rows:
        lines.append(
            f"| {name} | {rs} | {es} | `{rc}` | `{ec}` | {ct} | {v} |"
        )
    lines += [
        "",
        (f"**{n_pass}/{len(rows)} probes conform "
         "(status + parsed code + Content-Type).**"),
        "",
    ]
    out = emu_dir / "CONFORM.md"
    out.write_text("\n".join(lines))
    print(f"conformance → {out}  ({n_pass}/{len(rows)} conform)")
    return 0 if n_pass == len(rows) and rows else 1


def fidelity_main(argv: list[str]) -> int:
    """``microburst fidelity`` subcommand entry point."""
    import argparse

    ap = argparse.ArgumentParser(
        prog="microburst fidelity",
        description=(__doc__ or "").splitlines()[0],
    )
    ap.add_argument(
        "command",
        choices=[
            "capture", "report", "snapshot", "backfill-provenance",
            "conform",
        ],
    )
    ap.add_argument(
        "--services", default=None,
        help="comma-separated service subset for capture",
    )
    ap.add_argument(
        "--dir", "--out", dest="out_dir", default=str(DEFAULT_DIR),
        help="capture/report directory (default: ./fidelity)",
    )
    ap.add_argument(
        "--endpoint-url", default=None,
        help="capture against this endpoint instead of real AWS "
             "(emulator conformance)",
    )
    ap.add_argument(
        "--aws", dest="aws_dir", default=None,
        help="real-AWS captures dir for conform (default: <dir>)",
    )
    ap.add_argument(
        "--emu", dest="emu_dir", default=None,
        help="emulator captures dir for conform",
    )
    args = ap.parse_args(argv)
    out_dir = Path(args.out_dir)
    services = (
        [s.strip() for s in args.services.split(",") if s.strip()]
        if args.services else None
    )
    if args.command == "capture":
        return capture(services, out_dir, args.endpoint_url)
    if args.command == "report":
        return report(out_dir)
    if args.command == "snapshot":
        return model_snapshot(out_dir)
    if args.command == "conform":
        emu = Path(args.emu_dir) if args.emu_dir else out_dir
        aws = Path(args.aws_dir) if args.aws_dir else DEFAULT_DIR
        return conform(aws, emu)
    return backfill_provenance(out_dir)
