"""Live-AWS fidelity harness — capture real wire responses and diff them
against microburst's renderer (errors) or a second capture set (both).

Captures raw wire responses (status, headers, body) by wrapping botocore's
urllib3 send — real TLS, real AWS bytes, no proxy plumbing. Then diffs each
error capture against ``render_error()``: status, Content-Type, body
top-level fields, request-id placement, and — what matters most — the
``Error.Code`` the SDK parser recovers from each.

``PROBES`` produce errors on purpose; ``SUCCESS_PROBES`` are read-only
list/describe calls that return 200 on a fresh account, so
``conform``/``diff`` can compare success envelopes (emulator vs AWS) too.
Microburst never renders success bodies — it forwards them — so ``report``
skips success captures.

Credentials come from ``AWS_PROFILE``/env; every probe targets a
nonexistent resource or is a plain list/read (nothing is created or
mutated).

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

# read-only operations that return 200 on a fresh account (and on
# emulators) — the happy-path counterpart to PROBES. Captures are tagged
# ``kind: "success"`` and compared envelope-to-envelope by conform/diff;
# report skips them (microburst forwards success bodies, it never renders
# them). Kwargs stay minimal so every call works with empty datasets.
SUCCESS_PROBES: list[tuple[str, str, dict[str, Any], str]] = [
    # (service, boto3 method, kwargs, protocol family)
    ("dynamodb", "list_tables", {}, "json"),
    ("kms", "list_keys", {}, "json"),
    ("secretsmanager", "list_secrets", {}, "json"),
    ("logs", "describe_log_groups", {}, "json"),
    ("kinesis", "list_streams", {}, "json"),
    ("stepfunctions", "list_state_machines", {}, "json"),
    ("cognito-idp", "list_user_pools", {"MaxResults": 60}, "json"),
    ("wafv2", "list_web_acls", {"Scope": "REGIONAL"}, "json"),
    ("sqs", "list_queues", {}, "query"),
    ("sns", "list_topics", {}, "query"),
    ("iam", "list_users", {}, "query"),
    ("sts", "get_caller_identity", {}, "query"),
    ("elbv2", "describe_load_balancers", {}, "query"),
    ("rds", "describe_db_instances", {}, "query"),
    ("cloudformation", "describe_stacks", {}, "query"),
    ("ec2", "describe_instances", {}, "ec2"),
    ("ec2", "describe_regions", {}, "ec2"),
    ("s3", "list_buckets", {}, "rest-xml"),
    ("route53", "list_hosted_zones", {}, "rest-xml"),
    ("lambda", "list_functions", {}, "rest-json"),
    ("apigateway", "get_rest_apis", {}, "rest-json"),
    ("glacier", "list_vaults", {"accountId": "-"}, "rest-json"),
    ("sesv2", "list_email_identities", {}, "rest-json"),
    # query-compat JSON on the wire even though the model says rpc-v2-cbor
    ("cloudwatch", "list_dashboards", {}, "query-compat"),
    ("events", "list_event_buses", {}, "query-compat"),
]

# base filenames already claimed by error probes — a success probe that
# shares one (e.g. ec2 describe_instances with vs without InstanceIds)
# gets a ``__ok`` suffix so the two captures coexist and conform/diff
# filename matching stays unambiguous.
_ERROR_PROBE_NAMES = {f"{s}_{m}" for s, m, _, _ in PROBES}


def _capture_name(service: str, method: str, kind: str) -> str:
    base = f"{service}_{method}"
    if kind == "success" and base in _ERROR_PROBE_NAMES:
        base += "__ok"
    return f"{base}.json"


def _is_success(cap: dict) -> bool:
    """True when a capture is a happy-path probe. ``kind`` is
    authoritative on new captures; older captures predate the field so
    wire evidence decides — a sub-400 status or no parsed SDK error code
    means the call succeeded."""
    if cap.get("kind") is not None:
        return cap["kind"] == "success"
    return cap.get("status", 200) < 400 or cap.get("sdk_error_code") is None


def _rebind_region(kwargs: dict, region: str) -> dict:
    """Probe kwargs embed literal regions (ARN fields) — rebind so a
    multi-region capture exercises that region's own resources."""
    raw = json.dumps(kwargs).replace("us-east-1", region)
    return json.loads(raw)


def _store_capture(
    captures: dict,
    capture_dir: Path,
    *,
    service: str,
    method: str,
    family: str,
    model_service: str,
    sdk_error_code: str | None,
    kind: str,
    account_id: str | None,
    region: str,
) -> dict | None:
    """Pop the wrapped-send capture, redact the account id, tag it with
    probe metadata + provenance, and write ``captures/<name>.json``.
    Returns the capture dict, or ``None`` when no response was recorded."""
    cap = captures.pop("last", None)
    if cap is None:
        return None
    if account_id and cap.get("body"):
        cap["body"] = cap["body"].replace(account_id, "000000000000")
    cap.update(
        service=service, operation=method, family=family,
        model_service=model_service, sdk_error_code=sdk_error_code,
        kind=kind, ts=time.time(),
        provenance=_provenance(cap["headers"], region),
    )
    path = capture_dir / _capture_name(service, method, kind)
    path.write_text(json.dumps(cap, indent=2, sort_keys=True))
    return cap


def capture(
    services: list[str] | None,
    out_dir: Path,
    endpoint_url: str | None = None,
    region: str = "us-east-1",
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
            service, region_name=region, **client_kwargs
        )
        try:
            getattr(client, method)(**_rebind_region(kwargs, region))
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
        cap = _store_capture(
            captures, capture_dir,
            service=service, method=method, family=family,
            model_service=client.meta.service_model.service_name,
            sdk_error_code=code, kind="error",
            account_id=account_id, region=region,
        )
        if cap is None:
            print(f"  {service}.{method}: error but no capture")
            continue
        print(f"  {service}.{method} → {code} ({cap['status']}) captured")
        n_ok += 1

    # happy-path probes — successful wire responses whose envelopes
    # conform/diff compare (report skips them: microburst forwards
    # success bodies rather than rendering them)
    for service, method, kwargs, family in SUCCESS_PROBES:
        if wanted and service not in wanted:
            continue
        client = boto3.client(
            service, region_name=region, **client_kwargs
        )
        code = None
        note = ""
        try:
            getattr(client, method)(**_rebind_region(kwargs, region))
        except ClientError as e:
            # an endpoint erroring on a read-only op is conformance
            # evidence too — keep it tagged kind=success so conform flags
            # the status divergence against the AWS golden
            code = e.response.get("Error", {}).get("Code", "?")
            note = f" (expected success, got {code})"
        except EndpointConnectionError as e:
            print(f"  {service}.{method}: unreachable ({e})")
            continue
        except Exception as e:  # noqa: BLE001 — report and continue
            print(f"  {service}.{method}: {type(e).__name__}: {e}")
            continue
        cap = _store_capture(
            captures, capture_dir,
            service=service, method=method, family=family,
            model_service=client.meta.service_model.service_name,
            sdk_error_code=code, kind="success",
            account_id=account_id, region=region,
        )
        if cap is None:
            print(f"  {service}.{method}: no capture")
            continue
        print(f"  {service}.{method} → {cap['status']} captured{note}")
        n_ok += 1
    print(f"{n_ok} captures → {capture_dir}")
    return 0 if n_ok else 1


# Headers AWS stamps on real responses — the bits that let a reviewer
# sanity-check a capture is genuine wire evidence.
_REQUEST_ID_HEADERS = (
    "x-amzn-requestid", "x-amzn-request-id",
    "x-amz-request-id", "x-amz-id-2", "x-amz-requestid",
)


def _provenance(headers: dict, region: str = "us-east-1") -> dict:
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
        "region": region,
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


def _envelope_signature(body: str | None) -> dict:
    """Structural fingerprint of an error body — the wire *shape*,
    independent of the values it carries.

    XML: namespace-qualified element paths (text stripped — request ids
    and messages always vary) plus whether an ``<?xml`` declaration is
    present. This is what catches ``Response/Errors`` vs ``ErrorResponse``,
    a missing ``xmlns``, or ``RequestID`` vs ``RequestId``. JSON: sorted
    top-level keys plus the ``__type`` namespace prefix (``None`` when
    absent, so a bare code, a namespaced code, and a missing ``__type``
    all differ). Anything else degrades to a single marker.
    """
    import xml.etree.ElementTree as ET

    if not body:
        return {"kind": "empty"}
    if body.lstrip().startswith("<"):
        try:
            root = ET.fromstring(body)
        except ET.ParseError:
            return {"kind": "unparseable"}
        paths = {root.tag}

        def walk(el, prefix):
            for child in el:
                path = f"{prefix}/{child.tag}"
                paths.add(path)
                walk(child, path)

        walk(root, root.tag)
        return {
            "kind": "xml",
            "xml_decl": body.startswith("<?xml"),
            "tags": sorted(paths),
        }
    try:
        obj = json.loads(body)
    except ValueError:
        return {"kind": "other"}
    if not isinstance(obj, dict):
        return {"kind": "non-dict"}
    type_val = obj.get("__type")
    return {
        "kind": "json",
        "keys": sorted(obj),
        "type_ns": type_val.split("#", 1)[0] if type_val else None,
    }


def check_capture(cap: dict, session) -> dict[str, Any]:
    """Diff one capture against what microburst would render for it —
    the pure comparison behind ``report()``.

    Renders the capture's (service, error code) through ``render_error``
    and compares the quartet an SDK actually reads: HTTP status, the
    ``Error.Code`` botocore's parser recovers from each body, the
    Content-Type media type, and the envelope shape (XML element paths /
    JSON ``__type`` namespacing). No I/O, no printing — returns the four
    verdict flags plus the field detail needed to explain a mismatch, so
    tests can assert on the same numbers the report table shows.
    """
    from botocore.parsers import create_parser

    from microburst.protocols import render_error

    svc, op = cap["service"], cap["operation"]
    if _is_success(cap):
        raise ValueError(
            f"{svc}.{op}: success capture — microburst forwards "
            "successful responses verbatim rather than rendering them; "
            "compare success envelopes with conform()/diff()"
        )
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

    ours_body_text = ours_body.decode()
    return {
        "service": svc,
        "operation": op,
        "code": code,
        "real_status": real_status,
        "ours_status": ours_status,
        "real_ct": real_ct,
        "ours_ct": ours_ct,
        "status_ok": ours_status == real_status,
        "code_ok": ours_code == real_code,
        "ct_ok": ours_ct.split(";")[0] == real_ct.split(";")[0],
        "shape_ok": _envelope_signature(real_body)
        == _envelope_signature(ours_body_text),
        "detail": {
            "real_keys": _top_keys(real_body),
            "ours_keys": _top_keys(ours_body_text),
            "real_sig": _envelope_signature(real_body),
            "ours_sig": _envelope_signature(ours_body_text),
            "real_rid_headers": sorted(
                h for h in cap["headers"]
                if "request" in h.lower() or "id" in h.lower()),
            "ours_rid_headers": sorted(
                h for h in ours_headers
                if "request" in h.lower() or "id" in h.lower()),
            "real_body": real_body[:400],
            "ours_body": ours_body_text[:400],
        },
    }


def report(out_dir: Path) -> int:
    """Diff every capture against what microburst would have rendered."""
    from botocore.session import Session

    capture_dir = out_dir / "captures"
    report_path = out_dir / "REPORT.md"
    session = Session()
    rows = []
    n_success = 0
    for path in sorted(capture_dir.glob("*.json")):
        cap = json.loads(path.read_text())
        if _is_success(cap):
            # success captures have no rendered counterpart — microburst
            # forwards 2xx bodies verbatim; conform/diff compare them
            n_success += 1
            continue
        r = check_capture(cap, session)
        verdict = (
            "✅"
            if (r["status_ok"] and r["code_ok"]
                and r["ct_ok"] and r["shape_ok"])
            else "❌"
        )
        rows.append((
            r["service"], r["operation"], r["code"],
            r["real_status"], r["ours_status"],
            r["real_ct"] or "—", r["ours_ct"], verdict,
            r["status_ok"], r["code_ok"], r["ct_ok"], r["shape_ok"],
            r["detail"],
        ))

    lines = [
        "# Live-AWS fidelity report", "",
        (
            f"Captured {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())} "
            "against real AWS (us-east-1). Each probe targets a nonexistent "
            "resource; captures are raw wire bytes via botocore's transport."
        ),
    ]
    if n_success:
        lines.append("")
        lines.append(
            f"*{n_success} success-path capture(s) skipped — microburst "
            "forwards successful upstream responses rather than rendering "
            "them; `fidelity conform`/`fidelity diff` compare their "
            "envelopes (status + Content-Type + body shape).*"
        )
    lines += [
        "",
        ("| service | operation | error code | AWS status | ours | "
         "AWS CT | ours | verdict |"),
        "|---|---|---|---|---|---|---|---|",
    ]
    n_pass = 0
    for svc, op, code, rs, os_, rct, oct_, verdict, s_ok, c_ok, ct_ok, \
            sh_ok, d in rows:
        if s_ok and c_ok and ct_ok and sh_ok:
            n_pass += 1
        lines.append(
            f"| {svc} | {op} | `{code}` | {rs} | {os_} | `{rct}` | `{oct_}` | {verdict} |"
        )
    lines += [
        "",
        (f"**{n_pass}/{len(rows)} probes: status + parsed Error.Code "
         "+ Content-Type + envelope shape match.**"),
        "",
    ]

    # field-level detail for anything that isn't a clean match
    interesting = [r for r in rows if not (r[8] and r[9] and r[10] and r[11])]
    if interesting:
        lines += ["## Diffs", ""]
        for svc, op, code, rs, os_, rct, oct_, v, s_ok, c_ok, ct_ok, \
                sh_ok, d in interesting:
            lines.append(f"### {svc}.{op} (`{code}`)")
            if not s_ok:
                lines.append(f"- status: AWS {rs} vs ours {os_}")
            if not c_ok:
                lines.append("- parsed Error.Code differs")
            if not ct_ok:
                lines.append(f"- content-type: AWS `{rct}` vs ours `{oct_}`")
            if not sh_ok:
                lines.append(
                    f"- envelope shape: AWS `{d['real_sig']}` vs ours "
                    f"`{d['ours_sig']}`")
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
    see: HTTP status, Content-Type, the parsed ``Error.Code``, and the
    envelope shape — the quartet that drives retry classification and
    parsing. Writes ``CONFORM.md`` next to the emulator captures.
    """
    return _compare(
        aws_dir, emu_dir, a_label="AWS", b_label="emu",
        title="# Conformance report — emulator vs real AWS",
        verb="conform", out_name="CONFORM.md",
    )


def diff(dir_a: Path, dir_b: Path) -> int:
    """Compare two capture sets directly — emulator A vs B, a candidate
    patch vs the last capture, or a fresh AWS run vs the goldens —
    without routing through the committed set. Writes ``DIFF.md`` into
    ``dir_b``. Same fields as ``conform``."""
    return _compare(
        dir_a, dir_b, a_label="a", b_label="b",
        title="# Capture diff",
        verb="match", out_name="DIFF.md",
    )


def _compare(
    dir_a: Path,
    dir_b: Path,
    a_label: str,
    b_label: str,
    title: str,
    verb: str,
    out_name: str,
) -> int:
    from botocore.session import Session

    session = Session()
    a_caps = {
        p.name: json.loads(p.read_text())
        for p in sorted((dir_a / "captures").glob("*.json"))
    }
    b_caps = {
        p.name: json.loads(p.read_text())
        for p in sorted((dir_b / "captures").glob("*.json"))
    }
    rows = []
    for name, cap_a in a_caps.items():
        cap_b = b_caps.get(name)
        if cap_b is None:
            rows.append((name, "—", "missing", "—", "—", "—", "⬜",
                         False, None, None, _is_success(cap_a)))
            continue
        # success probes carry no Error.Code — the parsed code is None on
        # both sides; an endpoint that errors a read-only op still shows
        # up via status/code mismatch.
        success = _is_success(cap_a) or _is_success(cap_b)
        a_parsed, _ = _parse_capture(cap_a, session)
        b_parsed, _ = _parse_capture(cap_b, session)
        a_code = (a_parsed.get("Error") or {}).get("Code")
        b_code = (b_parsed.get("Error") or {}).get("Code")
        a_ct = cap_a["headers"].get("Content-Type", "").split(";")[0]
        b_ct = cap_b["headers"].get("Content-Type", "").split(";")[0]
        shape_ok = _envelope_signature(
            cap_a.get("body")
        ) == _envelope_signature(cap_b.get("body"))
        ok = (
            cap_a["status"] == cap_b["status"]
            and a_code == b_code
            and a_ct == b_ct
            and shape_ok
        )
        rows.append((
            name, cap_a["status"], cap_b["status"], a_code, b_code,
            f"{a_ct} → {b_ct}" if a_ct != b_ct else b_ct,
            "✅" if ok else "❌", shape_ok,
            cap_a.get("body"), cap_b.get("body"), success,
        ))

    n_pass = sum(1 for r in rows if r[6] == "✅")
    lines = [
        title, "",
        (f"Diffs each probe's {b_label} response against the {a_label} "
         "capture on the fields an SDK actually reads: HTTP status, "
         "parsed `Error.Code`, Content-Type — plus the envelope shape "
         "(XML element paths, `__type` namespacing)."),
        "",
        (f"| probe | {a_label} status | {b_label} status | {a_label} code | "
         f"{b_label} code | CT | shape | |"),
        "|---|---|---|---|---|---|---|---|",
    ]
    for name, rs, es, rc, ec, ct, v, sh_ok, _ab, _eb, success in rows:
        shape = "✅" if sh_ok else ("—" if v == "⬜" else "❌")
        probe = f"{name} _(success)_" if success else name
        # success bodies carry no error code — a dash, not `None`
        rc_s, ec_s = ("—", "—") if success else (f"`{rc}`", f"`{ec}`")
        lines.append(
            f"| {probe} | {rs} | {es} | {rc_s} | {ec_s} | {ct} | "
            f"{shape} | {v} |"
        )
    lines += [
        "",
        (f"**{n_pass}/{len(rows)} probes {verb} "
         "(status + parsed code + Content-Type + envelope shape).**"),
    ]
    n_success = sum(1 for r in rows if r[10])
    if n_success:
        lines.append(
            f"*{n_success} success-path probe(s) compared on status + "
            "Content-Type + envelope shape — success bodies carry no "
            "error code.*"
        )
    lines.append("")
    mismatched = [r for r in rows if r[6] == "❌" and not r[7]]
    if mismatched:
        lines += ["## Envelope shape diffs", ""]
        for name, _rs, _es, _rc, _ec, _ct, _v, _s, ab, eb, _ok_ in mismatched:
            lines.append(f"### {name}")
            lines.append(f"- {a_label}:   `{_envelope_signature(ab)}`")
            lines.append(f"- {b_label}:   `{_envelope_signature(eb)}`")
            lines.append("")
    out = dir_b / out_name
    out.write_text("\n".join(lines))
    print(f"conformance → {out}  ({n_pass}/{len(rows)} match)")
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
            "conform", "diff",
        ],
    )
    ap.add_argument(
        "paths", nargs="*",
        help="capture dirs for `diff`: A B (writes DIFF.md into B)",
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
        "--region", default="us-east-1",
        help="capture region (default: us-east-1)",
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
        return capture(services, out_dir, args.endpoint_url, args.region)
    if args.command == "report":
        return report(out_dir)
    if args.command == "snapshot":
        return model_snapshot(out_dir)
    if args.command == "conform":
        emu = Path(args.emu_dir) if args.emu_dir else out_dir
        aws = Path(args.aws_dir) if args.aws_dir else DEFAULT_DIR
        return conform(aws, emu)
    if args.command == "diff":
        if len(args.paths) != 2:
            print("fidelity diff needs two capture dirs: A B")
            return 2
        return diff(Path(args.paths[0]), Path(args.paths[1]))
    return backfill_provenance(out_dir)
