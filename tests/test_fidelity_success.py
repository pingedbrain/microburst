"""Happy-path (success) captures: ``SUCCESS_PROBES`` record real 2xx wire
responses so ``conform``/``diff`` can compare success envelopes —
emulator vs AWS — not just failure shapes. ``report`` and the golden
gate only make sense for errors (microburst forwards success bodies, it
never renders them) so they skip success captures explicitly.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from botocore.session import Session

from microburst.fidelity import (
    PROBES,
    SUCCESS_PROBES,
    _capture_name,
    _is_success,
    _parse_capture,
    _store_capture,
    check_capture,
    conform,
    diff,
    report,
)

_session = Session()
_GOLDENS = Path(__file__).resolve().parent.parent / "fidelity" / "captures"

_OK_CAP = {
    "service": "dynamodb",
    "operation": "list_tables",
    "model_service": "dynamodb",
    "sdk_error_code": None,
    "kind": "success",
    "status": 200,
    "headers": {
        "Content-Type": "application/x-amz-json-1.0",
        "x-amzn-RequestId": "RID",
    },
    "body": '{"TableNames":["orders","users"]}',
}


def _cap(root, name, **over):
    d = root / "captures"
    d.mkdir(parents=True, exist_ok=True)
    cap = {**_OK_CAP, **over}
    (d / name).write_text(json.dumps(cap))


# — the probe list —


def test_success_probes_cover_all_wire_families():
    families = {family for *_rest, family in SUCCESS_PROBES}
    assert {
        "json", "query", "ec2", "rest-xml", "rest-json", "query-compat",
    } <= families


def test_success_probes_are_read_only():
    """Success probes run against real accounts — every op must be a
    list/describe/get with empty or near-empty kwargs so it succeeds on
    a fresh account (and on emulators) with zero side effects."""
    assert 15 <= len(SUCCESS_PROBES) <= 30
    for service, method, kwargs, _family in SUCCESS_PROBES:
        verb = method.split("_", 1)[0]
        assert verb in {"list", "describe", "get", "head"}, (
            f"{service}.{method} is not obviously read-only"
        )
        assert isinstance(kwargs, dict)


# — capture naming + classification —


def test_capture_names_are_unique_across_probe_lists():
    """conform/diff match captures by filename — every probe must land
    in its own file."""
    err = {_capture_name(s, m, "error") for s, m, _, _ in PROBES}
    ok = {_capture_name(s, m, "success") for s, m, _, _ in SUCCESS_PROBES}
    assert len(err) == len(PROBES)
    assert len(ok) == len(SUCCESS_PROBES)
    assert not err & ok


def test_capture_name_suffixes_error_probe_collisions():
    # ec2 describe_instances is an error probe (bogus InstanceIds) AND a
    # success probe (no args) — the success file must not clobber the
    # error golden.
    assert _capture_name("ec2", "describe_instances", "error") == (
        "ec2_describe_instances.json"
    )
    assert _capture_name("ec2", "describe_instances", "success") == (
        "ec2_describe_instances__ok.json"
    )
    # no collision → no suffix
    assert _capture_name("dynamodb", "list_tables", "success") == (
        "dynamodb_list_tables.json"
    )


def test_is_success_kind_field_is_authoritative():
    assert _is_success({"kind": "success", "status": 200})
    assert not _is_success({"kind": "error", "status": 200})
    # legacy captures predate `kind` — wire evidence decides
    assert _is_success({"status": 200, "sdk_error_code": None})
    assert not _is_success(
        {"status": 400, "sdk_error_code": "ThrottlingException"}
    )


def test_store_capture_tags_kind_and_redacts_account(tmp_path):
    captures = {
        "last": {
            "status": 200,
            "headers": {"Content-Type": "text/xml"},
            "method": "POST",
            "url": "https://sts.amazonaws.com/",
            "body_b64": None,
            "body": "<Account>123456789012</Account>",
        }
    }
    cap = _store_capture(
        captures, tmp_path,
        service="sts", method="get_caller_identity", family="query",
        model_service="sts", sdk_error_code=None, kind="success",
        account_id="123456789012", region="us-east-1",
    )
    assert cap is not None
    assert cap["kind"] == "success"
    assert cap["sdk_error_code"] is None
    assert "123456789012" not in cap["body"]
    assert "000000000000" in cap["body"]
    written = json.loads(
        (tmp_path / "sts_get_caller_identity.json").read_text()
    )
    assert written["kind"] == "success"
    assert "provenance" in written


# — parse / check —


def test_parse_capture_on_success_body_yields_no_error():
    parsed, _ = _parse_capture(_OK_CAP, _session)
    assert (parsed.get("Error") or {}).get("Code") is None


def test_check_capture_rejects_success_captures():
    """Nothing to render: microburst forwards successful upstream
    responses verbatim — success envelopes only compare against other
    captures (conform/diff), never against render_error."""
    with pytest.raises(ValueError, match="success"):
        check_capture(_OK_CAP, _session)


# — conform / diff —


def test_conform_compares_success_envelopes(tmp_path):
    """AWS lists two tables, the emulator zero — values differ but the
    envelope shape (top-level keys) is identical → conform."""
    aws, emu = tmp_path / "aws", tmp_path / "emu"
    _cap(aws, "dynamodb_list_tables.json")
    _cap(emu, "dynamodb_list_tables.json", body='{"TableNames":[]}')
    assert conform(aws, emu) == 0
    md = (emu / "CONFORM.md").read_text()
    assert "1/1 probes conform" in md
    assert "success" in md


def test_conform_flags_success_shape_drift(tmp_path):
    aws, emu = tmp_path / "aws", tmp_path / "emu"
    _cap(aws, "dynamodb_list_tables.json")
    _cap(emu, "dynamodb_list_tables.json", body='{"tables":[]}')
    assert conform(aws, emu) == 1
    md = (emu / "CONFORM.md").read_text()
    assert "Envelope shape diffs" in md


def test_conform_flags_emulator_error_on_success_probe(tmp_path):
    """An emulator that 500s a read-only op diverges — status flips the
    verdict even though the AWS side has no error code to compare."""
    aws, emu = tmp_path / "aws", tmp_path / "emu"
    _cap(aws, "dynamodb_list_tables.json")
    _cap(
        emu, "dynamodb_list_tables.json", status=500,
        body='{"__type":"InternalError","message":"boom"}',
    )
    assert conform(aws, emu) == 1
    md = (emu / "CONFORM.md").read_text()
    assert "500" in md


def _s3_list_buckets(names):
    inner = "".join(
        f"<Bucket><Name>{n}</Name><CreationDate>t</CreationDate></Bucket>"
        for n in names
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<ListAllMyBucketsResult '
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        "<Owner><ID>x</ID><DisplayName>y</DisplayName></Owner>"
        f"<Buckets>{inner}</Buckets></ListAllMyBucketsResult>"
    )


_S3_OK = {
    "service": "s3", "operation": "list_buckets", "model_service": "s3",
    "headers": {"Content-Type": "application/xml"},
}


def test_conform_xml_success_envelopes_dedup_repeated_members(tmp_path):
    """XML success bodies fingerprint by element-path set — repeated
    ``Bucket`` members dedup, so 2 buckets vs 1 is the same shape."""
    aws, emu = tmp_path / "aws", tmp_path / "emu"
    _cap(aws, "s3_list_buckets.json", body=_s3_list_buckets(["a", "b"]),
         **_S3_OK)
    _cap(emu, "s3_list_buckets.json", body=_s3_list_buckets(["a"]),
         **_S3_OK)
    assert conform(aws, emu) == 0


def test_conform_xml_empty_vs_nonempty_list_is_a_shape_diff(tmp_path):
    """Zero-vs-nonzero cardinality drops the member element paths
    entirely — a real envelope difference, correctly flagged."""
    aws, emu = tmp_path / "aws", tmp_path / "emu"
    _cap(aws, "s3_list_buckets.json", body=_s3_list_buckets(["a"]),
         **_S3_OK)
    _cap(emu, "s3_list_buckets.json", body=_s3_list_buckets([]), **_S3_OK)
    assert conform(aws, emu) == 1


def test_diff_success_captures_missing_code_does_not_crash(tmp_path):
    """200 responses carry no Error.Code — both sides must parse to
    ``None`` rather than raising."""
    a, b = tmp_path / "a", tmp_path / "b"
    _cap(a, "dynamodb_list_tables.json")
    _cap(b, "dynamodb_list_tables.json", body='{"TableNames":[]}')
    assert diff(a, b) == 0


# — report —


def test_report_skips_success_captures(tmp_path):
    out = tmp_path / "fid"
    caps = out / "captures"
    caps.mkdir(parents=True)
    golden = json.loads((_GOLDENS / "sqs_get_queue_url.json").read_text())
    (caps / "sqs_get_queue_url.json").write_text(json.dumps(golden))
    (caps / "dynamodb_list_tables.json").write_text(json.dumps(_OK_CAP))
    assert report(out) == 0
    md = (out / "REPORT.md").read_text()
    assert "dynamodb_list_tables" not in md
    assert "success" in md.lower()
    assert "skipped" in md.lower()
    assert "1/1" in md
