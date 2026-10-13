"""Emulator conformance: diff a capture set against real-AWS goldens."""

from __future__ import annotations

import json

from microburst.fidelity import _rebind_region, conform, diff


def test_rebind_region_rewrites_embedded_arn_regions():
    kwargs = _rebind_region(
        {"TopicArn": "arn:aws:sns:us-east-1:000000000000:q",
         "Name": "x"}, "eu-west-1",
    )
    assert kwargs["TopicArn"] == "arn:aws:sns:eu-west-1:000000000000:q"
    assert kwargs["Name"] == "x"


def test_rebind_region_leaves_plain_values_alone():
    kwargs = _rebind_region({"TableName": "orders"}, "us-west-2")
    assert kwargs == {"TableName": "orders"}

_AWS_CAP = {
    "service": "dynamodb",
    "operation": "describe_table",
    "model_service": "dynamodb",
    "sdk_error_code": "ResourceNotFoundException",
    "status": 400,
    "headers": {
        "Content-Type": "application/x-amz-json-1.0",
        "x-amzn-RequestId": "RID",
    },
    "body": '{"__type":"com.amazonaws.dynamodb.v20120810#'
            'ResourceNotFoundException","message":"x"}',
}


def _cap(tmp, name, **over):
    d = tmp / "captures"
    d.mkdir(parents=True, exist_ok=True)
    cap = {**_AWS_CAP, **over}
    (d / name).write_text(json.dumps(cap))


def test_conform_all_match(tmp_path):
    aws, emu = tmp_path / "aws", tmp_path / "emu"
    _cap(aws, "dynamodb_describe_table.json")
    _cap(emu, "dynamodb_describe_table.json")
    assert conform(aws, emu) == 0
    md = (emu / "CONFORM.md").read_text()
    assert "1/1 probes conform" in md


def test_conform_detects_code_drift(tmp_path):
    aws, emu = tmp_path / "aws", tmp_path / "emu"
    _cap(aws, "dynamodb_describe_table.json")
    _cap(
        emu, "dynamodb_describe_table.json",
        body='{"__type":"ValidationException","message":"x"}',
    )
    assert conform(aws, emu) == 1
    md = (emu / "CONFORM.md").read_text()
    assert "0/1 probes conform" in md
    assert "ValidationException" in md


def test_conform_detects_content_type_and_status(tmp_path):
    aws, emu = tmp_path / "aws", tmp_path / "emu"
    _cap(aws, "dynamodb_describe_table.json")
    _cap(
        emu, "dynamodb_describe_table.json",
        status=404,
        headers={
            "Content-Type": "text/plain",
            "x-amzn-RequestId": "RID",
        },
    )
    assert conform(aws, emu) == 1
    md = (emu / "CONFORM.md").read_text()
    assert "404" in md and "text/plain" in md


def test_conform_missing_probe_is_reported(tmp_path):
    aws, emu = tmp_path / "aws", tmp_path / "emu"
    _cap(aws, "dynamodb_describe_table.json")
    _cap(aws, "lambda_get_function.json",
         service="lambda", operation="get_function",
         model_service="lambda")
    (emu / "captures").mkdir(parents=True, exist_ok=True)
    _cap(emu, "dynamodb_describe_table.json")
    assert conform(aws, emu) == 1
    md = (emu / "CONFORM.md").read_text()
    assert "missing" in md


def test_diff_compares_two_capture_sets(tmp_path):
    """`fidelity diff A B` — same fields as conform, neutral labels,
    DIFF.md written into B."""
    a, b = tmp_path / "a", tmp_path / "b"
    _cap(a, "dynamodb_describe_table.json")
    _cap(b, "dynamodb_describe_table.json")
    assert diff(a, b) == 0
    md = (b / "DIFF.md").read_text()
    assert "1/1 probes match" in md
    assert "| a status | b status |" in md


def test_diff_flags_shape_drift(tmp_path):
    """Envelopes botocore parses identically still diff on shape."""
    a, b = tmp_path / "a", tmp_path / "b"
    _cap(a, "dynamodb_describe_table.json")
    _cap(
        b, "dynamodb_describe_table.json",
        # bare __type — botocore parses the same code, but the wire
        # shape differs (namespace prefix absent)
        body='{"__type":"ResourceNotFoundException","message":"x"}',
    )
    assert diff(a, b) == 1
    md = (b / "DIFF.md").read_text()
    assert "Envelope shape diffs" in md
