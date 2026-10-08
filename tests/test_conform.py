"""Emulator conformance: diff a capture set against real-AWS goldens."""

from __future__ import annotations

import json

from microburst.fidelity import conform

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
