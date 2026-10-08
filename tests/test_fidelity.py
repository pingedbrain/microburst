"""Fidelity harness — headless: every service's error wire format is
rendered by microburst and then parsed by botocore's own protocol
parser. The contract is that ``Error.Code`` round-trips, because that
is what drives SDK retry classification.

This does NOT validate against real AWS (see ROADMAP for the live-AWS
diff harness); it validates that our renderers speak each protocol's
envelope correctly enough for the SDK parser to recover the error code.
"""

from __future__ import annotations

import pytest
from botocore.parsers import create_parser
from botocore.session import Session

from microburst.protocols import render_error

_session = Session()
_SERVICES = sorted(_session.get_available_services())


def _model(service):
    return _session.get_service_model(service)


@pytest.mark.parametrize("service", _SERVICES, ids=_SERVICES)
def test_error_code_roundtrips(service):
    model = _model(service)
    # one operation (for its output shape) + one modeled error if any
    op_name = model.operation_names[0]
    op = model.operation_model(op_name)
    code = (
        op.error_shapes[0].name
        if op.error_shapes
        else "InternalError"
    )
    status, headers, body = render_error(service, code, "fidelity check")
    parser = create_parser(model.protocol)
    parsed = parser.parse(
        {"status_code": status, "headers": dict(headers), "body": body},
        op.output_shape,
    )
    error = parsed.get("Error") or {}
    assert error.get("Code") == code, (
        f"{service}: parser recovered {error.get('Code')!r}, "
        f"expected {code!r}"
    )


def test_query_compat_mode_roundtrips():
    """Services migrated to rpc-v2-cbor still accept query-compat JSON —
    the error must carry x-amzn-query-error so the *json* parser
    recovers the code."""
    status, headers, body = render_error(
        "cloudwatch", "ThrottlingException", "boom",
        protocol="json", query_compat=True,
    )
    parsed = create_parser("json").parse(
        {"status_code": status, "headers": dict(headers), "body": body},
        None,
    )
    assert parsed["Error"]["Code"] == "ThrottlingException"
    assert parsed["Error"]["Type"] == "Sender"
