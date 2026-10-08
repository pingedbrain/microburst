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


def _unique_wire_codes(service):
    """{wire_code: modeled_http_status or None} deduped across all ops.

    The wire code is the ``error.code`` trait when present, else the shape
    name — i.e. the string AWS actually puts on the wire and users copy
    out of ``ClientError``.
    """
    model = _model(service)
    out: dict[str, int | None] = {}
    for op_name in model.operation_names:
        for shape in model.operation_model(op_name).error_shapes:
            err = shape.metadata.get("error") or {}
            code = err.get("code") or shape.name
            out.setdefault(code, err.get("httpStatusCode"))
    return out


@pytest.mark.parametrize("service", _SERVICES, ids=_SERVICES)
def test_every_modeled_error_roundtrips(service):
    """Fuzz: render every modeled error code of the service and verify the
    SDK parser recovers it, and the status matches the modeled
    ``httpStatusCode`` when the model declares one."""
    model = _model(service)
    cases = _unique_wire_codes(service)
    if not cases:
        pytest.skip("no modeled error shapes")
    parser = create_parser(model.protocol)
    failures = []
    for code, want_status in cases.items():
        status, headers, body = render_error(service, code, "fuzz")
        parsed = parser.parse(
            {"status_code": status, "headers": dict(headers), "body": body},
            None,
        )
        got = (parsed.get("Error") or {}).get("Code")
        if got != code:
            failures.append(f"code {code!r} -> {got!r}")
        if isinstance(want_status, int) and status != want_status:
            failures.append(
                f"status for {code!r}: rendered {status}, modeled {want_status}"
            )
    assert not failures, f"{service}: " + "; ".join(failures[:10])


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
