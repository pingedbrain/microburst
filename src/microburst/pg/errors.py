"""ErrorResponse / NoticeResponse rendering.

An ErrorResponse is a sequence of (field-code byte, cstring) pairs closed
by a single 0x00, framed as type 'E' ('N' for notices). Real PostgreSQL
always sends at least ``S`` (localized severity), ``V`` (non-localized
severity), ``C`` (SQLSTATE) and ``M`` (message); drivers classify on
``C`` + severity, so both ``S`` and ``V`` are emitted verbatim.

``severity: FATAL`` semantics live in the caller — real PG sends the
ErrorResponse and then closes the connection, and that close is the
only error class safe to inject inside an open transaction.
"""

from __future__ import annotations

from microburst.pg.proto import frame

# Friendly name → wire field code. Single-character keys in a `fields`
# map are used as the raw field code directly, so anything unlisted
# (e.g. internal_position 'p') is still expressible.
FIELD_CODES: dict[str, bytes] = {
    "severity": b"S",
    "severity_nonlocalized": b"V",
    "sqlstate": b"C",
    "message": b"M",
    "detail": b"D",
    "hint": b"H",
    "position": b"P",
    "internal_position": b"p",
    "internal_query": b"q",
    "where": b"W",
    "schema": b"s",
    "table": b"t",
    "column": b"c",
    "datatype": b"d",
    "constraint": b"n",
    "file": b"F",
    "line": b"L",
    "routine": b"R",
}


def _field(code: bytes, value: str) -> bytes:
    return code + str(value).encode("utf-8", "replace") + b"\x00"


def _fields_payload(
    severity: str,
    sqlstate: str,
    message: str,
    fields: dict | None,
) -> bytes:
    out = [
        _field(b"S", severity),
        _field(b"V", severity),
        _field(b"C", sqlstate),
        _field(b"M", message),
    ]
    for key, value in (fields or {}).items():
        code = FIELD_CODES.get(str(key).lower())
        if code is None and len(str(key)) == 1:
            code = str(key).encode()
        if code is None or code in (b"S", b"V", b"C", b"M"):
            continue  # unknown long name, or a duplicate of a core field
        out.append(_field(code, str(value)))
    out.append(b"\x00")
    return b"".join(out)


def error_response(
    *,
    sqlstate: str = "XX000",
    message: str = "",
    severity: str = "ERROR",
    fields: dict | None = None,
) -> bytes:
    """A wire-encoded ErrorResponse ('E')."""
    return frame(
        b"E", _fields_payload(severity.upper(), sqlstate, message, fields)
    )


def notice_response(
    *,
    sqlstate: str = "00000",
    message: str = "",
    severity: str = "NOTICE",
    fields: dict | None = None,
) -> bytes:
    """A wire-encoded NoticeResponse ('N')."""
    return frame(
        b"N", _fields_payload(severity.upper(), sqlstate, message, fields)
    )


def parse_error_fields(payload: bytes) -> dict[str, str]:
    """ErrorResponse/NoticeResponse payload → {field code letter: text}.

    Stops at the terminator or the first unparseable byte — this is a
    test/debug helper, not a validating parser.
    """
    out: dict[str, str] = {}
    off = 0
    while off < len(payload) and payload[off:off + 1] != b"\x00":
        code = payload[off:off + 1].decode("ascii", "replace")
        end = payload.find(b"\x00", off + 1)
        if end < 0:
            break
        out[code] = payload[off + 1:end].decode("utf-8", "replace")
        off = end + 1
    return out
