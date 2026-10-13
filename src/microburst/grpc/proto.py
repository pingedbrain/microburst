"""gRPC-over-h2c helpers — the codec half of the grpc transport.

Everything here is a thin layer over ``h2.connection.H2Connection``:
connection factories for both legs of the proxy, the canonical gRPC
status-code table, the ``grpc-message`` percent-encoding, header-list
builders for trailers-only and mid-stream trailers, and a chunked
``send_data`` (h2 refuses to split frames internally — the caller owns
window/frame-size clipping).

Wire-fidelity facts encoded here (evidence labels):

* The gRPC status travels in HEADERS — either a *trailers-only*
  response (one HEADERS carrying ``:status 200`` +
  ``content-type: application/grpc`` + ``grpc-status`` +
  ``grpc-message`` + END_STREAM, the shape real servers send for errors
  raised before the handler produces output) or trailing HEADERS after
  DATA frames. HTTP status stays 200 either way — grpc-level failure
  never maps to a non-200 on the wire (spec: grpc.io protocol draft).
* ``grpc-message`` is percent-encoded per the spec: bytes outside
  %x20-7E, plus ``%`` itself, escape to ``%HH``. Space is legal raw.
* A mid-call abort's h2 shape is RST_STREAM. ``INTERNAL_ERROR`` (0x2)
  is the code server libraries emit for a handler that died; gRPC
  clients surface an RST_STREAM mid-call as a retryable
  ``UNAVAILABLE``/``INTERNAL`` regardless of the code carried.
"""

from __future__ import annotations

import h2.config
import h2.connection

# RST_STREAM error code used for `reset` — INTERNAL_ERROR (0x2), the
# code server libraries emit for a handler that died mid-call.
RST_ERROR_CODE = 0x2

# The 17 canonical gRPC status codes (spec: grpc/statuscodes). Rules
# accept either the name ("UNAVAILABLE") or the number (14) — this map
# resolves both directions.
GRPC_CODES: dict[str, int] = {
    "OK": 0,
    "CANCELLED": 1,
    "UNKNOWN": 2,
    "INVALID_ARGUMENT": 3,
    "DEADLINE_EXCEEDED": 4,
    "NOT_FOUND": 5,
    "ALREADY_EXISTS": 6,
    "PERMISSION_DENIED": 7,
    "RESOURCE_EXHAUSTED": 8,
    "FAILED_PRECONDITION": 9,
    "ABORTED": 10,
    "OUT_OF_RANGE": 11,
    "UNIMPLEMENTED": 12,
    "INTERNAL": 13,
    "UNAVAILABLE": 14,
    "DATA_LOSS": 15,
    "UNAUTHENTICATED": 16,
}

DEFAULT_MESSAGE = "injected fault"


def new_server_conn() -> h2.connection.H2Connection:
    """Server-side h2 state machine — faces the downstream client."""
    return h2.connection.H2Connection(
        h2.config.H2Configuration(client_side=False, header_encoding="utf-8")
    )


def new_client_conn() -> h2.connection.H2Connection:
    """Client-side h2 state machine — faces the upstream gRPC server."""
    return h2.connection.H2Connection(
        h2.config.H2Configuration(client_side=True, header_encoding="utf-8")
    )


def grpc_status(code) -> tuple[str, str | None]:
    """Resolve a rule's ``error.code`` to a ``grpc-status`` digit string.

    Names resolve through GRPC_CODES (case-insensitive); numbers pass
    through after a 0–16 sanity check. An unresolvable code falls back
    to UNKNOWN(2) — the returned note explains the fallback for the
    fired log (a bad code must still inject a fault, never crash the
    proxy or leak through as a bogus trailer).
    """
    if code is None:
        return "14", None  # UNAVAILABLE — the defensible default fault
    if isinstance(code, int) and not isinstance(code, bool):
        if 0 <= code <= 16:
            return str(code), None
        return "2", f"grpc code {code!r} out of range — used UNKNOWN(2)"
    text = str(code).strip()
    if text.isdigit():
        if 0 <= int(text) <= 16:
            return str(int(text)), None
        return "2", f"grpc code {text!r} out of range — used UNKNOWN(2)"
    status = GRPC_CODES.get(text.upper())
    if status is not None:
        return str(status), None
    return "2", f"unknown grpc code {text!r} — used UNKNOWN(2)"


def encode_message(message: str) -> str:
    """Percent-encode a ``grpc-message`` value per the gRPC spec:
    bytes outside %x20-7E (and ``%`` itself) become ``%HH``; everything
    else — space included — passes through raw."""
    out = []
    for byte in message.encode("utf-8"):
        if 0x20 <= byte <= 0x7E and byte != 0x25:
            out.append(chr(byte))
        else:
            out.append(f"%{byte:02X}")
    return "".join(out)


def decode_message(value: str) -> str:
    """Reverse ``encode_message`` — test/client side helper."""
    out = bytearray()
    i = 0
    while i < len(value):
        if (
            value[i] == "%"
            and i + 2 < len(value)
            and all(c in "0123456789abcdefABCDEF" for c in value[i + 1 : i + 3])
        ):
            out.append(int(value[i + 1 : i + 3], 16))
            i += 3
        else:
            out.extend(value[i].encode("utf-8"))
            i += 1
    return bytes(out).decode("utf-8", "replace")


def headers_dict(headers) -> dict[str, str]:
    """h2 event headers (list of (name, value) tuples) → plain dict.
    Repeated names keep the last value — fine for the pseudo/regular
    headers gRPC carries."""
    return {str(k).lower(): str(v) for k, v in headers}


def is_grpc(headers) -> bool:
    """True when the request HEADERS declare ``application/grpc*`` —
    trailers-only injection is only protocol-correct there. Other h2
    traffic (plain REST-over-h2, WebSockets-over-h2) still proxies;
    transport faults apply but ``error:`` is skipped."""
    ctype = headers_dict(headers).get("content-type", "")
    return ctype.split(";", 1)[0].strip().startswith("application/grpc")


def operation_for(path: str) -> str:
    """``:path`` → rule ``operation``: the full ``package.Service/Method``
    path, lowercased, leading slash stripped — e.g.
    ``/helloworld.Greeter/SayHello`` → ``helloworld.greeter/sayhello``.
    Malformed paths (no slash) degrade to the bare lowercased token."""
    return path.lstrip("/").lower()


def trailers_only(code: str, message: str) -> list[tuple[str, str]]:
    """Trailers-only error response: one HEADERS frame carrying HTTP
    200 + the grpc status — the legal shape real servers send when the
    call fails before the handler writes anything."""
    return [
        (":status", "200"),
        ("content-type", "application/grpc"),
        ("grpc-status", code),
        ("grpc-message", encode_message(message)),
    ]


def error_trailers(code: str, message: str) -> list[tuple[str, str]]:
    """Mid-stream error trailers — HEADERS after DATA frames, no
    ``:status`` (trailers, not a fresh response)."""
    return [
        ("grpc-status", code),
        ("grpc-message", encode_message(message)),
    ]


def send_data_chunked(conn: h2.connection.H2Connection, sid: int, data: bytes) -> int:
    """Send as much of ``data`` as the outbound window allows, chunked
    to ``max_outbound_frame_size`` (h2 refuses to split internally).
    Returns bytes actually sent — the caller buffers/acks the rest.
    Raises nothing for flow control; propagates StreamClosedError."""
    sent = 0
    while sent < len(data):
        window = conn.local_flow_control_window(sid)
        if window <= 0:
            break
        chunk = data[sent : sent + min(window, conn.max_outbound_frame_size)]
        conn.send_data(sid, chunk)
        sent += len(chunk)
    return sent
