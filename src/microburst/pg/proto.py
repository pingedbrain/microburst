"""PostgreSQL v3 wire codec — framing, no semantics.

Reference: https://www.postgresql.org/docs/current/protocol-message-formats.html

Steady state, both directions: 1-byte type, Int32 length (counting
itself, *not* the type byte), payload. The client's first packet —
startup — drops the type byte: Int32 length (counting itself), Int32
protocol/request code, then key/value cstring pairs + a terminating NUL.

Reader contract (defensive, documented choice):

* ``None`` — clean EOF at a message boundary: the peer went away politely.
* ``EOFError`` — EOF *inside* a message: the link died mid-frame.
* ``PgProtocolError`` — a length field makes the stream unparseable.
  The proxy can't resync to a frame boundary, so the only honest
  recovery is closing both ends. Anything that still parses is relayed
  verbatim — the proxy only inspects what a rule decision needs.
"""

from __future__ import annotations

import asyncio
import struct

PROTOCOL_V3 = 196608            # 3.0
CANCEL_REQUEST_CODE = 80877102  # 1234.5678 — no reply expected; close both ends
SSL_REQUEST_CODE = 80877103     # 1234.5679 — answer 'N' or 'S'
GSSENC_REQUEST_CODE = 80877104  # 1234.5680 — answer 'N' or 'G'

MAX_STARTUP_BODY = 1 << 20      # real PG caps at 10000; headroom for params
MAX_FRAME_PAYLOAD = 1 << 30     # sanity bound — 1 GiB


class PgProtocolError(Exception):
    """Stream can't be resynced to a frame boundary — close both ends."""


def cstring(text: str) -> bytes:
    return text.encode() + b"\x00"


def read_cstring(payload: bytes, offset: int = 0) -> tuple[str, int]:
    """One NUL-terminated string; returns (text, offset past the NUL)."""
    end = payload.index(b"\x00", offset)
    return payload[offset:end].decode("utf-8", "replace"), end + 1


def frame(msg_type: bytes, payload: bytes = b"") -> bytes:
    """Wire-encode one steady-state message. ``msg_type`` is 1 byte."""
    return msg_type + struct.pack("!I", len(payload) + 4) + payload


def startup_packet(code: int, params: dict[str, str] | bytes = b"") -> bytes:
    """Type-less first packet: Int32 len, Int32 code, cstring pairs, NUL."""
    if isinstance(params, dict):
        body = b"".join(
            cstring(k) + cstring(v) for k, v in params.items()
        ) + b"\x00"
    else:
        body = params
    return struct.pack("!II", len(body) + 8, code) + body


def startup_wire(body: bytes) -> bytes:
    """Re-wrap a startup body (code + params, as read) with its length."""
    return struct.pack("!I", len(body) + 4) + body


def parse_startup_params(body: bytes) -> dict[str, str]:
    """Startup body (Int32 code + cstring pairs) → params dict."""
    params: dict[str, str] = {}
    off = 4  # skip the protocol/request code
    while off < len(body):
        if body[off:off + 1] == b"\x00":
            break
        key, off = read_cstring(body, off)
        value, off = read_cstring(body, off)
        params[key] = value
    return params


async def read_startup(reader: asyncio.StreamReader) -> tuple[int, bytes] | None:
    """Read one startup packet → (code, body incl. code) or None on clean EOF."""
    try:
        raw = await reader.readexactly(4)
    except asyncio.IncompleteReadError as e:
        if not e.partial:
            return None
        raise EOFError("truncated startup packet length") from e
    (length,) = struct.unpack("!I", raw)
    if length < 8 or length - 4 > MAX_STARTUP_BODY:
        raise PgProtocolError(f"startup packet length {length} out of range")
    try:
        body = await reader.readexactly(length - 4)
    except asyncio.IncompleteReadError as e:
        raise EOFError("truncated startup packet body") from e
    (code,) = struct.unpack("!I", body[:4])
    return code, body


async def read_frame(
    reader: asyncio.StreamReader,
) -> tuple[bytes, bytes] | None:
    """Read one typed frame → (type byte, payload) or None on clean EOF."""
    head = await reader.read(1)
    if not head:
        return None
    try:
        raw = await reader.readexactly(4)
    except asyncio.IncompleteReadError as e:
        raise EOFError("truncated frame header") from e
    (length,) = struct.unpack("!I", raw)
    if length < 4 or length - 4 > MAX_FRAME_PAYLOAD:
        raise PgProtocolError(f"frame length {length} out of range")
    try:
        payload = await reader.readexactly(length - 4)
    except asyncio.IncompleteReadError as e:
        raise EOFError("truncated frame payload") from e
    return head, payload


# --- backend message builders -------------------------------------------------
# Used by tests' fake upstream and by any response the proxy synthesizes
# (auth refusal is rendered via errors.error_response instead).

def auth_ok() -> bytes:
    return frame(b"R", struct.pack("!I", 0))


def parameter_status(name: str, value: str) -> bytes:
    return frame(b"S", cstring(name) + cstring(value))


def backend_key_data(pid: int = 12345, secret: int = 67890) -> bytes:
    return frame(b"K", struct.pack("!II", pid, secret))


def ready_for_query(status: bytes = b"I") -> bytes:
    """``status`` is the tx indicator: 'I' idle, 'T' in-tx, 'E' failed-tx."""
    return frame(b"Z", status)


def row_description(names: list[str]) -> bytes:
    """Minimal RowDescription: int4 columns, no table/type provenance."""
    out = [struct.pack("!H", len(names))]
    for name in names:
        out.append(
            cstring(name)
            + struct.pack("!IhIhiH", 0, 0, 23, 4, -1, 0)  # int4, text fmt
        )
    return frame(b"T", b"".join(out))


def data_row(values: list[bytes | None]) -> bytes:
    out = [struct.pack("!H", len(values))]
    for value in values:
        if value is None:
            out.append(struct.pack("!i", -1))
        else:
            out.append(struct.pack("!I", len(value)) + value)
    return frame(b"D", b"".join(out))


def command_complete(tag: str) -> bytes:
    return frame(b"C", cstring(tag))


def empty_query_response() -> bytes:
    return frame(b"I")


def parse_complete() -> bytes:
    return frame(b"1")


def bind_complete() -> bytes:
    return frame(b"2")


def close_complete() -> bytes:
    return frame(b"3")


def no_data() -> bytes:
    return frame(b"n")


def parse_message(statement: str, query: str) -> bytes:
    """Frontend Parse — used by tests to drive the extended protocol."""
    return frame(b"P", cstring(statement) + cstring(query) + struct.pack("!H", 0))


def bind_message(portal: str, statement: str) -> bytes:
    """Frontend Bind — unnamed portal, no params, default formats."""
    return frame(
        b"B",
        cstring(portal) + cstring(statement) + struct.pack("!HHH", 0, 0, 0),
    )


def describe_message(kind: bytes = b"P") -> bytes:
    """Frontend Describe — 'P' portal or 'S' prepared statement."""
    return frame(b"D", kind + cstring(""))


def execute_message(portal: str = "") -> bytes:
    """Frontend Execute — no row limit."""
    return frame(b"E", cstring(portal) + struct.pack("!I", 0))


def query_message(sql: str) -> bytes:
    """Frontend Query — simple protocol."""
    return frame(b"Q", cstring(sql))


def sync_message() -> bytes:
    return frame(b"S")


def terminate_message() -> bytes:
    return frame(b"X")
