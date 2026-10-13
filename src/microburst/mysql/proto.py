"""MySQL wire codec — packet framing, no command semantics.

Reference (labeled MYSQL DOCS throughout):
https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html

Every packet on the wire is:

    3-byte little-endian payload length | 1-byte sequence id | payload

A message whose payload is ``MAX_PAYLOAD`` bytes or more is split across
continuation packets; a message of exactly ``MAX_PAYLOAD`` bytes ends
with a trailing empty packet (docs: "Payload Type: length-encoded
split"). Sequence ids are protocol logic owned by the connection handler
— they reset to 0 on every command packet and increment per packet in a
reply. This module only moves them, it never interprets them.

Reader contract (mirrors ``pg/proto.py``):

* ``None`` — clean EOF at a packet boundary: the peer went away politely.
* ``EOFError`` — EOF *inside* a packet: the link died mid-write.
* ``MysqlProtocolError`` — the stream can't be resynced to a packet
  boundary. The only honest recovery is closing both ends. Anything that
  still parses is relayed verbatim — the proxy only inspects what a rule
  decision needs.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

MAX_PAYLOAD = 0xFFFFFF          # 16 MiB - 1 — the split point per MYSQL DOCS
MAX_PACKETS_PER_MESSAGE = 512   # sanity bound: ~8 GiB of continuations

PROTOCOL_V10 = 0x0A

# --- client capability flags (MYSQL DOCS: Capability Flags) ------------------
CLIENT_LONG_PASSWORD = 0x00000001
CLIENT_FOUND_ROWS = 0x00000002
CLIENT_LONG_FLAG = 0x00000004
CLIENT_CONNECT_WITH_DB = 0x00000008
CLIENT_COMPRESS = 0x00000020
CLIENT_LOCAL_FILES = 0x00000080
CLIENT_PROTOCOL_41 = 0x00000200
CLIENT_SSL = 0x00000800
CLIENT_TRANSACTIONS = 0x00002000
CLIENT_SECURE_CONNECTION = 0x00008000
CLIENT_MULTI_STATEMENTS = 0x00010000
CLIENT_MULTI_RESULTS = 0x00020000
CLIENT_PS_MULTI_RESULTS = 0x00040000
CLIENT_PLUGIN_AUTH = 0x00080000
CLIENT_CONNECT_ATTRS = 0x00100000
CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000
CLIENT_SESSION_TRACK = 0x00800000
CLIENT_DEPRECATE_EOF = 0x01000000
CLIENT_ZSTD_COMPRESSION = 0x04000000

# Capabilities the proxy strips from the relayed greeting: TLS and
# compression would re-frame or encrypt everything after the handshake,
# and this transport terminates neither. A client that requires TLS
# refuses the same way it would against a mysqld built without SSL.
GREETING_CLEAR_CAPS = (
    CLIENT_SSL | CLIENT_COMPRESS | CLIENT_ZSTD_COMPRESSION
)

# --- server status flags (MYSQL DOCS: Status Flags) --------------------------
SERVER_STATUS_IN_TRANS = 0x0001
SERVER_STATUS_AUTOCOMMIT = 0x0002
SERVER_MORE_RESULTS_EXISTS = 0x0008
SERVER_STATUS_CURSOR_EXISTS = 0x0040
SERVER_STATUS_LAST_ROW_SENT = 0x0080
SERVER_STATUS_METADATA_CHANGED = 0x0400
SERVER_PS_OUT_PARAMS = 0x1000

# --- command phase byte values (MYSQL DOCS: Command Phase) -------------------
COM_SLEEP = 0x00
COM_QUIT = 0x01
COM_INIT_DB = 0x02
COM_QUERY = 0x03
COM_FIELD_LIST = 0x04
COM_CREATE_DB = 0x05
COM_DROP_DB = 0x06
COM_REFRESH = 0x07
COM_SHUTDOWN = 0x08
COM_STATISTICS = 0x09
COM_PROCESS_INFO = 0x0A
COM_CONNECT = 0x0B
COM_PROCESS_KILL = 0x0C
COM_DEBUG = 0x0D
COM_PING = 0x0E
COM_TIME = 0x0F
COM_DELAYED_INSERT = 0x10
COM_CHANGE_USER = 0x11
COM_BINLOG_DUMP = 0x12
COM_TABLE_DUMP = 0x13
COM_CONNECT_OUT = 0x14
COM_REGISTER_SLAVE = 0x15
COM_STMT_PREPARE = 0x16
COM_STMT_EXECUTE = 0x17
COM_STMT_SEND_LONG_DATA = 0x18
COM_STMT_CLOSE = 0x19
COM_STMT_RESET = 0x1A
COM_SET_OPTION = 0x1B
COM_STMT_FETCH = 0x1C
COM_DAEMON = 0x1D
COM_BINLOG_DUMP_GTID = 0x1E
COM_RESET_CONNECTION = 0x1F

# Reply first-byte markers (MYSQL DOCS: Generic Response Packets).
HEAD_OK = 0x00
HEAD_AUTH_MORE_DATA = 0x01
HEAD_ERR = 0xFF
HEAD_EOF = 0xFE
HEAD_LOCAL_INFILE = 0xFB

# A 0xFE-led packet is an EOF (or the OK-as-EOF replacement under
# CLIENT_DEPRECATE_EOF) only when short — a text row whose first column
# is a lenenc string of >= 16 MiB also starts 0xFE but is always >= 9
# bytes (1B marker + 8B length). MYSQL DOCS: EOF_Packet, "check whether
# the packet length is less than 9".
EOF_MAX_LEN = 9


class MysqlProtocolError(Exception):
    """Stream can't be resynced to a packet boundary — close both ends."""


@dataclass
class Packet:
    """One wire packet: sequence id, payload, and the exact raw bytes."""

    seq: int
    payload: bytes
    raw: bytes


def packet_bytes(payload: bytes, seq: int) -> bytes:
    """Wire-encode one packet: 3B LE length + 1B seq + payload."""
    return len(payload).to_bytes(3, "little") + bytes([seq & 0xFF]) + payload


def message_packets(payload: bytes, seq: int = 0) -> list[tuple[int, bytes]]:
    """Split one logical message into wire packets → [(seq, chunk)].

    A payload of exactly ``MAX_PAYLOAD`` produces a full packet plus a
    trailing empty one (MYSQL DOCS: the continuation rule).
    """
    out: list[tuple[int, bytes]] = []
    off = 0
    while True:
        chunk = payload[off:off + MAX_PAYLOAD]
        out.append((seq, chunk))
        seq = (seq + 1) & 0xFF
        off += MAX_PAYLOAD
        if len(chunk) < MAX_PAYLOAD:
            return out


def message_bytes(payload: bytes, seq: int = 0) -> bytes:
    """``message_packets`` flattened to wire bytes."""
    return b"".join(
        packet_bytes(chunk, s) for s, chunk in message_packets(payload, seq)
    )


async def _exact(reader: asyncio.StreamReader, n: int, what: str) -> bytes:
    try:
        return await reader.readexactly(n)
    except asyncio.IncompleteReadError as e:
        raise EOFError(f"EOF inside {what}") from e


async def read_packet(reader: asyncio.StreamReader) -> Packet | None:
    """Read one packet → Packet, or None on clean EOF at a boundary."""
    head = await reader.read(4)
    if not head:
        return None
    if len(head) < 4:
        raise EOFError("truncated packet header")
    length = int.from_bytes(head[:3], "little")
    payload = await _exact(reader, length, "packet payload")
    return Packet(seq=head[3], payload=payload, raw=head + payload)


async def read_message(reader: asyncio.StreamReader) -> list[Packet] | None:
    """Read one logical message → its packets (continuations merged).

    Keeps packet-level granularity because the caller forwards ``raw``
    verbatim and derives the reply sequence id from the last packet.
    """
    first = await read_packet(reader)
    if first is None:
        return None
    packets = [first]
    while packets[-1].payload.__len__() == MAX_PAYLOAD:
        if len(packets) >= MAX_PACKETS_PER_MESSAGE:
            raise MysqlProtocolError("message exceeds continuation bound")
        nxt = await read_packet(reader)
        if nxt is None:
            raise EOFError("EOF inside multi-packet message")
        packets.append(nxt)
    return packets


def message_payload(packets: list[Packet]) -> bytes:
    return b"".join(p.payload for p in packets)


def message_raw(packets: list[Packet]) -> bytes:
    return b"".join(p.raw for p in packets)


# --- length-encoded integers/strings (MYSQL DOCS: Type: lenenc) ----------------

def lenenc_int(buf: bytes, off: int = 0) -> tuple[int | None, int]:
    """One length-encoded integer → (value, offset past it). None = NULL."""
    if off >= len(buf):
        raise MysqlProtocolError("lenenc int past end of buffer")
    b = buf[off]
    if b < 0xFB:
        return b, off + 1
    if b == 0xFB:
        return None, off + 1
    if b == 0xFC:
        return int.from_bytes(buf[off + 1:off + 3], "little"), off + 3
    if b == 0xFD:
        return int.from_bytes(buf[off + 1:off + 4], "little"), off + 4
    return int.from_bytes(buf[off + 1:off + 9], "little"), off + 9


def lenenc_str(data: bytes) -> bytes:
    """Encode bytes as a lenenc string (single-byte prefix when small)."""
    n = len(data)
    if n < 0xFB:
        return bytes([n]) + data
    if n < 0x10000:
        return b"\xfc" + n.to_bytes(2, "little") + data
    if n < 0x1000000:
        return b"\xfd" + n.to_bytes(3, "little") + data
    return b"\xfe" + n.to_bytes(8, "little") + data


# --- reply classification ------------------------------------------------------

def classify(payload: bytes) -> str:
    """First-reply-packet class: ``ok`` | ``err`` | ``eof`` | ``infile`` |
    ``auth_switch`` | ``other`` (a lenenc column count → ResultSet).

    ``auth_switch`` (0xFE, len >= 9) only appears during the auth phase
    and after COM_CHANGE_USER — in reply position it is distinct from an
    EOF terminator by length (MYSQL DOCS: AuthSwitchRequest).
    """
    if not payload:
        return "other"
    b = payload[0]
    if b == HEAD_OK:
        return "ok"
    if b == HEAD_ERR:
        return "err"
    if b == HEAD_LOCAL_INFILE:
        return "infile"
    if b == HEAD_EOF:
        return "eof" if len(payload) < EOF_MAX_LEN else "auth_switch"
    return "other"


def is_terminator(payload: bytes) -> bool:
    """End-of-rows marker in either EOF mode: 0xFE and len < 9 covers
    EOF_Packet and the OK_Packet-with-0xFE-header that replaces it under
    CLIENT_DEPRECATE_EOF (MYSQL DOCS: Resultset, EOF_Packet)."""
    return (
        len(payload) < EOF_MAX_LEN
        and len(payload) >= 1
        and payload[0] == HEAD_EOF
    )


def status_flags(payload: bytes) -> int | None:
    """SERVER_STATUS_* flags from an OK or EOF packet payload, else None.

    OK (header 0x00 or 0xFE-with-len>=7): lenenc affected_rows, lenenc
    last_insert_id, then 2B status + 2B warnings under PROTOCOL_41.
    EOF (0xFE, len 5): 2B warnings then 2B status (MYSQL DOCS layouts).
    """
    if len(payload) < 5:
        return None
    b0 = payload[0]
    if b0 == HEAD_EOF and len(payload) == 5:
        return int.from_bytes(payload[3:5], "little")
    if b0 in (HEAD_OK, HEAD_EOF) and len(payload) >= 7:
        try:
            off = 1
            _, off = lenenc_int(payload, off)
            _, off = lenenc_int(payload, off)
        except MysqlProtocolError:
            return None
        if off + 4 <= len(payload):
            return int.from_bytes(payload[off:off + 2], "little")
    return None


# --- handshake parsing ---------------------------------------------------------

@dataclass
class HandshakeInfo:
    """What the client told us in its handshake response."""

    capabilities: int
    username: str | None
    database: str | None
    ssl_request: bool        # short response + CLIENT_SSL → TLS starts next


def _greeting_caps_offset(payload: bytes) -> int | None:
    """Offset of the lower capability bytes inside an Initial Handshake
    payload, or None when it doesn't parse. Layout (MYSQL DOCS):
    0x0A | server-version cstring | conn-id 4 | auth-data-1 8 |
    filler 1 | caps-lower 2 | ..."""
    if len(payload) < 32 or payload[0] != PROTOCOL_V10:
        return None
    try:
        end = payload.index(b"\x00", 1)
    except ValueError:
        return None
    off = end + 1 + 4 + 8 + 1
    return off if off + 2 <= len(payload) else None


def greeting_capabilities(payload: bytes) -> int:
    """Server capability flags from an Initial Handshake payload."""
    off = _greeting_caps_offset(payload)
    if off is None:
        return 0
    caps = int.from_bytes(payload[off:off + 2], "little")
    upper_off = off + 2 + 1 + 2  # charset(1) + status(2) then caps-upper
    if upper_off + 2 <= len(payload):
        caps |= int.from_bytes(payload[upper_off:upper_off + 2], "little") << 16
    return caps


def strip_greeting_caps(payload: bytes, clear: int) -> bytes:
    """Clear capability bits in an Initial Handshake payload — the
    proxy's TLS/compression refusal. Returns the payload unchanged when
    it doesn't parse: never corrupt what can't be understood."""
    off = _greeting_caps_offset(payload)
    if off is None:
        return payload
    buf = bytearray(payload)
    lower = int.from_bytes(buf[off:off + 2], "little") & ~(clear & 0xFFFF)
    buf[off:off + 2] = lower.to_bytes(2, "little")
    upper_off = off + 2 + 1 + 2
    if upper_off + 2 <= len(buf):
        upper = (
            int.from_bytes(buf[upper_off:upper_off + 2], "little")
            & ~((clear >> 16) & 0xFFFF)
        )
        buf[upper_off:upper_off + 2] = upper.to_bytes(2, "little")
    return bytes(buf)


def _read_cstr(payload: bytes, off: int) -> tuple[str | None, int]:
    end = payload.find(b"\x00", off)
    if end < 0:
        return None, len(payload)
    return payload[off:end].decode("utf-8", "replace"), end + 1


def parse_handshake_response(payload: bytes) -> HandshakeInfo | None:
    """Client handshake response → capabilities + identity, best-effort.

    Layout under CLIENT_PROTOCOL_41 (MYSQL DOCS): caps 4 | max-packet 4 |
    charset 1 | reserved 23 | username cstr | auth-response | [db cstr
    if CONNECT_WITH_DB] | [plugin cstr if PLUGIN_AUTH] | ... A response
    of exactly the 32-byte head with CLIENT_SSL set is the SSLRequest —
    the client starts TLS after it and sends nothing more.
    """
    if len(payload) < 4:
        return None
    caps = int.from_bytes(payload[:4], "little")
    ssl_request = bool(caps & CLIENT_SSL) and len(payload) <= 32
    if not (caps & CLIENT_PROTOCOL_41) or len(payload) < 33:
        return HandshakeInfo(caps, None, None, ssl_request)
    username, off = _read_cstr(payload, 32)
    # auth-response length is negotiated three different ways
    try:
        if caps & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA:
            n, off = lenenc_int(payload, off)
            off += n or 0
        elif caps & CLIENT_SECURE_CONNECTION:
            off += 1 + payload[off] if off < len(payload) else 0
        else:
            _, off = _read_cstr(payload, off)
    except (IndexError, MysqlProtocolError):
        return HandshakeInfo(caps, username, None, ssl_request)
    database = None
    if caps & CLIENT_CONNECT_WITH_DB:
        database, _ = _read_cstr(payload, off)
    return HandshakeInfo(caps, username, database, ssl_request)


def stmt_id_of(payload: bytes) -> int | None:
    """statement_id (int<4>) from a COM_STMT_* command payload."""
    return int.from_bytes(payload[1:5], "little") if len(payload) >= 5 else None


# --- packet payload builders ----------------------------------------------------
# Used by tests' fake upstream and by any reply the proxy synthesizes
# (injected ERR packets are built in errors.py instead).

def initial_handshake(
    *,
    server_version: str = "8.0.36-microburst",
    connection_id: int = 42,
    auth_plugin: str = "mysql_native_password",
    auth_data: bytes = b"12345678abcdefgh",
    capabilities: int = (
        CLIENT_LONG_PASSWORD | CLIENT_LONG_FLAG | CLIENT_PROTOCOL_41
        | CLIENT_TRANSACTIONS | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH | CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS
        | CLIENT_PS_MULTI_RESULTS | CLIENT_DEPRECATE_EOF
    ),
) -> bytes:
    """Minimal Initial Handshake payload (MYSQL DOCS layout)."""
    salt = auth_data[:20].ljust(20, b"\x00")
    out = [
        bytes([PROTOCOL_V10]),
        server_version.encode() + b"\x00",
        connection_id.to_bytes(4, "little"),
        salt[:8],
        b"\x00",
        (capabilities & 0xFFFF).to_bytes(2, "little"),
        b"\x21",                                   # charset utf8mb4-ish
        SERVER_STATUS_AUTOCOMMIT.to_bytes(2, "little"),
        ((capabilities >> 16) & 0xFFFF).to_bytes(2, "little"),
        bytes([len(salt) + 1 if capabilities & CLIENT_PLUGIN_AUTH else 0]),
        b"\x00" * 10,
        salt[8:] + b"\x00",
    ]
    if capabilities & CLIENT_PLUGIN_AUTH:
        out.append(auth_plugin.encode() + b"\x00")
    return b"".join(out)


def ok_payload(
    *,
    affected_rows: int = 0,
    last_insert_id: int = 0,
    status: int = SERVER_STATUS_AUTOCOMMIT,
    warnings: int = 0,
    info: bytes = b"",
) -> bytes:
    """OK_Packet payload — PROTOCOL_41 layout."""
    return (
        bytes([HEAD_OK])
        + _lenenc_uint(affected_rows)
        + _lenenc_uint(last_insert_id)
        + status.to_bytes(2, "little")
        + warnings.to_bytes(2, "little")
        + info
    )


def _lenenc_uint(n: int) -> bytes:
    if n < 0xFB:
        return bytes([n])
    if n < 0x10000:
        return b"\xfc" + n.to_bytes(2, "little")
    if n < 0x1000000:
        return b"\xfd" + n.to_bytes(3, "little")
    return b"\xfe" + n.to_bytes(8, "little")


def eof_payload(
    *,
    status: int = SERVER_STATUS_AUTOCOMMIT,
    warnings: int = 0,
) -> bytes:
    """EOF_Packet payload (0xFE + warnings + status — 5 bytes)."""
    return (
        bytes([HEAD_EOF])
        + warnings.to_bytes(2, "little")
        + status.to_bytes(2, "little")
    )


def auth_switch_payload(
    plugin: str = "mysql_native_password", data: bytes = b"1234567890abcdefghij"
) -> bytes:
    """AuthSwitchRequest payload (0xFE, long — never an EOF)."""
    return bytes([HEAD_EOF]) + plugin.encode() + b"\x00" + data


def auth_more_data_payload(data: bytes = b"\x04") -> bytes:
    """AuthMoreData payload — 0x01 head (caching_sha2 exchanges)."""
    return bytes([HEAD_AUTH_MORE_DATA]) + data


def column_definition(name: str) -> bytes:
    """Minimal text column-definition payload: catalog 'def', int type."""
    fields = [b"def", b"", b"", b"", name.encode(), b""]
    out = b"".join(lenenc_str(f) for f in fields)
    out += b"\x0c"                     # fixed-len fields marker
    out += (0x21).to_bytes(2, "little")  # charset
    out += (11).to_bytes(4, "little")    # column length
    out += bytes([0x03])               # MYSQL_TYPE_LONG
    out += (0).to_bytes(2, "little")     # flags
    out += bytes([0])                  # decimals
    out += b"\x00\x00"                 # filler
    return out


def text_row(values: list[bytes | None]) -> bytes:
    """Text-protocol resultset row payload."""
    return b"".join(
        b"\xfb" if v is None else lenenc_str(v) for v in values
    )


def stmt_prepare_ok(
    *,
    statement_id: int = 1,
    num_columns: int = 0,
    num_params: int = 0,
    warnings: int = 0,
) -> bytes:
    """COM_STMT_PREPARE_OK payload (MYSQL DOCS layout)."""
    return (
        bytes([HEAD_OK])
        + statement_id.to_bytes(4, "little")
        + num_columns.to_bytes(2, "little")
        + num_params.to_bytes(2, "little")
        + b"\x00"
        + warnings.to_bytes(2, "little")
    )
