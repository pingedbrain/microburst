"""Unit tests: mysql packet codec, ERR renderer, command detector,
mysql rule DSL (errno/code → errno+sqlstate)."""

from __future__ import annotations

import asyncio

import pytest

from microburst.mysql.detect import command_facts, command_name, reply_kind
from microburst.mysql.errors import (
    err_payload,
    resolve_errno_sqlstate,
)
from microburst.mysql.proto import (
    CLIENT_CONNECT_WITH_DB,
    CLIENT_DEPRECATE_EOF,
    CLIENT_PLUGIN_AUTH,
    CLIENT_PROTOCOL_41,
    CLIENT_SECURE_CONNECTION,
    CLIENT_SSL,
    COM_INIT_DB,
    COM_PING,
    COM_QUERY,
    COM_QUIT,
    COM_STMT_CLOSE,
    COM_STMT_EXECUTE,
    COM_STMT_PREPARE,
    MAX_PAYLOAD,
    SERVER_STATUS_AUTOCOMMIT,
    SERVER_STATUS_IN_TRANS,
    MysqlProtocolError,
    classify,
    eof_payload,
    greeting_capabilities,
    initial_handshake,
    is_terminator,
    lenenc_int,
    lenenc_str,
    message_packets,
    message_payload,
    ok_payload,
    packet_bytes,
    parse_handshake_response,
    read_message,
    read_packet,
    status_flags,
    strip_greeting_caps,
    text_row,
)
from microburst.rules import from_dict, to_dict


def _read(fn, data: bytes):
    """Run ``fn`` against a StreamReader fed ``data`` + EOF, inside a loop."""
    async def go():
        reader = asyncio.StreamReader()
        reader.feed_data(data)
        reader.feed_eof()
        return await fn(reader)

    return asyncio.run(go())


# --- packet framing --------------------------------------------------------------

def test_packet_roundtrip():
    wire = packet_bytes(b"\x03select 1", 0)
    assert wire[:4] == b"\x09\x00\x00\x00"  # len 3B LE + seq
    pkt = _read(read_packet, wire)
    assert pkt is not None
    assert pkt.seq == 0
    assert pkt.payload == b"\x03select 1"
    assert pkt.raw == wire


def test_read_packet_eof_boundary_is_none():
    assert _read(read_packet, b"") is None


def test_read_packet_truncated_header_is_eoferror():
    with pytest.raises(EOFError):
        _read(read_packet, b"\x09\x00")


def test_read_packet_truncated_payload_is_eoferror():
    with pytest.raises(EOFError):
        _read(read_packet, b"\x05\x00\x00\x00\x03se")


def test_message_packets_split_boundary():
    # payload exactly MAX_PAYLOAD → full packet + trailing empty packet
    # (docs: the continuation rule)
    parts = message_packets(b"x" * MAX_PAYLOAD, seq=0)
    assert len(parts) == 2
    assert parts[0] == (0, b"x" * MAX_PAYLOAD)
    assert parts[1] == (1, b"")
    # one byte under → single packet
    assert len(message_packets(b"x" * (MAX_PAYLOAD - 1))) == 1


def test_read_message_reassembles_continuation():
    payload = b"\x03" + b"q" * (MAX_PAYLOAD + 5)
    wire = b"".join(packet_bytes(c, s) for s, c in message_packets(payload))
    pkts = _read(read_message, wire)
    assert pkts is not None
    assert len(pkts) == 2
    assert pkts[-1].seq == 1
    assert message_payload(pkts) == payload


def test_read_message_eof_inside_continuation():
    wire = packet_bytes(b"x" * MAX_PAYLOAD, 0)  # promises continuation, none comes
    with pytest.raises(EOFError):
        _read(read_message, wire)


# --- lenenc ----------------------------------------------------------------------

def test_lenenc_int_sizes():
    assert lenenc_int(b"\x2a") == (42, 1)
    assert lenenc_int(b"\xfb") == (None, 1)           # NULL
    assert lenenc_int(b"\xfc\x39\x30") == (12345, 3)
    assert lenenc_int(b"\xfd\xff\xff\x00") == (65535, 4)
    assert lenenc_int(b"\xfe\x01\x00\x00\x00\x01\x00\x00\x00") == (
        (1 << 32) + 1, 9)
    with pytest.raises(MysqlProtocolError):
        lenenc_int(b"")


def test_lenenc_str_roundtrip():
    assert lenenc_str(b"abc") == b"\x03abc"
    n, off = lenenc_int(lenenc_str(b"abc"))
    assert n == 3 and off == 1


# --- reply classification + status flags --------------------------------------------

def test_classify_reply_heads():
    assert classify(ok_payload()) == "ok"
    assert classify(b"\xff\x28\x04#42000boom") == "err"
    assert classify(eof_payload()) == "eof"
    assert classify(b"\xfb/tmp/f.csv") == "infile"
    # 0xFE >= 9 bytes is an auth switch request, not an EOF
    assert classify(b"\xfe" + b"mysql_native_password\x00" + b"salt") == "auth_switch"
    assert classify(b"\x03") == "other"      # lenenc column count
    assert classify(b"") == "other"


def test_is_terminator_covers_eof_and_deprecate_ok():
    assert is_terminator(eof_payload())
    # OK-packet-with-0xFE-header used under CLIENT_DEPRECATE_EOF —
    # 7 bytes, still < 9
    deprecate_term = b"\xfe\x00\x00" + SERVER_STATUS_AUTOCOMMIT.to_bytes(2, "little") + b"\x00\x00"
    assert is_terminator(deprecate_term)
    # a text row starting with a huge lenenc length is NOT a terminator
    assert not is_terminator(b"\xfe" + (1 << 24).to_bytes(8, "little") + b"data")


def test_status_flags_ok_and_eof():
    payload = ok_payload(status=SERVER_STATUS_IN_TRANS | SERVER_STATUS_AUTOCOMMIT)
    assert status_flags(payload) == SERVER_STATUS_IN_TRANS | SERVER_STATUS_AUTOCOMMIT
    assert status_flags(eof_payload(status=0)) == 0
    assert status_flags(b"\x03abc") is None   # not an OK/EOF packet


# --- greeting + handshake response ---------------------------------------------------

def test_greeting_capabilities_and_strip():
    caps = (
        CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
        | CLIENT_DEPRECATE_EOF | CLIENT_SSL | 0x20  # + COMPRESS
    )
    payload = initial_handshake(capabilities=caps)
    assert greeting_capabilities(payload) == caps
    stripped = strip_greeting_caps(payload, CLIENT_SSL | 0x20)
    got = greeting_capabilities(stripped)
    assert not (got & CLIENT_SSL)
    assert not (got & 0x20)
    assert got & CLIENT_DEPRECATE_EOF          # untouched
    assert len(stripped) == len(payload)       # in-place bit clearing


def test_strip_greeting_caps_unparseable_passthrough():
    assert strip_greeting_caps(b"junk", CLIENT_SSL) == b"junk"


def _handshake_response(caps: int, user=b"alice", db=b"shop") -> bytes:
    out = (
        caps.to_bytes(4, "little") + (1 << 24).to_bytes(4, "little")
        + b"\x21" + b"\x00" * 23
    )
    out += user + b"\x00"
    out += b"\x00"                          # empty secure auth response
    if caps & CLIENT_CONNECT_WITH_DB:
        out += db + b"\x00"
    if caps & CLIENT_PLUGIN_AUTH:
        out += b"mysql_native_password\x00"
    return out


def test_parse_handshake_response_full():
    caps = (
        CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
        | CLIENT_CONNECT_WITH_DB | CLIENT_DEPRECATE_EOF
    )
    info = parse_handshake_response(_handshake_response(caps))
    assert info is not None
    assert info.capabilities == caps
    assert info.username == "alice"
    assert info.database == "shop"
    assert not info.ssl_request


def test_parse_handshake_response_ssl_request():
    # SSLRequest = just the 32-byte head with CLIENT_SSL set
    caps = CLIENT_PROTOCOL_41 | CLIENT_SSL
    payload = caps.to_bytes(4, "little") + (1 << 24).to_bytes(4, "little") + b"\x21" + b"\x00" * 23
    info = parse_handshake_response(payload)
    assert info is not None
    assert info.ssl_request
    assert info.capabilities & CLIENT_SSL


def test_parse_handshake_response_no_db():
    caps = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION
    info = parse_handshake_response(_handshake_response(caps))
    assert info is not None
    assert info.database is None


# --- ERR payload byte shape ----------------------------------------------------------

def test_err_payload_bytes():
    p = err_payload(errno=1064, sqlstate="42000", message="syntax error")
    assert p[0] == 0xFF
    assert int.from_bytes(p[1:3], "little") == 1064
    assert p[3:9] == b"#42000"
    assert p[9:] == b"syntax error"


def test_err_payload_pre41_omits_sqlstate_marker():
    p = err_payload(errno=1105, sqlstate="HY000", message="x", protocol_41=False)
    assert p == b"\xff" + (1105).to_bytes(2, "little") + b"x"


def test_err_payload_sqlstate_coerced_to_five():
    p = err_payload(errno=1, sqlstate="42", message="")
    assert p[3:9] == b"#42   "
    p = err_payload(errno=1, sqlstate="420001234", message="")
    assert p[3:9] == b"#42000"


def test_resolve_errno_sqlstate_pairs():
    assert resolve_errno_sqlstate(1064, None) == (1064, "42000")
    assert resolve_errno_sqlstate(1213, None) == (1213, "40001")
    assert resolve_errno_sqlstate(None, "08004") == (1040, "08004")
    assert resolve_errno_sqlstate(9999, None) == (9999, "HY000")
    assert resolve_errno_sqlstate(None, "ZZZZZ") == (1105, "ZZZZZ")
    assert resolve_errno_sqlstate(None, None) == (1105, "HY000")
    assert resolve_errno_sqlstate(1040, "08004") == (1040, "08004")


# --- command detection ---------------------------------------------------------------

def test_command_facts_query():
    op, res, sql, sid = command_facts(b"\x03select * from orders", {})
    assert op == "select"
    assert res == "orders"
    assert sql == "select * from orders"
    assert sid is None


def test_command_facts_stmt_prepare():
    op, res, sql, _ = command_facts(b"\x16insert into orders values (?)", {})
    assert op == "stmt_prepare"
    assert res == "orders"
    assert "insert into orders" in sql


def test_command_facts_stmt_execute_resolves_sql():
    statements = {7: "select * from orders"}
    op, res, sql, sid = command_facts(
        b"\x17" + (7).to_bytes(4, "little") + b"\x00\x01\x00\x00\x00",
        statements,
    )
    assert op == "stmt_execute"
    assert res == "orders"
    assert sql == "select * from orders"
    assert sid == 7


def test_command_facts_stmt_close_resolves_and_names():
    op, _res, sql, sid = command_facts(
        b"\x19" + (7).to_bytes(4, "little"), {7: "select 1"}
    )
    assert op == "stmt_close" and sid == 7 and sql == "select 1"


def test_command_facts_init_db_and_misc():
    op, res, _, _ = command_facts(b"\x02shop", {})
    assert (op, res) == ("com_init_db", "shop")
    op, _, sql, _ = command_facts(b"\x0e", {})
    assert (op, sql) == ("com_ping", None)
    op, *_ = command_facts(b"\x99\xff", {})
    assert op == "com_99"
    assert command_name(COM_QUIT) == "com_quit"
    assert command_facts(b"", {}) == (None, None, None, None)


def test_reply_kind_map():
    assert reply_kind(COM_QUERY) == "generic"
    assert reply_kind(COM_STMT_PREPARE) == "prepare"
    assert reply_kind(COM_STMT_EXECUTE) == "generic"
    assert reply_kind(COM_STMT_CLOSE) == "none"
    assert reply_kind(COM_QUIT) == "quit"
    assert reply_kind(COM_INIT_DB) == "generic"
    assert reply_kind(COM_PING) == "generic"
    assert reply_kind(0x1C) == "splice"   # COM_STMT_FETCH
    assert reply_kind(0x12) == "splice"   # COM_BINLOG_DUMP
    assert reply_kind(0x99) == "generic"


# --- rule DSL -----------------------------------------------------------------------

def test_errno_rule_parses_and_roundtrips():
    rule = from_dict({
        "service": "mysql", "operation": "startup",
        "error": {"errno": 1040, "code": "08004",
                  "message": "Too many connections"},
    })
    assert rule.error is not None
    assert rule.error.errno == 1040
    assert rule.error.code == "08004"
    out = to_dict(rule)
    assert out["error"]["errno"] == 1040
    assert out["error"]["code"] == "08004"


def test_errno_only_rule_parses():
    rule = from_dict({"error": {"errno": 1064}})
    assert rule.error is not None
    assert rule.error.errno == 1064
    assert rule.error.code is None


def test_sqlstate_alias_still_maps_to_code():
    rule = from_dict({"error": {"sqlstate": "40001"}})
    assert rule.error is not None
    assert rule.error.code == "40001"


# --- test-side builders ------------------------------------------------------------

def test_ok_eof_column_row_builders():
    from microburst.mysql.proto import column_definition
    assert status_flags(ok_payload(status=SERVER_STATUS_IN_TRANS)) == SERVER_STATUS_IN_TRANS
    eof = eof_payload(status=SERVER_STATUS_AUTOCOMMIT)
    assert len(eof) == 5 and eof[0] == 0xFE
    row = text_row([b"1", None, b"ab"])
    assert row == b"\x011\xfb\x02ab"
    assert column_definition("n")[:4] == b"\x03def"
