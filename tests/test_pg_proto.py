"""Unit tests: pg codec, error renderer, SQL detector, pg rule DSL."""

from __future__ import annotations

import asyncio
import struct

import pytest

from microburst.pg.detect import sql_facts
from microburst.pg.errors import (
    error_response,
    notice_response,
    parse_error_fields,
)
from microburst.pg.proto import (
    GSSENC_REQUEST_CODE,
    PROTOCOL_V3,
    SSL_REQUEST_CODE,
    PgProtocolError,
    cstring,
    data_row,
    frame,
    parse_startup_params,
    read_frame,
    read_startup,
    ready_for_query,
    startup_packet,
)
from microburst.rules import from_dict, to_dict


def _read(fn, data: bytes):
    """Run ``fn`` against a StreamReader fed ``data`` + EOF, inside a loop
    (StreamReader construction itself needs a running loop on 3.13+)."""
    async def go():
        reader = asyncio.StreamReader()
        reader.feed_data(data)
        reader.feed_eof()
        return await fn(reader)

    return asyncio.run(go())


# --- startup packet ------------------------------------------------------------

def test_startup_packet_roundtrip():
    packet = startup_packet(
        PROTOCOL_V3, {"user": "alice", "database": "shop"}
    )
    (length,) = struct.unpack("!I", packet[:4])
    assert length == len(packet)
    result = _read(read_startup, packet)
    assert result is not None
    code, body = result
    assert code == PROTOCOL_V3
    assert parse_startup_params(body) == {
        "user": "alice", "database": "shop",
    }


def test_startup_codes_are_distinct():
    assert SSL_REQUEST_CODE != GSSENC_REQUEST_CODE


def test_read_startup_clean_eof_returns_none():
    assert _read(read_startup, b"") is None


def test_read_startup_truncated_raises_eof():
    packet = startup_packet(PROTOCOL_V3, {"user": "a"})
    with pytest.raises(EOFError):
        _read(read_startup, packet[:-3])


def test_read_startup_bad_length_raises_protocol_error():
    with pytest.raises(PgProtocolError):
        _read(read_startup, struct.pack("!I", 4))


# --- frames -----------------------------------------------------------------

def test_frame_roundtrip():
    wire = frame(b"Q", cstring("select 1"))
    result = _read(read_frame, wire)
    assert result is not None
    mtype, payload = result
    assert mtype == b"Q"
    assert payload == cstring("select 1")


def test_frame_shape_length_counts_itself_not_type():
    wire = frame(b"Z", b"I")
    assert wire[0:1] == b"Z"
    (length,) = struct.unpack("!I", wire[1:5])
    assert length == 5  # 4 len + 1 payload
    assert len(wire) == 1 + length


def test_read_frame_clean_eof_returns_none():
    assert _read(read_frame, b"") is None


def test_read_frame_truncated_payload_raises_eof():
    with pytest.raises(EOFError):
        _read(read_frame, frame(b"Q", b"select 1")[:-2])


def test_read_frame_bad_length_raises_protocol_error():
    with pytest.raises(PgProtocolError):
        _read(read_frame, b"Q" + struct.pack("!I", 2))


def test_pipelined_frames_read_in_order():
    wire = ready_for_query(b"I") + data_row([b"1"]) + ready_for_query(b"T")
    async def go():
        reader = asyncio.StreamReader()
        reader.feed_data(wire)
        reader.feed_eof()
        assert await read_frame(reader) == (b"Z", b"I")
        row = await read_frame(reader)
        assert row is not None and row[0] == b"D"
        assert await read_frame(reader) == (b"Z", b"T")

    asyncio.run(go())


# --- error renderer ----------------------------------------------------------

def test_error_response_byte_shape():
    wire = error_response(
        sqlstate="40001",
        message="could not serialize access",
        severity="ERROR",
    )
    assert wire[0:1] == b"E"
    (length,) = struct.unpack("!I", wire[1:5])
    assert len(wire) == 1 + length
    payload = wire[5:]
    assert b"C40001\x00" in payload          # SQLSTATE lands on the wire
    assert payload.endswith(b"\x00")          # field-list terminator
    fields = parse_error_fields(payload)
    assert fields["S"] == "ERROR"
    assert fields["V"] == "ERROR"             # non-localized severity too
    assert fields["C"] == "40001"
    assert fields["M"] == "could not serialize access"


def test_error_response_extra_fields_friendly_and_raw():
    wire = error_response(
        sqlstate="55P03",
        message="lock not available",
        severity="FATAL",
        fields={"detail": "nowait", "R": "LockWait"},
    )
    fields = parse_error_fields(wire[5:])
    assert fields["S"] == "FATAL"
    assert fields["D"] == "nowait"
    assert fields["R"] == "LockWait"
    # core fields can't be shadowed by `fields`
    wire2 = error_response(
        sqlstate="40001", message="m", fields={"sqlstate": "ZZZZZ"}
    )
    assert parse_error_fields(wire2[5:])["C"] == "40001"


def test_notice_response_type_n():
    wire = notice_response(sqlstate="00000", message="hi")
    assert wire[0:1] == b"N"
    assert parse_error_fields(wire[5:])["S"] == "NOTICE"


# --- SQL detector --------------------------------------------------------------

@pytest.mark.parametrize("sql,verb,table", [
    ("select 1", "select", None),
    ("  SELECT id FROM orders", "select", "orders"),
    ("INSERT INTO orders VALUES (1)", "insert", "orders"),
    ("update orders set x = 1", "update", "orders"),
    ("delete from cart where id = 1", "delete", "cart"),
    ("BEGIN", "begin", None),
    ("-- comment\nselect * from t1", "select", "t1"),
    ("/* x */ with a as (select 1) select * from a", "with", "a"),
    ("(select 1)", "select", None),
    ("create table if not exists t2 (id int)", "create", "t2"),
    ("select pg_sleep(1)", "select", None),
    ("", None, None),
    ("   ", None, None),
])
def test_sql_facts(sql, verb, table):
    assert sql_facts(sql) == (verb, table)


# --- pg rule DSL ---------------------------------------------------------------

def test_sqlstate_alias_maps_to_code():
    rule = from_dict({
        "service": "postgres",
        "error": {"sqlstate": "40001", "severity": "ERROR",
                  "message": "m"},
    })
    assert rule.error is not None
    assert rule.error.code == "40001"
    assert rule.error.severity == "ERROR"
    assert to_dict(rule)["error"]["code"] == "40001"
    assert to_dict(rule)["error"]["severity"] == "ERROR"


def test_timeout_true_maps_to_hang_sentinel():
    rule = from_dict({"operation": "select", "timeout": True})
    assert rule.timeout_ms is not None and rule.timeout_ms < 0
    assert to_dict(rule)["timeout"] is True
    assert "timeout_ms" not in to_dict(rule)


def test_sql_matcher_roundtrip_and_match():
    from microburst.core.context import RequestContext

    rule = from_dict({"sql": "into orders"})
    assert to_dict(rule)["sql"] == "into orders"
    ctx = RequestContext(service="postgres", sql="INSERT INTO orders VALUES (1)")
    assert rule.matches(ctx)
    ctx2 = RequestContext(service="postgres", sql="insert into items values (1)")
    assert not rule.matches(ctx2)
    ctx3 = RequestContext(service="dynamodb")  # sql=None → never matches
    assert not rule.matches(ctx3)


def test_partial_rows_roundtrip():
    rule = from_dict({"operation": "select", "partial_rows": 2})
    assert rule.partial_rows == 2
    assert to_dict(rule)["partial_rows"] == 2
    with pytest.raises(ValueError):
        from_dict({"partial_rows": -1})


def test_invalid_sql_regex_rejected():
    with pytest.raises(ValueError):
        from_dict({"sql": "("})
