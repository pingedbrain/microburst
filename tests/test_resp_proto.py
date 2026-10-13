"""Unit tests: RESP codec, error renderer, command detector, redis rule DSL."""

from __future__ import annotations

import asyncio

import pytest

from microburst.redis.detect import command_facts, command_text
from microburst.redis.errors import error_reply, moved
from microburst.redis.proto import (
    RespProtocolError,
    bulk,
    command_wire,
    read_command,
    read_reply,
    simple,
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


# --- command frames ----------------------------------------------------------

def test_multibulk_command_roundtrip():
    wire = command_wire("GET", "mykey")
    assert wire == b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n"
    cmd = _read(read_command, wire)
    assert cmd is not None
    assert cmd.args == [b"GET", b"mykey"]
    assert cmd.raw == wire
    assert not cmd.inline


def test_inline_command():
    cmd = _read(read_command, b"PING\r\n")
    assert cmd is not None
    assert cmd.inline
    assert cmd.args == [b"PING"]
    assert cmd.raw == b"PING\r\n"


def test_inline_command_multi_arg_and_lone_lf():
    cmd = _read(read_command, b"set k v\n")
    assert cmd is not None
    assert cmd.args == [b"set", b"k", b"v"]


def test_empty_inline_line_is_a_frame():
    cmd = _read(read_command, b"\r\n")
    assert cmd is not None
    assert cmd.args == []
    assert cmd.raw == b"\r\n"


def test_read_command_clean_eof_returns_none():
    assert _read(read_command, b"") is None


def test_read_command_truncated_bulk_raises_eof():
    wire = command_wire("GET", "mykey")
    with pytest.raises(EOFError):
        _read(read_command, wire[:-3])


def test_read_command_bad_multibulk_length():
    with pytest.raises(RespProtocolError):
        _read(read_command, b"*x\r\n")
    with pytest.raises(RespProtocolError):
        _read(read_command, b"*0\r\n")


def test_read_command_non_bulk_arg_rejected():
    with pytest.raises(RespProtocolError):
        _read(read_command, b"*2\r\n:1\r\n$3\r\nfoo\r\n")


def test_pipelined_commands_read_in_order():
    wire = command_wire("SET", "a", "1") + command_wire("GET", "a")
    async def go():
        reader = asyncio.StreamReader()
        reader.feed_data(wire)
        reader.feed_eof()
        first = await read_command(reader)
        second = await read_command(reader)
        assert first is not None and second is not None
        assert first.args == [b"SET", b"a", b"1"]
        assert second.args == [b"GET", b"a"]
        assert await read_command(reader) is None

    asyncio.run(go())


# --- reply frames --------------------------------------------------------------

def test_reply_simple_error_int():
    assert _read(read_reply, simple("OK")).value == "OK"
    err = _read(read_reply, b"-MOVED 1 h:1\r\n")
    assert err.kind == b"-" and err.value == "MOVED 1 h:1"
    assert _read(read_reply, b":42\r\n").value == 42


def test_reply_bulk_and_nulls():
    r = _read(read_reply, bulk(b"hello"))
    assert r.kind == b"$" and r.value == b"hello"
    assert _read(read_reply, b"$-1\r\n").value is None
    assert _read(read_reply, b"*-1\r\n").value is None
    assert _read(read_reply, b"_\r\n").value is None  # RESP3 null


def test_reply_nested_aggregate_roundtrip():
    wire = (
        b"*3\r\n$5\r\nhello\r\n:2\r\n*2\r\n+OK\r\n$3\r\nbye\r\n"
    )
    r = _read(read_reply, wire)
    assert r.raw == wire
    assert r.value == [b"hello", 2, ["OK", b"bye"]]


def test_reply_resp3_types():
    assert _read(read_reply, b"#t\r\n").value is True
    assert _read(read_reply, b",3.5\r\n").value == 3.5
    assert _read(read_reply, b",inf\r\n").value == float("inf")
    assert _read(read_reply, b"(3492890328409238509324850943850943825024385\r\n").value == (
        3492890328409238509324850943850943825024385
    )
    # blob error / verbatim share bulk framing
    assert _read(read_reply, b"!4\r\nSYO!\r\n").value == b"SYO!"
    assert _read(read_reply, b"=9\r\ntxt:hello\r\n").value == b"txt:hello"
    # set and push parse as element lists
    assert _read(read_reply, b"~2\r\n:1\r\n:2\r\n").value == [1, 2]
    push = _read(read_reply, b">2\r\n$4\r\nkind\r\n$4\r\ndata\r\n")
    assert push.kind == b">" and push.value == [b"kind", b"data"]
    # map decodes to key/value pairs
    m = _read(read_reply, b"%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n")
    assert m.value == [(b"a", 1), (b"b", 2)]
    # attribute frames carry metadata before the reply they annotate
    attr = _read(read_reply, b"|1\r\n$3\r\nkey\r\n$3\r\nval\r\n")
    assert attr.kind == b"|" and attr.value == [(b"key", b"val")]


def test_read_reply_clean_eof_returns_none():
    assert _read(read_reply, b"") is None


def test_read_reply_truncated_bulk_raises_eof():
    with pytest.raises(EOFError):
        _read(read_reply, bulk(b"hello")[:-4])


def test_read_reply_truncated_aggregate_raises_eof():
    with pytest.raises(EOFError):
        _read(read_reply, b"*2\r\n$5\r\nhello\r\n")


def test_read_reply_bad_type_byte():
    with pytest.raises(RespProtocolError):
        _read(read_reply, b"?wtf\r\n")


def test_read_reply_bad_bulk_length():
    with pytest.raises(RespProtocolError):
        _read(read_reply, b"$x\r\n")


# --- error renderer ----------------------------------------------------------

def test_error_reply_byte_shape():
    assert error_reply(code="ERR", message="boom") == b"-ERR boom\r\n"
    assert error_reply() == b"-ERR\r\n"


def test_error_reply_code_carries_whole_line():
    wire = error_reply(code="MOVED 3999 127.0.0.1:7001")
    assert wire == b"-MOVED 3999 127.0.0.1:7001\r\n"


def test_error_reply_redirect_fields_compose():
    wire = error_reply(
        code="MOVED",
        fields={"slot": 3999, "target": "127.0.0.1:7001"},
    )
    assert wire == b"-MOVED 3999 127.0.0.1:7001\r\n"
    wire = error_reply(
        code="ask", fields={"slot": 1, "target": "10.0.0.1:6380"}
    )
    assert wire == b"-ASK 1 10.0.0.1:6380\r\n"


def test_error_reply_message_appends():
    wire = error_reply(
        code="OOM", message="command not allowed when used memory > 'maxmemory'."
    )
    assert wire == b"-OOM command not allowed when used memory > 'maxmemory'.\r\n"


def test_moved_helper():
    assert moved(3999, "127.0.0.1:7001") == "MOVED 3999 127.0.0.1:7001"


# --- command detector ----------------------------------------------------------

@pytest.mark.parametrize("args,verb,key", [
    ([b"GET", b"user:1"], "get", "user:1"),
    ([b"set", b"k", b"v"], "set", "k"),
    ([b"SUBSCRIBE", b"news"], "subscribe", "news"),
    ([b"PING"], "ping", None),
    ([b"AUTH", b"default", b"hunter2"], "auth", None),   # credentials never surface
    ([b"SELECT", b"2"], "select", None),
    ([b"CLUSTER", b"SLOTS"], "cluster", None),
    ([b"CONFIG", b"GET", b"maxmemory"], "config", None),
    ([b"EVAL", b"return 1", b"2", b"k1", b"k2"], "eval", "k1"),
    ([b"EVALSHA", b"deadbeef", b"0"], "evalsha", None),
    ([b"XREAD", b"COUNT", b"5", b"STREAMS", b"events", b"0"],
     "xread", "events"),
    ([b"LMPOP", b"2", b"la", b"lb", b"LEFT"], "lmpop", "la"),
    ([b"MIGRATE", b"h", b"6379", b"k", b"0", b"100"], "migrate", "k"),
    ([b"XINFO", b"STREAM", b"ev"], "xinfo", "ev"),
    ([b"OBJECT", b"ENCODING", b"k"], "object", "k"),
    ([b"BITOP", b"AND", b"dest", b"k1"], "bitop", "dest"),
    ([b"MULTI"], "multi", None),
    ([b"WATCH", b"k"], "watch", "k"),
    ([], None, None),
])
def test_command_facts(args, verb, key):
    assert command_facts(args) == (verb, key)


def test_command_text():
    assert command_text([b"GET", b"user:1"]) == "GET user:1"
    assert command_text([]) == ""


# --- redis rule DSL -------------------------------------------------------------

def test_args_matcher_roundtrip_and_match():
    from microburst.core.context import RequestContext

    rule = from_dict({"args": "^set session:"})
    assert to_dict(rule)["args"] == "^set session:"
    hit = RequestContext(service="redis", args="SET session:42 x")
    assert rule.matches(hit)
    miss = RequestContext(service="redis", args="GET session:42")
    assert not rule.matches(miss)
    other = RequestContext(service="dynamodb")  # args=None → never matches
    assert not rule.matches(other)


def test_args_matcher_is_case_insensitive_like_sql():
    from microburst.core.context import RequestContext

    rule = from_dict({"args": "^get"})
    assert rule.matches(RequestContext(service="redis", args="GET k"))


def test_cut_reply_roundtrip():
    rule = from_dict({"operation": "get", "cut_reply": {"after_bytes": 12}})
    assert rule.cut_reply_bytes == 12
    assert to_dict(rule)["cut_reply"] == {"after_bytes": 12}


def test_cut_reply_validation():
    with pytest.raises(ValueError):
        from_dict({"cut_reply": {"after_bytes": -1}})
    with pytest.raises(TypeError):
        from_dict({"cut_reply": 12})
    with pytest.raises(ValueError):
        from_dict({"cut_reply": {}})


def test_invalid_args_regex_rejected():
    with pytest.raises(ValueError):
        from_dict({"args": "("})
