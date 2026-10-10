"""Unit tests: generic framing specs, GenericFramer, and the tcp-mode
rule surface (payload matcher, cut_reply after_messages, corrupt,
respond, top-level cut_upload)."""

from __future__ import annotations

import base64

import pytest

from microburst.core.context import RequestContext
from microburst.framing import (
    FramerSpec,
    GenericFramer,
    parse_framing,
)
from microburst.rules import from_dict, to_dict

# --- spec parsing ------------------------------------------------------------


def test_parse_none_is_unframed():
    assert parse_framing(None) is None


def test_parse_passthrough_spec():
    spec = FramerSpec(kind="fixed", size=8)
    assert parse_framing(spec) is spec


def test_parse_cli_string_length_prefix():
    spec = parse_framing(
        "length-prefix:size=4,offset=5,endian=big,includes_self=false"
    )
    assert spec.kind == "length-prefix"
    assert spec.size == 4
    assert spec.offset == 5
    assert spec.endian == "big"
    assert spec.includes_self is False


def test_parse_cli_string_delimiter_hex_and_escaped():
    assert parse_framing("delimiter:bytes=0d0a").delimiter == b"\r\n"
    assert parse_framing(r"delimiter:bytes=\r\n").delimiter == b"\r\n"
    assert parse_framing("delimiter:bytes=\\x00").delimiter == b"\x00"


def test_parse_mapping_mysql_style():
    spec = parse_framing(
        {"kind": "length-prefix", "size": 3, "adjust": 1,
         "endian": "little"}
    )
    assert (spec.size, spec.adjust, spec.endian) == (3, 1, "little")


def test_parse_rejects_bad_specs():
    with pytest.raises(ValueError):
        parse_framing({"kind": "nope"})
    with pytest.raises(ValueError):
        parse_framing({"kind": "length-prefix", "size": 5})
    with pytest.raises(ValueError):
        parse_framing({"kind": "delimiter", "bytes": ""})
    with pytest.raises(ValueError):
        parse_framing({"kind": "fixed", "size": 0})
    with pytest.raises(ValueError):
        parse_framing({"kind": "length-prefix", "endian": "middle"})
    with pytest.raises(ValueError):
        parse_framing("length-prefix:size")  # param without =


# --- GenericFramer -------------------------------------------------------------


def _frame_be(payload: bytes, size: int = 4) -> bytes:
    return len(payload).to_bytes(size, "big") + payload


def test_length_prefix_basic():
    f = GenericFramer(parse_framing({"kind": "length-prefix", "size": 4}))
    frames = f.feed(_frame_be(b"hello") + _frame_be(b"world"))
    assert frames == [_frame_be(b"hello"), _frame_be(b"world")]
    assert f.frames == 2


def test_length_prefix_split_across_feeds():
    f = GenericFramer(parse_framing({"kind": "length-prefix", "size": 2}))
    data = _frame_be(b"abcdef", size=2)
    out = []
    for i in range(0, len(data), 3):
        out += f.feed(data[i:i + 3])
    assert out == [data]
    assert f.drain() == b""


def test_length_prefix_offset_and_little_endian():
    # cassandra-ish: 5 header bytes, then a 4-byte LE length, then body
    spec = parse_framing(
        {"kind": "length-prefix", "size": 4, "offset": 5,
         "endian": "little"}
    )
    f = GenericFramer(spec)
    msg = b"\x05\x00\x00\x01\x09" + (3).to_bytes(4, "little") + b"xyz"
    assert f.feed(msg) == [msg]


def test_length_prefix_includes_self():
    # mongo-ish: 4-byte LE length counts the whole message
    spec = parse_framing(
        {"kind": "length-prefix", "size": 4, "endian": "little",
         "includes_self": True}
    )
    f = GenericFramer(spec)
    msg = (9).to_bytes(4, "little") + b"abcde"
    assert f.feed(msg) == [msg]
    assert f.drain() == b""


def test_length_prefix_adjust_covers_trailing_header():
    # mysql-ish: 3-byte LE length + 1 seq byte + payload
    spec = parse_framing(
        {"kind": "length-prefix", "size": 3, "adjust": 1,
         "endian": "little"}
    )
    f = GenericFramer(spec)
    pkt = b"\x05\x00\x00\x07hello" + b"\x02\x00\x00\x03hi"
    assert f.feed(pkt) == [b"\x05\x00\x00\x07hello", b"\x02\x00\x00\x03hi"]


def test_delimiter_includes_terminator():
    f = GenericFramer(parse_framing({"kind": "delimiter", "bytes": "0d0a"}))
    assert f.feed(b"one\r\ntwo\r\nthree") == [b"one\r\n", b"two\r\n"]
    assert f.drain() == b"three"


def test_fixed_size():
    f = GenericFramer(parse_framing({"kind": "fixed", "size": 3}))
    assert f.feed(b"abcdefg") == [b"abc", b"def"]
    assert f.drain() == b"g"


def test_malformed_latches_and_drain_replays_verbatim():
    f = GenericFramer(parse_framing({"kind": "length-prefix", "size": 4}))
    bogus = b"\xff\xff\xff\xff" + b"junk"  # declared ~4 GiB > _MAX_FRAME
    assert f.feed(bogus) == []
    assert f.failed
    assert f.feed(b"more") == []          # frozen
    assert f.drain() == bogus


def test_huge_undelimited_buffer_fails():
    from microburst.framing import _MAX_FRAME

    f = GenericFramer(parse_framing({"kind": "delimiter", "bytes": "0d0a"}))
    f.feed(b"x" * (_MAX_FRAME + 1))
    assert f.failed


# --- rule surface -------------------------------------------------------------


def test_payload_matcher_on_tcp_context():
    rule = from_dict({"service": "tcp", "payload": "hello", "reset": True})
    ctx = RequestContext(service="tcp", operation="c2s:frame",
                         payload="\x00\x01HELLO\xff".encode("latin-1")
                         .decode("latin-1"))
    assert rule.matches(ctx)
    assert not rule.matches(
        RequestContext(service="tcp", payload="goodbye")
    )


def test_bytes_alias_maps_to_payload():
    rule = from_dict({"bytes": "needle"})
    assert rule.payload == "needle"


def test_payload_falls_back_to_pg_sql_and_redis_args():
    rule = from_dict({"payload": "^select"})
    assert rule.matches(RequestContext(service="postgres",
                                       sql="SELECT 1"))
    assert not rule.matches(RequestContext(service="postgres",
                                           sql="UPDATE t"))
    rule2 = from_dict({"payload": "^get sess"})
    assert rule2.matches(RequestContext(service="redis",
                                        args="GET session:1"))
    assert not rule2.matches(RequestContext(service="redis",
                                            args="SET k v"))
    # http path has no text matcher input — never matches
    assert not rule.matches(RequestContext(service="s3"))


def test_invalid_payload_regex_rejected():
    with pytest.raises(ValueError, match="payload"):
        from_dict({"payload": "([unclosed"})


def test_cut_reply_after_messages():
    rule = from_dict({"cut_reply": {"after_messages": 3}})
    assert rule.cut_reply_messages == 3
    assert rule.cut_reply_bytes is None
    assert to_dict(rule)["cut_reply"] == {"after_messages": 3}
    both = from_dict({"cut_reply": {"after_bytes": 10,
                                    "after_messages": 2}})
    assert to_dict(both)["cut_reply"] == {"after_bytes": 10,
                                          "after_messages": 2}
    with pytest.raises(ValueError):
        from_dict({"cut_reply": {}})


def test_top_level_cut_upload_becomes_request_fault():
    rule = from_dict({"cut_upload": {"after_bytes": 100,
                                     "after_messages": 2}})
    assert rule.request == {"cut_upload": {"after_bytes": 100,
                                           "after_messages": 2}}
    from microburst.rules import RuleEngine

    engine = RuleEngine()
    engine.set_rules([{"operation": "conn",
                       "cut_upload": {"after_bytes": 7}}])
    decision = engine.decide(RequestContext(service="tcp",
                                            operation="conn",
                                            payload="x"))
    assert decision is not None
    assert decision.request_fault.after_bytes == 7


def test_corrupt_spec():
    rule = from_dict({"corrupt": {"at_bytes": 42}})
    assert rule.corrupt.at_bytes == 42
    assert rule.corrupt.bit == "flip"
    assert to_dict(rule)["corrupt"] == {"at_bytes": 42}
    with pytest.raises(ValueError):
        from_dict({"corrupt": {}})
    with pytest.raises(ValueError):
        from_dict({"corrupt": {"at_bytes": -1}})
    with pytest.raises(ValueError):
        from_dict({"corrupt": {"at_bytes": 1, "bit": "zero"}})


def test_respond_spec_encodings():
    r1 = from_dict({"respond": {"data": "héllo"}})  # latin-1 é = 0xe9
    assert r1.respond.data == b"h\xe9llo"
    r2 = from_dict({"respond": {"hex": "ff00aa"}})
    assert r2.respond.data == b"\xff\x00\xaa"
    r3 = from_dict({"respond": {"base64": base64.b64encode(b"xyz").decode()}})
    assert r3.respond.data == b"xyz"
    assert r1.respond.then == "forward"
    assert to_dict(r2)["respond"]["base64"] == "/wCq"
    with pytest.raises(ValueError):
        from_dict({"respond": {"data": "a", "hex": "aa"}})
    with pytest.raises(ValueError):
        from_dict({"respond": {"then": "explode", "data": "x"}})
    with pytest.raises(ValueError):
        from_dict({"respond": {"hex": "zzzz"}})
    with pytest.raises(ValueError):
        from_dict({"respond": {"base64": "!!!notb64!!!"}})


def test_service_tcp_scoping():
    rule = from_dict({"service": "tcp", "reset": True})
    assert rule.matches(RequestContext(service="tcp"))
    assert not rule.matches(RequestContext(service="redis"))
    star = from_dict({"service": "*", "reset": True})
    assert star.matches(RequestContext(service="tcp"))
