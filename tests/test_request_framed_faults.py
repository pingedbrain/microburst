"""Message-boundary request faults on framed upload bodies.

``cut_upload.after_messages`` counts framed *messages* instead of bytes
and resets the client connection on a message boundary;
``corrupt_upload.at_message`` passes N-1 messages verbatim then poisons
message N's checksum/length so the *upstream's* parser rejects it (the
client stays connected for the upstream's error).

Covered framed types: AWS event stream
(``application/vnd.amazon.eventstream``) and gRPC
(``application/grpc*``). Anything else is a no-op with a fired-event
note — never an error for the request.
"""

from __future__ import annotations

import http.client
import urllib.error
import urllib.request
import zlib

import pytest

from microburst import eventstream, framing

S3_AUTH = (
    "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, "
    "SignedHeaders=host;x-amz-date, Signature=deadbeef"
)
DDB_AUTH = (
    "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/dynamodb/"
    "aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef"
)
ES_CT = "application/vnd.amazon.eventstream"
GRPC_CT = "application/grpc"


def _es_frame(i: int, payload_size: int = 256) -> bytes:
    """A real eventstream message — prelude + payload + both CRCs."""
    return eventstream.build_message(
        {":message-type": "event", ":event-type": f"ev{i}"},
        (f"payload-{i}-".encode() * (payload_size // 12 + 1))[:payload_size],
    )


def _es_body(n: int, payload_size: int = 256) -> bytes:
    return b"".join(_es_frame(i, payload_size) for i in range(n))


def _grpc_msg(i: int, payload_size: int = 512) -> bytes:
    payload = (f"msg-{i}-".encode() * (payload_size // 6 + 1))[:payload_size]
    return b"\x00" + len(payload).to_bytes(4, "big") + payload


def _grpc_body(n: int) -> bytes:
    return b"".join(_grpc_msg(i) for i in range(n))


def _put(url: str, body: bytes, content_type: str) -> bytes:
    req = urllib.request.Request(
        url,
        data=body,
        method="PUT",
        headers={"Authorization": S3_AUTH, "Content-Type": content_type},
    )
    return urllib.request.urlopen(req, timeout=30).read()


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"Authorization": S3_AUTH})
    return urllib.request.urlopen(req, timeout=15).read()


def _ddb_post(url: str, body: bytes, content_type: str) -> bytes:
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": DDB_AUTH,
            "X-Amz-Target": "DynamoDB_20120810.PutItem",
            "Content-Type": content_type,
        },
    )
    return urllib.request.urlopen(req, timeout=30).read()


def _conn_fail(call, *args):
    """Run the request; pass iff the connection died with no HTTP response."""
    try:
        call(*args)
    except urllib.error.HTTPError as e:
        pytest.fail(f"got an HTTP response instead of a dead socket: {e}")
    except (urllib.error.URLError, http.client.HTTPException, OSError):
        return
    pytest.fail("request completed — expected a connection failure")


def _corrupt_es(frame: bytes) -> bytes:
    """Expected corruption: a flip inside the trailing message CRC."""
    return frame[:-1] + bytes([frame[-1] ^ 0xFF])


def _corrupt_grpc(frame: bytes) -> bytes:
    """Expected corruption: a nonsense-huge length prefix."""
    return frame[:1] + b"\xff\xff\xff\xff" + frame[5:]


# -- cut_upload.after_messages ------------------------------------------


def test_cut_after_messages_streaming_eventstream(
    upstub, microburst_server, aws_env
):
    """Streaming upload (PutObject): the client connection resets once
    the Nth complete eventstream message has been consumed — the SDK
    sees the link die between frames; nothing is forwarded."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"cut_upload": {"after_messages": 3}}}],
    )
    body = _es_body(5, payload_size=16 * 1024)  # frames span read chunks
    _conn_fail(_put, proxy.url + "/bucket/key", body, ES_CT)
    assert stub.count() == 0
    event = sq.fired[-1]
    assert "cut_upload:msg3" in event.action
    assert "reset after message 3" in event.note
    assert "frames_seen=3" in event.note


def test_cut_after_messages_buffered_body(upstub, microburst_server, aws_env):
    """Pre-buffered body (dynamodb PutItem carries a framed payload):
    the write already finished so the reset lands on the read path —
    still a dead socket, still nothing upstream."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "dynamodb",
                "request": {"cut_upload": {"after_messages": 2}}}],
    )
    _conn_fail(_ddb_post, proxy.url + "/", _es_body(5), ES_CT)
    assert stub.count() == 0
    assert "reset after message 2" in sq.fired[-1].note


def test_cut_after_messages_grpc(upstub, microburst_server, aws_env):
    """gRPC framing (1B compressed flag + 4B length + payload): same
    message-boundary cut semantics as eventstream."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"cut_upload": {"after_messages": 2}}}],
    )
    _conn_fail(_put, proxy.url + "/bucket/key", _grpc_body(4), GRPC_CT)
    assert stub.count() == 0
    assert "reset after message 2" in sq.fired[-1].note


def test_cut_after_messages_stream_ends_early(
    upstub, microburst_server, aws_env
):
    """Fewer than N messages before EOF → the upload completes and the
    request forwards normally (same as a short body under after_bytes)."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"cut_upload": {"after_messages": 10}}}],
    )
    body = _es_body(3)
    _put(proxy.url + "/bucket/key", body, ES_CT)
    assert stub.requests[0]["body"] == body
    assert "stream ended at 3 messages" in sq.fired[-1].note


def test_cut_after_messages_non_framed_ct_is_noop(
    upstub, microburst_server, aws_env
):
    """after_messages on a non-framed Content-Type: the cut no-ops, the
    request forwards, and the fired event explains why."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"cut_upload": {"after_messages": 1}}}],
    )
    payload = b"z" * 4096
    _put(proxy.url + "/bucket/key", payload, "application/octet-stream")
    assert stub.requests[0]["body"] == payload
    event = sq.fired[-1]
    assert "cut_upload:msg1" in event.action  # rule still evaluated
    assert "content-type not a framed stream" in event.note


def test_cut_after_messages_malformed_falls_back_to_bytes(
    upstub, microburst_server, aws_env
):
    """A garbage prelude stops message parsing; byte thresholds still
    apply — here the byte count crosses and the connection dies."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"cut_upload": {"after_messages": 5,
                                          "after_bytes": 1024}}}],
    )
    body = _es_body(2) + b"\xde\xad\xbe\xef" * 1024  # garbage after 2 frames
    _conn_fail(_put, proxy.url + "/bucket/key", body, ES_CT)
    assert stub.count() == 0
    assert "malformed frame" in sq.fired[-1].note


def test_cut_after_messages_malformed_no_byte_threshold_forwards(
    upstub, microburst_server, aws_env
):
    """Malformed framing with no byte fallback configured → forward
    verbatim; the fired event explains the degraded outcome."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"cut_upload": {"after_messages": 5}}}],
    )
    body = _es_body(1) + b"\xff" * 512  # bad prelude CRC on message 2
    _put(proxy.url + "/bucket/key", body, ES_CT)
    assert stub.requests[0]["body"] == body
    assert "malformed frame" in sq.fired[-1].note


def test_slow_upload_and_after_messages_compose(
    upstub, microburst_server, aws_env
):
    """slow_upload + after_messages: a paced read that cuts on a
    message boundary — pacing applies to bytes regardless of framing."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"slow_upload": {"rate_kbps": 512},
                            "cut_upload": {"after_messages": 2}}}],
    )
    _conn_fail(_put, proxy.url + "/bucket/key", _es_body(4), ES_CT)
    assert stub.count() == 0
    assert "slow_upload" in sq.fired[-1].action
    assert "cut_upload:msg2" in sq.fired[-1].action


def test_after_messages_no_body_is_noop_but_logged(
    upstub, microburst_server, aws_env
):
    """GET (no body): the framed fault can't apply, the match is still
    visible, and the note records the non-framed Content-Type."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"cut_upload": {"after_messages": 2}}}],
    )
    _get(proxy.url + "/bucket/key")
    assert stub.count() == 1
    event = sq.fired[-1]
    assert "cut_upload:msg2" in event.action
    assert "content-type not a framed stream" in event.note


# -- corrupt_upload.at_message -------------------------------------------


def test_corrupt_upload_eventstream_buffered(
    upstub, microburst_server, aws_env
):
    """Buffered framed body: N-1 messages pass verbatim, message N's
    trailing CRC is poisoned, the upstream sees the malformed frame and
    the client stays connected for its response."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "dynamodb",
                "request": {"corrupt_upload": {"at_message": 2}}}],
    )
    frames = [_es_frame(i) for i in range(3)]
    resp = _ddb_post(proxy.url + "/", b"".join(frames), ES_CT)
    assert resp  # upstream's 200 flows back — the link stayed up
    assert stub.requests[0]["body"] == (
        frames[0] + _corrupt_es(frames[1]) + frames[2]
    )
    assert "corrupted message 2" in sq.fired[-1].note


def test_corrupt_upload_eventstream_streaming(
    upstub, microburst_server, aws_env
):
    """Streaming upload: the transform runs on the upstream send path —
    same corrupted bytes, no client reset."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"corrupt_upload": {"at_message": 3}}}],
    )
    frames = [_es_frame(i, payload_size=8 * 1024) for i in range(4)]
    _put(proxy.url + "/bucket/key", b"".join(frames), ES_CT)
    assert stub.requests[0]["body"] == (
        frames[0] + frames[1] + _corrupt_es(frames[2]) + frames[3]
    )
    assert "corrupted message 3" in sq.fired[-1].note


def test_corrupt_upload_grpc(upstub, microburst_server, aws_env):
    """gRPC framing: the message's length prefix is rewritten to a
    nonsense-huge value — the upstream's parser chokes on it."""
    stub, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"corrupt_upload": {"at_message": 2}}}],
    )
    msgs = [_grpc_msg(i) for i in range(3)]
    _put(proxy.url + "/bucket/key", b"".join(msgs), "application/grpc+proto")
    assert stub.requests[0]["body"] == (
        msgs[0] + _corrupt_grpc(msgs[1]) + msgs[2]
    )


def test_corrupt_upload_short_stream_forwards_verbatim(
    upstub, microburst_server, aws_env
):
    """Fewer than N messages → no-op pass-through with an explanatory
    note; the client gets a normal upstream response."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"corrupt_upload": {"at_message": 9}}}],
    )
    body = _es_body(2)
    _put(proxy.url + "/bucket/key", body, ES_CT)
    assert stub.requests[0]["body"] == body
    assert "only 2 messages" in sq.fired[-1].note


def test_corrupt_upload_malformed_forwards_verbatim(
    upstub, microburst_server, aws_env
):
    """Malformed framing before the target message → the original bytes
    go upstream untouched (a parser bug must never corrupt the request
    it failed to understand)."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"corrupt_upload": {"at_message": 2}}}],
    )
    frames = [_es_frame(0)]
    body = frames[0] + b"\x00" * 4 + b"\xee" * 256  # bogus prelude on msg 2
    _put(proxy.url + "/bucket/key", body, ES_CT)
    assert stub.requests[0]["body"] == body
    assert "malformed frame at message 2" in sq.fired[-1].note


def test_corrupt_upload_after_unreached_cut(
    upstub, microburst_server, aws_env
):
    """cut_upload.after_messages past the stream end + corrupt_upload:
    the cut no-ops at EOF and the corrupt transform still applies to
    the forwarded payload."""
    stub, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"cut_upload": {"after_messages": 50},
                            "corrupt_upload": {"at_message": 2}}}],
    )
    frames = [_es_frame(i) for i in range(3)]
    _put(proxy.url + "/bucket/key", b"".join(frames), ES_CT)
    assert stub.requests[0]["body"] == (
        frames[0] + _corrupt_es(frames[1]) + frames[2]
    )


def test_corrupt_upload_behind_terminal_error_never_sends(
    upstub, microburst_server, aws_env
):
    """corrupt_upload is a send-path transform — when a terminal effect
    (error envelope) wins, nothing reaches upstream at all."""
    stub, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "error": {"code": "SlowDown", "status": 503},
                "request": {"corrupt_upload": {"at_message": 1}}}],
    )
    try:
        _put(proxy.url + "/bucket/key", _es_body(3), ES_CT)
        pytest.fail("expected the injected 503")
    except urllib.error.HTTPError as e:
        assert e.code == 503
    assert stub.count() == 0


# -- framing parser unit checks ------------------------------------------


def test_framed_stream_eventstream_incremental():
    """Frame boundaries are found regardless of how bytes split across
    feed() calls."""
    body = _es_body(3)
    scan = framing.FramedStream("eventstream")
    frames = []
    for i in range(0, len(body), 7):  # adversarial 7-byte dribble
        frames.extend(scan.feed(body[i:i + 7]))
    assert not scan.failed
    assert scan.frames == 3
    assert b"".join(frames) + scan.drain() == body


def test_framed_stream_eventstream_bad_prelude_crc():
    """A bogus prelude CRC marks the stream malformed and freezes the
    unparsed tail for verbatim replay."""
    good = _es_frame(0)
    bogus_crc = bytearray(_es_frame(1))
    bogus_crc[8] ^= 0xFF
    scan = framing.FramedStream("eventstream")
    frames = scan.feed(good + bytes(bogus_crc))
    assert frames == [good]
    assert scan.failed
    assert scan.drain() == bytes(bogus_crc)


def test_framed_stream_eventstream_implausible_lengths():
    """total_length smaller than the minimum frame (or headers_len
    overflowing the frame) is malformed, not a hang."""
    body = (4).to_bytes(4, "big") + b"\x00" * 8 + b"junk"
    scan = framing.FramedStream("eventstream")
    assert scan.feed(body) == []
    assert scan.failed


def test_framed_stream_grpc_boundaries():
    body = _grpc_body(3)
    scan = framing.FramedStream("grpc")
    frames = scan.feed(body[: len(body) - 3])  # truncated tail
    assert scan.frames == 2
    assert not scan.failed
    frames.extend(scan.feed(body[len(body) - 3 :]))
    assert scan.frames == 3
    assert b"".join(frames) == body


def test_framed_stream_grpc_bad_flag_and_huge_length():
    """Compressed-flag values >1 and absurd lengths are malformed."""
    scan = framing.FramedStream("grpc")
    assert scan.feed(b"\x07" + b"\x00" * 8) == []
    assert scan.failed
    scan = framing.FramedStream("grpc")
    assert scan.feed(b"\x00\xff\xff\xff\xff" + b"x" * 64) == []
    assert scan.failed


def test_framing_for_content_types():
    assert framing.framing_for(ES_CT) == "eventstream"
    assert framing.framing_for(ES_CT + "; charset=utf-8") == "eventstream"
    assert framing.framing_for("application/grpc") == "grpc"
    assert framing.framing_for("application/grpc+proto") == "grpc"
    assert framing.framing_for("application/grpc-web") == "grpc"
    assert framing.framing_for("application/octet-stream") is None
    assert framing.framing_for(None) is None


def test_request_framed_rule_roundtrip():
    """The new request: keys survive to_dict — rule CRUD and file
    reload keep them."""
    from microburst.rules import RuleEngine, to_dict

    engine = RuleEngine()
    spec = {
        "service": "s3",
        "request": {
            "cut_upload": {"after_messages": 4},
            "corrupt_upload": {"at_message": 2},
        },
    }
    engine.set_rules([spec])
    assert to_dict(engine.rules[0])["request"] == spec["request"]


def test_eventstream_test_frames_have_valid_crcs():
    """Guard the harness itself: frames built for these tests carry
    real prelude and message CRCs (zlib.crc32 — prelude covers the
    first 8 bytes, message CRC covers prelude+headers+payload)."""
    frame = _es_frame(0)
    assert zlib.crc32(frame[:8]) & 0xFFFFFFFF == int.from_bytes(
        frame[8:12], "big"
    )
    assert zlib.crc32(frame[:-4]) & 0xFFFFFFFF == int.from_bytes(
        frame[-4:], "big"
    )
