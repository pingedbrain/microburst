"""Event-stream mid-stream fault injection (application/vnd.amazon.eventstream)."""

from __future__ import annotations

import asyncio
import urllib.request

from aiohttp import web
from botocore.eventstream import EventStreamBuffer
from conftest import ServerThread

from microburst.eventstream import (
    build_error_frame,
    build_message,
    frame_length,
    splice_after_frames,
)


def _frames(buf: bytes) -> list[dict]:
    """Parse a byte buffer into event-stream messages via botocore's
    real decoder — validates our CRCs."""
    esb = EventStreamBuffer()
    esb.add_data(buf)
    out = []
    for event in esb:
        out.append(dict(event.headers))
    return out


def test_error_frame_roundtrips_botocore():
    frame = build_error_frame("KmsThrottlingException", "slow down")
    events = _frames(frame)
    assert len(events) == 1
    assert events[0][":message-type"] == "error"
    assert events[0][":error-code"] == "KmsThrottlingException"
    assert events[0][":error-message"] == "slow down"


def test_frame_length_partial():
    msg = build_message({":message-type": "event", ":event-type": "x"}, b"pay")
    assert frame_length(msg) == len(msg)
    assert frame_length(msg[:6]) is None           # prelude incomplete
    assert frame_length(msg[: len(msg) - 1]) is None  # frame incomplete


def test_splice_injects_terminal_error_frame():
    frames = [
        build_message({":message-type": "event", ":event-type": "shard"},
                      f"data-{i}".encode())
        for i in range(5)
    ]
    payload = b"".join(frames)
    # awkward chunking: split mid-frame
    chunks = _aiter([payload[:13], payload[13:40], payload[40:]])
    err = build_error_frame("ThrottlingException", "throttled mid-stream")
    out = asyncio.run(_collect(splice_after_frames(chunks, 2, err)))

    events = _frames(out)
    # 2 real events then the error frame — the stream stops there
    assert len(events) == 3
    assert events[0][":event-type"] == "shard"
    assert events[1][":event-type"] == "shard"
    assert events[2][":message-type"] == "error"
    assert events[2][":error-code"] == "ThrottlingException"


def test_splice_before_any_frame():
    chunks = _aiter([build_message({":message-type": "event"}, b"x")])
    err = build_error_frame("InternalError", "boom")
    out = asyncio.run(_collect(splice_after_frames(chunks, 0, err)))
    events = _frames(out)
    assert len(events) == 1
    assert events[0][":message-type"] == "error"


async def _collect(it) -> bytes:
    return b"".join([c async for c in it])


async def _aiter(items):
    for i in items:
        yield i


class _StreamStub:
    """Upstream serving a canned event-stream."""

    async def handle(self, request: web.Request) -> web.StreamResponse:
        body = b"".join(
            build_message({":message-type": "event", ":event-type": "rec"},
                          f"record-{i}".encode())
            for i in range(6)
        )
        resp = web.StreamResponse(
            status=200,
            headers={"Content-Type": "application/vnd.amazon.eventstream"},
        )
        await resp.prepare(request)
        for i in range(0, len(body), 17):  # mid-frame chunk splits
            await resp.write(body[i : i + 17])
        await resp.write_eof()
        return resp


def test_event_error_rule_e2e(microburst_server, aws_env):
    stub_app = web.Application()
    stub = _StreamStub()
    stub_app.router.add_route("*", "/{tail:.*}", stub.handle)
    upstream = ServerThread(stub_app).start()
    try:
        _, proxy = microburst_server(
            upstream.url,
            rules=[{
                "service": "kinesis",
                "operation": "SubscribeToShard",
                "response": {
                    "event_error": {
                        "code": "KmsThrottlingException",
                        "message": "spliced",
                        "after_frames": 3,
                    },
                },
            }],
        )
        req = urllib.request.Request(
            f"{proxy.url}/",
            data=b"{}",
            method="POST",
            headers={
                "Content-Type": "application/x-amz-json-1.1",
                "X-Amz-Target": "Kinesis_20131202.SubscribeToShard",
            },
        )
        with urllib.request.urlopen(req) as resp:
            data = resp.read()
    finally:
        upstream.stop()

    events = _frames(data)
    assert [e.get(":event-type") for e in events[:3]] == ["rec", "rec", "rec"]
    assert events[3][":message-type"] == "error"
    assert events[3][":error-code"] == "KmsThrottlingException"
