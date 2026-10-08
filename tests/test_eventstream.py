"""Event-stream mid-stream fault injection (application/vnd.amazon.eventstream)."""

from __future__ import annotations

import asyncio
import urllib.request

import pytest
from aiohttp import web
from botocore.eventstream import ChecksumMismatch, EventStreamBuffer
from conftest import ServerThread

from microburst.eventstream import (
    Mutation,
    build_error_frame,
    build_message,
    frame_length,
    mutate_frames,
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


def _events(buf: bytes):
    """(headers, payload) pairs from a buffer."""
    esb = EventStreamBuffer()
    esb.add_data(buf)
    return [(dict(e.headers), e.payload) for e in esb]


def _frame_stream(n: int) -> bytes:
    return b"".join(
        build_message({":message-type": "event", ":event-type": "rec"},
                      f"record-{i}".encode())
        for i in range(n)
    )


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


def test_mutate_drop_frame():
    out = asyncio.run(_collect(mutate_frames(
        _aiter([_frame_stream(4)]), (Mutation(at=1, drop=True),)
    )))
    events = _events(out)
    assert [p for _h, p in events] == [b"record-0", b"record-2", b"record-3"]


def test_mutate_replace_payload_keeps_headers():
    out = asyncio.run(_collect(mutate_frames(
        _aiter([_frame_stream(3)]),
        (Mutation(at=1, payload=b'{"rewritten": true}'),),
    )))
    events = _events(out)
    assert events[1][0][":event-type"] == "rec"      # headers preserved
    assert events[1][1] == b'{"rewritten": true}'  # payload replaced


def test_mutate_corrupt_payload_parses_as_garbage():
    out = asyncio.run(_collect(mutate_frames(
        _aiter([_frame_stream(2)]), (Mutation(at=0, corrupt_payload=True),)
    )))
    events = _events(out)
    # CRCs recomputed → botocore parses it; the payload is garbage
    assert events[0][1] != b"record-0"
    assert len(events[0][1]) == len(b"record-0")


def test_mutate_bad_crc_fails_checksum():
    out = asyncio.run(_collect(mutate_frames(
        _aiter([_frame_stream(2)]), (Mutation(at=0, bad_crc=True),)
    )))
    esb = EventStreamBuffer()
    with pytest.raises(ChecksumMismatch):
        esb.add_data(out)
        list(esb)


def test_mutate_inject_frame_before_index():
    stats = build_message(
        {":message-type": "event", ":event-type": "Stats"},
        b'{"BytesScanned": 42}',
    )
    out = asyncio.run(_collect(mutate_frames(
        _aiter([_frame_stream(2)]), (Mutation(at=1, inject=stats),)
    )))
    events = _events(out)
    assert [h.get(":event-type") for h, _p in events] == [
        "rec", "Stats", "rec",
    ]
    assert events[1][1] == b'{"BytesScanned": 42}'


def test_mutate_cut_ends_stream_mid_frame():
    out = asyncio.run(_collect(mutate_frames(
        _aiter([_frame_stream(4)]), (Mutation(at=2, cut=0.5),)
    )))
    # frames 0,1 intact + half of frame 2 — botocore only parses the two
    events = _events(out)
    assert [p for _h, p in events] == [b"record-0", b"record-1"]
    assert len(out) > len(_frame_stream(2))  # partial frame bytes present


def test_mutate_error_is_terminal():
    err = build_error_frame("ThrottlingException", "mid")
    out = asyncio.run(_collect(mutate_frames(
        _aiter([_frame_stream(5)]), (Mutation(at=2, error=err),)
    )))
    events = _events(out)
    assert len(events) == 3
    assert events[2][0][":message-type"] == "error"


def test_mutate_late_inject_still_fires():
    # index beyond the stream length — injects/errors aren't silently lost
    stats = build_message({":message-type": "event", ":event-type": "Stats"})
    out = asyncio.run(_collect(mutate_frames(
        _aiter([_frame_stream(2)]), (Mutation(at=99, inject=stats),)
    )))
    events = _events(out)
    assert [h.get(":event-type") for h, _p in events] == ["rec", "rec", "Stats"]


def test_event_frames_rule_e2e(microburst_server, aws_env):
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
                    "event_frames": [
                        {"at": 1, "drop": True},
                        {"at": 3, "inject": {
                            "event_type": "Stats",
                            "payload": {"BytesScanned": 7},
                        }},
                        {"at": 4, "payload": '{"rewritten": true}'},
                    ],
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

    events = _events(data)
    # upstream rec0..5, drop rec@1, Stats before @3, payload @4 rewritten
    assert [h.get(":event-type") for h, _p in events] == [
        "rec", "rec", "Stats", "rec", "rec", "rec",
    ]
    assert events[2][1] == b'{"BytesScanned":7}'
    assert events[4][1] == b'{"rewritten": true}'


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
