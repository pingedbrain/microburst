"""AWS event-stream framing (``application/vnd.amazon.eventstream``).

The binary format AWS uses for event-stream APIs (Kinesis
SubscribeToShard, S3 SelectObjectContent, ...): each message is

    prelude   = total_length u32 | headers_length u32 | prelude_crc32 u32
    headers   = headers_length bytes of typed name/value pairs
    payload   = total_length - 16 - headers_length - 4 bytes
    crc       = crc32 of everything before it

Mid-stream errors travel as a message with ``:message-type: error`` and
``:error-code``/``:error-message`` string headers — SDKs surface them as
exceptions at the point the frame lands in the stream. We only need
frame *boundaries* (the prelude lengths) to splice one in.
"""

from __future__ import annotations

import struct
import zlib
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass

CONTENT_TYPE = "application/vnd.amazon.eventstream"

_PRELUDE = struct.Struct("!II")   # total_length, headers_length
_LEN_U32 = struct.Struct("!I")
_LEN_U8 = struct.Struct("!B")
_LEN_U16 = struct.Struct("!H")
_TYPE_STRING = 7


def _header(name: str, value: str) -> bytes:
    n, v = name.encode(), value.encode()
    return (
        _LEN_U8.pack(len(n)) + n
        + _LEN_U8.pack(_TYPE_STRING) + _LEN_U16.pack(len(v)) + v
    )


def build_message(headers: dict[str, str], payload: bytes = b"") -> bytes:
    """A complete event-stream message with valid CRCs."""
    block = b"".join(_header(k, v) for k, v in headers.items())
    total = 8 + 4 + len(block) + len(payload) + 4
    prelude = _PRELUDE.pack(total, len(block))
    prelude += _LEN_U32.pack(zlib.crc32(prelude) & 0xFFFFFFFF)
    message = prelude + block + payload
    return message + _LEN_U32.pack(zlib.crc32(message) & 0xFFFFFFFF)


def build_error_frame(code: str, message: str) -> bytes:
    """The mid-stream error frame AWS emits — terminal for the stream."""
    return build_message({
        ":message-type": "error",
        ":error-code": code,
        ":error-message": message,
    })


def _repayload(frame: bytes, payload: bytes) -> bytes:
    """Rebuild a frame keeping its header block verbatim, with a new
    payload and recomputed CRCs — the SDK parses our bytes as real."""
    _total, hlen = _PRELUDE.unpack_from(frame)
    block = frame[_PRELUDE.size + 4 : _PRELUDE.size + 4 + hlen]
    total = _PRELUDE.size + 4 + len(block) + len(payload) + 4
    prelude = _PRELUDE.pack(total, len(block))
    prelude += _LEN_U32.pack(zlib.crc32(prelude) & 0xFFFFFFFF)
    msg = prelude + block + payload
    return msg + _LEN_U32.pack(zlib.crc32(msg) & 0xFFFFFFFF)


def _payload_of(frame: bytes) -> bytes:
    _total, hlen = _PRELUDE.unpack_from(frame)
    start = _PRELUDE.size + 4 + hlen
    return frame[start : len(frame) - 4]


def _corrupt_payload(frame: bytes) -> bytes:
    """Flip a byte in the payload, CRCs recomputed — the SDK parses
    structurally-valid garbage."""
    payload = _payload_of(frame)
    if not payload:
        return frame
    poisoned = bytes([payload[0] ^ 0xFF]) + payload[1:]
    return _repayload(frame, poisoned)


def _break_crc(frame: bytes) -> bytes:
    """Emit the frame with a broken message CRC — the SDK raises its
    checksum/checksum-mismatch error."""
    return frame[:-1] + bytes([frame[-1] ^ 0xFF])


@dataclass
class Mutation:
    """One mutation applied when the upstream stream reaches frame ``at``
    (0-based upstream frame index — indices count real frames, so drops
    and injects don't shift later indices).

    Actions, in the order they apply at one index: ``inject`` emits its
    frame first; ``error`` emits and ends the stream (AWS treats
    mid-stream errors as terminal); ``cut`` emits only that fraction of
    the frame's bytes then ends the stream; ``drop`` consumes the frame
    without emitting it; otherwise ``payload``/``corrupt_payload``/
    ``bad_crc`` transform the frame in place.
    """

    at: int
    inject: bytes | None = None
    error: bytes | None = None
    cut: float | None = None
    drop: bool = False
    payload: bytes | None = None
    corrupt_payload: bool = False
    bad_crc: bool = False


def frame_length(buf: bytes | bytearray) -> int | None:
    """Total length of the first frame in ``buf`` — None if the prelude
    hasn't fully arrived or the frame is incomplete."""
    if len(buf) < _PRELUDE.size:
        return None
    total, _headers_len = _PRELUDE.unpack_from(buf)
    if len(buf) < total:
        return None
    return total


async def mutate_frames(
    chunks: AsyncIterator[bytes],
    mutations: Sequence[Mutation],
) -> AsyncIterator[bytes]:
    """Yield the upstream stream with ``mutations`` applied at frame
    boundaries. Mid-stream ``error``/``cut`` end the stream; late
    ``inject``/``error`` entries beyond the stream length still fire
    before EOF so a rule isn't a silent no-op."""
    by_at: dict[int, list[Mutation]] = {}
    for m in mutations:
        by_at.setdefault(max(0, m.at), []).append(m)

    buf = bytearray()
    seen = 0
    async for chunk in chunks:
        buf.extend(chunk)
        while (flen := frame_length(buf)) is not None:
            frame = bytes(buf[:flen])
            del buf[:flen]
            ms = by_at.pop(seen, [])
            for m in ms:
                if m.inject is not None:
                    yield m.inject
                if m.error is not None:
                    yield m.error
                    return
                if m.cut is not None:
                    yield frame[: int(len(frame) * max(0.0, min(1.0, m.cut)))]
                    return
            if any(m.drop for m in ms):
                seen += 1
                continue
            for m in ms:
                if m.payload is not None:
                    frame = _repayload(frame, m.payload)
                if m.corrupt_payload:
                    frame = _corrupt_payload(frame)
                if m.bad_crc:
                    frame = _break_crc(frame)
            yield frame
            seen += 1
    if buf:
        yield bytes(buf)
    for at in sorted(by_at):
        for m in by_at[at]:
            if m.inject is not None:
                yield m.inject
            if m.error is not None:
                yield m.error


async def splice_after_frames(
    chunks: AsyncIterator[bytes],
    after_frames: int,
    frame: bytes,
) -> AsyncIterator[bytes]:
    """Yield the upstream stream; after ``after_frames`` complete frames
    splice in ``frame`` and stop — AWS treats stream errors as terminal."""
    async for c in mutate_frames(chunks, (Mutation(at=after_frames, error=frame),)):
        yield c
