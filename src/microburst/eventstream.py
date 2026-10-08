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
from collections.abc import AsyncIterator

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


def frame_length(buf: bytes | bytearray) -> int | None:
    """Total length of the first frame in ``buf`` — None if the prelude
    hasn't fully arrived or the frame is incomplete."""
    if len(buf) < _PRELUDE.size:
        return None
    total, _headers_len = _PRELUDE.unpack_from(buf)
    if len(buf) < total:
        return None
    return total


async def splice_after_frames(
    chunks: AsyncIterator[bytes],
    after_frames: int,
    frame: bytes,
) -> AsyncIterator[bytes]:
    """Yield the upstream stream; after ``after_frames`` complete frames
    splice in ``frame`` and stop — AWS treats stream errors as terminal."""
    if after_frames <= 0:
        yield frame
        return
    buf = bytearray()
    seen = 0
    async for chunk in chunks:
        buf.extend(chunk)
        while (flen := frame_length(buf)) is not None:
            yield bytes(buf[:flen])
            del buf[:flen]
            seen += 1
            if seen >= after_frames:
                yield frame
                return
    if buf:
        yield bytes(buf)
    # stream ended before N frames — append the fault anyway so the rule
    # isn't a silent no-op
    yield frame
