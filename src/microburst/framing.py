"""Message-boundary parsing for framed upload bodies.

Request-side faults (``request.cut_upload.after_messages``,
``request.corrupt_upload.at_message``) count *messages*, not bytes, when
the request Content-Type is a known framed wire format:

- AWS event stream (``application/vnd.amazon.eventstream``): prelude
  ``total_len u32 | headers_len u32 | prelude_crc32 u32`` + headers +
  payload + trailing message CRC32. The prelude CRC covers the first 8
  bytes; the message CRC covers prelude+headers+payload.
- gRPC (``application/grpc``, ``application/grpc+proto``, grpc-web
  variants): ``compressed_flag u8 | msg_len u32be`` + payload.

Parsing is DEFENSIVE by contract: a prelude/prefix that fails sanity
checks marks the stream malformed (``FramedStream.failed``) and parsing
stops — callers fall back to byte semantics or pass bytes through
verbatim. Frame lengths are attacker-controlled input; a parser bug must
never take down the proxy.
"""

from __future__ import annotations

import struct
import zlib
from collections.abc import AsyncIterable, AsyncIterator, Callable

EVENTSTREAM_CT = "application/vnd.amazon.eventstream"

# Sanity cap on a single frame's declared length. Real eventstream/gRPC
# messages are far smaller; a prelude claiming more is treated as
# malformed rather than trusted (a bogus length would otherwise pin an
# unbounded reassembly buffer on a still-streaming upload).
_MAX_FRAME = 32 * 1024 * 1024

_EVENTSTREAM_PRELUDE = struct.Struct("!II")  # total_length, headers_length
_LEN_U32 = struct.Struct("!I")


def framing_for(content_type: str | None) -> str | None:
    """Map a request Content-Type to a frame parser kind, or None when
    the body isn't a known framed stream."""
    if not content_type:
        return None
    ct = content_type.split(";", 1)[0].strip().lower()
    if ct == EVENTSTREAM_CT:
        return "eventstream"
    if ct.startswith("application/grpc"):
        return "grpc"
    return None


def _eventstream_len(buf: bytes | bytearray) -> int | None:
    """Length of the first complete eventstream frame in ``buf`` —
    None = need more data, -1 = malformed prelude."""
    if len(buf) < 12:  # 8B lengths + 4B prelude CRC
        return None
    total, headers_len = _EVENTSTREAM_PRELUDE.unpack_from(buf)
    # Minimum frame: 12B prelude + 4B message CRC. headers_len must fit
    # inside the frame between prelude and CRC.
    if total < 16 or headers_len > total - 16 or total > _MAX_FRAME:
        return -1
    prelude_crc = _LEN_U32.unpack_from(buf, 8)[0]
    if zlib.crc32(bytes(buf[:8])) & 0xFFFFFFFF != prelude_crc:
        return -1
    if len(buf) < total:
        return None
    return total


def _grpc_len(buf: bytes | bytearray) -> int | None:
    """Length of the first complete gRPC message in ``buf`` —
    None = need more data, -1 = malformed prefix."""
    if len(buf) < 5:
        return None
    if buf[0] > 1:  # compressed flag is 0 or 1
        return -1
    msg_len = _LEN_U32.unpack_from(buf, 1)[0]
    if msg_len > _MAX_FRAME:
        return -1
    if len(buf) < 5 + msg_len:
        return None
    return 5 + msg_len


class FramedStream:
    """Incremental frame splitter for one upload body.

    ``feed()`` consumes bytes and returns newly completed frames.
    ``failed`` latches on the first malformed prelude/prefix — parsing
    stops and the unparsed remainder stays in ``buf`` so a caller that
    gives up on framing can still emit the original bytes verbatim
    (``drain()``).
    """

    def __init__(self, kind: str):
        if kind not in ("eventstream", "grpc"):
            raise ValueError(f"unknown framing {kind!r}")
        self.kind = kind
        self.buf = bytearray()
        self.failed = False
        self.frames = 0

    def _frame_len(self) -> int | None:
        if self.kind == "eventstream":
            return _eventstream_len(self.buf)
        return _grpc_len(self.buf)

    def feed(self, data: bytes) -> list[bytes]:
        """Consume ``data``; return complete frames extracted this call.
        Returns ``[]`` once failed — ``buf`` stays frozen at the
        malformed point so ``drain()`` replays it byte-for-byte."""
        if self.failed:
            return []
        self.buf += data
        out: list[bytes] = []
        while True:
            flen = self._frame_len()
            if flen is None:
                break
            if flen < 0:
                self.failed = True
                break
            out.append(bytes(self.buf[:flen]))
            del self.buf[:flen]
            self.frames += 1
        return out

    def drain(self) -> bytes:
        """Return and clear the unparsed remainder (a partial trailing
        frame, or everything from the malformed point onward)."""
        out = bytes(self.buf)
        self.buf.clear()
        return out


def corrupt_frame(frame: bytes, kind: str) -> bytes:
    """Poison one complete frame so the UPSTREAM's parser rejects it.

    - eventstream: flip a byte inside the trailing message-CRC field —
      the frame is structurally intact but its checksum fails.
    - gRPC: rewrite the 4-byte length prefix to a nonsense huge value —
      the upstream tries to read a ~4 GiB payload that isn't there.
    """
    if kind == "eventstream":
        return frame[:-1] + bytes([frame[-1] ^ 0xFF])
    return frame[:1] + b"\xff\xff\xff\xff" + frame[5:]


def corrupt_body(body: bytes, kind: str, at: int) -> tuple[bytes, str | None]:
    """Rewrite a buffered ``body`` with message ``at`` (1-based)
    corrupted. Returns ``(payload, note)``; on a no-op the payload is
    the original bytes object and the note explains why."""
    scan = FramedStream(kind)
    frames = scan.feed(body)
    if scan.frames < at:
        if scan.failed:
            return body, (
                f"corrupt_upload: malformed frame at message "
                f"{scan.frames + 1} — forwarded verbatim"
            )
        return body, (
            f"corrupt_upload: only {scan.frames} messages in stream "
            f"— forwarded verbatim"
        )
    frames[at - 1] = corrupt_frame(frames[at - 1], kind)
    note = (
        f"corrupt_upload: corrupted message {at} "
        f"({kind}, frames_seen={scan.frames})"
    )
    if scan.failed:
        note += "; tail after malformed frame forwarded verbatim"
    return b"".join(frames) + bytes(scan.buf), note


async def corrupt_stream(
    chunks: AsyncIterable[bytes],
    kind: str,
    at: int,
    on_note: Callable[[str], None],
) -> AsyncIterator[bytes]:
    """Transform a streaming upload on the proxy→upstream send path:
    pass ``at - 1`` messages verbatim, corrupt message ``at``, then pass
    the rest through untouched. On malformed framing or fewer than
    ``at`` messages the stream forwards verbatim with an explanatory
    note. The client connection stays alive — the fault lands on the
    upstream's parser, and its error response flows back normally."""
    scan = FramedStream(kind)
    seen = 0
    done = False        # corruption delivered (or impossible)
    passthrough = False  # framing failed — raw bytes from here on
    async for chunk in chunks:
        if passthrough:
            yield chunk
            continue
        for frame in scan.feed(chunk):
            seen += 1
            if seen == at and not done:
                yield corrupt_frame(frame, kind)
                on_note(f"corrupt_upload: corrupted message {at} ({kind})")
                done = True
            else:
                yield frame
        if scan.failed:
            # Emit the malformed tail before switching to raw passthrough
            # so upstream still sees the client's exact byte sequence.
            tail = scan.drain()
            if tail:
                yield tail
            passthrough = True
            if not done:
                on_note(
                    f"corrupt_upload: malformed frame at message {seen + 1}"
                    f" — forwarded verbatim"
                )
                done = True
    if not passthrough and scan.buf:
        yield scan.drain()  # incomplete trailing frame — verbatim
    if not done:
        on_note(
            f"corrupt_upload: only {seen} messages in stream"
            f" — forwarded verbatim"
        )
