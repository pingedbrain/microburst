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

import re
import struct
import zlib
from collections.abc import AsyncIterable, AsyncIterator, Callable
from dataclasses import dataclass

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


# --- generic user-declared framing (tcp wire mode) -----------------------------

@dataclass(frozen=True)
class FramerSpec:
    """Parsed ``--framing`` / ``framing:`` declaration for tcp mode.

    Three kinds, mirroring what common binary wire protocols actually do:

    - ``length-prefix``: ``offset`` bytes, then a ``size``-byte integer in
      ``endian`` order, then payload. The declared value ``L`` resolves to
      a frame length of ``offset + size + L + adjust``; ``includes_self``
      subtracts ``size`` (protocols like MongoDB whose length counts the
      length field itself). ``adjust`` covers headers with bytes *after*
      the length field — MySQL's 3-byte length + 1-byte sequence id is
      ``{size: 3, adjust: 1, endian: little}``.
    - ``delimiter``: a frame ends at (and includes) ``delimiter`` —
      line-oriented and terminator protocols.
    - ``fixed``: every ``size`` bytes is a frame.
    """

    kind: str                       # length-prefix | delimiter | fixed
    size: int = 4                   # prefix width / fixed frame size
    offset: int = 0                 # bytes skipped before the length field
    endian: str = "big"             # big | little
    includes_self: bool = False     # declared length counts the field itself
    adjust: int = 0                 # extra header bytes after the field
    delimiter: bytes = b""          # delimiter kind only


_HEX_RE = re.compile(r"^(?:[0-9a-fA-F]{2})+$")


def _delimiter_bytes(value) -> bytes:
    """``bytes`` param: hex (``0d0a``), backslash escapes (``\\r\\n``),
    or a literal latin-1 string. Also accepts raw bytes.

    Hex needs at least one digit — otherwise ``"ab"`` would decode as
    ``\\xab`` when the user almost certainly meant the literal ``ab``.
    Pure-alpha literals use ``\\xNN`` escapes for binary bytes."""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if not isinstance(value, str):
        raise TypeError(
            f"framing delimiter must be a string/bytes, got {value!r}"
        )
    if _HEX_RE.match(value) and any(c.isdigit() for c in value):
        return bytes.fromhex(value)
    raw = value.encode("utf-8")
    if "\\" in value:
        # backslash escapes — \r \n \t \xNN \\ — decode like a literal
        raw = raw.decode("unicode_escape").encode("latin-1")
    return raw


def _bool(value, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() in ("true", "yes", "1", "on"):
            return True
        if value.lower() in ("false", "no", "0", "off"):
            return False
    raise ValueError(f"framing {name} must be a boolean, got {value!r}")


def parse_framing(value) -> FramerSpec | None:
    """Normalize a framing declaration to a ``FramerSpec``.

    Accepts ``None`` (no framing — the stream is unsegmented), an already
    parsed ``FramerSpec``, a config-file mapping
    (``{kind: length-prefix, size: 4, ...}``), or the CLI string form
    ``"kind:k=v,k=v"`` (e.g. ``"length-prefix:size=3,adjust=1,endian=little"``,
    ``"delimiter:bytes=0d0a"``, ``"fixed:size=64"``).
    """
    if value is None or isinstance(value, FramerSpec):
        return value
    if isinstance(value, str):
        kind, sep, rest = value.partition(":")
        params: dict = {"kind": kind.strip()}
        if sep:
            for pair in rest.split(","):
                pair = pair.strip()
                if not pair:
                    continue
                k, eq, v = pair.partition("=")
                if not eq:
                    raise ValueError(
                        f"invalid framing param {pair!r} — expected k=v"
                    )
                params[k.strip()] = v.strip()
        value = params
    if not isinstance(value, dict):
        raise TypeError(
            f"framing must be a mapping or 'kind:k=v,...' string, "
            f"got {value!r}"
        )
    kind = str(value.get("kind", "length-prefix")).strip()
    endian = str(value.get("endian", "big")).strip().lower()
    if endian not in ("big", "little"):
        raise ValueError(f"framing endian must be big|little, got {endian!r}")
    spec = FramerSpec(
        kind=kind,
        size=int(value.get("size", 4)),
        offset=int(value.get("offset", 0)),
        endian=endian,
        includes_self=_bool(value.get("includes_self", False), "includes_self"),
        adjust=int(value.get("adjust", 0)),
        delimiter=(
            _delimiter_bytes(value.get("bytes"))
            if value.get("bytes") is not None
            else b""
        ),
    )
    if spec.kind == "length-prefix":
        # 3 is allowed beyond the common 1|2|4|8 — MySQL and Cassandra
        # v5+ wire formats use three-byte little-endian length fields
        if spec.size not in (1, 2, 3, 4, 8):
            raise ValueError(
                f"length-prefix size must be 1|2|3|4|8, got {spec.size}"
            )
        if spec.offset < 0:
            raise ValueError("length-prefix offset must be >= 0")
    elif spec.kind == "delimiter":
        if not spec.delimiter:
            raise ValueError("delimiter framing requires non-empty bytes")
    elif spec.kind == "fixed":
        if spec.size <= 0:
            raise ValueError("fixed framing size must be > 0")
    else:
        raise ValueError(
            f"unknown framing kind {spec.kind!r} — valid: "
            "length-prefix, delimiter, fixed"
        )
    return spec


class GenericFramer:
    """Incremental frame splitter for a user-declared ``FramerSpec``.

    Same contract as ``FramedStream``: ``feed()`` returns newly completed
    frames, ``failed`` latches on malformed input and freezes ``buf`` so
    ``drain()`` replays the unparsed remainder verbatim. One framer per
    direction per connection — frame boundaries are directional state.

    A ``length-prefix`` frame is ``offset + size + declared + adjust``
    bytes (``includes_self`` subtracts ``size``). A declared length that
    resolves to less than the header or more than ``_MAX_FRAME`` marks the
    stream malformed — the length is attacker-controlled input, never
    trusted.
    """

    def __init__(self, spec: FramerSpec):
        self.spec = spec
        self.buf = bytearray()
        self.failed = False
        self.frames = 0

    def _frame_len(self) -> int | None:
        spec = self.spec
        if spec.kind == "fixed":
            return spec.size if len(self.buf) >= spec.size else None
        if spec.kind == "delimiter":
            i = self.buf.find(spec.delimiter)
            if i < 0:
                return None
            return i + len(spec.delimiter)
        # length-prefix
        if len(self.buf) < spec.offset + spec.size:
            return None
        declared = int.from_bytes(
            self.buf[spec.offset:spec.offset + spec.size], spec.endian
        )
        adjust = spec.adjust - (spec.size if spec.includes_self else 0)
        flen = spec.offset + spec.size + declared + adjust
        if declared > _MAX_FRAME or flen < spec.offset + spec.size:
            return -1
        if len(self.buf) < flen:
            return None
        return flen

    def feed(self, data: bytes) -> list[bytes]:
        """Consume ``data``; return complete frames extracted this call.
        Returns ``[]`` once failed — ``buf`` stays frozen so ``drain()``
        replays the original bytes."""
        if self.failed:
            return []
        self.buf += data
        out: list[bytes] = []
        while True:
            # Unbounded accumulation without a frame boundary is itself
            # malformed — a delimiter that never arrives would pin an
            # ever-growing reassembly buffer on a still-open stream.
            if len(self.buf) > _MAX_FRAME:
                self.failed = True
                break
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
        """Return and clear the unparsed remainder."""
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
