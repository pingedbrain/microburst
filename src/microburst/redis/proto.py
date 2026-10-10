"""RESP wire codec — framing, no command semantics.

Reference: https://redis.io/docs/latest/develop/reference/protocol-spec/
RESP2 and RESP3 share the wire; RESP3 adds type bytes.

Client→server frames: *inline* commands (one CRLF-terminated line of
space-separated tokens) and *multibulk* arrays of bulk strings
(``*N\\r\\n$len\\r\\narg\\r\\n…``). Server→client frames are
single-byte-typed recursive values: ``+ - : $ *`` (RESP2) plus
``_ , # ~ % | > = ! (`` (RESP3).

Reader contract (mirrors ``pg/proto.py``):

* ``None`` — clean EOF at a frame boundary: the peer went away politely.
* ``EOFError`` — EOF *inside* a frame: the link died mid-write.
* ``RespProtocolError`` — the stream can't be resynced to a frame
  boundary (bad length, wrong element type, nesting too deep). The only
  honest recovery is closing both ends. Anything that still parses is
  relayed verbatim — the proxy only inspects what a rule decision needs.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

# Sanity bounds. Real Redis caps request multibulk at 1M elements and
# inline commands at 64 KiB; proto-max-bulk-len is 512 MiB. Replies need
# the same bulk bound and a shallow aggregate/depth bound.
MAX_BULK = 512 * 1024 * 1024
MAX_AGGREGATE = 4 * 1024 * 1024
MAX_INLINE = 64 * 1024
MAX_DEPTH = 128

CRLF = b"\r\n"

# Line-terminated reply types: +simple -error :int _null #bool ,double
# (bignum.
_LINE_TYPES = b"+-:_#,("
# Length-prefixed reply types: $bulk !blob-error =verbatim.
_BULK_TYPES = b"$!="
# Element-count aggregates: *array ~set >push.
_AGG_TYPES = b"*~>"
# Pair-count aggregates: %map |attribute (attributes precede the reply
# they annotate — relayed as their own frame).
_MAP_TYPES = b"%|"


class RespProtocolError(Exception):
    """Stream can't be resynced to a frame boundary — close both ends."""


@dataclass
class Command:
    """One client→server frame, decoded enough to decide on."""

    args: list[bytes]
    raw: bytes            # exact wire bytes — forwarded verbatim
    inline: bool = False


@dataclass
class Reply:
    """One server→client frame: type byte, parsed value, raw bytes."""

    kind: bytes           # single type byte
    value: object         # str/bytes/int/float/bool/None/list; maps → list[tuple]
    raw: bytes


async def _exact(reader: asyncio.StreamReader, n: int) -> bytes:
    try:
        return await reader.readexactly(n)
    except asyncio.IncompleteReadError as e:
        raise EOFError("EOF inside a frame") from e


async def _line(reader: asyncio.StreamReader) -> tuple[bytes, bytes]:
    """One CRLF-terminated line → (content, raw including terminator).

    A lone ``\\n`` terminator is tolerated; anything else unterminated is
    a mid-frame EOF. Lines past the StreamReader limit are unparseable.
    """
    try:
        data = await reader.readline()
    except (ValueError, asyncio.LimitOverrunError) as e:
        raise RespProtocolError("line exceeds stream limit") from e
    if data.endswith(CRLF):
        return data[:-2], data
    if data.endswith(b"\n"):
        return data[:-1], data
    raise EOFError("EOF inside a frame")


def _int(raw: bytes, what: str) -> int:
    try:
        return int(raw)
    except ValueError:
        raise RespProtocolError(f"invalid {what} length {raw!r}") from None


async def read_command(
    reader: asyncio.StreamReader,
) -> Command | None:
    """Read one client command frame → Command, or None on clean EOF."""
    first = await reader.read(1)
    if not first:
        return None
    if first == b"*":
        return await _multibulk(reader, first)
    return await _inline(reader, first)


async def _multibulk(reader: asyncio.StreamReader, head: bytes) -> Command:
    content, raw = await _line(reader)
    n = _int(content, "multibulk")
    if n <= 0 or n > MAX_AGGREGATE:
        raise RespProtocolError(f"multibulk length {n} out of range")
    chunks = [head, raw]
    args: list[bytes] = []
    for _ in range(n):
        t = await reader.read(1)
        if not t:
            raise EOFError("EOF inside multibulk command")
        if t != b"$":
            # real Redis errors here too ("expected '$'") — non-bulk
            # command args are a protocol violation
            raise RespProtocolError(f"expected bulk arg, got {t!r}")
        content, raw = await _line(reader)
        length = _int(content, "bulk")
        if length < 0 or length > MAX_BULK:
            raise RespProtocolError(f"bulk length {length} out of range")
        body = await _exact(reader, length + 2)
        if not body.endswith(CRLF):
            raise RespProtocolError("bulk arg missing CRLF terminator")
        chunks += [t, raw, body]
        args.append(body[:-2])
    return Command(args, b"".join(chunks))


async def _inline(reader: asyncio.StreamReader, head: bytes) -> Command:
    content, raw = await _line(reader)
    wire = head + raw
    if len(wire) - 2 > MAX_INLINE:
        raise RespProtocolError("inline command too long")
    # real Redis parses inline args with sdssplitargs — quotes and
    # escapes included. split() is the honest approximation for
    # detection; the raw bytes forward untouched either way.
    return Command((head + content).split(), wire, inline=True)


def _line_value(t: bytes, content: bytes):
    if t == b":" or t == b"(":
        # :integer and (bignum — arbitrary precision, int() covers it
        return _int(content, "integer")
    if t == b",":
        try:
            return float(content)
        except ValueError:
            raise RespProtocolError(f"invalid double {content!r}") from None
    if t == b"#":
        if content not in (b"t", b"f"):
            raise RespProtocolError(f"invalid boolean {content!r}")
        return content == b"t"
    if t == b"_":
        return None
    return content.decode("utf-8", "replace")  # +simple, -error


async def read_reply(
    reader: asyncio.StreamReader, _depth: int = 0,
) -> Reply | None:
    """Read one server reply frame → Reply, or None on clean EOF.

    Aggregates recurse to a frame boundary, so a relay (or a truncating
    fault) always knows where "one reply" ends. RESP3 ``|`` attributes
    and ``>`` pushes are frames of their own — callers relay them and
    keep waiting for the command's actual reply.
    """
    if _depth > MAX_DEPTH:
        raise RespProtocolError("aggregate nesting too deep")
    head = await reader.read(1)
    if not head:
        return None
    t = head
    if t in _LINE_TYPES:
        content, raw = await _line(reader)
        return Reply(t, _line_value(t, content), t + raw)
    if t in _BULK_TYPES:
        content, raw = await _line(reader)
        length = _int(content, "bulk")
        if t == b"$" and length == -1:
            return Reply(t, None, t + raw)
        if length < 0 or length > MAX_BULK:
            raise RespProtocolError(f"bulk length {length} out of range")
        body = await _exact(reader, length + 2)
        if not body.endswith(CRLF):
            raise RespProtocolError("bulk reply missing CRLF terminator")
        return Reply(t, body[:-2], t + raw + body)
    if t in _AGG_TYPES or t in _MAP_TYPES:
        content, raw = await _line(reader)
        n = _int(content, "aggregate")
        if t == b"*" and n == -1:
            return Reply(t, None, t + raw)
        if n < 0 or n > MAX_AGGREGATE:
            raise RespProtocolError(f"aggregate length {n} out of range")
        count = n * 2 if t in _MAP_TYPES else n
        chunks = [t, raw]
        children: list[Reply] = []
        for _ in range(count):
            child = await read_reply(reader, _depth + 1)
            if child is None:
                raise EOFError("EOF inside aggregate")
            children.append(child)
            chunks.append(child.raw)
        if t in _MAP_TYPES:
            value = [
                (children[i].value, children[i + 1].value)
                for i in range(0, len(children), 2)
            ]
        else:
            value = [c.value for c in children]
        return Reply(t, value, b"".join(chunks))
    raise RespProtocolError(f"unknown reply type byte {t!r}")


# --- reply builders -----------------------------------------------------------
# Used by tests' fake upstream.

def simple(text: str) -> bytes:
    return b"+" + text.encode() + CRLF


def error_line(line: str) -> bytes:
    """A ``-LINE`` error frame — tests' fake upstream uses this."""
    return b"-" + line.encode() + CRLF


def integer(n: int) -> bytes:
    return b":" + str(n).encode() + CRLF


def bulk(data: bytes | str | None) -> bytes:
    if data is None:
        return b"$-1\r\n"
    if isinstance(data, str):
        data = data.encode()
    return b"$" + str(len(data)).encode() + CRLF + data + CRLF


def array(*frames: bytes) -> bytes:
    """Wrap already-encoded reply frames in an aggregate header."""
    return b"*" + str(len(frames)).encode() + CRLF + b"".join(frames)


def command_wire(*args: bytes | str) -> bytes:
    """Encode a client command multibulk — the test client's side."""
    parts = [b"*" + str(len(args)).encode() + CRLF]
    for arg in args:
        if isinstance(arg, str):
            arg = arg.encode()
        parts.append(b"$" + str(len(arg)).encode() + CRLF + arg + CRLF)
    return b"".join(parts)
