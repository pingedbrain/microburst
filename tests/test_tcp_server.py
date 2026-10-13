"""Integration tests: generic tcp wire proxy end to end over real sockets.

Fake upstreams: a raw echo server (unframed tests) and a 4-byte BE
length-prefixed echo server (framed tests). Proxy + control API share a
background event loop; test clients are plain blocking sockets.
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
import urllib.request

import pytest
from aiohttp import web

from microburst.app import make_control_app
from microburst.tcp.server import TcpProxy, handle_client

# --- fake upstreams ----------------------------------------------------------


def _frame(payload: bytes) -> bytes:
    return len(payload).to_bytes(4, "big") + payload


async def _echo_upstream(stack, reader, writer):
    """Raw echo — replies ``R:<chunk>`` so direction is unambiguous."""
    stack.upstream_live += 1
    try:
        while chunk := await reader.read(65536):
            stack.received.append(chunk)
            writer.write(b"R:" + chunk)
            await writer.drain()
    except (EOFError, ConnectionError, asyncio.IncompleteReadError,
            asyncio.CancelledError):
        pass
    finally:
        stack.upstream_live -= 1
        writer.close()


async def _framed_upstream(stack, reader, writer):
    """4-byte BE length-prefixed echo — replies ``R:<payload>`` framed."""
    stack.upstream_live += 1
    try:
        while True:
            hdr = await reader.readexactly(4)
            body = await reader.readexactly(int.from_bytes(hdr, "big"))
            stack.received.append(hdr + body)
            writer.write(_frame(b"R:" + body))
            await writer.drain()
    except (EOFError, ConnectionError, asyncio.IncompleteReadError,
            asyncio.CancelledError):
        pass
    finally:
        stack.upstream_live -= 1
        writer.close()


# --- test stack ----------------------------------------------------------------

class TcpStack:
    """Fake upstream + TcpProxy + control API on one background loop."""

    def __init__(self, rules, framing=None, upstream=_echo_upstream):
        self.rules = rules
        self.framing = framing
        self.upstream_handler = upstream
        self.loop = asyncio.new_event_loop()
        self.proxy: TcpProxy | None = None
        self.port: int | None = None
        self.upstream_port: int | None = None
        self.control_port: int | None = None
        self.received: list[bytes] = []
        self.upstream_live = 0
        self._servers: tuple = ()
        self._runner: web.AppRunner | None = None
        self._ready = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        asyncio.set_event_loop(self.loop)

        async def boot():
            up = await asyncio.start_server(
                lambda r, w: self.upstream_handler(self, r, w),
                "127.0.0.1", 0,
            )
            assert up.sockets is not None
            self.upstream_port = up.sockets[0].getsockname()[1]
            self.proxy = TcpProxy(
                "127.0.0.1", self.upstream_port,
                rules=self.rules, framing=self.framing,
            )
            proxy = self.proxy
            srv = await asyncio.start_server(
                lambda r, w: handle_client(proxy, r, w),
                "127.0.0.1", 0,
            )
            assert srv.sockets is not None
            self.port = srv.sockets[0].getsockname()[1]
            runner = web.AppRunner(make_control_app(self.proxy))
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            server = site._server
            assert server is not None
            self.control_port = server.sockets[0].getsockname()[1]  # pyright: ignore[reportAttributeAccessIssue]
            self._servers = (up, srv)
            self._runner = runner

        self.loop.run_until_complete(boot())
        self._ready.set()
        self.loop.run_forever()

        async def shutdown():
            for srv in self._servers:
                srv.close()
                await srv.wait_closed()
            if self._runner is not None:
                await self._runner.cleanup()

        self.loop.run_until_complete(shutdown())
        self.loop.close()

    def start(self) -> TcpStack:
        self.thread.start()
        assert self._ready.wait(10), "tcp stack did not start"
        return self

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(10)


@pytest.fixture
def tcp_stack():
    started = []

    def _start(rules=None, **kwargs):
        stack = TcpStack(rules or [], **kwargs).start()
        started.append(stack)
        return stack

    yield _start
    for stack in started:
        stack.stop()


# --- sync client helpers ------------------------------------------------------


def tcp_connect(port: int) -> socket.socket:
    sock = socket.create_connection(("127.0.0.1", port), timeout=10)
    sock.settimeout(10)
    return sock


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("peer closed")
        buf += chunk
    return buf


def recv_until_dead(sock: socket.socket) -> bytes:
    buf = b""
    try:
        while chunk := sock.recv(65536):
            buf += chunk
    except (ConnectionResetError, EOFError, TimeoutError):
        pass
    return buf


def _recv_frame(sock: socket.socket) -> bytes:
    hdr = _recv_exact(sock, 4)
    return _recv_exact(sock, int.from_bytes(hdr, "big"))


def wait_for(cond, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.02)
    return cond()


def control_get(stack: TcpStack, path: str):
    url = f"http://127.0.0.1:{stack.control_port}/_microburst/{path}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read())


FRAMING_LP = {"kind": "length-prefix", "size": 4, "endian": "big"}


# --- unframed passthrough --------------------------------------------------------


def test_unframed_passthrough_both_directions(tcp_stack):
    stack = tcp_stack()
    sock = tcp_connect(stack.port)
    sock.sendall(b"hello")
    assert _recv_exact(sock, 7) == b"R:hello"
    sock.sendall(b"\x00\xffbinary\x7f")
    # echo reply: R: + 9 bytes
    assert _recv_exact(sock, 11) == b"R:\x00\xffbinary\x7f"
    sock.close()
    assert wait_for(
        lambda: stack.received == [b"hello", b"\x00\xffbinary\x7f"]
    )


def test_unframed_conn_latency(tcp_stack):
    stack = tcp_stack([{"service": "tcp", "latency": 120}])
    sock = tcp_connect(stack.port)
    start = time.monotonic()
    sock.sendall(b"ping")
    assert _recv_exact(sock, 6) == b"R:ping"
    assert time.monotonic() - start >= 0.1
    sock.close()


def test_unframed_reset(tcp_stack):
    stack = tcp_stack([{"service": "tcp", "reset": True}])
    sock = tcp_connect(stack.port)
    sock.sendall(b"die")
    sock.settimeout(5)
    try:
        assert sock.recv(1024) == b""
    except ConnectionResetError:
        pass
    assert stack.received == []
    sock.close()


def test_unframed_timeout_true_hangs(tcp_stack):
    stack = tcp_stack([{"service": "tcp", "timeout": True}])
    sock = tcp_connect(stack.port)
    sock.sendall(b"stuck")
    sock.settimeout(1.5)
    with pytest.raises(TimeoutError):
        sock.recv(1024)
    sock.close()


def test_unframed_timeout_ms_then_forwards(tcp_stack):
    stack = tcp_stack([{"service": "tcp", "timeout_ms": 120}])
    sock = tcp_connect(stack.port)
    start = time.monotonic()
    sock.sendall(b"wait")
    assert _recv_exact(sock, 6) == b"R:wait"
    assert time.monotonic() - start >= 0.1
    sock.close()


def test_unframed_payload_matches_rolling_prefix(tcp_stack):
    """A payload rule that can't match chunk 1 fires when the 4KiB
    prefix accumulates enough bytes."""
    stack = tcp_stack([{"payload": "secret", "reset": True}])
    sock = tcp_connect(stack.port)
    sock.sendall(b"sec")          # prefix "sec" — no match, relays
    time.sleep(0.15)
    sock.sendall(b"ret")          # prefix "secret" — match → RST
    sock.settimeout(5)
    # the "R:sec" echo may land before the reset does
    assert recv_until_dead(sock) in (b"", b"R:sec")
    sock.close()
    # chunk 2 (the matching one) is never forwarded; if the two sends
    # coalesced into one chunk nothing is
    assert stack.received in ([b"sec"], [])


def test_unframed_cut_upload_after_bytes(tcp_stack):
    stack = tcp_stack([{"service": "tcp",
                        "cut_upload": {"after_bytes": 5}}])
    sock = tcp_connect(stack.port)
    sock.sendall(b"0123456789")
    sock.settimeout(5)
    # client link dies; the echo of the relayed prefix may partially land
    received = recv_until_dead(sock)
    assert b"R:01234".startswith(received)
    sock.close()
    assert wait_for(lambda: stack.received == [b"01234"])


def test_unframed_cut_reply_after_bytes(tcp_stack):
    stack = tcp_stack([{"service": "tcp",
                        "cut_reply": {"after_bytes": 3}}])
    sock = tcp_connect(stack.port)
    sock.sendall(b"ping")
    sock.settimeout(5)
    # 3 bytes of the "R:ping" echo, then the link dies mid-stream
    assert recv_until_dead(sock) == b"R:p"
    sock.close()


def test_unframed_corrupt_at_stream_offset(tcp_stack):
    stack = tcp_stack([{"service": "tcp",
                        "corrupt": {"at_bytes": 3}}])
    sock = tcp_connect(stack.port)
    sock.sendall(b"abcdef")
    # byte 3 ('d') flips to 'd'^0xFF = 0x9b before reaching upstream
    sock.settimeout(5)
    got = _recv_exact(sock, 8)
    assert got == b"R:abc\x9bef"
    sock.close()
    assert stack.received == [b"abc\x9bef"]


def test_error_rule_is_noop_with_note(tcp_stack):
    stack = tcp_stack([{"service": "tcp",
                        "error": {"code": "Boom",
                                  "message": "not renderable"}}])
    sock = tcp_connect(stack.port)
    sock.sendall(b"hello")
    # no renderer — the unit forwards untouched and the echo returns
    assert _recv_exact(sock, 7) == b"R:hello"
    sock.close()
    assert wait_for(lambda: len(stack.proxy.fired) == 1)
    event = stack.proxy.fired[0]
    assert "error has no renderer in tcp mode" in event.note
    assert event.note.startswith("unframed")


def test_unframed_fired_log_via_control(tcp_stack):
    stack = tcp_stack([{"service": "tcp", "latency": 10}])
    sock = tcp_connect(stack.port)
    sock.sendall(b"check")
    assert _recv_exact(sock, 7) == b"R:check"
    sock.close()
    assert wait_for(lambda: stack.proxy.fired)
    fired = control_get(stack, "fired")
    events = fired if isinstance(fired, list) else fired.get("fired", [])
    assert events[0]["service"] == "tcp"
    assert events[0]["operation"] == "conn"
    assert events[0]["note"] == "unframed"
    health = control_get(stack, "health")
    assert health["upstream"].startswith("tcp://")


def test_service_tcp_scoping(tcp_stack):
    """A redis-scoped rule never fires on the tcp transport."""
    stack = tcp_stack([{"service": "redis", "reset": True}])
    sock = tcp_connect(stack.port)
    sock.sendall(b"hello")
    assert _recv_exact(sock, 7) == b"R:hello"
    sock.close()
    assert stack.received == [b"hello"]


# --- framed passthrough + decisions ------------------------------------------------


def test_framed_passthrough(tcp_stack):
    stack = tcp_stack(framing=FRAMING_LP, upstream=_framed_upstream)
    sock = tcp_connect(stack.port)
    sock.sendall(_frame(b"one") + _frame(b"two"))
    assert _recv_frame(sock) == b"R:one"
    assert _recv_frame(sock) == b"R:two"
    sock.close()
    assert stack.received == [_frame(b"one"), _frame(b"two")]


def test_framed_payload_matcher_on_c2s(tcp_stack):
    stack = tcp_stack(
        [{"service": "tcp", "operation": "c2s:frame",
          "payload": "dropme", "reset": True}],
        framing=FRAMING_LP, upstream=_framed_upstream,
    )
    sock = tcp_connect(stack.port)
    sock.sendall(_frame(b"keep"))
    assert _recv_frame(sock) == b"R:keep"
    sock.sendall(_frame(b"xxdropmexx"))
    sock.settimeout(5)
    try:
        assert sock.recv(1024) == b""
    except ConnectionResetError:
        pass
    sock.close()
    # the matching frame never reached upstream
    assert stack.received == [_frame(b"keep")]


def test_framed_s2c_latency(tcp_stack):
    stack = tcp_stack(
        [{"service": "tcp", "operation": "s2c:frame",
          "latency": 120}],
        framing=FRAMING_LP, upstream=_framed_upstream,
    )
    sock = tcp_connect(stack.port)
    start = time.monotonic()
    sock.sendall(_frame(b"slow"))
    assert _recv_frame(sock) == b"R:slow"
    assert time.monotonic() - start >= 0.1
    sock.close()


def test_framed_cut_upload_after_messages(tcp_stack):
    stack = tcp_stack(
        [{"service": "tcp", "operation": "c2s:frame",
          "cut_upload": {"after_messages": 2}}],
        framing=FRAMING_LP, upstream=_framed_upstream,
    )
    sock = tcp_connect(stack.port)
    sock.sendall(_frame(b"a") + _frame(b"b") + _frame(b"c"))
    sock.settimeout(5)
    got = recv_until_dead(sock)
    # two relayed, then the link dies — replies for a/b may arrive first
    assert _frame(b"R:a") in got or got == b""
    sock.close()
    assert wait_for(lambda: len(stack.received) == 2)
    assert stack.received == [_frame(b"a"), _frame(b"b")]


def test_framed_cut_reply_after_messages(tcp_stack):
    stack = tcp_stack(
        [{"service": "tcp", "operation": "s2c:frame",
          "cut_reply": {"after_messages": 1}}],
        framing=FRAMING_LP, upstream=_framed_upstream,
    )
    sock = tcp_connect(stack.port)
    sock.sendall(_frame(b"a") + _frame(b"b"))
    sock.settimeout(5)
    got = recv_until_dead(sock)
    # first reply relays whole, second reply's frame kills the link
    assert got.startswith(_frame(b"R:a"))
    sock.close()


def test_framed_corrupt_inside_frame(tcp_stack):
    stack = tcp_stack(
        [{"service": "tcp", "operation": "c2s:frame",
          "corrupt": {"at_bytes": 5}}],
        framing=FRAMING_LP, upstream=_framed_upstream,
    )
    sock = tcp_connect(stack.port)
    sock.sendall(_frame(b"abcdefgh"))  # 12B: 4 prefix + 8 payload
    assert _recv_frame(sock) == b"R:" + b"a" + bytes([ord("b") ^ 0xFF]) + b"cdefgh"
    sock.close()
    assert stack.received[0][5] == ord("b") ^ 0xFF


def test_malformed_framing_falls_back_to_passthrough(tcp_stack):
    stack = tcp_stack(framing=FRAMING_LP, upstream=_echo_upstream)
    sock = tcp_connect(stack.port)
    bogus = b"\xff\xff\xff\xff" + b"junkjunk"
    sock.sendall(bogus)
    # framer latches failed → verbatim relay both ways
    assert _recv_exact(sock, len(b"R:") + len(bogus)) == b"R:" + bogus
    sock.close()
    assert stack.received == [bogus]
    assert wait_for(lambda: any(
        e.note and "malformed" in e.note for e in stack.proxy.fired
    ))


# --- respond ------------------------------------------------------------------------


def test_respond_data_then_forward(tcp_stack):
    stack = tcp_stack(
        [{"service": "tcp", "operation": "c2s:frame",
          "payload": "ping", "respond": {"data": "pong!"}}],
        framing=FRAMING_LP, upstream=_framed_upstream,
    )
    sock = tcp_connect(stack.port)
    sock.sendall(_frame(b"ping"))
    assert _recv_exact(sock, 5) == b"pong!"
    # unit never reached upstream; the next one flows normally
    sock.sendall(_frame(b"real"))
    assert _recv_frame(sock) == b"R:real"
    sock.close()
    assert stack.received == [_frame(b"real")]


def test_respond_hex_then_close(tcp_stack):
    stack = tcp_stack(
        [{"service": "tcp", "operation": "c2s:frame",
          "respond": {"hex": "ff00aa", "then": "close"}}],
        framing=FRAMING_LP, upstream=_framed_upstream,
    )
    sock = tcp_connect(stack.port)
    sock.sendall(_frame(b"hi"))
    sock.settimeout(5)
    assert recv_until_dead(sock) == b"\xff\x00\xaa"
    sock.close()
    assert stack.received == []


def test_respond_base64_then_hold(tcp_stack):
    import base64 as b64

    stack = tcp_stack(
        [{"service": "tcp", "operation": "c2s:frame",
          "payload": "once",
          "respond": {"base64": b64.b64encode(b"HELLO").decode(),
                      "then": "hold"}}],
        framing=FRAMING_LP, upstream=_framed_upstream,
    )
    sock = tcp_connect(stack.port)
    sock.sendall(_frame(b"once"))
    assert _recv_exact(sock, 5) == b"HELLO"
    # hold: the link stays open but swallows everything else
    sock.sendall(_frame(b"more"))
    sock.settimeout(1.0)
    with pytest.raises((TimeoutError, ConnectionResetError)):
        sock.recv(1024)
    sock.close()
    assert stack.received == []
