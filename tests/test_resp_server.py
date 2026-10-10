"""Integration tests: redis wire proxy end to end over real sockets.

Fake upstream: an asyncio TCP server speaking minimal RESP (parses
commands with the same codec, canned replies, minimal MULTI/EXEC queue
emulation, a pub/sub push task). The proxy under test and the HTTP
control plane share the same background event loop; test clients are
plain blocking sockets.
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
from microburst.redis.proto import (
    array,
    bulk,
    command_wire,
    error_line,
    integer,
    read_command,
    simple,
)
from microburst.redis.server import RedisProxy, handle_client

# --- fake upstream ---------------------------------------------------------

_BIG = b"x" * 200
_BIG_REPLY = bulk(_BIG)


async def _push_loop(stack, writer):
    """Spontaneous server-push frames while the connection lives —
    RESP2-style pub/sub message arrays."""
    try:
        while True:
            await asyncio.sleep(0.05)
            writer.write(
                array(bulk(b"message"), bulk(b"news"), bulk(b"hello"))
            )
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError, OSError):
        pass


async def _fake_upstream(stack: RedisStack, reader, writer):
    """Minimal Redis backend: canned replies, MULTI/EXEC queue tracking,
    `abort_me` = hard RST, `pushtest` = RESP3 push before the reply."""
    stack.upstream_live += 1
    try:
        in_multi = False
        queued = 0
        while True:
            cmd = await read_command(reader)
            if cmd is None:
                return
            if not cmd.args:
                continue  # real Redis ignores empty inline lines
            text = b" ".join(cmd.args).decode("utf-8", "replace")
            stack.commands.append(text)
            verb = cmd.args[0].lower()
            if verb == b"multi":
                if in_multi:
                    out = error_line("ERR MULTI calls can not be nested")
                else:
                    in_multi = True
                    queued = 0
                    out = simple("OK")
            elif verb == b"exec":
                in_multi = False
                out = array(*[simple("OK")] * queued)
                queued = 0
            elif verb in (b"discard", b"reset"):
                in_multi = False
                queued = 0
                out = simple("OK" if verb == b"discard" else "RESET")
            elif in_multi:
                queued += 1
                out = simple("QUEUED")
            elif verb == b"ping":
                out = simple("PONG")
            elif verb == b"quit":
                writer.write(simple("OK"))
                await writer.drain()
                return
            elif verb == b"abort_me":
                writer.transport.abort()  # hard RST mid-session
                return
            elif verb == b"get" and cmd.args[1:2] == [b"big"]:
                out = _BIG_REPLY
            elif verb == b"get":
                out = bulk(stack.kv.get(cmd.args[1]))
            elif verb == b"pushtest":
                # RESP3 push frame before the actual reply — the proxy
                # must relay it and keep waiting
                writer.write(
                    b">2\r\n$4\r\nnote\r\n$4\r\nhint\r\n" + simple("DONE")
                )
                await writer.drain()
                continue
            elif verb == b"subscribe":
                frames = b"".join(
                    array(bulk(b"subscribe"), bulk(ch), integer(1))
                    for ch in cmd.args[1:]
                )
                writer.write(frames)
                await writer.drain()
                stack.push_tasks.append(
                    asyncio.ensure_future(_push_loop(stack, writer))
                )
                continue
            else:
                out = simple("OK")
            writer.write(out)
            await writer.drain()
    except (EOFError, ConnectionError, asyncio.IncompleteReadError,
            asyncio.CancelledError):
        pass
    finally:
        stack.upstream_live -= 1
        writer.close()


# --- test stack --------------------------------------------------------------

class RedisStack:
    """Fake upstream + RedisProxy + control API on one background loop."""

    def __init__(self, rules: list[dict]):
        self.rules = rules
        self.loop = asyncio.new_event_loop()
        self.proxy: RedisProxy | None = None
        self.port: int | None = None
        self.upstream_port: int | None = None
        self.control_port: int | None = None
        self.commands: list[str] = []
        self.kv: dict[bytes, bytes] = {}
        self.upstream_live = 0
        self.push_tasks: list[asyncio.Task] = []
        self._servers: tuple = ()
        self._runner: web.AppRunner | None = None
        self._ready = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        asyncio.set_event_loop(self.loop)

        async def boot():
            up = await asyncio.start_server(
                lambda r, w: _fake_upstream(self, r, w), "127.0.0.1", 0
            )
            assert up.sockets is not None
            self.upstream_port = up.sockets[0].getsockname()[1]
            self.proxy = RedisProxy("127.0.0.1", self.upstream_port,
                                    rules=self.rules)
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
            for task in self.push_tasks:
                task.cancel()
            for srv in self._servers:
                srv.close()
                await srv.wait_closed()
            if self._runner is not None:
                await self._runner.cleanup()

        self.loop.run_until_complete(shutdown())
        self.loop.close()

    def start(self) -> RedisStack:
        self.thread.start()
        assert self._ready.wait(10), "redis stack did not start"
        return self

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(10)


@pytest.fixture
def redis_stack():
    started = []

    def _start(rules=None):
        stack = RedisStack(rules or []).start()
        started.append(stack)
        return stack

    yield _start
    for stack in started:
        stack.stop()


# --- synchronous RESP client -------------------------------------------------

def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("peer closed")
        buf += chunk
    return buf


def _recv_line(sock: socket.socket) -> bytes:
    buf = b""
    while not buf.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            raise EOFError("peer closed")
        buf += chunk
    return buf.rstrip(b"\r\n")


def read_reply_sync(sock: socket.socket):
    """Minimal sync RESP reader → (kind byte, value), mirroring proto.Reply."""
    t = _recv_exact(sock, 1)
    if t in b"+-_#,(:":
        return t, _recv_line(sock)
    if t in b"$!=":
        n = int(_recv_line(sock))
        if n < 0:
            return t, None
        return t, _recv_exact(sock, n + 2)[:-2]
    if t in b"*~>|%":
        n = int(_recv_line(sock))
        if n < 0:
            return t, None
        count = n * 2 if t in b"%|" else n
        return t, [read_reply_sync(sock) for _ in range(count)]
    raise AssertionError(f"bad reply type byte {t!r}")


def redis_connect(port: int) -> socket.socket:
    sock = socket.create_connection(("127.0.0.1", port), timeout=10)
    sock.settimeout(10)
    return sock


def wait_for(cond, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.02)
    return cond()


def control_get(stack: RedisStack, path: str):
    url = f"http://127.0.0.1:{stack.control_port}/_microburst/{path}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read())


def recv_until_dead(sock: socket.socket) -> bytes:
    buf = b""
    try:
        while chunk := sock.recv(65536):
            buf += chunk
    except (ConnectionResetError, EOFError, TimeoutError):
        pass
    return buf


# --- passthrough ---------------------------------------------------------------

def test_passthrough_get_set_ping(redis_stack):
    stack = redis_stack()
    stack.kv[b"user:1"] = b"alice"
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("SET", "k", "v"))
    assert read_reply_sync(sock) == (b"+", b"OK")
    sock.sendall(command_wire("GET", "user:1"))
    assert read_reply_sync(sock) == (b"$", b"alice")
    sock.sendall(command_wire("GET", "missing"))
    assert read_reply_sync(sock) == (b"$", None)
    sock.close()
    assert stack.commands == ["SET k v", "GET user:1", "GET missing"]


def test_inline_command_passthrough(redis_stack):
    stack = redis_stack()
    sock = redis_connect(stack.port)
    sock.sendall(b"PING\r\n")
    assert read_reply_sync(sock) == (b"+", b"PONG")
    sock.close()


def test_empty_inline_line_is_forwarded_and_ignored(redis_stack):
    stack = redis_stack()
    sock = redis_connect(stack.port)
    sock.sendall(b"PING\r\n\r\nPING\r\n")
    assert read_reply_sync(sock) == (b"+", b"PONG")
    assert read_reply_sync(sock) == (b"+", b"PONG")
    sock.close()


def test_pipelined_commands_in_order(redis_stack):
    stack = redis_stack()
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("SET", "a", "1") + command_wire("GET", "a"))
    assert read_reply_sync(sock) == (b"+", b"OK")
    assert read_reply_sync(sock) == (b"$", None)
    sock.close()


def test_quit_relays_ok_then_closes(redis_stack):
    stack = redis_stack()
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("QUIT"))
    assert read_reply_sync(sock) == (b"+", b"OK")
    assert recv_until_dead(sock) == b""
    sock.close()


def test_upstream_rst_relayed_to_client(redis_stack):
    stack = redis_stack()
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("abort_me"))
    assert recv_until_dead(sock) == b""
    sock.close()


def test_clean_eof_closes_upstream(redis_stack):
    stack = redis_stack()
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("PING"))
    assert read_reply_sync(sock) == (b"+", b"PONG")
    sock.close()
    assert wait_for(lambda: stack.upstream_live == 0)


# --- error faults ---------------------------------------------------------------

def test_error_moved_exact_wire_shape(redis_stack):
    stack = redis_stack([{
        "service": "redis",
        "operation": "get",
        "error": {"code": "MOVED 3999 127.0.0.1:7001"},
    }])
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("GET", "user:1"))
    assert _recv_exact(sock, 1) == b"-"
    assert _recv_line(sock) == b"MOVED 3999 127.0.0.1:7001"
    # command never reached upstream; the connection survives — Redis
    # errors don't close sessions
    assert stack.commands == []
    sock.sendall(command_wire("PING"))
    assert read_reply_sync(sock) == (b"+", b"PONG")
    sock.close()


def test_error_redirect_fields_compose(redis_stack):
    stack = redis_stack([{
        "operation": "get",
        "error": {"code": "ASK",
                  "fields": {"slot": 42, "target": "10.0.0.2:6380"}},
    }])
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("GET", "k"))
    assert _recv_exact(sock, 1) == b"-"
    assert _recv_line(sock) == b"ASK 42 10.0.0.2:6380"
    sock.close()


def test_error_code_plus_message(redis_stack):
    stack = redis_stack([{
        "operation": "set",
        "error": {"code": "READONLY",
                  "message": "You can't write against a read only replica."},
    }])
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("SET", "k", "v"))
    assert _recv_exact(sock, 1) == b"-"
    assert _recv_line(sock) == (
        b"READONLY You can't write against a read only replica."
    )
    sock.close()


def test_resource_matcher_on_first_key(redis_stack):
    stack = redis_stack([{
        "resource": "session:",
        "error": {"code": "CLUSTERDOWN", "message": "The cluster is down"},
    }])
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("GET", "other:1"))
    assert read_reply_sync(sock) == (b"$", None)  # forwarded
    sock.sendall(command_wire("GET", "session:9"))
    assert _recv_exact(sock, 1) == b"-"
    assert _recv_line(sock) == b"CLUSTERDOWN The cluster is down"
    sock.close()


def test_args_regex_matcher(redis_stack):
    stack = redis_stack([{
        "args": "^set session:",
        "error": {"code": "OOM", "message": "no memory for you"},
    }])
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("GET", "session:1"))
    assert read_reply_sync(sock) == (b"$", None)  # forwarded
    sock.sendall(command_wire("SET", "session:1", "x"))
    assert _recv_exact(sock, 1) == b"-"
    assert _recv_line(sock) == b"OOM no memory for you"
    sock.close()


# --- latency / reset / timeout ----------------------------------------------------

def test_latency_then_error(redis_stack):
    stack = redis_stack([{
        "operation": "get",
        "latency": 120,
        "error": {"code": "BUSY", "message": "script running"},
    }])
    sock = redis_connect(stack.port)
    start = time.monotonic()
    sock.sendall(command_wire("GET", "k"))
    kind, line = read_reply_sync(sock)
    assert time.monotonic() - start >= 0.1
    assert kind == b"-" and line == b"BUSY script running"
    sock.close()


def test_timeout_hangs_until_client_gives_up(redis_stack):
    stack = redis_stack([{"operation": "get", "timeout": True}])
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("GET", "k"))
    sock.settimeout(1.5)
    with pytest.raises(TimeoutError):
        sock.recv(1024)
    sock.close()
    assert wait_for(lambda: stack.upstream_live == 0)


def test_timeout_ms_then_forward(redis_stack):
    stack = redis_stack([{"operation": "ping", "timeout_ms": 120}])
    sock = redis_connect(stack.port)
    start = time.monotonic()
    sock.sendall(command_wire("PING"))
    assert read_reply_sync(sock) == (b"+", b"PONG")
    assert time.monotonic() - start >= 0.1
    sock.close()


def test_reset_aborts_connection(redis_stack):
    stack = redis_stack([{"operation": "get", "reset": True}])
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("GET", "k"))
    sock.settimeout(5)
    try:
        data = sock.recv(1024)
        assert data == b""  # FIN or RST — either way the link is dead
    except ConnectionResetError:
        pass
    sock.close()


# --- cut_reply ----------------------------------------------------------------------

def test_cut_reply_mid_bulk(redis_stack):
    stack = redis_stack([{
        "operation": "get",
        "cut_reply": {"after_bytes": 10},
    }])
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("GET", "big"))
    sock.settimeout(5)
    received = recv_until_dead(sock)
    # 10 bytes of a real $200 bulk frame, then the link died mid-string
    assert received == _BIG_REPLY[:10]
    assert received.startswith(b"$200\r\nx")
    sock.close()


# --- MULTI semantics ---------------------------------------------------------------

def test_error_skipped_inside_multi(redis_stack):
    stack = redis_stack([{
        "operation": "get",
        "error": {"code": "ERR", "message": "boom"},
    }])
    sock = redis_connect(stack.port)
    # outside MULTI the rule fires
    sock.sendall(command_wire("GET", "k"))
    assert _recv_exact(sock, 1) == b"-"
    assert _recv_line(sock) == b"ERR boom"
    # inside MULTI it must NOT — the command queues like upstream's
    sock.sendall(command_wire("MULTI"))
    assert read_reply_sync(sock) == (b"+", b"OK")
    sock.sendall(command_wire("GET", "k"))
    assert read_reply_sync(sock) == (b"+", b"QUEUED")
    sock.sendall(command_wire("EXEC"))
    kind, values = read_reply_sync(sock)
    assert kind == b"*" and values == [(b"+", b"OK")]
    assert "GET k" in stack.commands  # upstream saw the queued command
    sock.close()

    assert wait_for(lambda: len(stack.proxy.fired) == 2)
    skipped = [e for e in stack.proxy.fired
               if e.note == "skipped: in-multi"]
    assert len(skipped) == 1
    assert skipped[0].operation == "get"


def test_error_on_multi_leaves_no_tx(redis_stack):
    """An error injected on MULTI itself is safe: upstream never entered
    a transaction, so a following command is fair game."""
    stack = redis_stack([{
        "service": "redis",
        "error": {"code": "ERR", "message": "denied"},
    }])
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("MULTI"))
    assert _recv_exact(sock, 1) == b"-"
    assert _recv_line(sock) == b"ERR denied"
    sock.sendall(command_wire("GET", "k"))
    assert _recv_exact(sock, 1) == b"-"   # also denied — not +QUEUED
    assert _recv_line(sock) == b"ERR denied"
    assert stack.commands == []
    sock.close()


def test_error_on_exec_skipped_inside_multi(redis_stack):
    stack = redis_stack([{
        "operation": "exec",
        "error": {"code": "EXECABORT", "message": "tx aborted"},
    }])
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("MULTI"))
    assert read_reply_sync(sock) == (b"+", b"OK")
    sock.sendall(command_wire("SET", "k", "v"))
    assert read_reply_sync(sock) == (b"+", b"QUEUED")
    # EXEC inside a tx: injecting -EXECABORT would strand upstream's queue
    sock.sendall(command_wire("EXEC"))
    kind, values = read_reply_sync(sock)
    assert kind == b"*" and values == [(b"+", b"OK")]
    sock.close()
    assert wait_for(lambda: len(stack.proxy.fired) == 1)
    assert stack.proxy.fired[0].note == "skipped: in-multi"


def test_latency_still_applies_inside_multi(redis_stack):
    stack = redis_stack([{"operation": "get", "latency": 100}])
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("MULTI"))
    assert read_reply_sync(sock) == (b"+", b"OK")
    start = time.monotonic()
    sock.sendall(command_wire("GET", "k"))
    assert read_reply_sync(sock) == (b"+", b"QUEUED")
    assert time.monotonic() - start >= 0.08
    sock.close()


# --- push modes + RESP3 extras -------------------------------------------------------

def test_subscribe_enters_passthrough(redis_stack):
    stack = redis_stack()
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("SUBSCRIBE", "news"))
    kind, value = read_reply_sync(sock)
    assert kind == b"*" and value[0] == (b"$", b"subscribe")
    assert value[1] == (b"$", b"news")
    # spontaneous server pushes flow through un-decided
    kind, value = read_reply_sync(sock)
    assert value[0] == (b"$", b"message")
    assert value[2] == (b"$", b"hello")
    # and client→upstream still works in pump mode
    sock.sendall(command_wire("PING"))
    # next frame could be a push or the +PONG; consume until PONG
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        r = read_reply_sync(sock)
        if r == (b"+", b"PONG"):
            break
    else:
        pytest.fail("PONG never arrived through pump mode")
    sock.close()
    assert wait_for(lambda: stack.upstream_live == 0)


def test_push_frame_before_reply_relayed(redis_stack):
    stack = redis_stack()
    sock = redis_connect(stack.port)
    # pipelined: the push frame is NOT pushtest's reply — if the proxy
    # miscounted, +PONG would never arrive
    sock.sendall(command_wire("PUSHTEST") + command_wire("PING"))
    push = read_reply_sync(sock)
    assert push[0] == b">" and push[1] == [(b"$", b"note"), (b"$", b"hint")]
    assert read_reply_sync(sock) == (b"+", b"DONE")
    assert read_reply_sync(sock) == (b"+", b"PONG")
    sock.close()


# --- control plane --------------------------------------------------------------------

def test_control_api_shows_redis_events(redis_stack):
    stack = redis_stack([{
        "service": "redis",
        "operation": "get",
        "times": 1,
        "error": {"code": "MOVED 3999 127.0.0.1:7001"},
    }])
    sock = redis_connect(stack.port)
    sock.sendall(command_wire("GET", "user:1"))
    read_reply_sync(sock)
    sock.close()

    fired = control_get(stack, "fired?service=redis")
    assert len(fired) == 1
    event = fired[0]
    assert event["service"] == "redis"
    assert event["operation"] == "get"
    assert event["resource"] == "user:1"
    assert event["action"] == "error:MOVED 3999 127.0.0.1:7001"
    assert event["path"].startswith("GET user:1")

    rules = control_get(stack, "rules")
    assert rules[0]["service"] == "redis"

    health = control_get(stack, "health")
    assert health["status"] == "ok"
    assert health["upstream"].startswith("redis://")
