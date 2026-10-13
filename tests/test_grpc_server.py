"""Integration tests: the gRPC h2c wire proxy end to end over real
sockets, with real ``h2`` state machines on all three legs.

Fake upstream: a minimal h2c *server* — client-side proxy leg speaks
prior-knowledge HTTP/2 to it; per request stream it replies with
``reply_msgs`` grpc-framed DATA messages and a ``grpc-status`` trailer.
Test client: a sync h2 *client* over a blocking socket — real gRPC
clients (grpcurl, grpcio) would drive the same shape.

Verified here: trailers-only errors, mid-stream error trailers after
partial data, RST_STREAM aborts, stalls, TCP-level cuts, operation
matching, and the fired log. grpcio is NOT used (not a test dep) — the
h2-level shapes asserted are what grpc clients parse.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import threading
import time
import urllib.request

import pytest
from aiohttp import web
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import (
    ConnectionTerminated,
    DataReceived,
    RequestReceived,
    ResponseReceived,
    StreamEnded,
    StreamReset,
    TrailersReceived,
)
from h2.exceptions import ProtocolError

from microburst.app import make_control_app
from microburst.grpc.proto import decode_message, headers_dict
from microburst.grpc.server import GrpcProxy, handle_client


def grpc_msg(payload: bytes) -> bytes:
    """One gRPC length-prefixed message: flag u8 + len u32be + payload."""
    return b"\x00" + len(payload).to_bytes(4, "big") + payload


# --- fake upstream ----------------------------------------------------------


def _upstream_reply(conn, sid: int, stack) -> None:
    conn.send_headers(
        sid, [(":status", "200"), ("content-type", "application/grpc")]
    )
    if stack.raw_reply is not None:
        body = stack.raw_reply
    else:
        body = b"".join(
            grpc_msg(f"msg-{i}".encode()) for i in range(stack.reply_msgs)
        )
    if body:
        conn.send_data(sid, body)
    conn.send_headers(
        sid,
        [("grpc-status", stack.reply_status), ("grpc-message", "fine")],
        end_stream=True,
    )


async def _grpc_upstream(stack, reader, writer):
    """Minimal h2c gRPC server — per request stream, canned replies."""
    conn = H2Connection(
        H2Configuration(client_side=False, header_encoding="utf-8")
    )
    conn.initiate_connection()
    writer.write(conn.data_to_send())
    await writer.drain()
    stack.upstream_live += 1
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                return
            try:
                events = conn.receive_data(data)
            except ProtocolError:
                return
            for ev in events:
                if isinstance(ev, RequestReceived):
                    stack.request_headers[ev.stream_id] = dict(ev.headers)
                elif isinstance(ev, TrailersReceived):
                    stack.request_trailers[ev.stream_id] = dict(ev.headers)
                elif isinstance(ev, StreamEnded):
                    if ev.stream_id not in stack.request_headers:
                        continue
                    stack.calls.append(ev.stream_id)
                    if stack.delay:
                        await asyncio.sleep(stack.delay)
                    if stack.reset:
                        conn.reset_stream(ev.stream_id, error_code=0x8)
                    elif stack.goaway:
                        conn.close_connection()
                    else:
                        _upstream_reply(conn, ev.stream_id, stack)
                elif isinstance(ev, StreamReset):
                    stack.upstream_resets.append(ev.stream_id)
            out = conn.data_to_send()
            if out:
                writer.write(out)
                await writer.drain()
            if stack.goaway and stack.calls:
                return
    except (EOFError, ConnectionError, asyncio.IncompleteReadError,
            asyncio.CancelledError):
        pass
    finally:
        stack.upstream_live -= 1
        writer.close()


# --- test stack ----------------------------------------------------------------


class GrpcStack:
    """Fake h2c upstream + GrpcProxy + control API on one background loop."""

    def __init__(self, rules=None, **reply):
        self.rules = rules or []
        self.reply_msgs = reply.get("reply_msgs", 1)
        self.reply_status = reply.get("reply_status", "0")
        self.raw_reply = reply.get("raw_reply")
        self.delay = reply.get("delay", 0.0)
        self.reset = reply.get("reset", False)
        self.goaway = reply.get("goaway", False)
        self.loop = asyncio.new_event_loop()
        self.proxy: GrpcProxy | None = None
        self.port: int | None = None
        self.upstream_port: int | None = None
        self.control_port: int | None = None
        self.request_headers: dict[int, dict] = {}
        self.request_trailers: dict[int, dict] = {}
        self.calls: list[int] = []
        self.upstream_resets: list[int] = []
        self.upstream_live = 0
        self._servers: tuple = ()
        self._runner: web.AppRunner | None = None
        self._ready = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        asyncio.set_event_loop(self.loop)

        async def boot():
            up = await asyncio.start_server(
                lambda r, w: _grpc_upstream(self, r, w), "127.0.0.1", 0
            )
            assert up.sockets is not None
            self.upstream_port = up.sockets[0].getsockname()[1]
            assert self.upstream_port is not None
            self.proxy = GrpcProxy(
                "127.0.0.1", self.upstream_port, rules=self.rules
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
            server = site._server  # pyright: ignore[reportAttributeAccessIssue]
            assert isinstance(server, asyncio.Server)
            self.control_port = server.sockets[0].getsockname()[1]
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

    def start(self) -> GrpcStack:
        self.thread.start()
        assert self._ready.wait(10), "grpc stack did not start"
        return self

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(10)


@pytest.fixture
def grpc_stack():
    started = []

    def _start(rules=None, **kwargs):
        stack = GrpcStack(rules, **kwargs).start()
        started.append(stack)
        return stack

    yield _start
    for stack in started:
        stack.stop()


# --- sync h2 client ------------------------------------------------------------


class GrpcClient:
    """Minimal blocking h2 client — what grpcurl/grpcio drive on the wire."""

    def __init__(self, port: int):
        self.sock = socket.create_connection(
            ("127.0.0.1", port), timeout=10
        )
        self.conn = H2Connection(
            H2Configuration(client_side=True, header_encoding="utf-8")
        )
        self.conn.initiate_connection()
        self.sock.sendall(self.conn.data_to_send())
        self.events: list = []
        self.dead = False

    def call(
        self,
        path: str = "/pkg.Svc/Method",
        payload: bytes | None = b"req",
        content_type: str = "application/grpc",
        trailers: list[tuple[str, str]] | None = None,
    ) -> int:
        sid = self.conn.get_next_available_stream_id()
        self.conn.send_headers(
            sid,
            [
                (":method", "POST"),
                (":scheme", "http"),
                (":authority", "localhost"),
                (":path", path),
                ("content-type", content_type),
                ("te", "trailers"),
            ],
        )
        if payload:
            self.conn.send_data(
                sid, grpc_msg(payload), end_stream=trailers is None
            )
        elif trailers is None:
            self.conn.end_stream(sid)
        if trailers is not None:
            # legal h2: request trailers = HEADERS + END_STREAM
            self.conn.send_headers(sid, trailers, end_stream=True)
        self.sock.sendall(self.conn.data_to_send())
        return sid

    def pump(self, timeout: float = 5.0, stop=None) -> None:
        """Feed socket bytes through h2 until ``stop(ev)`` fires, the
        link dies, or the read times out."""
        self.sock.settimeout(timeout)
        try:
            while True:
                data = self.sock.recv(65536)
                if not data:
                    self.dead = True
                    return
                for ev in self.conn.receive_data(data):
                    self.events.append(ev)
                    if stop is not None and stop(ev):
                        self._flush()
                        return
                self._flush()
        except TimeoutError:
            return
        except (ConnectionResetError, BrokenPipeError, ProtocolError):
            self.dead = True
            return

    def reset(self, sid: int) -> None:
        with contextlib.suppress(Exception):
            self.conn.reset_stream(sid)
            self._flush()

    def _flush(self) -> None:
        out = self.conn.data_to_send()
        if out:
            self.sock.sendall(out)

    def close(self) -> None:
        with contextlib.suppress(OSError):
            self.sock.close()


def response_summary(client: GrpcClient, sid: int) -> dict:
    """Reduce the collected events for ``sid`` to client-visible facts."""
    out = {
        "status": None,
        "grpc_status": None,
        "grpc_message": None,
        "data": b"",
        "reset": False,
        "ended": False,
    }
    for ev in client.events:
        if getattr(ev, "stream_id", None) != sid:
            continue
        if isinstance(ev, (ResponseReceived, TrailersReceived)):
            h = headers_dict(ev.headers)
            if ":status" in h:
                out["status"] = h[":status"]
            if "grpc-status" in h:
                out["grpc_status"] = h["grpc-status"]
            if "grpc-message" in h:
                out["grpc_message"] = decode_message(h["grpc-message"])
        elif isinstance(ev, DataReceived):
            out["data"] += ev.data
        elif isinstance(ev, StreamReset):
            out["reset"] = True
        elif isinstance(ev, StreamEnded):
            out["ended"] = True
    return out


def _wait_for(cond, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.02)
    return cond()


def control_get(stack: GrpcStack, path: str):
    url = f"http://127.0.0.1:{stack.control_port}/_microburst/{path}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read())


# --- passthrough ---------------------------------------------------------------


def test_unary_passthrough(grpc_stack):
    stack = grpc_stack(reply_msgs=1)
    client = GrpcClient(stack.port)
    sid = client.call("/pkg.Svc/Method")
    client.pump(stop=lambda e: isinstance(e, StreamEnded))
    summary = response_summary(client, sid)
    assert summary["status"] == "200"
    assert summary["grpc_status"] == "0"
    assert summary["data"] == grpc_msg(b"msg-0")
    assert summary["ended"]
    client.close()
    # the upstream saw the call, path preserved
    assert _wait_for(lambda: len(stack.calls) == 1)
    hdrs = next(iter(stack.request_headers.values()))
    assert hdrs[":path"] == "/pkg.Svc/Method"


def test_client_request_trailers_forwarded(grpc_stack):
    """Client request trailers (legal h2) relay upstream, ending the
    request stream there."""
    stack = grpc_stack(reply_msgs=1)
    client = GrpcClient(stack.port)
    sid = client.call(trailers=[("x-checksum", "abc123")])
    client.pump(stop=lambda e: isinstance(e, StreamEnded))
    assert response_summary(client, sid)["grpc_status"] == "0"
    assert _wait_for(lambda: stack.request_trailers)
    trailer = next(iter(stack.request_trailers.values()))
    assert trailer["x-checksum"] == "abc123"
    assert len(stack.calls) == 1     # upstream saw END_STREAM
    client.close()


def test_server_streaming_passthrough(grpc_stack):
    stack = grpc_stack(reply_msgs=4)
    client = GrpcClient(stack.port)
    sid = client.call()
    client.pump(stop=lambda e: isinstance(e, StreamEnded))
    summary = response_summary(client, sid)
    assert summary["data"] == b"".join(
        grpc_msg(f"msg-{i}".encode()) for i in range(4)
    )
    assert summary["grpc_status"] == "0"
    client.close()


# --- trailers-only error ---------------------------------------------------------


def test_trailers_only_unavailable(grpc_stack):
    stack = grpc_stack(
        [{"service": "grpc",
          "error": {"code": "UNAVAILABLE", "message": "backend gone"}}]
    )
    client = GrpcClient(stack.port)
    sid = client.call("/pkg.Svc/Method")
    client.pump(stop=lambda e: isinstance(e, StreamEnded))
    summary = response_summary(client, sid)
    # one HEADERS carrying status+trailers, END_STREAM — no DATA
    assert summary["status"] == "200"
    assert summary["grpc_status"] == "14"
    assert summary["grpc_message"] == "backend gone"
    assert summary["data"] == b""
    assert summary["ended"]
    client.close()
    # never forwarded upstream
    time.sleep(0.1)
    assert stack.calls == []


def test_trailers_only_numeric_code(grpc_stack):
    stack = grpc_stack(
        [{"service": "grpc", "error": {"code": 8, "message": "quota"}}]
    )
    client = GrpcClient(stack.port)
    sid = client.call()
    client.pump(stop=lambda e: isinstance(e, StreamEnded))
    summary = response_summary(client, sid)
    assert summary["grpc_status"] == "8"
    client.close()


def test_operation_matching(grpc_stack):
    """`operation:` is the lowercased full method path."""
    stack = grpc_stack(
        [{"service": "grpc", "operation": "pkg.svc/method",
          "error": {"code": "PERMISSION_DENIED"}}]
    )
    client = GrpcClient(stack.port)
    sid = client.call("/pkg.Svc/Method")
    other = client.call("/other.Svc/Nope")
    client.pump(stop=lambda e: isinstance(e, StreamEnded)
                and e.stream_id == other)
    summary = response_summary(client, sid)
    assert summary["grpc_status"] == "7"
    ok = response_summary(client, other)
    assert ok["grpc_status"] == "0"
    client.close()


def test_non_grpc_stream_skips_error(grpc_stack):
    """Non-grpc h2 traffic still proxies; `error:` is skipped — a
    grpc-status trailer isn't a real error shape there."""
    stack = grpc_stack(
        [{"service": "grpc", "error": {"code": "UNAVAILABLE"}}]
    )
    client = GrpcClient(stack.port)
    sid = client.call(content_type="text/plain")
    client.pump(stop=lambda e: isinstance(e, StreamEnded))
    summary = response_summary(client, sid)
    assert summary["grpc_status"] == "0"      # upstream's real ending
    assert len(stack.calls) == 1
    client.close()
    assert _wait_for(lambda: len(stack.proxy.fired) == 1)
    assert "non-grpc content-type" in stack.proxy.fired[0].note


# --- mid-stream faults -----------------------------------------------------------


def test_partial_messages_then_error_trailers(grpc_stack):
    """N real messages relay, then the configured trailers replace
    upstream's ending — the mid-stream failure shape."""
    stack = grpc_stack(
        [{"service": "grpc", "partial_messages": 2,
          "error": {"code": "RESOURCE_EXHAUSTED",
                    "message": "quota mid-stream"}}],
        reply_msgs=5,
    )
    client = GrpcClient(stack.port)
    sid = client.call()
    client.pump(stop=lambda e: isinstance(e, StreamEnded))
    summary = response_summary(client, sid)
    assert summary["data"] == grpc_msg(b"msg-0") + grpc_msg(b"msg-1")
    assert summary["grpc_status"] == "8"
    assert summary["grpc_message"] == "quota mid-stream"
    client.close()


def test_partial_messages_alone_rsts(grpc_stack):
    """partial_messages without `error:` → RST_STREAM after N
    messages — the per-RPC abort shape."""
    stack = grpc_stack(
        [{"service": "grpc", "partial_messages": 1}], reply_msgs=5
    )
    client = GrpcClient(stack.port)
    sid = client.call()
    client.pump(stop=lambda e: isinstance(e, StreamReset))
    summary = response_summary(client, sid)
    assert summary["data"] == grpc_msg(b"msg-0")
    assert summary["reset"]
    client.close()


def test_partial_messages_upstream_ends_early(grpc_stack):
    """Upstream ending before the threshold relays its real trailers."""
    stack = grpc_stack(
        [{"service": "grpc", "partial_messages": 5,
          "error": {"code": "INTERNAL"}}],
        reply_msgs=2,
    )
    client = GrpcClient(stack.port)
    sid = client.call()
    client.pump(stop=lambda e: isinstance(e, StreamEnded))
    summary = response_summary(client, sid)
    assert summary["grpc_status"] == "0"   # upstream's real ending
    client.close()


def test_malformed_grpc_prefix_falls_back(grpc_stack):
    """DATA that isn't grpc-framed → verbatim passthrough + note."""
    stack = grpc_stack(
        [{"service": "grpc", "partial_messages": 2}],
        raw_reply=b"\xff\xffnot-a-grpc-message",
    )
    client = GrpcClient(stack.port)
    sid = client.call()
    client.pump(stop=lambda e: isinstance(e, StreamEnded))
    summary = response_summary(client, sid)
    assert summary["data"] == b"\xff\xffnot-a-grpc-message"
    assert summary["grpc_status"] == "0"
    client.close()
    assert _wait_for(
        lambda: stack.proxy.fired
        and "malformed grpc prefix" in (stack.proxy.fired[0].note or "")
    )


# --- transport faults --------------------------------------------------------------


def test_reset_sends_rst_stream(grpc_stack):
    stack = grpc_stack([{"service": "grpc", "reset": True}])
    client = GrpcClient(stack.port)
    sid = client.call()
    client.pump(stop=lambda e: isinstance(e, StreamReset))
    summary = response_summary(client, sid)
    assert summary["reset"]
    assert summary["grpc_status"] is None
    client.close()
    time.sleep(0.1)
    assert stack.calls == []   # the call never reached upstream


def test_latency_delays_response(grpc_stack):
    stack = grpc_stack([{"service": "grpc", "latency": 150}])
    client = GrpcClient(stack.port)
    start = time.monotonic()
    sid = client.call()
    client.pump(stop=lambda e: isinstance(e, StreamEnded))
    assert time.monotonic() - start >= 0.1
    assert response_summary(client, sid)["grpc_status"] == "0"
    client.close()


def test_timeout_ms_then_proceeds(grpc_stack):
    stack = grpc_stack([{"service": "grpc", "timeout_ms": 120}])
    client = GrpcClient(stack.port)
    start = time.monotonic()
    sid = client.call()
    client.pump(stop=lambda e: isinstance(e, StreamEnded))
    assert time.monotonic() - start >= 0.1
    assert response_summary(client, sid)["grpc_status"] == "0"
    client.close()


def test_timeout_true_parks_call(grpc_stack):
    """`timeout: true` — the call never forwards; the client sees
    nothing until it gives up (deadline → DEADLINE_EXCEEDED
    client-side)."""
    stack = grpc_stack([{"service": "grpc", "timeout": True}])
    client = GrpcClient(stack.port)
    sid = client.call()
    client.pump(timeout=1.0)   # nothing comes back
    assert response_summary(client, sid)["grpc_status"] is None
    # the stream was never opened upstream
    time.sleep(0.1)
    assert stack.calls == []
    client.reset(sid)
    client.close()


def test_cut_reply_mid_message(grpc_stack):
    """cut_reply.after_bytes → the reply DATA dies mid-message at
    TCP level."""
    stack = grpc_stack(
        [{"service": "grpc",
          "cut_reply": {"after_bytes": 8}}],
        reply_msgs=3,
    )
    client = GrpcClient(stack.port)
    sid = client.call()
    client.pump(timeout=5.0)
    summary = response_summary(client, sid)
    assert summary["data"] == grpc_msg(b"msg-0")[:8]
    client.close()


def test_cut_upload_abort(grpc_stack):
    """cut_upload.after_bytes → the client→server stream dies at TCP
    level mid-upload."""
    stack = grpc_stack(
        [{"service": "grpc", "cut_upload": {"after_bytes": 2}}]
    )
    client = GrpcClient(stack.port)
    client.call(payload=b"request-payload")
    client.pump(timeout=5.0)
    assert client.dead
    client.close()


# --- upstream-originated faults -----------------------------------------------------


def test_upstream_rst_propagates(grpc_stack):
    stack = grpc_stack(reset=True)
    client = GrpcClient(stack.port)
    sid = client.call()
    client.pump(stop=lambda e: isinstance(e, StreamReset))
    assert response_summary(client, sid)["reset"]
    client.close()


def test_upstream_goaway_propagates(grpc_stack):
    stack = grpc_stack(goaway=True)
    client = GrpcClient(stack.port)
    client.call()
    client.pump(stop=lambda e: isinstance(e, ConnectionTerminated),
                timeout=5.0)
    assert any(
        isinstance(e, ConnectionTerminated) for e in client.events
    )
    client.close()


# --- observability ---------------------------------------------------------------


def test_fired_log_via_control(grpc_stack):
    stack = grpc_stack(
        [{"service": "grpc", "error": {"code": "UNAVAILABLE"}}]
    )
    client = GrpcClient(stack.port)
    client.call("/pkg.Svc/Method")
    client.pump(stop=lambda e: isinstance(e, StreamEnded))
    client.close()
    assert _wait_for(lambda: stack.proxy.fired)
    fired = control_get(stack, "fired")
    events = fired if isinstance(fired, list) else fired.get("fired", [])
    assert events[0]["service"] == "grpc"
    assert events[0]["operation"] == "pkg.svc/method"
    assert events[0]["path"] == "/pkg.Svc/Method"
    assert "trailers-only" in events[0]["note"]
    health = control_get(stack, "health")
    assert health["upstream"].startswith("grpc://")


def test_service_scoping(grpc_stack):
    """A pg-scoped rule never fires on the grpc transport."""
    stack = grpc_stack([{"service": "postgres", "reset": True}])
    client = GrpcClient(stack.port)
    sid = client.call()
    client.pump(stop=lambda e: isinstance(e, StreamEnded))
    assert response_summary(client, sid)["grpc_status"] == "0"
    client.close()
