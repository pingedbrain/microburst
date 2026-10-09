"""Integration tests: pg wire proxy end to end over real sockets.

Fake upstream: an asyncio TCP server speaking the minimal backend
protocol (auth-ok handshake, canned result sets, BEGIN/COMMIT tx-status
tracking). The proxy under test and the HTTP control plane share the
same background event loop; test clients are plain blocking sockets.
"""

from __future__ import annotations

import asyncio
import json
import re
import socket
import struct
import threading
import time
import urllib.request

import pytest
from aiohttp import web

from microburst.app import make_control_app
from microburst.pg.errors import parse_error_fields
from microburst.pg.proto import (
    GSSENC_REQUEST_CODE,
    PROTOCOL_V3,
    SSL_REQUEST_CODE,
    auth_ok,
    backend_key_data,
    bind_complete,
    bind_message,
    command_complete,
    cstring,
    data_row,
    describe_message,
    execute_message,
    frame,
    parameter_status,
    parse_complete,
    parse_message,
    query_message,
    read_cstring,
    read_frame,
    read_startup,
    ready_for_query,
    row_description,
    startup_packet,
    sync_message,
    terminate_message,
)
from microburst.pg.server import PgProxy, handle_client

# --- fake upstream ---------------------------------------------------------

async def _fake_upstream(stack: PgStack, reader, writer):
    """Minimal PG backend: auth-ok handshake, canned SELECT rows,
    BEGIN/COMMIT tx status, `select abort` = hard RST."""
    stack.upstream_live += 1
    try:
        first = await read_startup(reader)
        if first is None:
            return
        code, _body = first
        while code in (SSL_REQUEST_CODE, GSSENC_REQUEST_CODE):
            writer.write(b"N")
            await writer.drain()
            nxt = await read_startup(reader)
            if nxt is None:
                return
            code, _body = nxt
        if stack.scram:
            # Minimal SASL exchange: R10 → 'p', R11 → 'p', R12 → AuthOk.
            # R12 expects NO client reply — a proxy that waits for one
            # deadlocks here, which is exactly what this exercises.
            writer.write(frame(b"R", struct.pack("!I", 10)))
            await writer.drain()
            if await read_frame(reader) is None:
                return
            writer.write(frame(b"R", struct.pack("!I", 11)))
            await writer.drain()
            if await read_frame(reader) is None:
                return
            writer.write(frame(b"R", struct.pack("!I", 12)))
        writer.write(
            auth_ok()
            + parameter_status("server_version", "15.0-fake")
            + backend_key_data()
            + ready_for_query(b"I")
        )
        await writer.drain()
        tx = b"I"
        while True:
            msg = await read_frame(reader)
            if msg is None:
                return
            mtype, payload = msg
            if mtype == b"Q":
                sql, _ = read_cstring(payload)
                stack.queries.append(sql)
                low = sql.strip().lower()
                if "abort_me" in low:
                    writer.transport.abort()  # hard RST mid-query
                    return
                if low.startswith(("begin", "start transaction")):
                    tx = b"T"
                    out = command_complete("BEGIN")
                elif low.startswith(("commit", "rollback", "end", "abort")):
                    tx = b"I"
                    out = command_complete("COMMIT")
                elif low.startswith("select"):
                    m = re.search(r"\d+", low)
                    n = int(m.group(0)) if m else 1
                    out = (
                        row_description(["n"])
                        + b"".join(
                            data_row([str(i).encode()]) for i in range(n)
                        )
                        + command_complete(f"SELECT {n}")
                    )
                else:
                    out = command_complete("OK")
                writer.write(out + ready_for_query(tx))
            elif mtype == b"X":
                return
            elif mtype == b"S":
                # end of an extended-protocol batch — canned exchange
                writer.write(
                    parse_complete()
                    + bind_complete()
                    + row_description(["n"])
                    + data_row([b"7"])
                    + command_complete("SELECT 1")
                    + ready_for_query(tx)
                )
            # P/B/D/E/C/H and anything else: answered at Sync
            await writer.drain()
    except (EOFError, ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        stack.upstream_live -= 1
        writer.close()


# --- test stack --------------------------------------------------------------

class PgStack:
    """Fake upstream + PgProxy + control API on one background loop."""

    def __init__(self, rules: list[dict], scram: bool = False):
        self.rules = rules
        self.scram = scram
        self.loop = asyncio.new_event_loop()
        self.proxy: PgProxy | None = None
        self.port: int | None = None
        self.upstream_port: int | None = None
        self.control_port: int | None = None
        self.queries: list[str] = []
        self.upstream_live = 0
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
            uport = up.sockets[0].getsockname()[1]
            self.upstream_port = uport
            self.proxy = PgProxy("127.0.0.1", uport, rules=self.rules)
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

    def start(self) -> PgStack:
        self.thread.start()
        assert self._ready.wait(10), "pg stack did not start"
        return self

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(10)


@pytest.fixture
def pg_stack():
    started = []

    def _start(rules=None, scram: bool = False):
        stack = PgStack(rules or [], scram=scram).start()
        started.append(stack)
        return stack

    yield _start
    for stack in started:
        stack.stop()


# --- synchronous pg client -----------------------------------------------------

def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("peer closed")
        buf += chunk
    return buf


def read_frame_sync(sock: socket.socket):
    t = sock.recv(1)
    if not t:
        return None
    (length,) = struct.unpack("!I", _recv_exact(sock, 4))
    return t, _recv_exact(sock, length - 4)


def read_until_z(sock: socket.socket) -> list[tuple[bytes, bytes]]:
    out = []
    while True:
        msg = read_frame_sync(sock)
        assert msg is not None, "connection closed before ReadyForQuery"
        out.append(msg)
        if msg[0] == b"Z":
            return out


def pg_connect(port: int, ssl: bool = False, gss: bool = False):
    sock = socket.create_connection(("127.0.0.1", port), timeout=10)
    if ssl:
        sock.sendall(startup_packet(SSL_REQUEST_CODE))
        assert sock.recv(1) == b"N"
    if gss:
        sock.sendall(startup_packet(GSSENC_REQUEST_CODE))
        assert sock.recv(1) == b"N"
    sock.sendall(
        startup_packet(
            PROTOCOL_V3, {"user": "tester", "database": "shop"}
        )
    )
    return sock, read_until_z(sock)


def pg_query(sock: socket.socket, sql: str):
    sock.sendall(query_message(sql))
    return read_until_z(sock)


def wait_for(cond, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.02)
    return cond()


def control_get(stack: PgStack, path: str):
    url = f"http://127.0.0.1:{stack.control_port}/_microburst/{path}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read())


# --- passthrough ---------------------------------------------------------------

def test_passthrough_query(pg_stack):
    stack = pg_stack()
    sock, startup_msgs = pg_connect(stack.port)
    types = [t for t, _ in startup_msgs]
    assert b"R" in types and b"S" in types and types[-1] == b"Z"

    msgs = pg_query(sock, "select 1")
    types = [t for t, _ in msgs]
    assert types[:3] == [b"T", b"D", b"C"]
    assert msgs[-1] == (b"Z", b"I")
    assert stack.queries == ["select 1"]
    sock.close()


def test_scram_auth_passthrough(pg_stack):
    """SASL exchange relays client 'p' responses and, crucially, does
    not wait for a reply after SASLFinal (R12)."""
    stack = pg_stack(scram=True)
    sock = socket.create_connection(("127.0.0.1", stack.port), timeout=10)
    sock.sendall(
        startup_packet(PROTOCOL_V3, {"user": "u", "database": "d"})
    )
    sock.settimeout(10)
    msg = read_frame_sync(sock)
    assert msg is not None
    mtype, payload = msg
    assert mtype == b"R"
    (code,) = struct.unpack("!I", payload[:4])
    assert code == 10  # AuthenticationSASL — expects SASLInitialResponse
    sock.sendall(frame(b"p", cstring("SCRAM-SHA-256") + b"client-first"))
    msg = read_frame_sync(sock)
    assert msg is not None
    assert struct.unpack("!I", msg[1][:4]) == (11,)  # SASLContinue
    sock.sendall(frame(b"p", b"client-final"))
    msg = read_frame_sync(sock)
    assert msg is not None
    assert struct.unpack("!I", msg[1][:4]) == (12,)  # SASLFinal — no reply
    rest = read_until_z(sock)  # AuthOk, ParameterStatus, BackendKeyData, Z
    assert any(
        t == b"R" and struct.unpack("!I", p[:4]) == (0,) for t, p in rest
    )
    msgs = pg_query(sock, "select 1")
    assert msgs[-1] == (b"Z", b"I")
    sock.close()


def test_ssl_and_gss_requests_refused_then_session(pg_stack):
    stack = pg_stack()
    sock, msgs = pg_connect(stack.port, ssl=True, gss=True)
    assert msgs[-1][0] == b"Z"
    sock.close()


def test_terminate_and_clean_eof_close_upstream(pg_stack):
    stack = pg_stack()
    sock, _ = pg_connect(stack.port)
    sock.sendall(terminate_message())
    sock.close()
    assert wait_for(lambda: stack.upstream_live == 0)

    sock2, _ = pg_connect(stack.port)
    sock2.close()  # clean EOF, no Terminate
    assert wait_for(lambda: stack.upstream_live == 0)


def test_upstream_rst_relayed_to_client(pg_stack):
    stack = pg_stack()
    sock, _ = pg_connect(stack.port)
    sock.sendall(query_message("select abort_me"))
    try:
        msg = read_frame_sync(sock)
        assert msg is None or msg[0] == b"E"  # RST or clean close
    except (ConnectionResetError, EOFError):
        pass  # relayed RST — the honest outcome
    sock.close()


# --- startup faults --------------------------------------------------------------

def test_startup_fatal_53300(pg_stack):
    stack = pg_stack([{
        "service": "postgres",
        "operation": "startup",
        "error": {
            "sqlstate": "53300",
            "severity": "FATAL",
            "message": "too many connections",
        },
    }])
    sock = socket.create_connection(("127.0.0.1", stack.port), timeout=10)
    sock.sendall(startup_packet(PROTOCOL_V3, {"user": "u", "database": "d"}))
    msg = read_frame_sync(sock)
    assert msg is not None
    mtype, payload = msg
    assert mtype == b"E"
    fields = parse_error_fields(payload)
    assert fields["S"] == "FATAL"
    assert fields["C"] == "53300"
    assert "too many connections" in fields["M"]
    assert read_frame_sync(sock) is None  # FATAL closes, like real PG
    sock.close()


# --- query faults --------------------------------------------------------------

def test_error_idle_then_connection_survives(pg_stack):
    stack = pg_stack([{
        "service": "postgres",
        "operation": "select",
        "times": 1,
        "error": {"sqlstate": "40001",
                  "message": "could not serialize access"},
    }])
    sock, _ = pg_connect(stack.port)
    msgs = pg_query(sock, "select 1")
    types = [t for t, _ in msgs]
    assert types == [b"E", b"Z"]
    fields = parse_error_fields(msgs[0][1])
    assert fields["C"] == "40001"
    assert fields["S"] == "ERROR"
    assert msgs[1] == (b"Z", b"I")

    # same connection still works — the error was idle-state only
    msgs2 = pg_query(sock, "select 1")
    assert msgs2[0][0] == b"T"
    sock.close()


def test_error_skipped_in_transaction(pg_stack):
    stack = pg_stack([{
        "operation": "select",
        "error": {"sqlstate": "40001"},
    }])
    sock, _ = pg_connect(stack.port)
    begin = pg_query(sock, "begin")
    assert begin[-1] == (b"Z", b"T")

    # in 'T' the rule must NOT fire — the query reaches upstream
    msgs = pg_query(sock, "select 1")
    assert msgs[0][0] == b"T"
    assert msgs[-1] == (b"Z", b"T")
    assert "select 1" in stack.queries
    sock.close()

    assert wait_for(lambda: len(stack.proxy.fired) == 1)
    event = stack.proxy.fired[0]
    assert event.note == "skipped: in-transaction"
    assert event.operation == "select"


def test_latency_then_query_canceled(pg_stack):
    stack = pg_stack([{
        "operation": "select",
        "latency": 120,
        "error": {"sqlstate": "57014",
                  "message": "canceling statement due to user request"},
    }])
    sock, _ = pg_connect(stack.port)
    start = time.monotonic()
    msgs = pg_query(sock, "select 1")
    elapsed = time.monotonic() - start
    assert elapsed >= 0.1
    assert msgs[0][0] == b"E"
    assert parse_error_fields(msgs[0][1])["C"] == "57014"
    sock.close()


def test_timeout_hangs_until_client_gives_up(pg_stack):
    stack = pg_stack([{"operation": "select", "timeout": True}])
    sock, _ = pg_connect(stack.port)
    sock.sendall(query_message("select 1"))
    sock.settimeout(1.5)
    with pytest.raises(TimeoutError):
        sock.recv(1024)
    sock.close()
    assert wait_for(lambda: stack.upstream_live == 0)


def test_reset_aborts_connection(pg_stack):
    stack = pg_stack([{"operation": "select", "reset": True}])
    sock, _ = pg_connect(stack.port)
    sock.sendall(query_message("select 1"))
    sock.settimeout(5)
    try:
        data = sock.recv(1024)
        assert data == b""  # FIN or RST — either way the link is dead
    except ConnectionResetError:
        pass
    sock.close()


def test_partial_rows_then_abort(pg_stack):
    stack = pg_stack([{"operation": "select", "partial_rows": 2}])
    sock, _ = pg_connect(stack.port)
    sock.sendall(query_message("select 5"))
    sock.settimeout(5)
    rows = 0
    dead = False
    try:
        while True:
            msg = read_frame_sync(sock)
            if msg is None:
                dead = True
                break
            if msg[0] == b"D":
                rows += 1
    except (ConnectionResetError, EOFError):
        dead = True
    assert dead
    assert rows == 2  # two real rows, then the ResultSet died mid-stream
    sock.close()


def test_fatal_mid_query_closes(pg_stack):
    stack = pg_stack([{
        "operation": "select",
        "error": {"sqlstate": "57P01", "severity": "FATAL",
                  "message": "terminating connection due to administrator "
                             "command"},
    }])
    sock, _ = pg_connect(stack.port)
    sock.sendall(query_message("select 1"))
    msg = read_frame_sync(sock)
    assert msg is not None
    mtype, payload = msg
    assert mtype == b"E"
    fields = parse_error_fields(payload)
    assert fields["C"] == "57P01"
    assert fields["S"] == "FATAL"
    assert read_frame_sync(sock) is None
    sock.close()


# --- extended protocol -----------------------------------------------------------

def test_extended_batch_passthrough(pg_stack):
    stack = pg_stack()
    sock, _ = pg_connect(stack.port)
    sock.sendall(
        parse_message("", "select 1")
        + bind_message("", "")
        + describe_message(b"P")
        + execute_message("")
        + sync_message()
    )
    msgs = read_until_z(sock)
    types = [t for t, _ in msgs]
    assert types == [b"1", b"2", b"T", b"D", b"C", b"Z"]
    sock.close()


def test_extended_batch_error(pg_stack):
    stack = pg_stack([{
        "operation": "select",
        "error": {"sqlstate": "40001"},
    }])
    sock, _ = pg_connect(stack.port)
    sock.sendall(
        parse_message("", "select 1")
        + bind_message("", "")
        + describe_message(b"P")
        + execute_message("")
        + sync_message()
    )
    msgs = read_until_z(sock)
    assert msgs[0][0] == b"E"
    assert parse_error_fields(msgs[0][1])["C"] == "40001"
    assert msgs[1] == (b"Z", b"I")
    # upstream never saw the batch
    assert stack.queries == []
    sock.close()


# --- sql matcher + control plane -----------------------------------------------

def test_sql_regex_matcher(pg_stack):
    stack = pg_stack([{
        "sql": "into orders",
        "error": {"sqlstate": "55P03"},
    }])
    sock, _ = pg_connect(stack.port)
    ok = pg_query(sock, "insert into items values (1)")
    assert ok[0][0] == b"C"  # forwarded — command complete
    hit = pg_query(sock, "insert into orders values (1)")
    assert hit[0][0] == b"E"
    assert parse_error_fields(hit[0][1])["C"] == "55P03"
    sock.close()


def test_control_api_shows_pg_events(pg_stack):
    stack = pg_stack([{
        "service": "postgres",
        "operation": "select",
        "times": 1,
        "error": {"sqlstate": "40001"},
    }])
    sock, _ = pg_connect(stack.port)
    pg_query(sock, "select 1")
    sock.close()

    fired = control_get(stack, "fired?service=postgres")
    assert len(fired) == 1
    event = fired[0]
    assert event["service"] == "postgres"
    assert event["operation"] == "select"
    assert event["action"] == "error:40001"
    assert "select 1" in event["path"]

    rules = control_get(stack, "rules")
    assert rules[0]["service"] == "postgres"
    assert rules[0]["error"]["code"] == "40001"

    health = control_get(stack, "health")
    assert health["status"] == "ok"
    assert health["upstream"].startswith("postgresql://")
