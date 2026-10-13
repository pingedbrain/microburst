"""Integration tests: mysql wire proxy end to end over real sockets.

Fake upstream: an asyncio TCP server speaking the minimal server
protocol (Initial Handshake → OK auth → per-command canned replies with
IN_TRANS status tracking). The proxy under test and the HTTP control
plane share the same background event loop; test clients are plain
blocking sockets.
"""

from __future__ import annotations

import asyncio
import json
import re
import socket
import threading
import time
import urllib.request

import pytest
from aiohttp import web

from microburst.app import make_control_app
from microburst.mysql.proto import (
    CLIENT_CONNECT_WITH_DB,
    CLIENT_DEPRECATE_EOF,
    CLIENT_PLUGIN_AUTH,
    CLIENT_PROTOCOL_41,
    CLIENT_SECURE_CONNECTION,
    CLIENT_SSL,
    COM_FIELD_LIST,
    COM_INIT_DB,
    COM_PING,
    COM_QUERY,
    COM_QUIT,
    COM_STATISTICS,
    COM_STMT_CLOSE,
    COM_STMT_EXECUTE,
    COM_STMT_PREPARE,
    SERVER_MORE_RESULTS_EXISTS,
    SERVER_STATUS_AUTOCOMMIT,
    SERVER_STATUS_IN_TRANS,
    auth_switch_payload,
    column_definition,
    eof_payload,
    greeting_capabilities,
    initial_handshake,
    is_terminator,
    message_payload,
    ok_payload,
    packet_bytes,
    parse_handshake_response,
    read_message,
    read_packet,
    stmt_prepare_ok,
    text_row,
)
from microburst.mysql.server import MysqlProxy, handle_client

AUTO = SERVER_STATUS_AUTOCOMMIT
CLIENT_CAPS = (
    CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
    | CLIENT_CONNECT_WITH_DB
)


def _term(status: int, deprecate: bool) -> bytes:
    """Row-terminator payload honoring CLIENT_DEPRECATE_EOF."""
    if deprecate:
        # OK_Packet with a 0xFE header (7 bytes — still < 9)
        return (
            b"\xfe\x00\x00" + status.to_bytes(2, "little") + b"\x00\x00"
        )
    return eof_payload(status=status)


def _resultset(n: int, status: int, deprecate: bool) -> list[bytes]:
    """Column count + def + [EOF] + n rows + terminator, as payloads."""
    out = [b"\x01", column_definition("n")]
    if not deprecate:
        out.append(eof_payload(status=status))
    out += [text_row([str(i).encode()]) for i in range(n)]
    out.append(_term(status, deprecate))
    return out


# --- fake upstream ---------------------------------------------------------

async def _fake_upstream(stack: MysqlStack, reader, writer):
    """Minimal MySQL server: greeting, optional auth-switch round, OK
    auth, then per-command canned replies with IN_TRANS tracking."""
    stack.upstream_live += 1
    try:
        caps = (
            CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH | CLIENT_DEPRECATE_EOF | CLIENT_SSL
        )
        writer.write(
            packet_bytes(initial_handshake(capabilities=caps), 0)
        )
        await writer.drain()
        resp = await read_packet(reader)
        if resp is None:
            return
        info = parse_handshake_response(resp.payload)
        client_caps = info.capabilities if info else 0
        deprecate = bool(client_caps & CLIENT_DEPRECATE_EOF)
        if stack.auth_switch:
            writer.write(packet_bytes(auth_switch_payload(), 2))
            await writer.drain()
            resp = await read_packet(reader)
            if resp is None:
                return
        writer.write(packet_bytes(ok_payload(), 2))  # auth OK
        await writer.drain()
        in_tx = False
        while True:
            pkts = await read_message(reader)
            if pkts is None:
                return
            payload = message_payload(pkts)
            cmd = payload[0]
            seq = (pkts[-1].seq + 1) & 0xFF
            out: list[bytes] | None = None
            status = AUTO | (SERVER_STATUS_IN_TRANS if in_tx else 0)
            if cmd == COM_QUIT:
                return
            if cmd == COM_QUERY:
                sql = payload[1:].decode("utf-8", "replace")
                stack.queries.append(sql)
                low = sql.strip().lower()
                if "abort_me" in low:
                    writer.transport.abort()  # hard RST mid-query
                    return
                if low.startswith(("begin", "start transaction")):
                    in_tx = True
                    status = AUTO | SERVER_STATUS_IN_TRANS
                    out = [ok_payload(status=status)]
                elif low.startswith(("commit", "rollback")):
                    in_tx = False
                    status = AUTO
                    out = [ok_payload(status=status)]
                elif low.startswith("select two_parts"):
                    # multi-statement: two resultsets, first terminator
                    # carries SERVER_MORE_RESULTS_EXISTS
                    more = status | SERVER_MORE_RESULTS_EXISTS
                    out = (
                        [b"\x01", column_definition("a")]
                        + ([] if deprecate else [eof_payload(status=status)])
                        + [text_row([b"a"]), _term(more, deprecate)]
                        + [b"\x01", column_definition("b")]
                        + ([] if deprecate else [eof_payload(status=status)])
                        + [text_row([b"b"]), _term(status, deprecate)]
                    )
                elif low.startswith("select infile"):
                    writer.write(
                        packet_bytes(b"\xfb/data/x.csv", seq)
                    )
                    await writer.drain()
                    while True:
                        piece = await read_packet(reader)
                        if piece is None or not piece.payload:
                            break
                    out = [ok_payload(status=status)]
                elif low.startswith("select"):
                    m = re.search(r"\d+", low)
                    n = int(m.group(0)) if m else 1
                    out = _resultset(n, status, deprecate)
                else:
                    out = [ok_payload(status=status)]
            elif cmd == COM_PING or cmd == COM_INIT_DB:
                out = [ok_payload(status=status)]
            elif cmd == COM_STATISTICS:
                out = [b"Uptime: 42  Threads: 1"]   # bare string packet
            elif cmd == COM_FIELD_LIST:
                out = [
                    column_definition("a"), column_definition("b"),
                    _term(status, deprecate),
                ]
            elif cmd == COM_STMT_PREPARE:
                sql = payload[1:].decode("utf-8", "replace")
                stack.prepares.append(sql)
                stack.next_stmt_id += 1
                out = [
                    stmt_prepare_ok(
                        statement_id=stack.next_stmt_id, num_columns=1
                    ),
                    column_definition("n"),
                ]
                if not deprecate:
                    out.append(eof_payload(status=status))
            elif cmd == COM_STMT_EXECUTE:
                stack.executes.append(payload[1:5])
                out = _resultset(1, status, deprecate)
            elif cmd == COM_STMT_CLOSE:
                continue                    # no reply — real server
            else:
                out = [ok_payload(status=status)]
            writer.write(
                b"".join(
                    packet_bytes(p, (seq + i) & 0xFF)
                    for i, p in enumerate(out)
                )
            )
            await writer.drain()
    except (EOFError, ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        stack.upstream_live -= 1
        writer.close()


# --- test stack --------------------------------------------------------------

class MysqlStack:
    """Fake upstream + MysqlProxy + control API on one background loop."""

    def __init__(self, rules: list[dict], auth_switch: bool = False):
        self.rules = rules
        self.auth_switch = auth_switch
        self.loop = asyncio.new_event_loop()
        self.proxy: MysqlProxy | None = None
        self.port: int | None = None
        self.upstream_port: int | None = None
        self.control_port: int | None = None
        self.queries: list[str] = []
        self.prepares: list[str] = []
        self.executes: list[bytes] = []
        self.next_stmt_id = 0
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
            self.proxy = MysqlProxy("127.0.0.1", uport, rules=self.rules)
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

    def start(self) -> MysqlStack:
        self.thread.start()
        assert self._ready.wait(10), "mysql stack did not start"
        return self

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(10)


@pytest.fixture
def mysql_stack():
    started = []

    def _start(rules=None, auth_switch: bool = False):
        stack = MysqlStack(rules or [], auth_switch=auth_switch).start()
        started.append(stack)
        return stack

    yield _start
    for stack in started:
        stack.stop()


# --- synchronous mysql client -------------------------------------------------

def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("peer closed")
        buf += chunk
    return buf


def read_packet_sync(sock: socket.socket):
    head = sock.recv(4)
    if not head:
        return None
    length = int.from_bytes(head[:3], "little")
    payload = _recv_exact(sock, length) if length else b""
    return head[3], payload


def parse_err(payload: bytes) -> tuple[int, str, str]:
    """ERR payload → (errno, sqlstate, message)."""
    assert payload[0] == 0xFF
    errno = int.from_bytes(payload[1:3], "little")
    assert payload[3] == ord("#")
    return errno, payload[4:9].decode(), payload[9:].decode()


def _handshake_response(deprecate: bool) -> bytes:
    caps = CLIENT_CAPS | (CLIENT_DEPRECATE_EOF if deprecate else 0)
    out = (
        caps.to_bytes(4, "little") + (1 << 24).to_bytes(4, "little")
        + b"\x21" + b"\x00" * 23
    )
    out += b"tester\x00"
    out += b"\x00"                              # empty auth response
    out += b"shop\x00"                          # CONNECT_WITH_DB
    out += b"mysql_native_password\x00"
    return out


def mysql_connect(
    port: int, deprecate: bool = True, timeout: float = 10.0
) -> tuple[socket.socket, bytes]:
    """Full handshake → (sock, greeting payload). Assumes OK auth."""
    sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    msg = read_packet_sync(sock)
    assert msg is not None
    _seq, greeting = msg
    sock.sendall(packet_bytes(_handshake_response(deprecate), 1))
    reply = read_packet_sync(sock)
    assert reply is not None and reply[1][0] == 0x00  # auth OK
    return sock, greeting


def mysql_command(sock: socket.socket, cmd: int, arg: bytes = b""):
    sock.sendall(packet_bytes(bytes([cmd]) + arg, 0))


def read_reply(
    sock: socket.socket, deprecate: bool = True
) -> list[tuple[int, bytes]]:
    """Read one full command reply (multi-resultset aware).

    Mirrors the proxy's own relay logic: OK/ERR is a single packet;
    anything else is a lenenc column count starting a ResultSet (N defs,
    a defs EOF in non-deprecate mode, then rows until the <9-byte 0xFE
    terminator). A terminator carrying SERVER_MORE_RESULTS_EXISTS means
    the next sub-resultset follows.
    """
    from microburst.mysql.proto import lenenc_int, status_flags

    out = []

    def read() -> tuple[int, bytes]:
        pkt = read_packet_sync(sock)
        assert pkt is not None, "connection closed mid-reply"
        return pkt

    while True:
        first = read()
        out.append(first)
        head = first[1][0] if first[1] else -1
        if head == 0xFF:
            return out
        if head == 0x00:
            if (status_flags(first[1]) or 0) & SERVER_MORE_RESULTS_EXISTS:
                continue
            return out
        if head == 0xFB:
            return out  # LOCAL_INFILE — caller drives the sub-dialog
        count, _ = lenenc_int(first[1])
        for _ in range(count or 0):
            out.append(read())
        if not deprecate:
            out.append(read())          # column-defs EOF
        while True:
            pkt = read()
            out.append(pkt)
            payload = pkt[1]
            if payload[:1] == b"\xff":
                return out
            if is_terminator(payload):
                if (status_flags(payload) or 0) & SERVER_MORE_RESULTS_EXISTS:
                    break             # next sub-resultset
                return out


def wait_for(cond, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.02)
    return cond()


def control_get(stack: MysqlStack, path: str):
    url = f"http://127.0.0.1:{stack.control_port}/_microburst/{path}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read())


# --- passthrough --------------------------------------------------------------

def test_passthrough_handshake_and_query(mysql_stack):
    stack = mysql_stack()
    sock, greeting = mysql_connect(stack.port)
    # the relayed greeting had TLS stripped but kept everything else
    caps = greeting_capabilities(greeting)
    assert not (caps & CLIENT_SSL)
    assert caps & CLIENT_PLUGIN_AUTH

    mysql_command(sock, COM_QUERY, b"select 3")
    reply = read_reply(sock)
    payloads = [p for _, p in reply]
    assert payloads[0] == b"\x01"                  # column count
    rows = [p for p in payloads if p[:1] == b"\x01" and len(p) == 2]
    assert len(rows) == 3
    assert is_terminator(payloads[-1])
    assert stack.queries == ["select 3"]
    sock.close()


def test_deprecate_eof_false_relays_eof_packets(mysql_stack):
    stack = mysql_stack()
    sock, _ = mysql_connect(stack.port, deprecate=False)
    mysql_command(sock, COM_QUERY, b"select 2")
    reply = read_reply(sock, deprecate=False)
    kinds = [p[0] for _, p in reply]
    # count, coldef, EOF, row, row, EOF
    assert kinds == [0x01, 0x03, 0xFE, 0x01, 0x01, 0xFE]
    sock.close()


def test_auth_switch_passthrough(mysql_stack):
    """A mid-auth AuthSwitchRequest relays the client's reply and does
    not deadlock — the caching_sha2 shape."""
    stack = mysql_stack(auth_switch=True)
    sock = socket.create_connection(("127.0.0.1", stack.port), timeout=10)
    msg = read_packet_sync(sock)
    assert msg is not None
    sock.sendall(packet_bytes(_handshake_response(True), 1))
    reply = read_packet_sync(sock)
    assert reply is not None and reply[1][0] == 0xFE  # auth switch
    sock.sendall(packet_bytes(b"auth-data", 3))
    reply = read_packet_sync(sock)
    assert reply is not None and reply[1][0] == 0x00  # auth OK
    mysql_command(sock, COM_PING)
    reply = read_reply(sock)
    assert reply[0][1][0] == 0x00
    sock.close()


def test_ssl_request_refused_with_bad_handshake(mysql_stack):
    """A client that ignores the stripped caps and sends an SSLRequest
    anyway gets a real ERR 1043/08S01, then a close."""
    stack = mysql_stack()
    sock = socket.create_connection(("127.0.0.1", stack.port), timeout=10)
    msg = read_packet_sync(sock)
    assert msg is not None
    caps = CLIENT_PROTOCOL_41 | CLIENT_SSL
    ssl_req = (
        caps.to_bytes(4, "little") + (1 << 24).to_bytes(4, "little")
        + b"\x21" + b"\x00" * 23
    )
    sock.sendall(packet_bytes(ssl_req, 1))
    reply = read_packet_sync(sock)
    assert reply is not None
    errno, sqlstate, _msg = parse_err(reply[1])
    assert (errno, sqlstate) == (1043, "08S01")
    assert read_packet_sync(sock) is None
    sock.close()


def test_quit_and_clean_eof_close_upstream(mysql_stack):
    stack = mysql_stack()
    sock, _ = mysql_connect(stack.port)
    mysql_command(sock, COM_QUIT)
    sock.close()
    assert wait_for(lambda: stack.upstream_live == 0)

    sock2, _ = mysql_connect(stack.port)
    sock2.close()
    assert wait_for(lambda: stack.upstream_live == 0)


def test_upstream_rst_relayed_to_client(mysql_stack):
    stack = mysql_stack()
    sock, _ = mysql_connect(stack.port)
    mysql_command(sock, COM_QUERY, b"select abort_me")
    try:
        msg = read_packet_sync(sock)
        assert msg is None  # dead link answers dead link
    except (ConnectionResetError, EOFError):
        pass
    sock.close()


def test_com_statistics_bare_string_reply(mysql_stack):
    """COM_STATISTICS answers a bare string — the 'single' reply shape
    must not be misread as a column count."""
    stack = mysql_stack()
    sock, _ = mysql_connect(stack.port)
    mysql_command(sock, COM_STATISTICS)
    reply = read_packet_sync(sock)
    assert reply is not None
    assert reply[1] == b"Uptime: 42  Threads: 1"
    # connection still works
    mysql_command(sock, COM_PING)
    assert read_reply(sock)[0][1][0] == 0x00
    sock.close()


def test_multi_statement_relays_all_subresults(mysql_stack):
    stack = mysql_stack()
    sock, _ = mysql_connect(stack.port, deprecate=False)
    mysql_command(sock, COM_QUERY, b"select two_parts")
    reply = read_reply(sock, deprecate=False)
    # two full resultsets relayed: count/def/eof/row/term each
    assert len(reply) == 10
    assert reply[4][1][:1] == b"\xfe"  # first terminator (MORE flag)
    assert is_terminator(reply[-1][1])
    sock.close()


def test_local_infile_dialog(mysql_stack):
    stack = mysql_stack()
    sock, _ = mysql_connect(stack.port)
    mysql_command(sock, COM_QUERY, b"select infile")
    msg = read_packet_sync(sock)
    assert msg is not None and msg[1][0] == 0xFB  # infile request
    sock.sendall(packet_bytes(b"1,foo\n", msg[0] + 1))
    sock.sendall(packet_bytes(b"", msg[0] + 2))   # end-of-data
    reply = read_packet_sync(sock)
    assert reply is not None and reply[1][0] == 0x00
    sock.close()


# --- startup faults --------------------------------------------------------------

def test_startup_err_first_packet_too_many_connections(mysql_stack):
    stack = mysql_stack([{
        "service": "mysql",
        "operation": "startup",
        "error": {
            "errno": 1040, "code": "08004",
            "message": "Too many connections",
        },
    }])
    sock = socket.create_connection(("127.0.0.1", stack.port), timeout=10)
    msg = read_packet_sync(sock)
    assert msg is not None
    seq, payload = msg
    assert seq == 0  # ERR replaces the greeting — the 1040 shape
    errno, sqlstate, message = parse_err(payload)
    assert (errno, sqlstate) == (1040, "08004")
    assert "Too many connections" in message
    assert read_packet_sync(sock) is None
    sock.close()


def test_startup_reset_and_latency(mysql_stack):
    stack = mysql_stack([{
        "operation": "startup", "latency": 80,
        "error": {"errno": 1129, "message": "host blocked"},
    }])
    start = time.monotonic()
    sock = socket.create_connection(("127.0.0.1", stack.port), timeout=10)
    msg = read_packet_sync(sock)
    assert time.monotonic() - start >= 0.07
    assert msg is not None
    errno, _, _ = parse_err(msg[1])
    assert errno == 1129  # unmapped errno → HY000 sqlstate
    sock.close()


# --- query faults --------------------------------------------------------------

def test_error_errno_maps_sqlstate_and_survives(mysql_stack):
    stack = mysql_stack([{
        "service": "mysql",
        "operation": "select",
        "times": 1,
        "error": {"errno": 1064,
                  "message": "You have an error in your SQL syntax"},
    }])
    sock, _ = mysql_connect(stack.port)
    mysql_command(sock, COM_QUERY, b"select 1")
    reply = read_packet_sync(sock)
    assert reply is not None
    seq, payload = reply
    assert seq == 1  # command seq 0 → reply seq 1
    errno, sqlstate, message = parse_err(payload)
    assert (errno, sqlstate) == (1064, "42000")
    assert "syntax" in message

    # same connection still works — the error consumed the rule
    mysql_command(sock, COM_QUERY, b"select 1")
    assert read_reply(sock)[0][1] == b"\x01"
    assert stack.queries == ["select 1"]  # first never reached upstream
    sock.close()


def test_error_code_maps_errno(mysql_stack):
    stack = mysql_stack([{
        "operation": "select",
        "error": {"code": "40001", "message": "Deadlock found"},
    }])
    sock, _ = mysql_connect(stack.port)
    mysql_command(sock, COM_QUERY, b"select 1")
    reply = read_packet_sync(sock)
    assert reply is not None
    errno, sqlstate, message = parse_err(reply[1])
    assert (errno, sqlstate) == (1213, "40001")
    assert "Deadlock" in message
    sock.close()


def test_error_skipped_in_transaction(mysql_stack):
    stack = mysql_stack([{
        "operation": "select",
        "error": {"errno": 1213, "message": "Deadlock found"},
    }])
    sock, _ = mysql_connect(stack.port)
    mysql_command(sock, COM_QUERY, b"begin")
    reply = read_packet_sync(sock)
    assert reply is not None
    flags = int.from_bytes(reply[1][3:5], "little")
    assert flags & SERVER_STATUS_IN_TRANS

    mysql_command(sock, COM_QUERY, b"select 1")
    reply = read_reply(sock)
    assert reply[0][1] == b"\x01"   # real resultset, not an ERR
    assert "select 1" in stack.queries
    sock.close()

    assert wait_for(lambda: len(stack.proxy.fired) == 1)
    event = stack.proxy.fired[0]
    assert event.note == "skipped: in-transaction"
    assert event.operation == "select"


def test_fatal_severity_closes_after_err(mysql_stack):
    stack = mysql_stack([{
        "operation": "select",
        "error": {"errno": 1153, "severity": "FATAL",
                  "message": "Got a packet bigger than 'max_allowed_packet'"},
    }])
    sock, _ = mysql_connect(stack.port)
    mysql_command(sock, COM_QUERY, b"select 1")
    msg = read_packet_sync(sock)
    assert msg is not None
    errno, sqlstate, _ = parse_err(msg[1])
    assert (errno, sqlstate) == (1153, "08S01")
    assert read_packet_sync(sock) is None
    sock.close()


def test_latency_then_error(mysql_stack):
    stack = mysql_stack([{
        "operation": "select",
        "latency": 120,
        "error": {"errno": 1205,
                  "message": "Lock wait timeout exceeded"},
    }])
    sock, _ = mysql_connect(stack.port)
    start = time.monotonic()
    mysql_command(sock, COM_QUERY, b"select 1")
    reply = read_reply(sock)
    assert time.monotonic() - start >= 0.1
    errno, sqlstate, _ = parse_err(reply[-1][1])
    assert (errno, sqlstate) == (1205, "HY000")
    sock.close()


def test_timeout_hangs_until_client_gives_up(mysql_stack):
    stack = mysql_stack([{"operation": "select", "timeout": True}])
    sock, _ = mysql_connect(stack.port)
    mysql_command(sock, COM_QUERY, b"select 1")
    sock.settimeout(1.5)
    with pytest.raises(TimeoutError):
        sock.recv(1024)
    sock.close()
    assert wait_for(lambda: stack.upstream_live == 0)


def test_reset_aborts_connection(mysql_stack):
    stack = mysql_stack([{"operation": "select", "reset": True}])
    sock, _ = mysql_connect(stack.port)
    mysql_command(sock, COM_QUERY, b"select 1")
    sock.settimeout(5)
    try:
        data = sock.recv(1024)
        assert data == b""
    except ConnectionResetError:
        pass
    sock.close()


def test_partial_rows_then_abort(mysql_stack):
    stack = mysql_stack([{"operation": "select", "partial_rows": 2}])
    sock, _ = mysql_connect(stack.port)
    mysql_command(sock, COM_QUERY, b"select 5")
    sock.settimeout(5)
    rows = 0
    dead = False
    try:
        while True:
            msg = read_packet_sync(sock)
            if msg is None:
                dead = True
                break
            p = msg[1]
            if len(p) == 2 and p[:1] == b"\x01" and p[1:2].isdigit():
                rows += 1
    except (ConnectionResetError, EOFError):
        dead = True
    assert dead
    assert rows == 2  # two real rows, then the ResultSet died mid-stream
    sock.close()


def test_cut_reply_after_messages(mysql_stack):
    stack = mysql_stack([{
        "operation": "select", "cut_reply": {"after_messages": 2},
    }])
    sock, _ = mysql_connect(stack.port)
    mysql_command(sock, COM_QUERY, b"select 5")
    sock.settimeout(5)
    got = 0
    dead = False
    try:
        while True:
            msg = read_packet_sync(sock)
            if msg is None:
                dead = True
                break
            got += 1
    except (ConnectionResetError, EOFError):
        dead = True
    assert dead and got == 2
    sock.close()


# --- extended protocol -----------------------------------------------------------

def test_stmt_prepare_then_execute_passthrough(mysql_stack):
    stack = mysql_stack()
    sock, _ = mysql_connect(stack.port, deprecate=False)
    mysql_command(sock, COM_STMT_PREPARE, b"select * from orders")
    reply = read_packet_sync(sock)
    assert reply is not None and reply[1][0] == 0x00
    stmt_id = int.from_bytes(reply[1][1:5], "little")
    # column def + terminator follow the prepare-OK
    _def = read_packet_sync(sock)
    term = read_packet_sync(sock)
    assert term is not None and is_terminator(term[1])

    mysql_command(
        sock, COM_STMT_EXECUTE,
        stmt_id.to_bytes(4, "little") + b"\x00\x01\x00\x00\x00",
    )
    reply = read_reply(sock)
    assert is_terminator(reply[-1][1])
    mysql_command(sock, COM_STMT_CLOSE, stmt_id.to_bytes(4, "little"))
    assert stack.prepares == ["select * from orders"]
    sock.close()


def test_sql_matcher_hits_stmt_execute(mysql_stack):
    """COM_STMT_EXECUTE carries only an id — the proxy's stmt map makes
    sql: rules match executes (the pg named-statement analog)."""
    stack = mysql_stack([{
        "operation": "stmt_execute",
        "sql": "into orders",
        "error": {"errno": 1213, "message": "Deadlock found"},
    }])
    sock, _ = mysql_connect(stack.port, deprecate=False)
    mysql_command(sock, COM_STMT_PREPARE, b"insert into orders values (?)")
    reply = read_packet_sync(sock)
    assert reply is not None and reply[1][0] == 0x00
    stmt_id = int.from_bytes(reply[1][1:5], "little")
    read_packet_sync(sock)   # col def
    read_packet_sync(sock)   # terminator
    mysql_command(
        sock, COM_STMT_EXECUTE,
        stmt_id.to_bytes(4, "little") + b"\x00\x01\x00\x00\x00",
    )
    msg = read_packet_sync(sock)
    assert msg is not None
    errno, sqlstate, _ = parse_err(msg[1])
    assert (errno, sqlstate) == (1213, "40001")
    sock.close()
    assert wait_for(lambda: len(stack.proxy.fired) == 1)
    assert "insert into orders" in stack.proxy.fired[0].path
    assert stack.executes == []   # never reached upstream


def test_stmt_execute_unknown_id_no_match(mysql_stack):
    stack = mysql_stack([{
        "sql": "orders",
        "error": {"errno": 1213},
    }])
    sock, _ = mysql_connect(stack.port)
    mysql_command(
        sock, COM_STMT_EXECUTE,
        (99).to_bytes(4, "little") + b"\x00\x01\x00\x00\x00",
    )
    reply = read_reply(sock)  # no sql resolved → forwards normally
    assert is_terminator(reply[-1][1])
    sock.close()


# --- sql matcher + control plane -----------------------------------------------

def test_sql_regex_matcher_and_com_operations(mysql_stack):
    stack = mysql_stack([{
        "sql": "into orders",
        "error": {"errno": 1205, "message": "lock wait timeout"},
    }])
    sock, _ = mysql_connect(stack.port)
    mysql_command(sock, COM_QUERY, b"insert into items values (1)")
    assert read_reply(sock)[0][1][0] == 0x00   # forwarded OK
    mysql_command(sock, COM_QUERY, b"insert into orders values (1)")
    msg = read_packet_sync(sock)
    assert msg is not None
    errno, _, _ = parse_err(msg[1])
    assert errno == 1205
    mysql_command(sock, COM_INIT_DB, b"shop2")
    assert read_reply(sock)[0][1][0] == 0x00
    sock.close()


def test_control_api_shows_mysql_events(mysql_stack):
    stack = mysql_stack([{
        "service": "mysql",
        "operation": "select",
        "times": 1,
        "error": {"errno": 1213},
    }])
    sock, _ = mysql_connect(stack.port)
    mysql_command(sock, COM_QUERY, b"select 1")
    read_packet_sync(sock)
    sock.close()

    fired = control_get(stack, "fired?service=mysql")
    assert len(fired) == 1
    event = fired[0]
    assert event["service"] == "mysql"
    assert event["operation"] == "select"
    assert event["action"] == "error:errno=1213"
    assert "select 1" in event["path"]

    rules = control_get(stack, "rules")
    assert rules[0]["service"] == "mysql"
    assert rules[0]["error"]["errno"] == 1213

    health = control_get(stack, "health")
    assert health["status"] == "ok"
    assert health["upstream"].startswith("mysql://")


def test_no_reply_command_error_skips(mysql_stack):
    """An error rule on COM_STMT_CLOSE can't render — the command has
    no reply; it forwards with an explanatory note."""
    stack = mysql_stack([{
        "operation": "stmt_close",
        "error": {"errno": 1243},
    }])
    sock, _ = mysql_connect(stack.port, deprecate=False)
    mysql_command(sock, COM_STMT_PREPARE, b"select 1")
    reply = read_packet_sync(sock)
    stmt_id = int.from_bytes(reply[1][1:5], "little")
    read_packet_sync(sock)
    read_packet_sync(sock)
    mysql_command(sock, COM_STMT_CLOSE, stmt_id.to_bytes(4, "little"))
    sock.settimeout(2)
    mysql_command(sock, COM_PING)   # still healthy — close forwarded
    assert read_reply(sock)[0][1][0] == 0x00
    sock.close()
    assert wait_for(lambda: len(stack.proxy.fired) == 1)
    assert stack.proxy.fired[0].note == "skipped: command has no reply"
