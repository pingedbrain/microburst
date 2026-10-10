"""MySQL data plane — a sibling transport to the HTTP pipeline.

MySQL is a long-lived framed stream: server greets first (Initial
Handshake), the client answers (Handshake Response), auth runs to its
OK/ERR, then a strict request → response command phase — one command
packet in (seq resets to 0), one reply sequence out. The proxy decides
per command: a ``RequestContext`` (``service="mysql"``, ``operation`` =
SQL verb / ``stmt_*`` / ``com_*`` / ``startup``, ``sql`` = query text)
goes through the same ``RuleEngine``, latency knob, ``emit_fired`` path
and control plane as every other transport.

Fidelity rules enforced here (evidence: MYSQL DOCS =
dev.mysql.com/doc/dev/mysql-server protocol pages; the MySQL server
error reference for errno/SQLSTATE pairs; labeled in errors.py):

* ``error:`` → a real ERR_Packet (``0xFF errno '#' sqlstate message``)
  at the reply sequence id the client expects — then nothing: the
  command phase is over. ``severity: FATAL``/``PANIC`` additionally
  closes the connection (a dead session is the only error class safe
  inside an open transaction).
* …but a non-fatal ``error:`` is injected only while the connection is
  NOT in a transaction — tracked from ``SERVER_STATUS_IN_TRANS`` in the
  status flags of every relayed OK/EOF packet. An injected ERR the
  upstream never saw would diverge the session (real ``1213`` rolls the
  tx back; an injected one claims a rollback upstream never performed):
  inside a tx the rule is skipped, the query forwards untouched, and
  the fired event notes ``skipped: in-transaction``.
* ``partial_rows: N`` → relay N real row packets, then TCP-abort. No
  envelope can express "server died mid-ResultSet".
* Startup faults (``operation: startup``) send the ERR as the FIRST
  packet instead of a greeting — evidence: the documented/observed
  shape of a server refusing a session before the handshake exists
  (``1040 Too many connections``, ``1129 Host is blocked``; client
  libraries are required to accept ERR as the first packet). A
  post-handshake-response refusal is the other real shape — not the
  one implemented here.
* TLS is refused, not terminated: ``CLIENT_SSL`` (and compression —
  it re-frames every later packet) are stripped from the relayed
  greeting, so clients fall back to plaintext or refuse client-side
  exactly as against a mysqld without SSL. A client that sends an
  SSLRequest anyway gets a real ERR ``1043/08S01 Bad handshake`` and a
  close (evidence label: inference — 1043 is the documented malformed-
  handshake errno; what a non-SSL server does with an unsolicited
  SSLRequest is not separately documented).
* Auth is passthrough only — the whole greeting → response →
  (auth-switch / more-data)* → OK/ERR exchange is relayed verbatim, so
  caching_sha2_password and mysql_native_password both work; the proxy
  never synthesizes auth success.
* Multi-statement replies (``CLIENT_MULTI_STATEMENTS``) relay
  sub-resultsets until a terminator without
  ``SERVER_MORE_RESULTS_EXISTS``.
* Replication streams (binlog-dump family) and ``COM_STMT_FETCH``
  cursor flows become unbounded server→client streams with ambiguous
  heads — after forwarding, the proxy splices the sockets and stops
  deciding, like redis pub/sub.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import Counter, deque
from dataclasses import dataclass
from types import SimpleNamespace

from aiohttp import web

from microburst.control import handle_control
from microburst.core.context import RequestContext
from microburst.mysql.detect import command_facts, reply_kind
from microburst.mysql.errors import (
    DEFAULT_MESSAGE,
    err_payload,
    resolve_errno_sqlstate,
)
from microburst.mysql.proto import (
    CLIENT_DEPRECATE_EOF,
    CLIENT_PROTOCOL_41,
    COM_QUIT,
    COM_RESET_CONNECTION,
    COM_STMT_CLOSE,
    COM_STMT_PREPARE,
    GREETING_CLEAR_CAPS,
    HEAD_ERR,
    SERVER_MORE_RESULTS_EXISTS,
    SERVER_STATUS_IN_TRANS,
    MysqlProtocolError,
    Packet,
    classify,
    greeting_capabilities,
    is_terminator,
    lenenc_int,
    message_payload,
    packet_bytes,
    parse_handshake_response,
    read_message,
    read_packet,
    status_flags,
    stmt_id_of,
    strip_greeting_caps,
)
from microburst.observe import emit_fired
from microburst.rules import FiredEvent, RuleEngine, describe
from microburst.stats import ProxyStats

logger = logging.getLogger("microburst.mysql")

SERVICE = "mysql"

# Severities that close the session after the ERR — mirrors pg mode;
# a dead connection can't diverge from upstream state.
_FATAL_SEVERITIES = {"FATAL", "PANIC"}

# Cap the auth-exchange relay — a misbehaving peer bouncing auth
# packets forever must not pin the connection.
_MAX_AUTH_ROUNDS = 16

# What real MySQL answers an unsolicited SSLRequest with when SSL is
# off — evidence: ER_HANDSHAKE_ERROR is the documented "bad handshake"
# errno (MYSQL error reference); the packet being a full ERR is
# protocol-required (docs-specified), this specific refusal path is
# inference.
_SSL_REFUSAL = (1043, "08S01", "Bad handshake")


class MysqlProxy:
    """Decision-plane state for mysql mode — the surface control.py reads.

    Mirrors ``pg.PgProxy``: ``engine``/``fired``/``fault_counts``/
    ``stats``/``requests_seen``/``_listeners`` behave identically.
    """

    def __init__(
        self,
        upstream_host: str,
        upstream_port: int,
        rules: list[dict] | None = None,
        fired_capacity: int = 2000,
    ) -> None:
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port
        self.stats = ProxyStats()
        self.engine = RuleEngine()
        if rules:
            self.engine.set_rules(rules)
        self.fired: deque[FiredEvent] = deque(maxlen=fired_capacity)
        self.fault_counts: Counter = Counter()
        self._listeners: set[asyncio.Queue] = set()
        self.requests_seen = 0
        # control.py /health reads .upstream.base_url and .resign
        self.upstream = SimpleNamespace(
            base_url=f"mysql://{upstream_host}:{upstream_port}",
            resign=False,
        )

    def subscribe_fired(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._listeners.add(q)
        return q

    def unsubscribe_fired(self, q: asyncio.Queue) -> None:
        self._listeners.discard(q)

    async def handle_control(self, request: web.Request):
        return await handle_control(self, request)


@dataclass
class _Unit:
    """One decided command-phase unit — the mysql 'request'."""

    packets: list[Packet]
    payload: bytes             # reassembled command payload
    kind: str                  # command | quit | eof
    operation: str | None = None
    resource: str | None = None
    sql: str | None = None
    command: int | None = None


class _ReplyBudget:
    """``cut_reply`` accounting on the reply stream: ``after_bytes``
    bounds relayed wire bytes, ``after_messages`` bounds packets."""

    def __init__(self, after_bytes: int | None, after_messages: int | None):
        self.bytes_left = after_bytes
        self.msgs_left = after_messages

    def take(self, raw: bytes) -> tuple[bytes, bool]:
        """(bytes to write, abort-after-write). Empty + True = abort now."""
        if self.msgs_left is not None:
            if self.msgs_left <= 0:
                return b"", True
            self.msgs_left -= 1
        if self.bytes_left is not None:
            if self.bytes_left <= 0:
                return b"", True
            if len(raw) > self.bytes_left:
                data = raw[: self.bytes_left]
                self.bytes_left = 0
                return data, True
            self.bytes_left -= len(raw)
        return raw, False


class MysqlConnection:
    """One client connection: greeting → auth relay → per-command decide."""

    def __init__(
        self,
        proxy: MysqlProxy,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.proxy = proxy
        self.reader = reader
        self.writer = writer
        self.ureader: asyncio.StreamReader | None = None
        self.uwriter: asyncio.StreamWriter | None = None
        # Negotiated capability flags (client-set ∩ server-advertised).
        self.caps = 0
        # SERVER_STATUS_IN_TRANS from the last status-bearing packet.
        self.in_tx = False
        # prepared statement id → SQL, learned from forwarded
        # COM_STMT_PREPARE text so COM_STMT_EXECUTE resolves `sql:`.
        self.statements: dict[int, str] = {}
        self._pending_prepare_sql: str | None = None

    # -- lifecycle ------------------------------------------------------------

    async def run(self) -> None:
        self.proxy.requests_seen += 1
        self.proxy.stats.record_request(SERVICE)
        # Startup decision BEFORE dialing: a refused mysql connection is
        # an ERR packet instead of the Initial Handshake — upstream need
        # not be touched at all (evidence: the 1040/1129 refusal shape).
        decision = self._decide("startup", None, None)
        if decision is not None and await self._apply_startup(
            decision
        ) == "closed":
            return

        try:
            self.ureader, self.uwriter = await asyncio.open_connection(
                self.proxy.upstream_host, self.proxy.upstream_port
            )
        except OSError as e:
            # Upstream unreachable: the honest shape is a dead link —
            # a refused mysql connection has no greeting to fake.
            logger.debug("mysql upstream connect failed: %r", e)
            return

        if not await self._relay_greeting():
            return
        if not await self._relay_auth():
            return

        while True:
            unit = await self._read_unit()
            if unit.kind == "eof":
                return
            if unit.kind == "quit":
                self._uwrite(unit)
                with contextlib.suppress(Exception):
                    await self.uwriter.drain()
                return
            if await self._serve_unit(unit) == "closed":
                return

    def close(self) -> None:
        for w in (self.writer, self.uwriter):
            if w is not None:
                with contextlib.suppress(Exception):
                    w.close()

    # -- handshake phase ---------------------------------------------------------

    async def _apply_startup(self, decision) -> str:
        """Startup-phase fault: an ERR as the first packet — the
        pre-greeting refusal shape (1040/1129 class)."""
        event = FiredEvent(
            ts=time.time(),
            rule_id=decision.rule.id,
            service=SERVICE,
            operation="startup",
            resource=None,
            region=None,
            action=describe(decision),
            path="connect",
        )
        emit_fired(self.proxy, event)
        if decision.latency_ms:
            await asyncio.sleep(decision.latency_ms / 1000)
        if decision.reset:
            self._abort_client()
            return "closed"
        if decision.timeout_ms is not None:
            if decision.timeout_ms < 0:
                await self._wait_client_eof()
                return "closed"
            await asyncio.sleep(decision.timeout_ms / 1000)
        if decision.error is not None:
            errno, sqlstate = resolve_errno_sqlstate(
                decision.error.errno, decision.error.code
            )
            self.writer.write(
                packet_bytes(
                    err_payload(
                        errno=errno,
                        sqlstate=sqlstate,
                        message=decision.error.message
                        or f"ERROR {errno}: connection refused",
                    ),
                    seq=0,          # replaces the greeting — server seq 0
                )
            )
            with contextlib.suppress(Exception):
                await self.writer.drain()
            return "closed"
        return "ok"

    async def _relay_greeting(self) -> bool:
        """Upstream's Initial Handshake → client, minus TLS/compression.

        Stripping capabilities is the mysql analog of pg's 'N' answer to
        SSLRequest: a client that needs TLS refuses client-side, everyone
        else falls back to plaintext — and compressed framing can never
        silently replace the packet codec downstream.
        """
        assert self.ureader is not None and self.uwriter is not None
        try:
            greeting = await read_packet(self.ureader)
        except (EOFError, ConnectionError, MysqlProtocolError):
            return False
        if greeting is None:
            return False
        payload = greeting.payload
        if payload[:1] == bytes([HEAD_ERR]):
            # Upstream itself refused the session — relay the ERR and
            # let its close propagate; nothing to negotiate.
            self.writer.write(greeting.raw)
            with contextlib.suppress(Exception):
                await self.writer.drain()
            return False
        self.caps = greeting_capabilities(payload)
        self.writer.write(
            packet_bytes(
                strip_greeting_caps(payload, GREETING_CLEAR_CAPS),
                greeting.seq,
            )
        )
        await self.writer.drain()

        # The client's handshake response — TLS requests die here.
        try:
            resp = await read_packet(self.reader)
        except (EOFError, ConnectionError, MysqlProtocolError):
            return False
        if resp is None:
            return False
        info = parse_handshake_response(resp.payload)
        if info is not None:
            self.caps &= info.capabilities
            if info.ssl_request:
                errno, sqlstate, message = _SSL_REFUSAL
                self.writer.write(
                    packet_bytes(
                        err_payload(
                            errno=errno,
                            sqlstate=sqlstate,
                            message=message,
                            protocol_41=bool(
                                info.capabilities & CLIENT_PROTOCOL_41
                            ),
                        ),
                        seq=(resp.seq + 1) & 0xFF,
                    )
                )
                with contextlib.suppress(Exception):
                    await self.writer.drain()
                return False
        self.uwriter.write(resp.raw)
        await self.uwriter.drain()
        return True

    async def _relay_auth(self) -> bool:
        """Relay the rest of the auth exchange until OK or ERR.

        Everything is passthrough — auth-switch (0xFE) and more-data
        (0x01) rounds both work because credential bytes are never
        interpreted, only relayed (caching_sha2_password included).
        """
        assert self.ureader is not None and self.uwriter is not None
        for _ in range(_MAX_AUTH_ROUNDS):
            try:
                pkt = await read_packet(self.ureader)
            except (EOFError, ConnectionError, MysqlProtocolError):
                self._abort_client()
                return False
            if pkt is None:
                return False
            self.writer.write(pkt.raw)
            await self.writer.drain()
            cls = classify(pkt.payload)
            if cls == "ok":
                self._track_status(pkt.payload)
                return True
            if cls == "err":
                # Auth refused — upstream closes; the ERR was relayed.
                return False
            # auth_switch / more-data / anything else expects a reply
            try:
                resp = await read_packet(self.reader)
            except (EOFError, ConnectionError, MysqlProtocolError):
                return False
            if resp is None:
                return False
            self.uwriter.write(resp.raw)
            await self.uwriter.drain()
        return False

    # -- steady state -----------------------------------------------------------

    async def _read_unit(self) -> _Unit:
        """Read one command message → _Unit (multi-packet reassembled)."""
        packets = await read_message(self.reader)
        if packets is None:
            return _Unit([], b"", "eof")
        payload = message_payload(packets)
        command = payload[0] if payload else None
        if command == COM_QUIT:  # forward, close, no decision
            return _Unit(packets, payload, "quit", command=command)
        op, resource, sql, _stmt = command_facts(payload, self.statements)
        return _Unit(
            packets, payload, "command",
            operation=op, resource=resource, sql=sql, command=command,
        )

    def _decide(
        self, operation: str | None, sql: str | None, resource: str | None
    ):
        ctx = RequestContext(
            service=SERVICE,
            operation=operation,
            resource=resource,
            sql=sql,
        )
        return self.proxy.engine.decide(ctx)

    async def _serve_unit(self, unit: _Unit) -> str:
        """decide → effect-or-forward for one command."""
        self.proxy.requests_seen += 1
        self.proxy.stats.record_request(SERVICE)
        decision = self._decide(unit.operation, unit.sql, unit.resource)
        kind = reply_kind(unit.command)
        if decision is None:
            return await self._forward(unit, kind)
        event = FiredEvent(
            ts=time.time(),
            rule_id=decision.rule.id,
            service=SERVICE,
            operation=unit.operation,
            resource=unit.resource,
            region=None,
            action=describe(decision),
            path=(
                unit.sql
                if unit.sql is not None
                else f"0x{unit.command:02x}" if unit.command is not None
                else ""
            )[:80],
        )
        emit_fired(self.proxy, event)

        # Effect order mirrors the other transports: latency → reset →
        # timeout → error → forward (partial_rows/cut_reply act on the
        # response path).
        if decision.latency_ms:
            await asyncio.sleep(decision.latency_ms / 1000)
        if decision.reset:
            self._abort_client()
            return "closed"
        if decision.timeout_ms is not None:
            if decision.timeout_ms < 0:
                await self._wait_client_eof()
                return "closed"
            await asyncio.sleep(decision.timeout_ms / 1000)

        error = decision.error
        if error is not None:
            severity = (error.severity or "").upper()
            reply_seq = (unit.packets[-1].seq + 1) & 0xFF
            if severity in _FATAL_SEVERITIES:
                self._write_err(error, reply_seq)
                with contextlib.suppress(Exception):
                    await self.writer.drain()
                return "closed"
            if kind == "none" or kind == "quit":
                # A reply-less command — a synthesized ERR is one the
                # client never reads; skip rather than lie on the wire.
                event.note = "skipped: command has no reply"
            elif self.in_tx:
                # An injected ERR upstream never saw would diverge the
                # session (a real 1213 rolls the tx back) — skip,
                # forward, say why.
                event.note = "skipped: in-transaction"
            else:
                self._write_err(error, reply_seq)
                await self.writer.drain()
                return "ok"

        return await self._forward(
            unit,
            kind,
            partial_rows=decision.rule.partial_rows,
            cut_bytes=decision.rule.cut_reply_bytes,
            cut_msgs=decision.rule.cut_reply_messages,
            event=event,
        )

    def _write_err(self, error, seq: int) -> None:
        errno, sqlstate = resolve_errno_sqlstate(error.errno, error.code)
        self.writer.write(
            packet_bytes(
                err_payload(
                    errno=errno,
                    sqlstate=sqlstate,
                    message=error.message or DEFAULT_MESSAGE,
                    protocol_41=bool(self.caps & CLIENT_PROTOCOL_41),
                ),
                seq=seq,
            )
        )

    # -- forwarding ---------------------------------------------------------------

    def _uwrite(self, unit: _Unit) -> None:
        assert self.uwriter is not None
        for pkt in unit.packets:
            self.uwriter.write(pkt.raw)

    async def _forward(
        self,
        unit: _Unit,
        kind: str,
        partial_rows: int | None = None,
        cut_bytes: int | None = None,
        cut_msgs: int | None = None,
        event: FiredEvent | None = None,
    ) -> str:
        assert self.uwriter is not None
        # Remember the prepared SQL so a later COM_STMT_PREPARE_OK can
        # bind it to its statement id; CLOSE drops the mapping.
        if unit.command == COM_STMT_PREPARE:
            self._pending_prepare_sql = unit.sql
        elif unit.command == COM_STMT_CLOSE:
            sid = stmt_id_of(unit.payload)
            if sid is not None:
                self.statements.pop(sid, None)
        self._uwrite(unit)
        await self.uwriter.drain()

        if unit.command == COM_RESET_CONNECTION:  # session state resets
            self.in_tx = False
            self.statements.clear()

        if kind in ("none",):
            return "ok"
        if kind == "splice":
            if event is not None:
                event.note = "streaming sub-protocol — passthrough"
            await self._pump()
            return "closed"
        budget = _ReplyBudget(cut_bytes, cut_msgs)
        if kind == "single":
            pkt = await self._upstream_packet()
            if pkt is None:
                return "closed"
            if not await self._send(pkt.raw, budget):
                self._note_cut(event)
                return "closed"
            self._track_status(pkt.payload)
            self.proxy.stats.record_forward()
            return "ok"
        if kind == "prepare":
            return await self._relay_prepare(budget, event)
        return await self._relay_generic(budget, partial_rows, event)

    # -- reply relay ---------------------------------------------------------------

    async def _upstream_packet(self) -> Packet | None:
        """One upstream packet; link failures abort the client."""
        assert self.ureader is not None
        try:
            return await read_packet(self.ureader)
        except (EOFError, ConnectionError, MysqlProtocolError):
            self._abort_client()
            return None

    async def _send(self, raw: bytes, budget: _ReplyBudget) -> bool:
        """Relay ``raw`` under the cut budget → False when aborted."""
        data, abort = budget.take(raw)
        if data:
            self.writer.write(data)
            with contextlib.suppress(Exception):
                await self.writer.drain()
        if abort:
            self._abort_client()
            return False
        return True

    def _track_status(self, payload: bytes) -> int:
        """Update in_tx from an OK/EOF payload → its status flags."""
        flags = status_flags(payload) or 0
        self.in_tx = bool(flags & SERVER_STATUS_IN_TRANS)
        return flags

    async def _relay_generic(
        self,
        budget: _ReplyBudget,
        partial_rows: int | None,
        event: FiredEvent | None,
    ) -> str:
        """Reply relay for a query-ish command, multi-resultset aware.

        First packet classifies: OK/ERR single, 0xFB LOCAL_INFILE
        sub-dialog, anything else a lenenc column count starting a
        ResultSet (N defs, [EOF], rows till a <9-byte 0xFE terminator).
        ``SERVER_MORE_RESULTS_EXISTS`` on the terminator means another
        sub-resultset follows (multi-statement).
        """
        rows = 0
        started = time.monotonic()
        while True:  # sub-resultset loop
            pkt = await self._upstream_packet()
            if pkt is None:
                return "closed"
            if not await self._send(pkt.raw, budget):
                self._note_cut(event)
                return "closed"
            cls = classify(pkt.payload)
            if cls == "err":
                break
            if cls in ("ok", "eof"):
                if not (self._track_status(pkt.payload)
                        & SERVER_MORE_RESULTS_EXISTS):
                    break
                continue
            if cls == "auth_switch":
                # COM_CHANGE_USER re-auth: relay the client's reply and
                # the server's verdict, then the command is done.
                resp = await read_packet(self.reader)
                if resp is None:
                    return "closed"
                assert self.uwriter is not None
                self.uwriter.write(resp.raw)
                await self.uwriter.drain()
                verdict = await self._upstream_packet()
                if verdict is None:
                    return "closed"
                if not await self._send(verdict.raw, budget):
                    self._note_cut(event)
                    return "closed"
                break
            if cls == "infile":
                if not await self._relay_infile(budget):
                    self._note_cut(event)
                    return "closed"
                break
            # ResultSet: lenenc column count, defs, [EOF], rows.
            try:
                col_count, _ = lenenc_int(pkt.payload)
            except MysqlProtocolError:
                col_count = 0
            for _ in range(col_count or 0):
                pkt = await self._upstream_packet()
                if pkt is None:
                    return "closed"
                if not await self._send(pkt.raw, budget):
                    self._note_cut(event)
                    return "closed"
            if not (self.caps & CLIENT_DEPRECATE_EOF):
                pkt = await self._upstream_packet()
                if pkt is None:
                    return "closed"
                if not await self._send(pkt.raw, budget):
                    self._note_cut(event)
                    return "closed"
            while True:
                pkt = await self._upstream_packet()
                if pkt is None:
                    return "closed"
                if is_terminator(pkt.payload):
                    if not await self._send(pkt.raw, budget):
                        self._note_cut(event)
                        return "closed"
                    flags = self._track_status(pkt.payload)
                    if flags & SERVER_MORE_RESULTS_EXISTS:
                        break  # next sub-resultset
                    self.proxy.stats.record_upstream_ms(
                        (time.monotonic() - started) * 1000
                    )
                    self.proxy.stats.record_forward()
                    return "ok"
                if classify(pkt.payload) == "err":
                    if not await self._send(pkt.raw, budget):
                        self._note_cut(event)
                        return "closed"
                    self.proxy.stats.record_forward()
                    return "ok"
                rows += 1
                if partial_rows is not None and rows > partial_rows:
                    self._abort_client()
                    if event is not None:
                        event.note = (
                            f"partial_rows: {partial_rows} rows "
                            "relayed then aborted"
                        )
                    return "closed"
                if not await self._send(pkt.raw, budget):
                    self._note_cut(event)
                    return "closed"
        self.proxy.stats.record_upstream_ms(
            (time.monotonic() - started) * 1000
        )
        self.proxy.stats.record_forward()
        return "ok"

    async def _relay_infile(self, budget: _ReplyBudget) -> bool:
        """LOCAL INFILE sub-dialog: client file packets until an empty
        payload, then the server's single OK/ERR verdict."""
        assert self.uwriter is not None
        while True:
            try:
                pkt = await read_packet(self.reader)
            except (EOFError, ConnectionError, MysqlProtocolError):
                return False
            if pkt is None:
                return False
            self.uwriter.write(pkt.raw)
            await self.uwriter.drain()
            if not pkt.payload:
                break
        verdict = await self._upstream_packet()
        if verdict is None:
            return False
        if not await self._send(verdict.raw, budget):
            return False
        self._track_status(verdict.payload)
        return True

    async def _relay_prepare(
        self, budget: _ReplyBudget, event: FiredEvent | None
    ) -> str:
        """COM_STMT_PREPARE reply: prepare-OK, then param defs + column
        defs (each followed by EOF unless CLIENT_DEPRECATE_EOF). On a
        real prepare-OK, binds the stmt id to the prepared SQL so
        COM_STMT_EXECUTE resolves `sql:` (pg named-statement analog)."""
        started = time.monotonic()
        pkt = await self._upstream_packet()
        if pkt is None:
            return "closed"
        if not await self._send(pkt.raw, budget):
            self._note_cut(event)
            return "closed"
        if classify(pkt.payload) != "ok":
            return "ok"  # ERR — nothing else comes
        stmt_id = int.from_bytes(pkt.payload[1:5], "little") \
            if len(pkt.payload) >= 9 else None
        num_cols = int.from_bytes(pkt.payload[5:7], "little") \
            if len(pkt.payload) >= 9 else 0
        num_params = int.from_bytes(pkt.payload[7:9], "little") \
            if len(pkt.payload) >= 9 else 0
        if stmt_id is not None and self._pending_prepare_sql is not None:
            self.statements[stmt_id] = self._pending_prepare_sql
        self._pending_prepare_sql = None
        deprecate = bool(self.caps & CLIENT_DEPRECATE_EOF)
        for group in (num_params, num_cols):
            for _ in range(group):
                pkt = await self._upstream_packet()
                if pkt is None:
                    return "closed"
                if not await self._send(pkt.raw, budget):
                    self._note_cut(event)
                    return "closed"
            if group and not deprecate:
                pkt = await self._upstream_packet()
                if pkt is None:
                    return "closed"
                if not await self._send(pkt.raw, budget):
                    self._note_cut(event)
                    return "closed"
        self.proxy.stats.record_upstream_ms(
            (time.monotonic() - started) * 1000
        )
        self.proxy.stats.record_forward()
        return "ok"

    async def _pump(self) -> None:
        """Socket splice for streaming sub-protocols — no decisions,
        just bytes, like redis's pub/sub passthrough."""
        assert self.uwriter is not None and self.ureader is not None

        async def pipe(src, dst) -> None:
            try:
                while chunk := await src.read(65536):
                    dst.write(chunk)
                    await dst.drain()
            except (ConnectionError, OSError, asyncio.IncompleteReadError):
                pass
            finally:
                with contextlib.suppress(Exception):
                    dst.write_eof()

        await asyncio.gather(
            pipe(self.reader, self.uwriter),
            pipe(self.ureader, self.writer),
        )

    # -- small helpers ------------------------------------------------------------

    def _note_cut(self, event: FiredEvent | None) -> None:
        if event is not None and event.note is None:
            event.note = "cut_reply: reply stream aborted"

    async def _wait_client_eof(self) -> None:
        """`timeout: true` — hang until the client gives up, draining any
        bytes it still sends (a real hung server never answers)."""
        while await self.reader.read(65536):
            pass

    def _abort_client(self) -> None:
        """TCP RST to the client — mirrors reset.apply on the HTTP path."""
        transport = getattr(self.writer, "transport", None)
        if transport is not None:
            transport.abort()


async def handle_client(
    proxy: MysqlProxy,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """asyncio.start_server callback — one MysqlConnection per client."""
    conn = MysqlConnection(proxy, reader, writer)
    try:
        await conn.run()
    except (EOFError, ConnectionError, MysqlProtocolError) as e:
        logger.debug("mysql connection closed: %r", e)
    except Exception:  # defensive proxy boundary — never leak a client
        logger.exception("mysql connection handler failed")
    finally:
        conn.close()


async def _serve(
    proxy: MysqlProxy,
    host: str,
    port: int,
    control_port: int,
    watch_config: str | None,
) -> None:
    from microburst.app import make_control_app

    server = await asyncio.start_server(
        lambda r, w: handle_client(proxy, r, w), host, port
    )
    app = make_control_app(proxy, watch_config=watch_config)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, control_port)
    await site.start()

    bound = server.sockets[0].getsockname()[1]
    print(
        f"microburst listening on mysql://{host}:{bound} → "
        f"mysql://{proxy.upstream_host}:{proxy.upstream_port}",
        flush=True,
    )
    print(
        f"control API: http://{host}:{control_port}/_microburst/health",
        flush=True,
    )
    print(
        f"point your app: mysql -h {host} -P {bound} -u <user>",
        flush=True,
    )
    async with server:
        await server.serve_forever()


def run_mysql(
    host: str,
    port: int,
    upstream_host: str,
    upstream_port: int,
    control_port: int,
    rules: list[dict] | None,
    watch_config: str | None = None,
) -> int:
    """Entry point for ``microburst --protocol mysql``."""
    proxy = MysqlProxy(upstream_host, upstream_port, rules=rules)
    try:
        asyncio.run(_serve(proxy, host, port, control_port, watch_config))
    except KeyboardInterrupt:
        pass
    return 0
