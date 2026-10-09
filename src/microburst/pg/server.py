"""PostgreSQL data plane — a sibling transport to the HTTP pipeline.

The HTTP pipeline is one-request → one-response; PG is a long-lived
bidirectional framed stream. What the two share is the decision plane:
each query unit gets a ``RequestContext`` (``service="postgres"``,
``operation`` = SQL verb, ``resource`` = table-ish token, ``sql`` = raw
text) and goes through the same ``RuleEngine``, the same latency knob,
and the same ``emit_fired`` path — so rules, ``/_microburst/fired``,
metrics and stats work identically in both modes.

Fidelity rules enforced here (postgresql.org/docs/protocol-flow):

* ``severity: FATAL``/``PANIC`` → ErrorResponse then close. That close
  is what real PG does — and it's the only error class safe to inject
  inside an open transaction, because a dead connection can't diverge
  from upstream state.
* Any other severity → only injected while the upstream's last
  ReadyForQuery says ``I`` (idle). Inside ``T``/``E`` the rule is
  skipped and the query forwards untouched — an injected ERROR the
  upstream never saw would strand the client in a failed-transaction
  state the server doesn't share. The fired event's ``note`` records
  ``skipped: in-transaction``.
* ``partial_rows: N`` → relay N real DataRow frames, then TCP-abort.
  No protocol envelope can express "server died mid-ResultSet"; the
  link simply has to die.
* Auth is passthrough only (SCRAM included) — the proxy never
  synthesizes credentials; AuthenticationOk is upstream's to send.

Startup negotiation handles ``SSLRequest``/``GSSENCRequest`` with 'N'
(refuse) and keeps waiting — real drivers require this before the real
startup packet arrives. ``CancelRequest`` is forwarded and both ends
close (no reply is defined for it).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import struct
import time
from collections import Counter, deque
from dataclasses import dataclass
from types import SimpleNamespace

from aiohttp import web

from microburst.control import handle_control
from microburst.core.context import RequestContext
from microburst.observe import emit_fired
from microburst.pg.detect import sql_facts
from microburst.pg.errors import error_response
from microburst.pg.proto import (
    CANCEL_REQUEST_CODE,
    GSSENC_REQUEST_CODE,
    SSL_REQUEST_CODE,
    PgProtocolError,
    frame,
    parse_startup_params,
    read_cstring,
    read_frame,
    read_startup,
    ready_for_query,
    startup_wire,
)
from microburst.rules import FiredEvent, RuleEngine, describe
from microburst.stats import ProxyStats

logger = logging.getLogger("microburst.pg")

SERVICE = "postgres"

# Frontend messages that form an extended-protocol batch: accumulate
# until Sync ('S'). A Flush ('H') arriving alone is relayed by itself.
_EXTENDED = {b"P", b"B", b"D", b"E", b"C", b"S"}

# Severity that closes the session after the ErrorResponse — real PG
# behavior for FATAL and PANIC.
_FATAL_SEVERITIES = {"FATAL", "PANIC"}

# Authentication request codes (the Int32 inside 'R') that the client
# answers with a 'p' message: cleartext 3, MD5 5, GSS 7, GSSContinue 8,
# SASL 10, SASLContinue 11. Notably NOT 12 (SASLFinal — server-side
# final step, no reply) or 0 (AuthOk).
_AUTH_EXPECTS_REPLY = {3, 5, 7, 8, 10, 11}


@dataclass
class _Unit:
    """One decided frontend unit — the pg 'request'."""
    messages: list[tuple[bytes, bytes]]
    kind: str           # query | batch | call | flush | passthrough | terminate | eof
    sql: str | None = None


class PgProxy:
    """Decision-plane state for pg mode — the surface control.py reads.

    Mirrors the parts of ``Microburst`` the control API touches, without
    the HTTP upstream client: ``engine``/``fired``/``fault_counts``/
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
            base_url=f"postgresql://{upstream_host}:{upstream_port}",
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


class PgConnection:
    """One client connection: startup → auth relay → per-unit decide loop."""

    def __init__(
        self,
        proxy: PgProxy,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.proxy = proxy
        self.reader = reader
        self.writer = writer
        self.ureader: asyncio.StreamReader | None = None
        self.uwriter: asyncio.StreamWriter | None = None
        # Last ReadyForQuery status byte the upstream sent ('I'/'T'/'E').
        self.tx = b"I"
        # Prepared-statement name → SQL, learned from Parse messages so a
        # Parse-less Bind/Execute batch still resolves an operation.
        self.statements: dict[str, str] = {}

    # -- lifecycle ------------------------------------------------------------

    async def run(self) -> None:
        code, body = await self._negotiate_startup()
        if code is None:
            return
        if code == CANCEL_REQUEST_CODE:
            await self._relay_cancel(body)
            return

        params = parse_startup_params(body)
        path = (
            f"connect user={params.get('user', '?')} "
            f"db={params.get('database', '?')}"
        )
        self.proxy.requests_seen += 1
        self.proxy.stats.record_request(SERVICE)
        resource = params.get("database")
        decision = self._decide("startup", sql=None, resource=resource)
        if decision is not None and await self._apply_startup(
            decision, path, resource
        ) == "closed":
            return

        try:
            self.ureader, self.uwriter = await asyncio.open_connection(
                self.proxy.upstream_host, self.proxy.upstream_port
            )
        except OSError as e:
            # Upstream unreachable: behave like the server vanished
            # mid-handshake — no synthesized error, just a dead link.
            logger.debug("pg upstream connect failed: %r", e)
            return

        self.uwriter.write(startup_wire(body))
        await self.uwriter.drain()
        if not await self._relay_auth():
            return

        while True:
            unit = await self._read_unit()
            if unit.kind == "eof":
                return
            if unit.kind == "terminate":
                self._uwrite_all(unit.messages)
                with contextlib.suppress(Exception):
                    await self.uwriter.drain()
                return
            if unit.kind in ("flush", "passthrough"):
                self._uwrite_all(unit.messages)
                await self.uwriter.drain()
                continue
            if await self._serve_unit(unit) == "closed":
                return

    def close(self) -> None:
        for w in (self.writer, self.uwriter):
            if w is not None:
                with contextlib.suppress(Exception):
                    w.close()

    # -- startup phase ----------------------------------------------------------

    async def _negotiate_startup(self) -> tuple[int | None, bytes]:
        """Read the real startup packet, refusing SSL/GSS upgrade requests.

        Drivers (libpq among them) send SSLRequest/GSSENCRequest first;
        replying 'N' keeps the session on plaintext — the MVP terminates
        no TLS, matching the unencrypted local dev workflow.
        """
        first = await read_startup(self.reader)
        if first is None:
            return None, b""
        code, body = first
        while code in (SSL_REQUEST_CODE, GSSENC_REQUEST_CODE):
            self.writer.write(b"N")
            await self.writer.drain()
            nxt = await read_startup(self.reader)
            if nxt is None:
                return None, b""
            code, body = nxt
        return code, body

    async def _relay_cancel(self, body: bytes) -> None:
        """CancelRequest: forward verbatim, close both ends — no reply."""
        try:
            _ur, uw = await asyncio.open_connection(
                self.proxy.upstream_host, self.proxy.upstream_port
            )
        except OSError:
            return
        uw.write(startup_wire(body))
        with contextlib.suppress(Exception):
            await uw.drain()
        uw.close()

    async def _apply_startup(
        self, decision, path: str, resource: str | None
    ) -> str:
        """Startup-phase fault. Any error here ends the session — real PG
        never proceeds past a refused connection (53300 et al. are FATAL
        on the wire precisely because there is no session to recover)."""
        event = FiredEvent(
            ts=time.time(),
            rule_id=decision.rule.id,
            service=SERVICE,
            operation="startup",
            resource=resource,
            region=None,
            action=describe(decision),
            path=path,
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
            sev = (decision.error.severity or "FATAL").upper()
            self.writer.write(
                error_response(
                    sqlstate=decision.error.code or "53300",
                    message=decision.error.message or "connection refused",
                    severity=sev,
                    fields=decision.error.fields,
                )
            )
            with contextlib.suppress(Exception):
                await self.writer.drain()
            return "closed"
        return "ok"

    async def _relay_auth(self) -> bool:
        """Upstream-driven auth exchange until the first ReadyForQuery.

        Everything is passthrough — trust, password, and SCRAM all work
        because credential bytes are never interpreted, only relayed.
        """
        assert self.ureader is not None and self.uwriter is not None
        while True:
            try:
                msg = await read_frame(self.ureader)
            except (EOFError, ConnectionError, PgProtocolError):
                self._abort_client()
                return False
            if msg is None:
                return False
            mtype, payload = msg
            self.writer.write(frame(mtype, payload))
            await self.writer.drain()
            if mtype == b"Z":
                self.tx = payload[:1]
                return True
            if mtype == b"R" and len(payload) >= 4:
                (auth_code,) = struct.unpack("!I", payload[:4])
                if auth_code in _AUTH_EXPECTS_REPLY:
                    resp = await read_frame(self.reader)
                    if resp is None:
                        return False
                    self.uwriter.write(frame(*resp))
                    await self.uwriter.drain()
            # N (notice), S (ParameterStatus), K (key data), E (refusal —
            # upstream will close; the next read sees it) need no reply.

    # -- steady state -----------------------------------------------------------

    async def _read_unit(self) -> _Unit:
        """Read one decidable frontend unit.

        A batch is everything from Parse/Bind/Describe/Execute/Close up
        to and including Sync — one decision covers it because the SQL
        text travels in the Parse. Everything else is forwarded verbatim.
        """
        msg = await read_frame(self.reader)
        if msg is None:
            return _Unit([], "eof")
        mtype, payload = msg
        if mtype == b"Q":
            sql, _ = read_cstring(payload)
            return _Unit([msg], "query", sql=sql)
        if mtype == b"X":
            return _Unit([msg], "terminate")
        if mtype == b"F":
            return _Unit([msg], "call")
        if mtype == b"H":
            return _Unit([msg], "flush")
        if mtype in _EXTENDED:
            messages = [msg]
            while messages[-1][0] != b"S":
                nxt = await read_frame(self.reader)
                if nxt is None:
                    return _Unit(messages, "eof")
                messages.append(nxt)
                if nxt[0] == b"X":  # Terminate inside a batch — rare
                    return _Unit(messages, "terminate")
            return _Unit(messages, "batch", sql=self._batch_sql(messages))
        # 'd'/'c'/'f' COPY frames, 'p', unknown types: relay verbatim —
        # still parseable, so the documented fallback is passthrough.
        return _Unit([msg], "passthrough")

    def _batch_sql(self, messages: list[tuple[bytes, bytes]]) -> str | None:
        """SQL for a batch: Parse payload first, else the named-statement
        map, else a Bind that references a known statement."""
        sql = None
        bind_stmt = None
        for mtype, payload in messages:
            try:
                if mtype == b"P":
                    name, off = read_cstring(payload)
                    query, _ = read_cstring(payload, off)
                    self.statements[name] = query
                    sql = query
                elif mtype == b"B":
                    _portal, off = read_cstring(payload)
                    bind_stmt, _ = read_cstring(payload, off)
            except (ValueError, IndexError):
                continue  # malformed inner payload — relay handles it
        if sql is None and bind_stmt is not None:
            sql = self.statements.get(bind_stmt)
        return sql

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

    def _decide_for(self, unit: _Unit):
        verb, table = sql_facts(unit.sql) if unit.sql else (None, None)
        return verb, table, self._decide(verb, unit.sql, table)

    async def _serve_unit(self, unit: _Unit) -> str:
        """decide → effect-or-forward for one query unit."""
        self.proxy.requests_seen += 1
        self.proxy.stats.record_request(SERVICE)
        verb, table, decision = self._decide_for(unit)
        if decision is None:
            return await self._forward_unit(unit)
        event = FiredEvent(
            ts=time.time(),
            rule_id=decision.rule.id,
            service=SERVICE,
            operation=verb,
            resource=table,
            region=None,
            action=describe(decision),
            path=(unit.sql or "")[:80],
        )
        emit_fired(self.proxy, event)

        # Effect order mirrors HTTP: latency → reset → timeout → error
        # → forward (with partial_rows acting on the response path).
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
            severity = (error.severity or "ERROR").upper()
            if severity in _FATAL_SEVERITIES:
                self.writer.write(
                    error_response(
                        sqlstate=error.code or "XX000",
                        message=error.message or "injected fault",
                        severity=severity,
                        fields=error.fields,
                    )
                )
                with contextlib.suppress(Exception):
                    await self.writer.drain()
                return "closed"
            if self.tx != b"I":
                # An injected ERROR inside a tx would diverge client and
                # upstream tx state — skip, forward, and say why.
                event.note = "skipped: in-transaction"
                return await self._forward_unit(unit)
            self.writer.write(
                error_response(
                    sqlstate=error.code or "XX000",
                    message=error.message or "injected fault",
                    severity=severity,
                    fields=error.fields,
                ) + ready_for_query(b"I")
            )
            await self.writer.drain()
            return "ok"

        return await self._forward_unit(
            unit, partial_rows=decision.rule.partial_rows, event=event
        )

    # -- forwarding ---------------------------------------------------------------

    async def _forward_unit(
        self,
        unit: _Unit,
        partial_rows: int | None = None,
        event: FiredEvent | None = None,
    ) -> str:
        self._uwrite_all(unit.messages)
        assert self.uwriter is not None
        await self.uwriter.drain()
        if unit.kind in ("query", "batch", "call"):
            return await self._relay_response(
                partial_rows=partial_rows, event=event
            )
        return "ok"

    async def _relay_response(
        self,
        partial_rows: int | None = None,
        event: FiredEvent | None = None,
    ) -> str:
        """Upstream → client, framed, until ReadyForQuery.

        Tracks tx status from every 'Z', counts DataRows for
        ``partial_rows``, and pumps client → upstream verbatim while a
        COPY-IN sub-protocol is active (passthrough, never intercepted).
        An upstream RST aborts the client — dead link answers dead link.
        """
        assert self.ureader is not None
        rows = 0
        copy_pump: asyncio.Task | None = None
        started = time.monotonic()
        try:
            while True:
                try:
                    msg = await read_frame(self.ureader)
                except (EOFError, ConnectionError, PgProtocolError):
                    self._abort_client()
                    return "closed"
                if msg is None:
                    return "closed"  # upstream EOF — close() finishes the client
                mtype, payload = msg
                if mtype == b"G" and copy_pump is None:
                    copy_pump = asyncio.create_task(self._pump_copy_in())
                if mtype == b"D":
                    rows += 1
                    if partial_rows is not None and rows > partial_rows:
                        self._abort_client()
                        if event is not None:
                            event.note = (
                                f"partial_rows: {partial_rows} rows "
                                "relayed then aborted"
                            )
                        return "closed"
                self.writer.write(frame(mtype, payload))
                await self.writer.drain()
                if mtype == b"Z":
                    self.tx = payload[:1] or b"I"
                    self.proxy.stats.record_upstream_ms(
                        (time.monotonic() - started) * 1000
                    )
                    self.proxy.stats.record_forward()
                    return "ok"
        finally:
            if copy_pump is not None:
                copy_pump.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await copy_pump

    async def _pump_copy_in(self) -> None:
        """Client → upstream while CopyInResponse ('G') is active — 'd'
        rows until CopyDone/CopyFail terminate the sub-protocol."""
        assert self.uwriter is not None
        try:
            while True:
                msg = await read_frame(self.reader)
                if msg is None:
                    return
                mtype, payload = msg
                self.uwriter.write(frame(mtype, payload))
                await self.uwriter.drain()
                if mtype in (b"c", b"f"):
                    return
        except (EOFError, ConnectionError, PgProtocolError):
            return

    # -- small helpers ------------------------------------------------------------

    def _uwrite_all(self, messages: list[tuple[bytes, bytes]]) -> None:
        assert self.uwriter is not None
        for mtype, payload in messages:
            self.uwriter.write(frame(mtype, payload))

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
    proxy: PgProxy,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """asyncio.start_server callback — one PgConnection per client."""
    conn = PgConnection(proxy, reader, writer)
    try:
        await conn.run()
    except (EOFError, ConnectionError, PgProtocolError) as e:
        logger.debug("pg connection closed: %r", e)
    except Exception:  # defensive proxy boundary — never leak a client
        logger.exception("pg connection handler failed")
    finally:
        conn.close()


async def _serve(
    proxy: PgProxy,
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
        f"microburst listening on postgres://{host}:{bound} → "
        f"postgresql://{proxy.upstream_host}:{proxy.upstream_port}",
        flush=True,
    )
    print(
        f"control API: http://{host}:{control_port}/_microburst/health",
        flush=True,
    )
    print(
        f"point your app: psql 'host={host} port={bound} dbname=<db>'",
        flush=True,
    )
    async with server:
        await server.serve_forever()


def run_postgres(
    host: str,
    port: int,
    upstream_host: str,
    upstream_port: int,
    control_port: int,
    rules: list[dict] | None,
    watch_config: str | None = None,
) -> int:
    """Entry point for ``microburst --protocol postgres``."""
    proxy = PgProxy(upstream_host, upstream_port, rules=rules)
    try:
        asyncio.run(_serve(proxy, host, port, control_port, watch_config))
    except KeyboardInterrupt:
        pass
    return 0
