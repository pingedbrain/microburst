"""Redis data plane — a third sibling transport next to HTTP and pg.

Redis is a long-lived request→reply stream: one command frame in, one
reply frame out, no startup packet, no tx-status byte. The proxy decides
per command: build a ``RequestContext`` (``service="redis"``,
``operation`` = verb, ``resource`` = first key, ``args`` = decoded
command text) and run the same ``RuleEngine`` / ``emit_fired`` /
control-API machinery as the other transports.

Fidelity rules enforced here:

* ``error:`` → ``-CODE message`` reply in place of the upstream's
  answer. The code token is what clients branch on — cluster redirects
  keep their ``slot host:port`` tail parseable (see errors.py).
* …but never inside MULTI. An injected ``-ERR`` upstream never saw would
  desync the queued transaction: the client believes the command failed
  to queue, upstream still queues it. While ``in_multi`` the rule is
  skipped, the command forwards, and the fired event notes
  ``skipped: in-multi``. Latency/reset/timeout still apply — a dead or
  slow link can't diverge upstream state. Redis has no ``severity``
  knob; ``error.severity`` is pg-only and ignored here.
* ``cut_reply: {after_bytes: N}`` → relay N bytes of the real upstream
  reply, then TCP-abort — a mid-bulk-string death RESP can't express in
  band. (pg's ``partial_rows`` counts DataRows; RESP replies aren't
  row-shaped, so bytes is the honest unit.)
* Pub/sub and MONITOR are server-push modes: replies become unbounded
  and interleaved, so after a forwarded SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE/
  MONITOR the proxy splices the sockets and stops deciding — documented
  passthrough, no injection once subscribed. RESP3 ``>`` push and ``|``
  attribute frames arriving mid-reply are relayed and don't count as
  the command's reply.

Auth (AUTH/HELLO/SELECT) is just commands here — ``operation: auth``
etc. match the verb; there is no separate handshake phase.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import Counter, deque
from types import SimpleNamespace

from aiohttp import web

from microburst.control import handle_control
from microburst.core.context import RequestContext
from microburst.observe import emit_fired
from microburst.redis.detect import command_facts, command_text
from microburst.redis.errors import default_message, error_reply
from microburst.redis.proto import (
    Command,
    RespProtocolError,
    read_command,
    read_reply,
)
from microburst.rules import FiredEvent, RuleEngine, describe
from microburst.stats import ProxyStats

logger = logging.getLogger("microburst.redis")

SERVICE = "redis"

# Commands that flip the connection to server-push streaming — after
# forwarding, replies stop being one-per-command, so the proxy splices
# the sockets and gets out of the way.
_STREAMING = {"subscribe", "psubscribe", "ssubscribe", "monitor"}

# Verbs that end the transaction queue (RESET is the RESP3 connection
# reset — it clears MULTI state too).
_TX_CLOSE = {"exec", "discard", "reset"}


class RedisProxy:
    """Decision-plane state for redis mode — the surface control.py reads.

    Same shape as ``pg.PgProxy``: ``engine``/``fired``/``fault_counts``/
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
            base_url=f"redis://{upstream_host}:{upstream_port}",
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


class RedisConnection:
    """One client connection: eager upstream dial → per-command decide loop."""

    def __init__(
        self,
        proxy: RedisProxy,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.proxy = proxy
        self.reader = reader
        self.writer = writer
        self.ureader: asyncio.StreamReader | None = None
        self.uwriter: asyncio.StreamWriter | None = None
        # Whether upstream is queuing a MULTI transaction — tracked from
        # forwarded verbs, not upstream replies (it always answers +OK).
        self.in_multi = False

    async def run(self) -> None:
        try:
            self.ureader, self.uwriter = await asyncio.open_connection(
                self.proxy.upstream_host, self.proxy.upstream_port
            )
        except OSError as e:
            # Upstream unreachable: the honest shape is a dead link, not
            # a synthesized error (there's no command to answer yet).
            logger.debug("redis upstream connect failed: %r", e)
            return
        while True:
            cmd = await read_command(self.reader)
            if cmd is None:
                return
            if await self._serve_command(cmd) == "closed":
                return

    def close(self) -> None:
        for w in (self.writer, self.uwriter):
            if w is not None:
                with contextlib.suppress(Exception):
                    w.close()

    # -- steady state -----------------------------------------------------------

    def _decide(self, verb: str | None, key: str | None, text: str):
        ctx = RequestContext(
            service=SERVICE,
            operation=verb,
            resource=key,
            args=text,
        )
        return self.proxy.engine.decide(ctx)

    async def _serve_command(self, cmd: Command) -> str:
        """decide → effect-or-forward for one command frame."""
        verb, key = command_facts(cmd.args)
        text = command_text(cmd.args)
        self.proxy.requests_seen += 1
        self.proxy.stats.record_request(SERVICE)
        decision = self._decide(verb, key, text)
        if decision is None:
            return await self._forward(cmd, verb)
        event = FiredEvent(
            ts=time.time(),
            rule_id=decision.rule.id,
            service=SERVICE,
            operation=verb,
            resource=key,
            region=None,
            action=describe(decision),
            path=text[:80],
        )
        emit_fired(self.proxy, event)

        # Effect order mirrors the other transports: latency → reset →
        # timeout → error → forward (with cut_reply on the response path).
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
            if self.in_multi:
                # An -ERR upstream never saw would desync the queued tx:
                # client thinks dirty-abort, upstream still queues.
                event.note = "skipped: in-multi"
            else:
                self.writer.write(
                    error_reply(
                        code=decision.error.code,
                        message=decision.error.message
                        or default_message(decision.error.code),
                        fields=decision.error.fields,
                    )
                )
                with contextlib.suppress(Exception):
                    await self.writer.drain()
                return "ok"

        return await self._forward(
            cmd, verb, cut_bytes=decision.rule.cut_reply_bytes, event=event
        )

    # -- forwarding ---------------------------------------------------------------

    async def _forward(
        self,
        cmd: Command,
        verb: str | None,
        cut_bytes: int | None = None,
        event: FiredEvent | None = None,
    ) -> str:
        assert self.uwriter is not None
        self.uwriter.write(cmd.raw)
        await self.uwriter.drain()
        # Tx tracking mirrors what upstream now believes — set on the
        # forwarded command, not on a reply we don't interpret.
        if verb == "multi":
            self.in_multi = True
        elif verb in _TX_CLOSE:
            self.in_multi = False
        if verb in _STREAMING:
            if event is not None:
                event.note = "push-mode passthrough"
            await self._pump()
            return "closed"
        if not cmd.args:
            # Empty inline line — real Redis ignores it and sends no
            # reply; forward the bytes, wait for nothing.
            return "ok"
        return await self._relay_reply(verb, cut_bytes, event)

    async def _relay_reply(
        self,
        verb: str | None,
        cut_bytes: int | None,
        event: FiredEvent | None,
    ) -> str:
        """Upstream → client until the command's reply has been relayed.

        RESP3 ``>`` push and ``|`` attribute frames may precede the
        reply proper — relayed but not counted as it. An upstream RST
        aborts the client: dead link answers dead link.
        """
        assert self.ureader is not None
        started = time.monotonic()
        while True:
            try:
                reply = await read_reply(self.ureader)
            except (EOFError, ConnectionError, RespProtocolError):
                self._abort_client()
                return "closed"
            if reply is None:
                return "closed"  # upstream EOF — close() finishes the client
            if cut_bytes is not None:
                # Mid-reply death: prefix of the real frame, then RST.
                self.writer.write(reply.raw[:cut_bytes])
                with contextlib.suppress(Exception):
                    await self.writer.drain()
                self._abort_client()
                if event is not None:
                    event.note = (
                        f"cut_reply: {cut_bytes}B relayed then aborted"
                    )
                return "closed"
            self.writer.write(reply.raw)
            await self.writer.drain()
            if reply.kind in (b">", b"|"):
                continue  # push/attribute — the actual reply is still coming
            self.proxy.stats.record_upstream_ms(
                (time.monotonic() - started) * 1000
            )
            self.proxy.stats.record_forward()
            # real Redis closes the connection after +OK to QUIT
            return "closed" if verb == "quit" else "ok"

    async def _pump(self) -> None:
        """Socket splice for server-push modes — no decisions, just bytes.

        Client→upstream keeps relaying (a subscribed client may still
        send commands); each side's EOF propagates as a half-close to
        the other so the survivor can drain.
        """
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
    proxy: RedisProxy,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """asyncio.start_server callback — one RedisConnection per client."""
    conn = RedisConnection(proxy, reader, writer)
    try:
        await conn.run()
    except (EOFError, ConnectionError, RespProtocolError) as e:
        logger.debug("redis connection closed: %r", e)
    except Exception:  # defensive proxy boundary — never leak a client
        logger.exception("redis connection handler failed")
    finally:
        conn.close()


async def _serve(
    proxy: RedisProxy,
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
        f"microburst listening on redis://{host}:{bound} → "
        f"redis://{proxy.upstream_host}:{proxy.upstream_port}",
        flush=True,
    )
    print(
        f"control API: http://{host}:{control_port}/_microburst/health",
        flush=True,
    )
    print(
        f"point your app: redis-cli -p {bound}",
        flush=True,
    )
    async with server:
        await server.serve_forever()


def run_redis(
    host: str,
    port: int,
    upstream_host: str,
    upstream_port: int,
    control_port: int,
    rules: list[dict] | None,
    watch_config: str | None = None,
) -> int:
    """Entry point for ``microburst --protocol redis``."""
    proxy = RedisProxy(upstream_host, upstream_port, rules=rules)
    try:
        asyncio.run(_serve(proxy, host, port, control_port, watch_config))
    except KeyboardInterrupt:
        pass
    return 0
