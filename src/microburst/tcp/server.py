"""Generic TCP data plane — the byte-stream sibling transport.

Unlike pg/redis (request → reply protocols with codecs), tcp mode is a
dumb duplex pipe that decides on *transport units*, not operations:

- **Framed** (``--framing`` / ``framing:``): every complete frame is a
  decision unit, each direction parsed by its own ``GenericFramer``.
  ``operation`` is ``c2s:frame`` or ``s2c:frame``; ``payload:`` matches
  the frame bytes as latin-1 (byte-identity regex).
- **Unframed** (default): the stream is unsegmented. The rule engine
  evaluates once per connection — on the first client→server chunk,
  and re-evaluates on subsequent chunks against a growing 4 KiB prefix
  until a rule fires or the prefix fills (``operation: conn``). A match
  resolves the decision; connection-level faults (latency/reset/
  timeout/cuts/corrupt/respond) apply to the link, then chunks stream
  verbatim. There are no s2c decisions unframed — reply faults must be
  armed by the conn decision (``cut_reply`` counts reply bytes).

Effects (both directions where sensible):

- ``latency`` / ``timeout_ms`` / ``timeout: true`` — per-unit delay /
  stall / drain-until-peer-EOF.
- ``reset`` — TCP RST to the client.
- ``cut_upload: {after_bytes|after_messages}`` (also ``request:
  {cut_upload: …}``) — relay N bytes/frames of client→server traffic
  *after the rule fires*, then abort the client connection.
- ``cut_reply: {after_bytes|after_messages}`` — same, server→client.
- ``corrupt: {at_bytes: N, bit: flip}`` — XOR 0xFF the byte at absolute
  offset N of the client→server stream.
- ``respond: {data|hex|base64, then: forward|close|hold}`` — write
  crafted bytes to the client without forwarding the unit upstream;
  then relay normally / close / swallow the rest of the client's
  stream. c2s only — it is the synthetic-reply escape hatch, not a
  protocol renderer.
- ``error:`` — no renderer exists in tcp mode: the rule fires (the
  event is logged with ``note: error has no renderer in tcp mode``)
  and the unit forwards untouched.

Defensive contract, same as the HTTP framing seam: a malformed frame
latches ``framer.failed`` → the stream falls back to verbatim
passthrough and a fired-log note records it (``malformed … framing —
passthrough``). Upstream unreachable → dead link. Reply paths stream —
nothing buffers a whole reply to apply a cut.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from types import SimpleNamespace

from aiohttp import web

from microburst.control import handle_control
from microburst.core.context import RequestContext
from microburst.framing import GenericFramer, parse_framing
from microburst.observe import emit_fired
from microburst.rules import FiredEvent, RuleEngine, describe
from microburst.stats import ProxyStats

logger = logging.getLogger("microburst.tcp")

SERVICE = "tcp"

# fired.path = hex preview of the unit's first bytes — enough to
# recognize the protocol shape without dumping payloads.
_PATH_PREVIEW = 32

# Unframed content matching sees at most this much of the stream — a
# growing prefix (not a sliding window): the handshake and first bytes
# are where protocol identity lives.
_MATCH_PREFIX = 4096


@dataclass
class _Gate:
    """Armed stream faults for one direction of one connection.

    ``cut_bytes``/``cut_msgs`` count DOWN from arming — the firing unit
    is the first relayed unit. ``corrupt`` holds absolute client→server
    stream offsets still awaiting their byte. ``sent`` is the absolute
    count of bytes relayed in this direction (corrupt offsets and
    "already relayed" checks resolve against it).
    """
    cut_bytes: int | None = None
    cut_msgs: int | None = None
    corrupt: list[int] = field(default_factory=list)
    sent: int = 0


class TcpProxy:
    """Decision-plane state for tcp mode — the surface control.py reads.

    Same shape as ``pg.PgProxy`` / ``redis.RedisProxy``:
    ``engine``/``fired``/``fault_counts``/``stats``/``requests_seen``/
    ``_listeners`` behave identically.
    """

    def __init__(
        self,
        upstream_host: str,
        upstream_port: int,
        rules: list[dict] | None = None,
        framing=None,
        fired_capacity: int = 2000,
    ) -> None:
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port
        self.framing = parse_framing(framing)
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
            base_url=f"tcp://{upstream_host}:{upstream_port}",
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


class TcpConnection:
    """One client connection: duplex pumps deciding per transport unit."""

    def __init__(
        self,
        proxy: TcpProxy,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.proxy = proxy
        self.reader = reader          # client → proxy
        self.writer = writer          # proxy → client
        self.ureader: asyncio.StreamReader | None = None
        self.uwriter: asyncio.StreamWriter | None = None
        self.c2s = _Gate()
        self.s2c = _Gate()
        self.decided = False          # unframed: conn decision resolved
        self.counted = False          # unframed: requests_seen bumped
        self.prefix = bytearray()     # unframed: rolling content prefix
        self.hold = False             # respond.then=hold: c2s blackholes
        self.framing_failed = False

    # -- lifecycle ------------------------------------------------------------

    async def run(self) -> None:
        try:
            self.ureader, self.uwriter = await asyncio.open_connection(
                self.proxy.upstream_host, self.proxy.upstream_port
            )
        except OSError as e:
            # Upstream unreachable: dead link, no synthesized error —
            # there is no protocol envelope to refuse inside anyway.
            logger.debug("tcp upstream connect failed: %r", e)
            return
        if self.proxy.framing is not None:
            pumps = [
                self._pump_framed(
                    self.reader, self.uwriter, self.c2s, "c2s"
                ),
                self._pump_framed(
                    self.ureader, self.writer, self.s2c, "s2c"
                ),
            ]
        else:
            pumps = [self._pump_c2s_stream(), self._pump_s2c_stream()]
        # First pump to end tears the connection down — an idle peer
        # must not pin the survivor's read forever after the link died
        # (the EOF each pump propagates only helps when the peer closes).
        tasks = [asyncio.ensure_future(p) for p in pumps]
        _done, pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            # a read() blocked on the dead link's survivor won't wake
            # from close() — cancel it outright
            task.cancel()
        self.close()
        # consume results so task exceptions don't warn on gc
        await asyncio.gather(*tasks, return_exceptions=True)

    def close(self) -> None:
        for w in (self.writer, self.uwriter):
            if w is not None:
                with contextlib.suppress(Exception):
                    w.close()

    # -- framed pumps ------------------------------------------------------------

    async def _pump_framed(
        self,
        src: asyncio.StreamReader,
        dst: asyncio.StreamWriter,
        gate: _Gate,
        direction: str,
    ) -> None:
        """Chunked read → frame split → per-frame decide → relay.

        Once the framer latches ``failed`` the stream degrades to raw
        passthrough — same contract as the HTTP upload framer.
        """
        framer = GenericFramer(self.proxy.framing)
        op = f"{direction}:frame"
        try:
            while True:
                chunk = await src.read(65536)
                if not chunk:
                    _write_eof(dst)
                    return
                if framer.failed:
                    if await self._relay(dst, gate, chunk) == "closed":
                        return
                    continue
                for frame in framer.feed(chunk):
                    if self.hold and direction == "c2s":
                        continue  # respond.then=hold — client talks to a void
                    if await self._serve_unit(
                        frame, direction, op, dst, gate, message=True
                    ) == "closed":
                        return
                if framer.failed:
                    tail = framer.drain()
                    if tail and await self._relay(
                        dst, gate, tail
                    ) == "closed":
                        return
                    self._log_passthrough(
                        f"malformed {direction} framing — passthrough"
                    )
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            return

    # -- unframed pumps ------------------------------------------------------------

    async def _pump_c2s_stream(self) -> None:
        """Unsegmented client→upstream relay. Until a rule fires (or the
        4 KiB match prefix fills) each chunk re-evaluates the rules
        against the growing prefix — content matchers get their first
        KiB to find the protocol's identity; a connection-level rule
        with no ``payload:`` fires on chunk one."""
        assert self.uwriter is not None
        try:
            while True:
                chunk = await self.reader.read(65536)
                if not chunk:
                    _write_eof(self.uwriter)
                    return
                if self.hold:
                    continue
                if not self.decided:
                    room = _MATCH_PREFIX - len(self.prefix)
                    if room > 0:
                        self.prefix += chunk[:room]
                    if not self.counted:
                        self.counted = True
                        self.proxy.requests_seen += 1
                        self.proxy.stats.record_request(SERVICE)
                    r = await self._serve_unit(
                        chunk, "c2s", "conn", self.uwriter, self.c2s,
                        message=False, match=bytes(self.prefix),
                    )
                    # the chunk was relayed (or the link died) inside —
                    # either way this iteration is done
                    if r == "closed":
                        return
                    continue
                if await self._relay(
                    self.uwriter, self.c2s, chunk
                ) == "closed":
                    return
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            return

    async def _pump_s2c_stream(self) -> None:
        """Unsegmented upstream→client relay — byte-level armed faults
        (cut_reply) still apply; nothing decides here."""
        assert self.ureader is not None
        try:
            while True:
                chunk = await self.ureader.read(65536)
                if not chunk:
                    _write_eof(self.writer)
                    return
                if await self._relay(self.writer, self.s2c, chunk) == "closed":
                    return
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            return

    # -- decide → effect-or-forward -------------------------------------------------

    async def _serve_unit(
        self,
        unit: bytes,
        direction: str,
        op: str,
        dst: asyncio.StreamWriter,
        gate: _Gate,
        *,
        message: bool,
        match: bytes | None = None,
    ) -> str:
        """One decision unit: match → apply → relay (or fault)."""
        ctx = RequestContext(
            service=SERVICE,
            operation=op,
            payload=(match if match is not None else unit).decode("latin-1"),
        )
        decision = self.proxy.engine.decide(ctx)
        if decision is None:
            if match is not None and len(self.prefix) >= _MATCH_PREFIX:
                self.decided = True  # prefix full, no rule — stop evaluating
            return await self._relay(dst, gate, unit, message=message)
        if match is not None:
            self.decided = True
        if direction == "c2s" and match is None:
            # framed c2s unit — unframed counts once via self.counted
            self.proxy.requests_seen += 1
            self.proxy.stats.record_request(SERVICE)
        preview = (match if match is not None else unit)[:_PATH_PREVIEW]
        event = FiredEvent(
            ts=time.time(),
            rule_id=decision.rule.id,
            service=SERVICE,
            operation=op,
            resource=None,
            region=None,
            action=describe(decision),
            path=preview.hex(),
            note="unframed" if match is not None else None,
        )
        emit_fired(self.proxy, event)

        # Effect order mirrors the other transports: latency → reset →
        # timeout → error → respond → forward (stream faults arm before
        # the firing unit's relay so it counts toward the budget).
        if decision.latency_ms:
            await asyncio.sleep(decision.latency_ms / 1000)
        if decision.reset:
            self._abort_client()
            return "closed"
        if decision.timeout_ms is not None:
            if decision.timeout_ms < 0:
                # `timeout: true` — drain the peer whose stream this is:
                # a hung server never answers, a hung upstream never
                # delivers. Either way the link looks dead-but-open.
                src = self.reader if direction == "c2s" else self.ureader
                await self._wait_peer_eof(src)
                return "closed"
            await asyncio.sleep(decision.timeout_ms / 1000)

        if decision.error is not None:
            _note(event, "error has no renderer in tcp mode")

        respond = decision.rule.respond
        if respond is not None:
            if direction != "c2s":
                _note(event, "respond applies to c2s units only")
            else:
                self.writer.write(respond.data)
                with contextlib.suppress(Exception):
                    await self.writer.drain()
                _note(
                    event,
                    f"respond: wrote {len(respond.data)}B, "
                    f"then={respond.then}",
                )
                if respond.then == "close":
                    return "closed"
                if respond.then == "hold":
                    self.hold = True
                return "ok"  # unit never reaches upstream

        self._arm(decision, direction, gate, event)
        return await self._relay(dst, gate, unit, message=message)

    def _arm(self, decision, direction: str, gate: _Gate, event) -> None:
        """Install stream faults from a fired decision. Thresholds count
        relayed bytes/frames from this point — the firing unit is first."""
        rule = decision.rule
        xf = decision.request_fault
        if xf is not None:
            if direction == "c2s":
                # only arm when idle — a rule firing on every unit must
                # not reset the countdown each time
                if xf.after_bytes is not None and gate.cut_bytes is None:
                    gate.cut_bytes = xf.after_bytes
                if xf.after_messages is not None:
                    if self.proxy.framing is None:
                        _note(
                            event,
                            "cut_upload.after_messages has no units "
                            "in unframed mode",
                        )
                    elif gate.cut_msgs is None:
                        gate.cut_msgs = xf.after_messages
            elif xf.after_bytes is not None or xf.after_messages is not None:
                _note(event, "cut_upload applies to the c2s stream only")
        # cut_reply arms the s2c gate whichever unit carried the
        # decision — the conn decision (unframed) and c2s frames both
        # cut the reply stream, same as redis's per-command cut_reply
        if rule.cut_reply_bytes is not None and self.s2c.cut_bytes is None:
            self.s2c.cut_bytes = rule.cut_reply_bytes
        if rule.cut_reply_messages is not None:
            if self.proxy.framing is None:
                _note(
                    event,
                    "cut_reply.after_messages has no units in "
                    "unframed mode",
                )
            elif self.s2c.cut_msgs is None:
                self.s2c.cut_msgs = rule.cut_reply_messages
        if rule.corrupt is not None:
            if direction != "c2s":
                _note(event, "corrupt applies to the c2s stream only")
            elif rule.corrupt.at_bytes < self.c2s.sent:
                _note(
                    event,
                    f"corrupt: offset {rule.corrupt.at_bytes} already "
                    "relayed — no-op",
                )
            else:
                self.c2s.corrupt.append(rule.corrupt.at_bytes)
                _note(
                    event,
                    f"corrupt armed at stream byte {rule.corrupt.at_bytes}",
                )

    # -- relay path ---------------------------------------------------------------

    async def _relay(
        self,
        dst: asyncio.StreamWriter,
        gate: _Gate,
        data: bytes,
        *,
        message: bool = False,
    ) -> str:
        """Write one unit through the armed gates. Cut thresholds abort
        the CLIENT connection — whichever direction is cut, the link the
        client sees dies."""
        if gate.corrupt:
            data = self._apply_corrupt(gate, data)
        overflow = False
        if gate.cut_bytes is not None:
            if len(data) > gate.cut_bytes:
                data, overflow = data[: gate.cut_bytes], True
            gate.cut_bytes -= len(data)
        if data:
            try:
                dst.write(data)
                gate.sent += len(data)
                await dst.drain()
            except (ConnectionError, OSError):
                return "closed"
        if message and gate.cut_msgs is not None:
            gate.cut_msgs -= 1
            if gate.cut_msgs <= 0:
                self._abort_client()
                return "closed"
        if overflow or (gate.cut_bytes is not None and gate.cut_bytes <= 0):
            self._abort_client()
            return "closed"
        self.proxy.stats.record_forward()
        return "ok"

    def _apply_corrupt(self, gate: _Gate, data: bytes) -> bytes:
        """Flip every pending offset that falls inside this unit."""
        buf = bytearray(data)
        start = gate.sent
        done = []
        for off in gate.corrupt:
            if start <= off < start + len(buf):
                buf[off - start] ^= 0xFF
                done.append(off)
            elif off < start:
                done.append(off)  # offset already streamed past — drop
        for off in done:
            gate.corrupt.remove(off)
        return bytes(buf)

    # -- small helpers ------------------------------------------------------------

    def _log_passthrough(self, note: str) -> None:
        """A framing failure is observable like a fired fault — append
        a note event straight to the fired log (no rule fired, so the
        fault counters don't inflate)."""
        self.framing_failed = True
        self.proxy.fired.append(
            FiredEvent(
                ts=time.time(),
                rule_id=0,
                service=SERVICE,
                operation=None,
                resource=None,
                region=None,
                action="passthrough",
                path="",
                note=note,
            )
        )
        logger.warning("tcp: %s", note)

    async def _wait_peer_eof(self, src: asyncio.StreamReader) -> None:
        """`timeout: true` — drain the peer until it gives up."""
        with contextlib.suppress(ConnectionError, OSError):
            while await src.read(65536):
                pass

    def _abort_client(self) -> None:
        """TCP RST to the client — mirrors reset.apply on the HTTP path."""
        transport = getattr(self.writer, "transport", None)
        if transport is not None:
            transport.abort()


def _note(event: FiredEvent, text: str) -> None:
    event.note = f"{event.note}; {text}" if event.note else text


def _write_eof(dst: asyncio.StreamWriter) -> None:
    """Propagate a peer EOF as a half-close so the other side drains."""
    with contextlib.suppress(Exception):
        dst.write_eof()


async def handle_client(
    proxy: TcpProxy,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """asyncio.start_server callback — one TcpConnection per client."""
    conn = TcpConnection(proxy, reader, writer)
    try:
        await conn.run()
    except (EOFError, ConnectionError) as e:
        logger.debug("tcp connection closed: %r", e)
    except Exception:  # defensive proxy boundary — never leak a client
        logger.exception("tcp connection handler failed")
    finally:
        conn.close()


async def _serve(
    proxy: TcpProxy,
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
    framed = (
        f" framing={proxy.framing.kind}" if proxy.framing else " unframed"
    )
    print(
        f"microburst listening on tcp://{host}:{bound} → "
        f"tcp://{proxy.upstream_host}:{proxy.upstream_port}{framed}",
        flush=True,
    )
    print(
        f"control API: http://{host}:{control_port}/_microburst/health",
        flush=True,
    )
    print(f"point your app: nc {host} {bound}", flush=True)
    async with server:
        await server.serve_forever()


def run_tcp(
    host: str,
    port: int,
    upstream_host: str,
    upstream_port: int,
    control_port: int,
    rules: list[dict] | None,
    watch_config: str | None = None,
    framing=None,
) -> int:
    """Entry point for ``microburst --protocol tcp``."""
    proxy = TcpProxy(
        upstream_host, upstream_port, rules=rules, framing=framing
    )
    try:
        asyncio.run(_serve(proxy, host, port, control_port, watch_config))
    except KeyboardInterrupt:
        pass
    return 0
