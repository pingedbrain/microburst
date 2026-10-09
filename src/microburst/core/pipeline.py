"""Pipeline orchestrator: detect → decide → effect-or-forward.

``Microburst`` owns the shared state (rule engine, upstream client, fired
log) and exposes the two request handlers. This is deliberately NOT an
aiohttp middleware chain — the semantics are "decide and respond", not
"decorate the request".
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter, deque

from aiohttp import web

from microburst.control import handle_control
from microburst.detection import detect, should_buffer
from microburst.effects import apply_decision
from microburst.forward import Upstream, consume_upload
from microburst.rules import FiredEvent, RuleEngine, describe
from microburst.stats import ProxyStats
from microburst.tracing import fault_span

logger = logging.getLogger("microburst")


class Microburst:
    def __init__(self, upstream: str, rules: list[dict] | None = None,
                 resign: bool | None = None, fired_capacity: int = 2000,
                 http2: bool = False, cassette=None):
        self.stats = ProxyStats()
        self.upstream = Upstream(
            upstream,
            resign=(".amazonaws.com" in upstream) if resign is None else resign,
            http2=http2,
            cassette=cassette,
            stats=self.stats,
        )
        self.engine = RuleEngine()
        if rules:
            self.engine.set_rules(rules)
        self.fired: deque[FiredEvent] = deque(maxlen=fired_capacity)
        # Prometheus counters keyed by label tuple — survives deque eviction
        self.fault_counts: Counter = Counter()
        # SSE subscribers: each is an asyncio.Queue receiving event dicts.
        # Bounded — a slow consumer drops events rather than growing memory.
        self._listeners: set[asyncio.Queue] = set()
        self.requests_seen = 0

    def subscribe_fired(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._listeners.add(q)
        return q

    def unsubscribe_fired(self, q: asyncio.Queue) -> None:
        self._listeners.discard(q)

    async def start(self, app: web.Application) -> None:
        await self.upstream.start()

    async def stop(self, app: web.Application) -> None:
        await self.upstream.stop()

    async def handle_proxy(self, request: web.Request):
        self.requests_seen += 1
        path = request.rel_url.raw_path
        query = request.rel_url.query
        content_length = request.content_length

        # Peek at the operation cheaply first for streaming decisions on
        # REST services; body-dependent ops buffer below if needed.
        prelim = detect(request.headers, request.method, path, query, None)
        body = None
        if request.can_read_body and (
            should_buffer(prelim.service, prelim.operation, content_length)
            # cassette keys include the body — always buffer when one is
            # active (json services all POST to /; the body discriminates)
            or self.upstream.cassette is not None
        ):
            body = await request.read()

        ctx = detect(request.headers, request.method, path, query, body)
        self.stats.record_request(ctx.service)

        decision = self.engine.decide(ctx)
        if decision is not None:
            ctx.decision = decision
            action = describe(decision)
            event = FiredEvent(
                ts=time.time(),
                rule_id=decision.rule.id,
                service=ctx.service,
                operation=ctx.operation,
                resource=ctx.resource,
                region=ctx.region,
                action=action,
                path=request.rel_url.raw_path_qs,
            )
            self.fired.append(event)
            self.fault_counts[(ctx.service, ctx.operation, action)] += 1
            self.stats.record_fault(decision.rule.id, action)
            payload = event.to_dict()
            for q in self._listeners:
                try:
                    q.put_nowait(payload)
                except asyncio.QueueFull:
                    pass  # slow consumer drops events, never blocks the proxy
            logger.info(
                "FIRED rule=%s %s %s %s",
                decision.rule.id, ctx.service, ctx.operation, action,
            )
            with fault_span(
                decision.rule.id, ctx.service, ctx.operation, action
            ):
                # Request-side faults act on the client→proxy upload, so
                # they run first — a cut link dies before any response
                # (error envelope or otherwise) could exist.
                data = None
                if decision.request_fault is not None:
                    data, cut = await consume_upload(
                        request, body, decision.request_fault
                    )
                    if cut:
                        # socket already dead — nothing can be written
                        return web.Response(status=200)
                fault = await apply_decision(request, ctx, decision)
                if fault is not None:
                    return fault
                # terminal effects didn't fire → forward, possibly
                # mutating the response on the way back
                self.stats.record_forward()
                return await self.upstream.relay(
                    request, body,
                    mutator=decision.response_fault,
                    data=data,
                )

        self.stats.record_forward()
        return await self.upstream.relay(request, body)

    async def handle_control(self, request: web.Request):
        return await handle_control(self, request)
