"""Shared fired-event emission — append + counters + SSE fan-out.

Used by every data plane (the HTTP pipeline and the pg wire server) so a
fired fault always lands in the same places: the bounded fired deque, the
Prometheus counters that survive deque eviction, per-rule stats, and any
``/_microburst/fired/stream`` subscribers.
"""

from __future__ import annotations

import asyncio
import logging

from microburst.rules import FiredEvent

logger = logging.getLogger("microburst")


def emit_fired(owner, event: FiredEvent) -> None:
    """Record ``event`` on ``owner`` — anything carrying the shared
    decision-plane state: ``.fired``, ``.fault_counts``, ``.stats`` and
    ``._listeners`` (``Microburst`` and ``pg.PgProxy`` both qualify)."""
    owner.fired.append(event)
    owner.fault_counts[(event.service, event.operation, event.action)] += 1
    owner.stats.record_fault(event.rule_id, event.action)
    payload = event.to_dict()
    for q in owner._listeners:
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            pass  # slow consumer drops events, never blocks the proxy
    logger.info(
        "FIRED rule=%s %s %s %s",
        event.rule_id, event.service, event.operation, event.action,
    )
