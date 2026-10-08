"""Fault effects — composable stages, not alternatives.

Effects are not a registry of mutually-exclusive handlers: a single
decision can mean "sleep 800ms AND return this error". They run in a
fixed order — latency first (it models degradation of the request
itself), then the terminal stages in severity order: reset, timeout,
error. A stage returns a Response when it terminates the request or
None to let the next stage run. ``None`` at the end means forward to
the upstream (a latency-only fault still forwards after delaying).
"""

from __future__ import annotations

from aiohttp import web

from microburst.core.context import RequestContext
from microburst.effects import error, latency, reset, timeout
from microburst.rules import Decision

__all__ = ["apply_decision"]


async def apply_decision(
    request: web.Request,
    ctx: RequestContext,
    decision: Decision,
) -> web.StreamResponse | web.Response | None:
    await latency.apply(decision)
    for stage in (
        lambda: reset.apply(request, decision),
        lambda: timeout.apply(ctx, decision),
        lambda: error.apply(ctx, decision),
    ):
        response = await stage()
        if response is not None:
            return response
    return None
