"""Reset effect — abort the TCP connection (connection reset by peer)."""

from __future__ import annotations

from aiohttp import web

from microburst.rules import Decision


async def apply(request: web.Request, decision: Decision) -> web.Response | None:
    if not decision.reset:
        return None
    transport = getattr(request, "transport", None)
    if transport is not None:
        transport.abort()
    return web.Response(status=200)  # never written: the socket is dead
