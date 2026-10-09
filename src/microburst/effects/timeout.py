"""Timeout effect — hold the connection open, then return 504."""

from __future__ import annotations

import asyncio

from aiohttp import web

from microburst.protocols import render_error
from microburst.rules import Decision


async def apply(ctx, decision: Decision) -> web.Response | None:
    if not decision.timeout_ms:
        return None
    await asyncio.sleep(decision.timeout_ms / 1000)
    status, headers, body = render_error(
        ctx.service,
        "RequestTimeout",
        "Request timed out",
        504,
        protocol=ctx.protocol,
        query_compat=ctx.query_compat,
        request_ct=ctx.headers.get("Content-Type"),
    )
    return web.Response(status=status, headers=headers, body=body)
