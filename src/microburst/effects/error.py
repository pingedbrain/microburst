"""Error effect — return a protocol-correct AWS fault response."""

from __future__ import annotations

from aiohttp import web

from microburst.protocols import render_error
from microburst.rules import Decision


async def apply(ctx, decision: Decision) -> web.Response | None:
    if decision.error is None:
        return None
    status, headers, body = render_error(
        ctx.service,
        decision.error.code or "InternalError",
        decision.error.message or "",
        decision.error.status,
        protocol=ctx.protocol,
        query_compat=ctx.query_compat,
        request_ct=ctx.headers.get("Content-Type"),
    )
    if ctx.method == "HEAD":
        body = b""
    return web.Response(status=status, headers=headers, body=body)
