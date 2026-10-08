"""Latency effect — delay the request, then let it continue."""

from __future__ import annotations

import asyncio

from microburst.rules import Decision


async def apply(decision: Decision) -> None:
    if decision.latency_ms:
        await asyncio.sleep(decision.latency_ms / 1000)
