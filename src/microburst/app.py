"""Application wiring: routes + lifecycle for the aiohttp app."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os

import yaml
from aiohttp import web

from microburst.control import CONTROL_PREFIX
from microburst.core.pipeline import Microburst

logger = logging.getLogger("microburst.app")


def make_control_app(
    microburst, watch_config: str | None = None
) -> web.Application:
    """The ``/_microburst/*`` surface alone — non-HTTP data planes (the
    pg wire mode) run it on their own port."""
    app = web.Application()
    if watch_config:
        app.on_startup.append(
            lambda app: _start_watch(app, microburst, watch_config)
        )
        app.on_cleanup.append(_stop_watch)
    app.router.add_route(
        "*", f"{CONTROL_PREFIX}/{{tail:.*}}", microburst.handle_control
    )
    app.router.add_route("*", CONTROL_PREFIX, microburst.handle_control)
    return app


def make_app(
    microburst: Microburst, watch_config: str | None = None
) -> web.Application:
    app = make_control_app(microburst, watch_config)
    app.on_startup.append(microburst.start)
    app.on_cleanup.append(microburst.stop)
    app.router.add_route("*", "/{tail:.*}", microburst.handle_proxy)
    return app


def load_config(path: str) -> dict:
    with open(path) as fh:
        return yaml.safe_load(fh) or {}


def _start_watch(app: web.Application, microburst: Microburst, path: str):
    app["_watch_task"] = asyncio.create_task(_watch_rules(microburst, path))


async def _stop_watch(app: web.Application):
    task = app.pop("_watch_task", None)
    if task is not None:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def _watch_rules(
    microburst: Microburst, path: str, interval: float = 1.0
) -> None:
    """Poll the config file's mtime and reload rules on change.

    The file is the source of truth while watching — a reload replaces
    the whole ruleset, including rules added through the control API.
    A broken file keeps the previous ruleset (logged, not fatal).
    """
    try:
        last = os.path.getmtime(path)
    except OSError:
        last = None
        logger.warning("watch: %s not readable yet", path)
    while True:
        await asyncio.sleep(interval)
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            mtime = None
        if mtime is None or mtime == last:
            continue
        last = mtime
        try:
            rules = load_config(path).get("rules") or []
            microburst.engine.set_rules(rules)
            logger.info("watch: reloaded %d rules from %s", len(rules), path)
        except Exception:
            logger.exception("watch: reload of %s failed — keeping rules", path)
