"""Application wiring: routes + lifecycle for the aiohttp app."""

from __future__ import annotations

import yaml
from aiohttp import web

from microburst.control import CONTROL_PREFIX
from microburst.core.pipeline import Microburst


def make_app(microburst: Microburst) -> web.Application:
    app = web.Application()
    app.on_startup.append(microburst.start)
    app.on_cleanup.append(microburst.stop)
    app.router.add_route(
        "*", f"{CONTROL_PREFIX}/{{tail:.*}}", microburst.handle_control
    )
    app.router.add_route("*", CONTROL_PREFIX, microburst.handle_control)
    app.router.add_route("*", "/{tail:.*}", microburst.handle_proxy)
    return app


def load_config(path: str) -> dict:
    with open(path) as fh:
        return yaml.safe_load(fh) or {}
