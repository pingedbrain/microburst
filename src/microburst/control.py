"""Control plane: the /_microburst/* API.

Local-development surface — unauthenticated by design. Rule CRUD, the
fired-fault audit log, and presets. Never expose it beyond localhost.
"""

from __future__ import annotations

import json

from aiohttp import web

from microburst.rules import PRESETS, to_dict

CONTROL_PREFIX = "/_microburst"


async def handle_control(microburst, request: web.Request):
    tail = request.match_info.get("tail", "").strip("/")
    method = request.method

    if tail == "health" and method == "GET":
        return web.json_response({
            "status": "ok",
            "upstream": microburst.upstream.base_url,
            "resign": microburst.upstream.resign,
            "rules": len(microburst.engine.rules),
            "requests_seen": microburst.requests_seen,
        })

    if tail == "rules":
        if method == "GET":
            return web.json_response(
                [to_dict(r) for r in microburst.engine.rules]
            )
        try:
            data = await request.json()
        except json.JSONDecodeError:
            return web.json_response(
                {"error": "expected a JSON body"}, status=400
            )
        if not isinstance(data, list):
            return web.json_response(
                {"error": "expected a JSON list of rules"}, status=400
            )
        if method == "POST":
            rules = microburst.engine.set_rules(data)
        elif method == "PATCH":
            rules = microburst.engine.add_rules(data)
        elif method == "DELETE":
            removed = microburst.engine.delete_matching(data)
            return web.json_response({"removed": removed})
        else:
            raise web.HTTPMethodNotAllowed(
                method, ["GET", "POST", "PATCH", "DELETE"]
            )
        return web.json_response([to_dict(r) for r in rules])

    if tail == "fired":
        if method == "GET":
            limit = int(request.rel_url.query.get("limit", "100"))
            events = list(microburst.fired)[-limit:]
            return web.json_response([e.to_dict() for e in events])
        if method == "DELETE":
            microburst.fired.clear()
            return web.json_response({"cleared": True})

    if tail == "presets" and method == "GET":
        return web.json_response(PRESETS)

    if tail.startswith("presets/") and method == "POST":
        name = tail.split("/", 1)[1]
        preset = PRESETS.get(name)
        if preset is None:
            return web.json_response(
                {"error": f"unknown preset {name!r}",
                 "available": sorted(PRESETS)},
                status=404,
            )
        rules = microburst.engine.add_rules([preset])
        return web.json_response([to_dict(r) for r in rules])

    return web.json_response({"error": "unknown control path"}, status=404)
