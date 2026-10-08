"""microburst proxy core: forwarding, fault effects, control API, SigV4 re-sign."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from collections import deque
from urllib.parse import quote

import aiohttp
from aiohttp import web

from microburst.detect import RequestInfo, detect, should_buffer
from microburst.errors import render_error
from microburst.rules import PRESETS, FiredEvent, RuleEngine, describe, to_dict

logger = logging.getLogger("microburst")

_HOP_BY_HOP = frozenset(
    h.lower()
    for h in (
        "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
        "TE", "Trailers", "Transfer-Encoding", "Upgrade", "Host",
        "Content-Length",
    )
)

CONTROL_PREFIX = "/_microburst"


# ---------------------------------------------------------------------------
# SigV4 re-signing (for --resign mode against real AWS upstreams)
# ---------------------------------------------------------------------------

_CRED_RE = __import__("re").compile(
    r"Credential=(?P<key>[^/,]+)/(?P<date>\d{8})/(?P<region>[^/]+)/"
    r"(?P<service>[^/]+)/aws4_request"
)


def _hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def resign_headers(
    method: str,
    path: str,
    query_string: str,
    headers: dict[str, str],
    payload_hash: str,
) -> dict[str, str] | None:
    """Re-sign a request for the real AWS upstream using env credentials.

    Returns updated headers, or None if no credentials are available.
    The incoming credential scope (region, service) is preserved so the
    signature stays valid for the service the client intended.
    """
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    access = os.environ.get("AWS_ACCESS_KEY_ID")
    token = os.environ.get("AWS_SESSION_TOKEN")
    if not secret or not access:
        return None

    match = _CRED_RE.search(headers.get("authorization", ""))
    if match:
        region = match.group("region")
        scope = match.group("service")
        amz_date = headers.get("x-amz-date", "")
        date_stamp = match.group("date")
    else:
        now = time.gmtime()
        amz_date = time.strftime("%Y%m%dT%H%M%SZ", now)
        date_stamp = time.strftime("%Y%m%d", now)
        region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        scope = "execute-api"

    out = dict(headers)
    out["x-amz-date"] = amz_date
    out["x-amz-content-sha256"] = payload_hash
    if token:
        out["x-amz-security-token"] = token
    else:
        out.pop("x-amz-security-token", None)

    sign_names = sorted(
        name
        for name in out
        if name == "host" or name == "content-type" or name.startswith("x-amz-")
    )
    canonical_headers = "".join(
        f"{name}:{' '.join(str(out[name]).split())}\n" for name in sign_names
    )
    signed_headers = ";".join(sign_names)

    canonical_query = "&".join(
        sorted(
            f"{quote(k, safe='-_.~')}={quote(v, safe='-_.~')}"
            for k, v in (
                pair.split("=", 1) if "=" in pair else (pair, "")
                for pair in query_string.split("&")
                if pair
            )
        )
    )

    canonical_request = "\n".join(
        [method, quote(path, safe="/-_.~") or "/", canonical_query,
         canonical_headers, signed_headers, payload_hash]
    )
    string_to_sign = "\n".join(
        ["AWS4-HMAC-SHA256", amz_date,
         f"{date_stamp}/{region}/{scope}/aws4_request",
         hashlib.sha256(canonical_request.encode()).hexdigest()]
    )
    key = _hmac_sha256(b"AWS4" + secret.encode(), date_stamp)
    key = _hmac_sha256(key, region)
    key = _hmac_sha256(key, scope)
    key = _hmac_sha256(key, "aws4_request")
    signature = hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()

    out["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access}/{date_stamp}/{region}/{scope}"
        f"/aws4_request, SignedHeaders={signed_headers}, Signature={signature}"
    )
    return out


# ---------------------------------------------------------------------------
# Proxy app
# ---------------------------------------------------------------------------

class Microburst:
    def __init__(self, upstream: str, rules: list[dict] | None = None,
                 resign: bool | None = None, fired_capacity: int = 2000):
        self.upstream = upstream.rstrip("/")
        self.engine = RuleEngine()
        if rules:
            self.engine.set_rules(rules)
        self.fired: deque[FiredEvent] = deque(maxlen=fired_capacity)
        self.session: aiohttp.ClientSession | None = None
        if resign is None:
            resign = ".amazonaws.com" in self.upstream
        self.resign = resign
        self.requests_seen = 0

    async def start(self, app: web.Application) -> None:
        timeout = aiohttp.ClientTimeout(total=600, connect=30)
        self.session = aiohttp.ClientSession(
            timeout=timeout, auto_decompress=False
        )
        if self.resign and not (
            os.environ.get("AWS_ACCESS_KEY_ID")
            and os.environ.get("AWS_SECRET_ACCESS_KEY")
        ):
            logger.warning(
                "re-signing enabled but AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                "are not set; requests will be forwarded unsigned"
            )

    async def stop(self, app: web.Application) -> None:
        if self.session:
            await self.session.close()

    # -- fault decision -----------------------------------------------------

    async def _maybe_fault(self, request: web.Request, info: RequestInfo):
        """Returns a Response/StreamResponse if a fault fires, else None."""
        decision = self.engine.decide(info)
        if decision is None:
            return None

        action = describe(decision)
        self.fired.append(
            FiredEvent(
                ts=time.time(),
                rule_id=decision.rule.id,
                service=info.service,
                operation=info.operation,
                resource=info.resource,
                region=info.region,
                action=action,
                path=request.rel_url.raw_path_qs,
            )
        )
        logger.info(
            "FIRED rule=%s %s %s %s",
            decision.rule.id, info.service, info.operation, action,
        )

        if decision.latency_ms:
            await asyncio.sleep(decision.latency_ms / 1000)

        if decision.reset:
            transport = getattr(request, "transport", None)
            if transport is not None:
                transport.abort()
            return web.Response(status=200)  # never written: socket is dead

        if decision.timeout_ms:
            await asyncio.sleep(decision.timeout_ms / 1000)
            status, headers, body = render_error(
                info.service, "RequestTimeout", "Request timed out", 504
            )
            return web.Response(status=status, headers=headers, body=body)

        if decision.error:
            status, headers, body = render_error(
                info.service,
                decision.error.code or "InternalError",
                decision.error.message or "",
                decision.error.status,
            )
            return web.Response(status=status, headers=headers, body=body)

        return None  # latency-only rule: forward after delay

    # -- forwarding ----------------------------------------------------------

    async def _forward(self, request: web.Request, body: bytes | None):
        assert self.session is not None
        url = self.upstream + request.rel_url.raw_path_qs
        headers = {
            k.lower(): v
            for k, v in request.headers.items()
            if k.lower() not in _HOP_BY_HOP
        }
        upstream_host = self.upstream.split("//", 1)[-1].split("/")[0]
        headers["host"] = upstream_host

        if self.resign:
            if body is not None:
                payload_hash = hashlib.sha256(body).hexdigest()
            else:
                payload_hash = "UNSIGNED-PAYLOAD"
            signed = resign_headers(
                request.method,
                request.rel_url.raw_path,
                request.rel_url.query_string,
                headers,
                payload_hash,
            )
            if signed is not None:
                headers = signed

        data = body if body is not None else request.content
        try:
            upstream = await self.session.request(
                request.method,
                url,
                headers=headers,
                data=data,
                allow_redirects=False,
            )
        except aiohttp.ClientError as exc:
            logger.error("upstream error: %s", exc)
            return web.Response(
                status=502,
                text=f"microburst: upstream {self.upstream} unreachable: {exc}",
            )

        resp_headers = {
            k: v for k, v in upstream.headers.items()
            if k.lower() not in _HOP_BY_HOP
        }
        resp = web.StreamResponse(status=upstream.status, headers=resp_headers)
        await resp.prepare(request)
        async for chunk in upstream.content.iter_any():
            await resp.write(chunk)
        await resp.write_eof()
        return resp

    # -- handlers ------------------------------------------------------------

    async def handle_proxy(self, request: web.Request):
        self.requests_seen += 1
        path = request.rel_url.raw_path
        query = request.rel_url.query
        content_length = request.content_length

        # Peek at operation cheaply first for streaming decisions on REST
        # services; body-dependent ops buffer below if needed.
        prelim = detect(request.headers, request.method, path, query, None)
        body = None
        if request.can_read_body and should_buffer(
            prelim.service, prelim.operation, content_length
        ):
            body = await request.read()

        info = detect(request.headers, request.method, path, query, body)

        fault = await self._maybe_fault(request, info)
        if fault is not None:
            return fault
        return await self._forward(request, body)

    async def handle_control(self, request: web.Request):
        tail = request.match_info.get("tail", "").strip("/")
        method = request.method

        if tail == "health" and method == "GET":
            return web.json_response({
                "status": "ok",
                "upstream": self.upstream,
                "resign": self.resign,
                "rules": len(self.engine.rules),
                "requests_seen": self.requests_seen,
            })

        if tail == "rules":
            if method == "GET":
                return web.json_response(
                    [to_dict(r) for r in self.engine.rules]
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
                rules = self.engine.set_rules(data)
            elif method == "PATCH":
                rules = self.engine.add_rules(data)
            elif method == "DELETE":
                removed = self.engine.delete_matching(data)
                return web.json_response({"removed": removed})
            else:
                raise web.HTTPMethodNotAllowed(method, ["GET", "POST", "PATCH", "DELETE"])
            return web.json_response([to_dict(r) for r in rules])

        if tail == "fired":
            if method == "GET":
                limit = int(request.rel_url.query.get("limit", "100"))
                events = list(self.fired)[-limit:]
                return web.json_response([e.to_dict() for e in events])
            if method == "DELETE":
                self.fired.clear()
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
            rules = self.engine.add_rules([preset])
            return web.json_response([to_dict(r) for r in rules])

        return web.json_response({"error": "unknown control path"}, status=404)


def make_app(microburst: Microburst) -> web.Application:
    app = web.Application()
    app.on_startup.append(microburst.start)
    app.on_cleanup.append(microburst.stop)
    app.router.add_route("*", f"{CONTROL_PREFIX}/{{tail:.*}}", microburst.handle_control)
    app.router.add_route("*", f"{CONTROL_PREFIX}", microburst.handle_control)
    app.router.add_route("*", "/{tail:.*}", microburst.handle_proxy)
    return app


def load_config(path: str) -> dict:
    import yaml

    with open(path) as fh:
        return yaml.safe_load(fh) or {}
