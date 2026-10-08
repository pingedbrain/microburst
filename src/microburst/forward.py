"""Upstream forwarding: request relay, streaming responses, SigV4 re-sign.

Data plane only — nothing here decides anything. Detection and fault
injection happen before a request reaches this module.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import re
import time
from urllib.parse import quote

import aiohttp
from aiohttp import web

logger = logging.getLogger("microburst.forward")

HOP_BY_HOP = frozenset(
    h.lower()
    for h in (
        "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
        "TE", "Trailers", "Transfer-Encoding", "Upgrade", "Host",
        "Content-Length",
    )
)

_CRED_RE = re.compile(
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
    """Re-sign a request for a real AWS upstream using env credentials.

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


class Upstream:
    """Client for the upstream endpoint: session lifecycle + relay."""

    def __init__(self, base_url: str, resign: bool = False):
        self.base_url = base_url.rstrip("/")
        self.resign = resign
        self.session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
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

    async def stop(self) -> None:
        if self.session:
            await self.session.close()

    async def relay(
        self, request: web.Request, body: bytes | None
    ) -> web.StreamResponse:
        assert self.session is not None
        url = self.base_url + request.rel_url.raw_path_qs
        headers = {
            k.lower(): v
            for k, v in request.headers.items()
            if k.lower() not in HOP_BY_HOP
        }
        headers["host"] = self.base_url.split("//", 1)[-1].split("/")[0]

        if self.resign:
            payload_hash = (
                hashlib.sha256(body).hexdigest()
                if body is not None
                else "UNSIGNED-PAYLOAD"
            )
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
                text=f"microburst: upstream {self.base_url} unreachable: {exc}",
            )

        resp_headers = {
            k: v for k, v in upstream.headers.items()
            if k.lower() not in HOP_BY_HOP
        }
        resp = web.StreamResponse(status=upstream.status, headers=resp_headers)
        await resp.prepare(request)
        async for chunk in upstream.content.iter_any():
            await resp.write(chunk)
        await resp.write_eof()
        return resp
