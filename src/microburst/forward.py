"""Upstream forwarding: request relay, streaming responses, SigV4 re-sign.

Data plane only — nothing here decides anything. Detection and fault
injection happen before a request reaches this module; ``ResponseFault``
is a transport-mutation spec the caller hands in, not a rule.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import re
import secrets
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import quote

import aiohttp
from aiohttp import web

from microburst import eventstream
from microburst.cassette import Cassette, entry_response_body

if TYPE_CHECKING:
    import httpx
else:
    try:
        import httpx
    except ImportError:  # optional — only needed for --http2
        httpx = None

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


@dataclass
class ResponseFault:
    """Post-forward response mutation. All fields optional; combine freely.

    - ``truncate_bytes``/``truncate_frac``: send a valid envelope with a
      shortened body (Content-Length stripped → clean truncation; the
      client "succeeds" with a partial payload — the nasty kind of fault).
    - ``abort_bytes``/``abort_frac``: send that much, then kill the
      connection mid-stream (client sees an incomplete read).
    - ``corrupt_bytes``: flip that many bytes in the buffered body.
    - ``bandwidth_kbps``: cap downstream throughput of the body stream.
    - ``event_error_*``: splice a well-formed event-stream ``:error``
      frame mid-stream (Kinesis SubscribeToShard, S3 Select, ...) after
      ``event_error_after`` upstream frames.
    """
    truncate_bytes: int | None = None
    truncate_frac: float | None = None
    abort_bytes: int | None = None
    abort_frac: float | None = None
    corrupt_bytes: int = 0
    bandwidth_kbps: float | None = None
    event_error_code: str | None = None
    event_error_message: str | None = None
    event_error_after: int = 3

    def resolve(self, content_length: int | None) -> None:
        """Turn frac fields into absolute byte counts once the upstream
        Content-Length is known."""
        if content_length is None:
            return
        if self.truncate_frac is not None and self.truncate_bytes is None:
            self.truncate_bytes = int(content_length * self.truncate_frac)
        if self.abort_frac is not None and self.abort_bytes is None:
            self.abort_bytes = int(content_length * self.abort_frac)


class Upstream:
    """Client for the upstream endpoint: session lifecycle + relay.

    ``http2=True`` swaps the aiohttp client for httpx with HTTP/2 enabled
    (needs ``microburst[h2]``). Only the *upstream* side negotiates H2 —
    the downstream side stays HTTP/1.1 because boto3/urllib3 doesn't
    speak H2 anyway.
    """

    def __init__(self, base_url: str, resign: bool = False,
                 http2: bool = False, cassette: Cassette | None = None):
        self.base_url = base_url.rstrip("/")
        self.resign = resign
        self.http2 = http2
        self.cassette = cassette
        self.session: aiohttp.ClientSession | None = None
        self.hx: httpx.AsyncClient | None = None  # set when http2

    async def start(self) -> None:
        if self.http2:
            try:
                import httpx
            except ImportError:
                logger.warning(
                    "--http2 requires `pip install microburst[h2]`; "
                    "falling back to HTTP/1.1"
                )
                self.http2 = False
            else:
                self.hx = httpx.AsyncClient(
                    http2=True,
                    timeout=httpx.Timeout(600, connect=30),
                )
        if not self.http2:
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
        if self.hx is not None:
            await self.hx.aclose()

    def _outbound_headers(self, request: web.Request) -> dict[str, str]:
        headers = {
            k.lower(): v
            for k, v in request.headers.items()
            if k.lower() not in HOP_BY_HOP
        }
        headers["host"] = self.base_url.split("//", 1)[-1].split("/")[0]
        return headers

    async def relay(
        self,
        request: web.Request,
        body: bytes | None,
        mutator: ResponseFault | None = None,
    ) -> web.StreamResponse:
        cass_key = None
        if self.cassette is not None:
            cass_key = Cassette.key_for(
                request.method, request.rel_url.raw_path_qs, body
            )
            if self.cassette.mode == "replay":
                entry = self.cassette.get(cass_key)
                if entry is None:
                    return web.Response(
                        status=503,
                        text="microburst: no cassette entry for "
                        f"{request.method} {request.rel_url.raw_path_qs} "
                        "(record with --record first)",
                    )
                entry_body = entry_response_body(entry)
                if mutator is not None:
                    mutator.resolve(len(entry_body))

                async def once():
                    yield entry_body

                return await _stream_to_client(
                    request, entry["status"], entry["headers"], mutator,
                    once(),
                )
        if self.hx is not None:
            return await self._relay_httpx(request, body, mutator, cass_key)
        return await self._relay_aiohttp(request, body, mutator, cass_key)

    async def _relay_httpx(
        self,
        request: web.Request,
        body: bytes | None,
        mutator: ResponseFault | None,
        cass_key: str | None,
    ) -> web.StreamResponse:
        assert self.hx is not None  # relay() only routes here when set
        url = self.base_url + request.rel_url.raw_path_qs
        headers = self._outbound_headers(request)

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

        async def content_stream():
            if body is not None:
                yield body
            else:
                async for chunk in request.content.iter_any():
                    yield chunk

        req = self.hx.build_request(
            request.method, url, headers=headers, content=content_stream()
        )
        try:
            upstream = await self.hx.send(req, stream=True)
        except httpx.HTTPError as exc:
            logger.error("upstream error: %s", exc)
            return web.Response(
                status=502,
                text=f"microburst: upstream {self.base_url} unreachable: {exc}",
            )
        chunk_size = 8192 if mutator and mutator.bandwidth_kbps else None
        # aiter_raw: no decoding — we forward bytes verbatim, the client
        # owns Content-Encoding semantics
        chunks = upstream.aiter_raw(chunk_size=chunk_size)
        if cass_key is not None and self.cassette is not None:
            chunks = self.cassette.tee(
                chunks, cass_key, upstream.status_code, upstream.headers
            )
        return await _stream_to_client(
            request,
            upstream.status_code,
            upstream.headers,
            mutator,
            chunks,
        )

    async def _relay_aiohttp(
        self,
        request: web.Request,
        body: bytes | None,
        mutator: ResponseFault | None,
        cass_key: str | None,
    ) -> web.StreamResponse:
        assert self.session is not None
        url = self.base_url + request.rel_url.raw_path_qs
        headers = self._outbound_headers(request)

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

        chunk_size = 8192 if mutator and mutator.bandwidth_kbps else 0
        chunks = (
            upstream.content.iter_chunked(chunk_size)
            if chunk_size
            else upstream.content.iter_any()
        )
        if cass_key is not None and self.cassette is not None:
            chunks = self.cassette.tee(
                chunks, cass_key, upstream.status, upstream.headers
            )
        return await _stream_to_client(
            request, upstream.status, upstream.headers, mutator, chunks
        )


async def _stream_to_client(
    request: web.Request,
    status: int,
    headers,
    mutator: ResponseFault | None,
    chunks,
) -> web.StreamResponse:
    """Shared response writer for both transports: copies upstream headers,
    applies ResponseFault mutations, streams chunks downstream."""
    resp_headers = {
        k: v for k, v in headers.items() if k.lower() not in HOP_BY_HOP
    }

    inject_frame = None
    if mutator is not None:
        cl = headers.get("content-length")
        mutator.resolve(int(cl) if cl else None)
        # Truncation drops Content-Length so the client reads a
        # complete-but-short body; abort keeps it so the client
        # sees an incomplete read. Corrupt keeps it — same length,
        # wrong bytes, the nastiest outcome.
        if mutator.truncate_bytes is not None:
            resp_headers.pop("Content-Length", None)
        if mutator.event_error_code and str(
            headers.get("content-type", "")
        ).startswith(eventstream.CONTENT_TYPE):
            # the stream grows by a frame — Content-Length no longer
            # describes it (streams usually lack one anyway)
            resp_headers.pop("Content-Length", None)
            inject_frame = eventstream.build_error_frame(
                mutator.event_error_code,
                mutator.event_error_message or mutator.event_error_code,
            )

    resp = web.StreamResponse(status=status, headers=resp_headers)
    await resp.prepare(request)

    if inject_frame is not None:
        # inject_frame is only set when mutator has event_error_code
        assert mutator is not None
        chunks = eventstream.splice_after_frames(
            chunks, mutator.event_error_after, inject_frame
        )

    if mutator is not None and mutator.corrupt_bytes:
        # corrupt needs the whole buffered body — same length, wrong bytes
        data = bytearray()
        async for c in chunks:
            data.extend(c)
        for _ in range(min(mutator.corrupt_bytes, len(data))):
            data[secrets.randbelow(len(data))] ^= 0xFF
        await resp.write(bytes(data))
        await resp.write_eof()
        return resp

    sent = 0
    async for chunk in chunks:
        if mutator is not None:
            if mutator.bandwidth_kbps:
                await asyncio.sleep(len(chunk) / (mutator.bandwidth_kbps * 1024))
            if mutator.truncate_bytes is not None:
                remaining = mutator.truncate_bytes - sent
                if remaining <= 0:
                    break
                chunk = chunk[:remaining]
            if (
                mutator.abort_bytes is not None
                and sent + len(chunk) > mutator.abort_bytes
            ):
                keep = mutator.abort_bytes - sent
                if keep > 0:
                    await resp.write(chunk[:keep])
                # die mid-stream: partial body already sent, now
                # the connection drops — the client's read fails
                transport = request.transport
                if transport is not None:
                    transport.abort()
                return resp
        await resp.write(chunk)
        sent += len(chunk)
    await resp.write_eof()
    return resp
