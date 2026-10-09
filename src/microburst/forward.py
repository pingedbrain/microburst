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
from collections.abc import AsyncIterable
from dataclasses import dataclass, field
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
    - ``event_mutations``: frame-level event-stream surgery — drop,
      repayload, corrupt, inject, or cut frames at given indices
      (``response.event_frames`` in the DSL).
    - ``set_headers``/``strip_headers``: mutate response headers —
      wrong Content-Type on a 200, stripped ``x-amz-*`` headers —
      exercises SDK parse paths body corruption doesn't reach.
      Applied after the built-in mutations so explicit intent wins.
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
    event_mutations: tuple[eventstream.Mutation, ...] = ()
    set_headers: dict[str, str] = field(default_factory=dict)
    strip_headers: tuple[str, ...] = ()

    def resolve(self, content_length: int | None) -> None:
        """Turn frac fields into absolute byte counts once the upstream
        Content-Length is known."""
        if content_length is None:
            return
        if self.truncate_frac is not None and self.truncate_bytes is None:
            self.truncate_bytes = int(content_length * self.truncate_frac)
        if self.abort_frac is not None and self.abort_bytes is None:
            self.abort_bytes = int(content_length * self.abort_frac)


@dataclass
class RequestFault:
    """Client→proxy upload mutation. All fields optional; combine freely.

    - ``rate_kbps`` (``request.slow_upload.rate_kbps``): throttle the
      proxy's read of the client body. On a still-streaming upload this
      backpressures the SDK's write path; on a pre-buffered body the
      write already finished, so the pacing shifts to the upstream send
      (the upstream observes the slow client).
    - ``after_bytes``/``after_frac`` (``request.cut_upload``): after the
      proxy has consumed that much of the client upload, reset the client
      connection WITHOUT forwarding — no response is ever produced. On a
      still-streaming upload the SDK sees ECONNRESET mid-PUT; on a
      pre-buffered body the write already finished, so the reset lands on
      its read path (same observable as ``reset: true``).

    Fidelity caveat — aiohttp does not pre-buffer request bodies, but
    microburst does whenever detection needs one (non-streaming ops
    ≤4 MiB, body matchers, cassette mode). Only streaming uploads (S3
    PutObject/UploadPart, large payloads) can be cut truly mid-write.
    """
    rate_kbps: float | None = None
    after_bytes: int | None = None
    after_frac: float | None = None

    def resolve(self, content_length: int | None) -> None:
        """Turn ``after_frac`` into an absolute byte count once the
        request body size is known."""
        if content_length is None:
            return
        if self.after_frac is not None and self.after_bytes is None:
            self.after_bytes = int(content_length * self.after_frac)


async def consume_upload(
    request: web.Request,
    body: bytes | None,
    fault: RequestFault,
) -> tuple[bytes | AsyncIterable[bytes] | None, bool]:
    """Apply the client→proxy half of a fired rule.

    Returns ``(data, cut)``. ``data`` is the payload source ``relay()``
    should send upstream (``None`` → unchanged default: the buffered body
    or ``request.content``). ``cut`` means the client connection was
    reset — the caller must not forward and any response it returns dies
    on the dead socket.

    A cut fires only if the proxy's read of the client body actually
    crosses ``after_bytes`` before EOF — a body shorter than the
    threshold uploads completely and forwards. Because a streaming cut
    can't know the threshold was crossed until it reads, it buffers what
    it consumes; a threshold past the body end therefore reads the whole
    upload into memory (same outcome as the normal buffered path).
    """
    if body is not None:
        # Body was pre-buffered for detection — the client's write is
        # already done. The buffered length is the truth (chunked uploads
        # carry no Content-Length to resolve a frac against).
        fault.resolve(len(body))
        if fault.after_bytes is not None and len(body) >= fault.after_bytes:
            _abort_client(request)
            return None, True
        if fault.rate_kbps:
            return _pace(_byte_chunks(body), fault.rate_kbps), False
        return None, False

    if not request.can_read_body:
        return None, False  # no body — request faults no-op

    fault.resolve(request.content_length)
    if fault.after_bytes is not None:
        # Receive the bytes the fault models — a real client→proxy link
        # failure means the client wrote them before the reset, so the
        # proxy's read must get that far. The upstream never sees them.
        collected = bytearray()
        async for chunk in request.content.iter_chunked(8192):
            if fault.rate_kbps:
                await asyncio.sleep(len(chunk) / (fault.rate_kbps * 1024))
            collected += chunk
            if len(collected) >= fault.after_bytes:
                _abort_client(request)
                return None, True
        return bytes(collected), False  # body ended before the threshold

    if fault.rate_kbps:
        return _pace(request.content.iter_chunked(8192), fault.rate_kbps), False
    return None, False


def _abort_client(request: web.Request) -> None:
    """Kill the client connection — the write the caller returns will
    never reach the socket."""
    transport = getattr(request, "transport", None)
    if transport is not None:
        transport.abort()


async def _byte_chunks(data: bytes, size: int = 8192):
    for i in range(0, len(data), size):
        yield data[i:i + size]


async def _pace(chunks, rate_kbps: float):
    """Re-yield a chunk stream throttled to ``rate_kbps`` — mirrors the
    response-side bandwidth pacing (sleep per chunk before passing it on)."""
    async for chunk in chunks:
        await asyncio.sleep(len(chunk) / (rate_kbps * 1024))
        yield chunk


class Upstream:
    """Client for the upstream endpoint: session lifecycle + relay.

    ``http2=True`` swaps the aiohttp client for httpx with HTTP/2 enabled
    (needs ``microburst[h2]``). Only the *upstream* side negotiates H2 —
    the downstream side stays HTTP/1.1 because boto3/urllib3 doesn't
    speak H2 anyway.
    """

    def __init__(self, base_url: str, resign: bool = False,
                 http2: bool = False, cassette: Cassette | None = None,
                 stats=None):
        self.base_url = base_url.rstrip("/")
        self.resign = resign
        self.http2 = http2
        self.cassette = cassette
        # ProxyStats sink for upstream latency samples (None = don't record)
        self.stats = stats
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
        data: bytes | AsyncIterable[bytes] | None = None,
    ) -> web.StreamResponse:
        """``data`` overrides the outbound payload source (bytes or an
        async iterable — e.g. a throttled upload stream from
        ``consume_upload``); ``body`` stays the metadata body used for
        cassette keys and SigV4 payload hashing."""
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
            return await self._relay_httpx(
                request, body, mutator, cass_key, data
            )
        return await self._relay_aiohttp(
            request, body, mutator, cass_key, data
        )

    async def _relay_httpx(
        self,
        request: web.Request,
        body: bytes | None,
        mutator: ResponseFault | None,
        cass_key: str | None,
        data: bytes | AsyncIterable[bytes] | None,
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
            payload = data if data is not None else body
            if payload is None:
                async for chunk in request.content.iter_any():
                    yield chunk
            elif isinstance(payload, (bytes, bytearray)):
                yield bytes(payload)
            else:
                async for chunk in payload:
                    yield chunk

        req = self.hx.build_request(
            request.method, url, headers=headers, content=content_stream()
        )
        # Latency seam: send → response headers received. Downstream body
        # streaming (and any injected bandwidth/abort mutation) stays out
        # of the sample — it measures the upstream, not the fault.
        t0 = time.monotonic()
        try:
            upstream = await self.hx.send(req, stream=True)
        except httpx.HTTPError as exc:
            logger.error("upstream error: %s", exc)
            return web.Response(
                status=502,
                text=f"microburst: upstream {self.base_url} unreachable: {exc}",
            )
        if self.stats is not None:
            self.stats.record_upstream_ms((time.monotonic() - t0) * 1000)
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
        data: bytes | AsyncIterable[bytes] | None,
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

        payload = data if data is not None else (
            body if body is not None else request.content
        )
        # Latency seam: send → response headers received — same convention
        # as _relay_httpx; a 502 (upstream unreachable) records no sample.
        t0 = time.monotonic()
        try:
            upstream = await self.session.request(
                request.method,
                url,
                headers=headers,
                data=payload,
                allow_redirects=False,
            )
        except aiohttp.ClientError as exc:
            logger.error("upstream error: %s", exc)
            return web.Response(
                status=502,
                text=f"microburst: upstream {self.base_url} unreachable: {exc}",
            )
        if self.stats is not None:
            self.stats.record_upstream_ms((time.monotonic() - t0) * 1000)

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

    if mutator is not None:
        cl = headers.get("content-length")
        mutator.resolve(int(cl) if cl else None)
        # Truncation drops Content-Length so the client reads a
        # complete-but-short body; abort keeps it so the client
        # sees an incomplete read. Corrupt keeps it — same length,
        # wrong bytes, the nastiest outcome.
        if mutator.truncate_bytes is not None:
            resp_headers.pop("Content-Length", None)
        if str(headers.get("content-type", "")).startswith(
            eventstream.CONTENT_TYPE
        ):
            event_mutations = mutator.event_mutations
            if mutator.event_error_code:
                event_mutations = (
                    *event_mutations,
                    eventstream.Mutation(
                        at=mutator.event_error_after,
                        error=eventstream.build_error_frame(
                            mutator.event_error_code,
                            mutator.event_error_message
                            or mutator.event_error_code,
                        ),
                    ),
                )
            if event_mutations:
                # the stream changes shape — Content-Length no longer
                # describes it (streams usually lack one anyway)
                resp_headers.pop("Content-Length", None)
                chunks = eventstream.mutate_frames(chunks, event_mutations)
        # Explicit header mutation wins over the built-in adjustments:
        # strip first, then set (a set wins over a strip of the same
        # name — order in the spec is meaningless, intent isn't).
        if mutator.strip_headers:
            strip = {h.lower() for h in mutator.strip_headers}
            resp_headers = {
                k: v for k, v in resp_headers.items()
                if k.lower() not in strip
            }
        if mutator.set_headers:
            replaced = {k.lower() for k in mutator.set_headers}
            resp_headers = {
                k: v for k, v in resp_headers.items()
                if k.lower() not in replaced
            }
            resp_headers.update(mutator.set_headers)

    resp = web.StreamResponse(status=status, headers=resp_headers)
    await resp.prepare(request)

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
