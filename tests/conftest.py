"""Test fixtures: run aiohttp apps on real sockets so real SDKs can talk."""

from __future__ import annotations

import asyncio
import threading
from urllib.parse import parse_qs

import pytest
from aiohttp import web


class ServerThread:
    """Run an aiohttp app on a background event loop on a random port."""

    def __init__(self, app: web.Application):
        self.app = app
        self.loop = asyncio.new_event_loop()
        self.port: int | None = None
        self._runner: web.AppRunner | None = None
        self._started = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        asyncio.set_event_loop(self.loop)
        runner = web.AppRunner(self.app)
        self.loop.run_until_complete(runner.setup())
        site = web.TCPSite(runner, "127.0.0.1", 0)
        self.loop.run_until_complete(site.start())
        self._runner = runner
        self.port = site._server.sockets[0].getsockname()[1]
        self._started.set()
        self.loop.run_forever()
        self.loop.run_until_complete(runner.cleanup())

    def start(self) -> ServerThread:
        self.thread.start()
        assert self._started.wait(10), "server did not start"
        return self

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(10)


class Upstub:
    """Minimal upstream that answers AWS-shaped success responses."""

    def __init__(self):
        self.requests: list[dict] = []

    async def handle(self, request: web.Request) -> web.Response:
        body = await request.read()
        self.requests.append(
            {
                "method": request.method,
                "path": request.rel_url.raw_path_qs,
                "headers": dict(request.headers),
                "body": body,
            }
        )

        target = request.headers.get("X-Amz-Target", "")
        if target:
            # JSON protocol (dynamodb, kinesis, sqs on newer botocore, ...)
            return web.Response(
                status=200,
                content_type="application/x-amz-json-1.0",
                body=b"{}",
                headers={"x-amzn-RequestId": "stub-req-id"},
            )

        params = dict(request.rel_url.query)
        if "Action" not in params and body:
            params = {k: v[0] for k, v in parse_qs(body.decode()).items()}
        action = params.get("Action")
        if action:
            # Query protocol XML envelope
            xml = (
                f"<{action}Response><{action}Result></{action}Result>"
                "<ResponseMetadata><RequestId>stub-req-id</RequestId>"
                "</ResponseMetadata></" + f"{action}Response>"
            )
            return web.Response(
                status=200, content_type="text/xml", body=xml.encode()
            )

        # REST services (s3, lambda, ...) — empty success
        return web.Response(status=200, body=b"", content_type="application/octet-stream")

    def count(self) -> int:
        return len(self.requests)


@pytest.fixture
def upstub():
    stub = Upstub()
    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", stub.handle)
    server = ServerThread(app).start()
    yield stub, server
    server.stop()


@pytest.fixture
def microburst_server():
    """Factory fixture: start microburst pointing at a given upstream with rules."""
    from microburst.app import make_app
    from microburst.core.pipeline import Microburst

    started = []

    def _start(upstream: str, rules: list[dict] | None = None):
        sq = Microburst(upstream, rules=rules, resign=False)
        server = ServerThread(make_app(sq)).start()
        started.append(server)
        return sq, server

    yield _start
    for server in started:
        server.stop()


@pytest.fixture
def aws_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
