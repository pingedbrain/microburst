"""httpx upstream transport (--http2) + dashboard SSE reader."""

from __future__ import annotations

import importlib.util
import queue
import threading
import urllib.error
import urllib.request

import boto3
import pytest

requires_httpx = pytest.mark.skipif(
    importlib.util.find_spec("httpx") is None,
    reason="httpx not installed (microburst[h2])",
)


def _ddb(endpoint: str):
    return boto3.client(
        "dynamodb", region_name="us-east-1", endpoint_url=endpoint
    )


def _put(ddb):
    ddb.put_item(
        TableName="t", Item={"id": {"S": "1"}}
    )


@requires_httpx
def test_httpx_transport_relays(upstub, microburst_server, aws_env):
    """--http2 swaps the upstream client to httpx. Cleartext upstreams
    stay h1 (no h2c); the relay path is what this exercises."""
    _, proxy = microburst_server(upstub[1].url, http2=True)
    _put(_ddb(proxy.url))  # raises if the relay is broken
    assert upstub[0].count() == 1


@requires_httpx
def test_httpx_transport_with_fault(upstub, microburst_server, aws_env):
    from botocore.config import Config
    from botocore.exceptions import ClientError

    _, proxy = microburst_server(
        upstub[1].url,
        http2=True,
        rules=[
            {"service": "dynamodb",
             "error": {"code": "AccessDeniedException"}}
        ],
    )
    ddb = boto3.client(
        "dynamodb", region_name="us-east-1", endpoint_url=proxy.url,
        config=Config(retries={"total_max_attempts": 1}),
    )
    with pytest.raises(ClientError) as ei:
        _put(ddb)
    assert ei.value.response["Error"]["Code"] == "AccessDeniedException"


def test_httpx_fallback_without_flag(upstub, microburst_server, aws_env):
    """No --http2 → aiohttp session, not httpx."""
    sq, _ = microburst_server(upstub[1].url)
    assert sq.upstream.hx is None
    assert sq.upstream.session is not None


@requires_httpx
def test_http2_builds_httpx_client(upstub, microburst_server, aws_env):
    sq, _ = microburst_server(upstub[1].url, http2=True)
    assert sq.upstream.hx is not None


# -- dashboard SSE reader ----------------------------------------------------


def test_sse_events_reads_stream(upstub, microburst_server, aws_env):
    """The dashboard's background reader gets fired events off the wire."""
    from microburst.dashboard import sse_events

    _, proxy = microburst_server(
        upstub[1].url,
        rules=[
            {"service": "dynamodb",
             "error": {"code": "ThrottlingException"}}
        ],
    )
    out: queue.Queue = queue.Queue(maxsize=10)
    stop = threading.Event()
    reader = threading.Thread(
        target=sse_events,
        args=(proxy.url + "/_microburst/fired/stream", out, stop),
        daemon=True,
    )
    reader.start()
    try:
        with _suppress():
            _put(_ddb(proxy.url))
        event = out.get(timeout=10)
        assert event["service"] == "dynamodb"
        assert "ThrottlingException" in event["action"]
    finally:
        stop.set()
        reader.join(timeout=5)


def test_sse_events_ignores_keepalives():
    """Comment lines and blank lines never reach the queue."""
    import io

    from microburst.dashboard import sse_events

    class FakeResp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    payload = (
        b": keepalive\n\n"
        b'data: {"service":"s3","action":"error:SlowDown"}\n\n'
        b": another\n\n"
    )
    # the stream "ends" after the first payload — reconnects must not
    # replay it (urlopen starts failing)
    orig = urllib.request.urlopen
    calls = {"n": 0}

    def fake_urlopen(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeResp(payload)
        raise urllib.error.URLError("stream closed")

    urllib.request.urlopen = fake_urlopen
    try:
        out: queue.Queue = queue.Queue()
        stop = threading.Event()
        t = threading.Thread(
            target=sse_events, args=("http://x/stream", out, stop),
            daemon=True,
        )
        t.start()
        ev = out.get(timeout=5)
        stop.set()
        t.join(timeout=5)
        assert ev["service"] == "s3"
        assert out.empty()  # keepalives/comments never queued
    finally:
        urllib.request.urlopen = orig


class _suppress:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return True
