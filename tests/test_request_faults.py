"""Request-side faults: throttled and cut client→proxy uploads.

These exercise the request path — the SDK's *write* direction — not the
response path. ``slow_upload`` paces the proxy's read of the client body;
``cut_upload`` resets the client connection mid-upload without forwarding.
"""

from __future__ import annotations

import http.client
import time
import urllib.error
import urllib.request

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ConnectionClosedError

S3_AUTH = (
    "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, "
    "SignedHeaders=host;x-amz-date, Signature=deadbeef"
)
DDB_AUTH = (
    "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/dynamodb/"
    "aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef"
)


def _put(url: str, body: bytes) -> bytes:
    req = urllib.request.Request(
        url,
        data=body,
        method="PUT",
        headers={
            "Authorization": S3_AUTH,
            "Content-Type": "application/octet-stream",
        },
    )
    return urllib.request.urlopen(req, timeout=30).read()


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"Authorization": S3_AUTH})
    return urllib.request.urlopen(req, timeout=15).read()


def _ddb_post(url: str, body: bytes) -> bytes:
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": DDB_AUTH,
            "X-Amz-Target": "DynamoDB_20120810.PutItem",
            "Content-Type": "application/x-amz-json-1.0",
        },
    )
    return urllib.request.urlopen(req, timeout=30).read()


def _conn_fail(call, *args):
    """Run the request; pass iff the connection died with no HTTP response."""
    try:
        call(*args)
    except urllib.error.HTTPError as e:
        pytest.fail(f"got an HTTP response instead of a dead socket: {e}")
    except (urllib.error.URLError, http.client.HTTPException, OSError):
        return
    pytest.fail("request completed — expected a connection failure")


# -- slow_upload --------------------------------------------------------------


def test_slow_upload_streams_body_intact(upstub, microburst_server, aws_env):
    """Streaming op (PutObject): the proxy paces its read of the client
    body; the full payload still reaches upstream."""
    stub, upstream = upstub
    payload = b"z" * (64 * 1024)
    _, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"slow_upload": {"rate_kbps": 32}}}],
    )
    start = time.monotonic()
    _put(proxy.url + "/bucket/key", payload)
    elapsed = time.monotonic() - start
    assert elapsed >= 1.5  # 64 KiB at 32 KiB/s ≈ 2s floor
    assert stub.requests[0]["body"] == payload


def test_slow_upload_buffered_body_paces_send(
    upstub, microburst_server, aws_env
):
    """Pre-buffered body (dynamodb PutItem): the client's write already
    finished when rules evaluate, so the pacing shifts to the upstream
    send — the upstream observes a slow client and gets the full body."""
    stub, upstream = upstub
    payload = b'{"TableName":"t","Item":{"pad":{"S":"' + b"y" * 32768 + b'"}}}'
    _, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "dynamodb",
                "request": {"slow_upload": {"rate_kbps": 32}}}],
    )
    start = time.monotonic()
    _ddb_post(proxy.url + "/", payload)
    elapsed = time.monotonic() - start
    assert elapsed >= 0.7  # ~33 KiB at 32 KiB/s ≈ 1s floor
    assert stub.requests[0]["body"] == payload


def test_slow_upload_logged(upstub, microburst_server, aws_env):
    _, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"slow_upload": {"rate_kbps": 128}}}],
    )
    _put(proxy.url + "/bucket/key", b"z" * 1024)
    assert any("slow_upload" in e.action for e in sq.fired)


# -- cut_upload ---------------------------------------------------------------


def test_cut_upload_mid_stream(upstub, microburst_server, aws_env):
    """Streaming op: after N bytes of the upload are consumed the client
    connection resets — a real mid-upload failure; nothing is forwarded."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"cut_upload": {"after_bytes": 1024}}}],
    )
    _conn_fail(_put, proxy.url + "/bucket/key", b"z" * (64 * 1024))
    assert stub.count() == 0
    assert any("cut_upload" in e.action for e in sq.fired)


def test_cut_upload_buffered_body(upstub, microburst_server, aws_env):
    """Pre-buffered body: the client's write already finished, so the cut
    lands on its read path — still a connection error, still nothing
    upstream (documented divergence from a true mid-upload reset)."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "dynamodb",
                "request": {"cut_upload": {"after_bytes": 512}}}],
    )
    _conn_fail(_ddb_post, proxy.url + "/", b'{"TableName":"t"}' * 64)
    assert stub.count() == 0
    assert any("cut_upload" in e.action for e in sq.fired)


def test_cut_upload_after_frac(upstub, microburst_server, aws_env):
    """after_frac resolves against the request Content-Length."""
    stub, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"cut_upload": {"after_frac": 0.5}}}],
    )
    _conn_fail(_put, proxy.url + "/bucket/key", b"z" * (64 * 1024))
    assert stub.count() == 0


def test_cut_upload_past_body_end_forwards(
    upstub, microburst_server, aws_env
):
    """Threshold never crossed before EOF → the upload completes and the
    request forwards normally."""
    stub, upstream = upstub
    payload = b"z" * 1024
    _, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"cut_upload": {"after_bytes": 1 << 20}}}],
    )
    _put(proxy.url + "/bucket/key", payload)
    assert stub.requests[0]["body"] == payload


def test_slow_and_cut_compose(upstub, microburst_server, aws_env):
    """slow_upload + cut_upload: a throttled read that then gets cut."""
    stub, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"slow_upload": {"rate_kbps": 64},
                            "cut_upload": {"after_bytes": 4096}}}],
    )
    _conn_fail(_put, proxy.url + "/bucket/key", b"z" * (64 * 1024))
    assert stub.count() == 0


def test_request_fault_no_body_is_noop_but_logged(
    upstub, microburst_server, aws_env
):
    """GET (no body): request faults can't apply, but the match is still
    visible in the fired log — every fired fault is observable."""
    stub, upstream = upstub
    sq, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"cut_upload": {"after_bytes": 16}}}],
    )
    _get(proxy.url + "/bucket/key")
    assert stub.count() == 1  # forwarded normally
    assert any("cut_upload" in e.action for e in sq.fired)


def test_cut_upload_beats_terminal_effects(
    upstub, microburst_server, aws_env
):
    """A mid-upload cut kills the link before any response can be sent —
    a rule with error + cut produces a dead socket, not the error."""
    _, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "error": {"code": "SlowDown", "status": 503},
                "request": {"cut_upload": {"after_bytes": 64}}}],
    )
    _conn_fail(_put, proxy.url + "/bucket/key", b"z" * 4096)


def test_sdk_sees_connection_error_and_retries(
    upstub, microburst_server, aws_env
):
    """boto3 classifies the mid-upload reset as a connection-level error
    (ConnectionClosedError — retried, and a different SDK path than a
    response-body fault produces)."""
    stub, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3",
                "request": {"cut_upload": {"after_bytes": 256}}}],
    )
    client = boto3.client(
        "s3", endpoint_url=proxy.url, region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test",
        config=Config(retries={"max_attempts": 2},
                      s3={"addressing_style": "path"}),
    )
    with pytest.raises(ConnectionClosedError):
        client.put_object(Bucket="b", Key="k", Body=b"z" * (64 * 1024))
    assert stub.count() == 0  # nothing ever reached upstream


def test_request_rule_roundtrip():
    """`request:` survives to_dict — rule CRUD and file reload keep it."""
    from microburst.rules import RuleEngine, to_dict

    engine = RuleEngine()
    spec = {
        "service": "s3",
        "request": {
            "slow_upload": {"rate_kbps": 8},
            "cut_upload": {"after_bytes": 1024, "after_frac": 0.5},
        },
    }
    engine.set_rules([spec])
    assert to_dict(engine.rules[0])["request"] == spec["request"]
