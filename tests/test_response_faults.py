"""Response-side faults: truncate, mid-stream abort, corrupt, bandwidth."""

from __future__ import annotations

import http.client
import time
import urllib.error
import urllib.request

import pytest
from aiohttp import web
from conftest import ServerThread

S3_AUTH = (
    "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, "
    "SignedHeaders=host;x-amz-date, Signature=deadbeef"
)


@pytest.fixture
def fat_upstream():
    """Upstream that serves a 64 KiB object on any GET."""
    app = web.Application()

    async def handle(request: web.Request) -> web.Response:
        return web.Response(
            status=200,
            body=b"x" * (64 * 1024),
            content_type="application/octet-stream",
        )

    app.router.add_route("*", "/{tail:.*}", handle)
    server = ServerThread(app).start()
    yield server
    server.stop()


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"Authorization": S3_AUTH})
    return urllib.request.urlopen(req, timeout=15).read()


def _s3_rule(**response_spec):
    return {"service": "s3", "response": response_spec}


def test_truncate_delivers_short_body(fat_upstream, microburst_server):
    _, proxy = microburst_server(
        fat_upstream.url, rules=[_s3_rule(truncate_frac=0.25)]
    )
    body = _get(proxy.url + "/bucket/key")
    assert 0 < len(body) < 64 * 1024
    assert len(body) <= 64 * 1024 // 4 + 8192  # frac + at most one chunk


def test_corrupt_same_length_different_bytes(fat_upstream, microburst_server):
    _, proxy = microburst_server(
        fat_upstream.url, rules=[_s3_rule(corrupt_bytes=64)]
    )
    body = _get(proxy.url + "/bucket/key")
    assert len(body) == 64 * 1024
    assert body != b"x" * (64 * 1024)


def test_abort_midstream_breaks_read(fat_upstream, microburst_server):
    _, proxy = microburst_server(
        fat_upstream.url, rules=[_s3_rule(abort_bytes=512)]
    )
    try:
        body = _get(proxy.url + "/bucket/key")
        # some clients surface it as a clean-ish short read
        assert len(body) < 64 * 1024
    except (urllib.error.URLError, http.client.HTTPException, OSError):
        pass  # incomplete read / reset — the expected outcome


def test_bandwidth_shaping_paces_stream(fat_upstream, microburst_server):
    # 64 KiB at 32 KiB/s ≈ 2s floor
    _, proxy = microburst_server(
        fat_upstream.url, rules=[_s3_rule(bandwidth_kbps=32)]
    )
    start = time.monotonic()
    body = _get(proxy.url + "/bucket/key")
    elapsed = time.monotonic() - start
    assert len(body) == 64 * 1024
    assert elapsed >= 1.5  # generous floor; theory says ~2s


def test_response_fault_logged(fat_upstream, microburst_server):
    sq, proxy = microburst_server(
        fat_upstream.url, rules=[_s3_rule(truncate_bytes=100)]
    )
    _get(proxy.url + "/bucket/key")
    assert any("truncate" in e.action for e in sq.fired)


def test_error_rule_still_wins_over_response(fat_upstream, microburst_server):
    """A rule with both error + response fires the terminal error."""
    _, proxy = microburst_server(
        fat_upstream.url,
        rules=[
            {
                "service": "s3",
                "error": {"code": "SlowDown", "status": 503},
                "response": {"truncate_bytes": 10},
            }
        ],
    )
    req = urllib.request.Request(
        proxy.url + "/bucket/key", headers={"Authorization": S3_AUTH}
    )
    with pytest.raises(urllib.error.HTTPError) as ei:
        urllib.request.urlopen(req, timeout=15)
    assert ei.value.status == 503


# -- XML body matchers -------------------------------------------------------


def test_xml_body_matcher(microburst_server, fat_upstream):
    from microburst.core.context import RequestContext
    from microburst.rules import RuleEngine

    engine = RuleEngine()
    engine.set_rules(
        [{"service": "s3",
          "body": "Tagging.TagSet.Tag[?Key=='env'].Value | [0] == 'prod'"}]
    )
    rule = engine.rules[0]
    xml = (
        b"<Tagging><TagSet>"
        b"<Tag><Key>env</Key><Value>prod</Value></Tag>"
        b"<Tag><Key>team</Key><Value>core</Value></Tag>"
        b"</TagSet></Tagging>"
    )
    ctx = RequestContext(
        service="s3", operation="PutBucketTagging", region="us-east-1",
        resource="bucket", headers={}, body=xml,
    )
    assert rule.matches(ctx)
    ctx2 = RequestContext(
        service="s3", operation="PutBucketTagging", region="us-east-1",
        resource="bucket", headers={},
        body=xml.replace(b"prod", b"dev"),
    )
    assert not rule.matches(ctx2)
