"""Presigned URL / SigV4A scope parsing + deterministic rule draws."""

from __future__ import annotations

from microburst.core.context import RequestContext
from microburst.detection import detect
from microburst.rules import RuleEngine


def test_presigned_url_scope():
    ctx = detect(
        {},
        "GET",
        "/photos/cat.jpg",
        {
            "X-Amz-Credential": "AKID/20240101/us-east-1/s3/aws4_request",
            "X-Amz-Date": "20240101T000000Z",
            "X-Amz-Signature": "abc",
            "X-Amz-Expires": "3600",
        },
        None,
    )
    assert ctx.service == "s3"
    assert ctx.region == "us-east-1"
    assert ctx.access_key == "AKID"
    assert ctx.operation == "GetObject"


def test_presigned_lowercase_param():
    ctx = detect(
        {},
        "GET",
        "/b/k",
        {"x-amz-credential": "AK/20240101/eu-west-1/s3/aws4_request"},
        None,
    )
    assert ctx.service == "s3"
    assert ctx.region == "eu-west-1"


def test_sigv4a_scope():
    headers = {
        "Authorization": (
            "AWS4-ECDSA-P256-SHA256 Credential=AKID/20240101/*/s3/aws4_request, "
            "SignedHeaders=host, Signature=abc"
        )
    }
    ctx = detect(headers, "GET", "/bucket/key", {}, None)
    assert ctx.service == "s3"
    assert ctx.region == "*"


def test_authorization_wins_over_presigned():
    headers = {
        "Authorization": (
            "AWS4-HMAC-SHA256 Credential=AK/20240101/us-east-1/sqs/aws4_request, "
            "SignedHeaders=host, Signature=abc"
        )
    }
    ctx = detect(
        headers, "POST", "/",
        {"X-Amz-Credential": "AK/20240101/us-east-1/s3/aws4_request"},
        None,
    )
    assert ctx.service == "sqs"


def test_malformed_presigned_ignored():
    ctx = detect(
        {}, "GET", "/",
        {"X-Amz-Credential": "not-a-scope"},
        None,
    )
    assert ctx.service is None


# -- deterministic draws ---------------------------------------------------


def _ctx(resource):
    return RequestContext(service="s3", operation="GetObject", resource=resource)


def test_deterministic_same_resource_same_decision():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "s3", "probability": 0.5, "deterministic": True,
          "error": {"code": "SlowDown"}}]
    )
    ctx = _ctx("photos/cat.jpg")
    first = engine.decide(ctx)
    for _ in range(20):
        assert (engine.decide(ctx) is not None) == (first is not None)


def test_deterministic_splits_resources():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "s3", "probability": 0.5, "deterministic": True,
          "error": {"code": "SlowDown"}}]
    )
    fired = sum(
        engine.decide(_ctx(f"obj-{i}")) is not None for i in range(200)
    )
    # not all, not none — a real split
    assert 50 < fired < 150


def test_deterministic_tiers_nest():
    """Resources failing at p=0.2 must be a subset of those failing at p=0.8."""
    small = RuleEngine()
    small.set_rules([{"service": "s3", "probability": 0.2, "deterministic": True}])
    big = RuleEngine()
    big.set_rules([{"service": "s3", "probability": 0.8, "deterministic": True}])
    for i in range(200):
        ctx = _ctx(f"r-{i}")
        if small.decide(ctx) is not None:
            assert big.decide(ctx) is not None


def test_random_rule_still_varies():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "s3", "probability": 0.5, "error": {"code": "SlowDown"}}]
    )
    decisions = [engine.decide(_ctx("same")) is not None for _ in range(50)]
    assert any(decisions) and not all(decisions)
