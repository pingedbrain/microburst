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


# -- header matchers ---------------------------------------------------------


def _ctx_h(headers, service="s3"):
    return RequestContext(
        service=service, operation="GetObject", headers=headers
    )


def test_header_substring_match():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "s3", "headers": {"x-amz-acl": "public-read"},
          "error": {"code": "AccessDenied"}}]
    )
    assert engine.decide(_ctx_h({"X-Amz-Acl": "public-read-write"})) is not None
    assert engine.decide(_ctx_h({"X-Amz-Acl": "private"})) is None
    assert engine.decide(_ctx_h({})) is None


def test_header_presence_check():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "s3", "headers": {"x-amz-copy-source": ""},
          "error": {"code": "SlowDown"}}]
    )
    # CopyObject carries x-amz-copy-source — presence alone matches
    assert engine.decide(_ctx_h({"x-amz-copy-source": "b/k"})) is not None
    assert engine.decide(_ctx_h({})) is None


def test_header_multiple_and():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "s3", "headers": {"x-amz-acl": "public", "x-amz-meta-team": "core"},
          "error": {"code": "SlowDown"}}]
    )
    assert engine.decide(_ctx_h({"x-amz-acl": "public-read", "x-amz-meta-team": "core"})) is not None
    assert engine.decide(_ctx_h({"x-amz-acl": "public-read"})) is None


def test_header_case_insensitive_name():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "s3", "headers": {"X-Copy-Source": "bucket"},
          "error": {"code": "SlowDown"}}]
    )
    assert engine.decide(_ctx_h({"x-copy-source": "bucket/k"})) is not None


def test_header_yaml_int_normalized():
    from microburst.rules import from_dict
    rule = from_dict({"service": "s3", "headers": {"x-attempt": 1}})
    assert rule.headers == {"x-attempt": "1"}


# -- body matchers ----------------------------------------------------------


def _ctx_body(body: bytes | None, service="dynamodb"):
    return RequestContext(service=service, operation="PutItem", body=body)


def test_body_jmespath_json():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "dynamodb", "body": "TableName == 'orders'",
          "error": {"code": "ProvisionedThroughputExceededException"}}]
    )
    hit = _ctx_body(b'{"TableName": "orders", "Item": {}}')
    miss = _ctx_body(b'{"TableName": "other", "Item": {}}')
    assert engine.decide(hit) is not None
    assert engine.decide(miss) is None


def test_body_jmespath_nested():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "dynamodb", "body": "Item.tenant.S == 'vip'",
          "error": {"code": "ThrottlingException"}}]
    )
    hit = _ctx_body(b'{"TableName": "t", "Item": {"tenant": {"S": "vip"}}}')
    miss = _ctx_body(b'{"TableName": "t", "Item": {"tenant": {"S": "free"}}}')
    assert engine.decide(hit) is not None
    assert engine.decide(miss) is None


def test_body_form_encoded():
    # query-protocol services post Action=...&Param=... form bodies
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "sns", "body": "TopicArn == 'arn:aws:sns:us-east-1:1:t'",
          "error": {"code": "ThrottledException"}}]
    )
    hit = _ctx_body(b"Action=Publish&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A1%3At", "sns")
    assert engine.decide(hit) is not None


def test_body_unparsable_no_match():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "dynamodb", "body": "TableName == 'x'",
          "error": {"code": "ThrottlingException"}}]
    )
    assert engine.decide(_ctx_body(b"\x00\xff binary garbage")) is None
    assert engine.decide(_ctx_body(None)) is None


def test_body_invalid_expression_rejected():
    from microburst.rules import from_dict
    try:
        from_dict({"body": "[[[invalid"})
    except ValueError as e:
        assert "invalid body jmespath" in str(e)
    else:
        raise AssertionError("expected ValueError")


# -- rate + sequence ----------------------------------------------------------


def test_rate_limit_window():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "s3", "rate": {"count": 2, "window_s": 60},
          "error": {"code": "SlowDown"}}]
    )
    ctx = _ctx("a")
    assert engine.decide(ctx) is not None
    assert engine.decide(ctx) is not None
    assert engine.decide(ctx) is None  # budget exhausted


def test_sequence_fail_pass():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "s3", "sequence": {"fail": 2, "pass": 2},
          "error": {"code": "SlowDown"}}]
    )
    ctx = _ctx("a")
    outcomes = [engine.decide(ctx) is not None for _ in range(8)]
    assert outcomes == [True, True, False, False, True, True, False, False]
