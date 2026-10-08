"""SSE fired-log streaming, rule TTL, and fired-log filters."""

from __future__ import annotations

import contextlib
import json
import time
import urllib.request

from test_proxy import _control, _ddb, _put_item

from microburst.core.context import RequestContext
from microburst.rules import RuleEngine, to_dict


def test_ttl_expires_rule():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "s3", "ttl_s": 0.05, "error": {"code": "SlowDown"}}]
    )
    ctx = RequestContext(service="s3", operation="GetObject")
    assert engine.decide(ctx) is not None
    time.sleep(0.07)
    assert engine.decide(ctx) is None


def test_ttl_remaining_reported():
    engine = RuleEngine()
    (rule,) = engine.set_rules([{"service": "s3", "ttl_s": 60}])
    d = to_dict(rule)
    assert d["ttl_s"] == 60.0
    assert 0 < d["ttl_remaining_s"] <= 60.0


def test_fired_filters(upstub, microburst_server, aws_env):
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[
            {"service": "dynamodb", "times": 1,
             "error": {"code": "ProvisionedThroughputExceededException"}},
            {"service": "sqs", "times": 1, "error": {"code": "ThrottledException"}},
        ],
    )
    # fault may exhaust retries — the fired log is written either way
    with contextlib.suppress(Exception):
        _put_item(_ddb(proxy.url))

    _control(proxy.url, "GET", "/_microburst/fired")  # warm
    ddb = _control(proxy.url, "GET", "/_microburst/fired?service=dynamodb")
    assert ddb and all(e["service"] == "dynamodb" for e in ddb)
    none = _control(proxy.url, "GET", "/_microburst/fired?service=s3")
    assert none == []


def test_fired_stream_sse(upstub, microburst_server, aws_env):
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[
            {"service": "dynamodb", "times": 1,
             "error": {"code": "ThrottlingException"}},
        ],
    )

    req = urllib.request.Request(proxy.url + "/_microburst/fired/stream")
    resp = urllib.request.urlopen(req, timeout=10)
    assert resp.headers["Content-Type"] == "text/event-stream"

    # fault may surface to the caller — the SSE event is what we assert
    with contextlib.suppress(Exception):
        _put_item(_ddb(proxy.url))

    # read until the first data: line (keepalives/comments skipped)
    deadline = time.time() + 10
    event = None
    for raw in resp:
        line = raw.decode().strip()
        if line.startswith("data:"):
            event = json.loads(line[5:].strip())
            break
        if time.time() > deadline:
            break
    resp.close()

    assert event is not None
    assert event["service"] == "dynamodb"
    assert event["operation"] == "PutItem"
    assert "ThrottlingException" in event["action"]
