"""SSE fired-log streaming, rule TTL, and fired-log filters."""

from __future__ import annotations

import contextlib
import json
import time
import urllib.request

import pytest
from test_proxy import _control, _ddb, _put_item

from microburst.core.context import RequestContext
from microburst.rules import (
    LATENCY_PRESETS,
    RuleEngine,
    describe,
    from_dict,
    to_dict,
)


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


def test_active_at_defers_rule():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "s3", "active_at": time.time() + 0.05,
          "error": {"code": "SlowDown"}}]
    )
    ctx = RequestContext(service="s3", operation="GetObject")
    assert engine.decide(ctx) is None
    time.sleep(0.07)
    assert engine.decide(ctx) is not None


def test_until_expires_rule():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "s3", "until": time.time() + 0.05,
          "error": {"code": "SlowDown"}}]
    )
    ctx = RequestContext(service="s3", operation="GetObject")
    assert engine.decide(ctx) is not None
    time.sleep(0.07)
    assert engine.decide(ctx) is None


def test_window_reported_in_to_dict():
    engine = RuleEngine()
    (rule,) = engine.set_rules(
        [{"service": "s3", "active_at": time.time() + 60,
          "until": time.time() + 120}]
    )
    d = to_dict(rule)
    assert 0 < d["starts_in_s"] <= 60.0
    assert 0 < d["ends_in_s"] <= 120.0


def test_window_accepts_iso_and_datetime():
    from datetime import datetime, timezone

    from microburst.rules import from_dict

    iso = "2999-01-01T00:00:00Z"
    epoch = datetime(2999, 1, 1, tzinfo=timezone.utc).timestamp()
    assert from_dict({"active_at": iso}).active_at == epoch
    naive_dt = datetime.fromisoformat("2999-01-01")  # naive → host-local tz
    assert from_dict({"until": naive_dt}).until == naive_dt.timestamp()
    assert from_dict({"active_at": 1234.5}).active_at == 1234.5


def test_sigv4_presets_pin_auth_status():
    """Auth faults are 400/403 on the real wire — rest protocols would
    default to 503 without the pin."""
    from microburst.rules import PRESETS

    engine = RuleEngine()
    expected = {
        "expired-token": ("ExpiredTokenException", 400),
        "clock-skew": ("RequestTimeTooSkewed", 403),
        "bad-signature": ("SignatureDoesNotMatch", 403),
    }
    for name, (code, status) in expected.items():
        engine.set_rules([dict(PRESETS[name])])
        d = engine.decide(
            RequestContext(service="s3", operation="GetObject")
        )
        assert d is not None and d.error.code == code
        assert d.error.status == status


def test_watch_rules_reloads_on_change(tmp_path):
    import asyncio

    import yaml

    from microburst.app import _watch_rules
    from microburst.core.pipeline import Microburst

    sq = Microburst("http://x", rules=[{"service": "s3"}], resign=False)
    cfg = tmp_path / "chaos.yml"
    cfg.write_text(yaml.dump({"rules": [{"service": "sqs"}]}))

    async def go():
        task = asyncio.create_task(
            _watch_rules(sq, str(cfg), interval=0.02)
        )
        await asyncio.sleep(0.05)
        cfg.write_text(yaml.dump({"rules": [{"service": "dynamodb"}]}))
        for _ in range(100):
            await asyncio.sleep(0.02)
            if sq.engine.rules[0].service == "dynamodb":
                break
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(go())
    assert sq.engine.rules[0].service == "dynamodb"


def test_watch_rules_keeps_rules_on_bad_file(tmp_path):
    import asyncio

    from microburst.app import _watch_rules
    from microburst.core.pipeline import Microburst

    sq = Microburst("http://x", rules=[{"service": "s3"}], resign=False)
    cfg = tmp_path / "chaos.yml"
    cfg.write_text("rules:\n  - service: sqs\n")

    async def go():
        task = asyncio.create_task(
            _watch_rules(sq, str(cfg), interval=0.02)
        )
        await asyncio.sleep(0.05)
        cfg.write_text("rules: [{\n")  # broken yaml mid-edit
        await asyncio.sleep(0.1)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(go())
    assert sq.engine.rules[0].service == "s3"


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


# -- /metrics ------------------------------------------------------------


def test_metrics_endpoint(upstub, microburst_server, aws_env):
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[
            {"service": "dynamodb", "operation": "PutItem", "times": 2,
             "error": {"code": "ThrottlingException"}},
        ],
    )
    with contextlib.suppress(Exception):
        _put_item(_ddb(proxy.url))

    import urllib.request
    body = urllib.request.urlopen(
        proxy.url + "/_microburst/metrics", timeout=5
    ).read().decode()
    assert "microburst_requests_total" in body
    assert "microburst_faults_total{" in body
    assert 'service="dynamodb"' in body
    assert 'operation="PutItem"' in body
    assert "microburst_rules_active 1" in body


# -- latency distributions ---------------------------------------------------


def test_latency_gaussian_clamped():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "s3", "latency": {"dist": "gaussian", "mean": 500,
          "stddev": 50, "min": 100, "max": 900}}]
    )
    rule = engine.rules[0]
    assert rule.latency is not None
    samples = [rule.latency.sample() for _ in range(500)]
    assert all(100 <= s <= 900 for s in samples)
    # concentrated near the mean, not uniform across [100, 900]
    assert 400 <= sum(samples) / len(samples) <= 600


def test_latency_spike():
    engine = RuleEngine()
    engine.set_rules(
        [{"service": "s3", "latency": {"dist": "spike", "min": 10, "max": 50,
          "spike_ms": 3000, "spike_p": 0.2}}]
    )
    rule = engine.rules[0]
    assert rule.latency is not None
    samples = [rule.latency.sample() for _ in range(500)]
    spikes = [s for s in samples if s >= 3000]
    baseline = [s for s in samples if s < 3000]
    assert 60 < len(spikes) < 150       # ~20% of 500
    assert all(10 <= s <= 50 for s in baseline)


def test_latency_uniform_still_default():
    rule = from_dict({"latency": {"min": 5, "max": 10}})
    assert rule.latency is not None
    assert rule.latency.dist == "uniform"
    samples = [rule.latency.sample() for _ in range(100)]
    assert all(5 <= s <= 10 for s in samples)


# -- measured service latency presets -----------------------------------------


def test_latency_preset_resolves_measured_params():
    rule = from_dict({"latency": {"preset": "dynamodb"}})
    assert rule.latency is not None
    assert rule.latency.dist == "gaussian"
    # real AWS us-east-1, 2026-02: ListTables p50 163.4ms − ~155ms
    # host→region network floor → ~8ms service-side residual
    assert rule.latency.mean_ms == pytest.approx(8.4)
    assert rule.latency.stddev_ms >= 5.0


def test_latency_preset_explicit_keys_override():
    rule = from_dict(
        {"latency": {"preset": "dynamodb", "mean": 120, "max": 500}}
    )
    assert rule.latency is not None
    assert rule.latency.mean_ms == 120.0          # explicit wins
    assert rule.latency.max_ms == 500.0
    # untouched keys still come from the preset
    assert rule.latency.stddev_ms == pytest.approx(
        LATENCY_PRESETS["dynamodb"]["stddev"]
    )


def test_latency_preset_explicit_dist_overrides():
    rule = from_dict(
        {"latency": {"preset": "s3", "dist": "uniform", "min": 10, "max": 20}}
    )
    assert rule.latency is not None
    assert rule.latency.dist == "uniform"
    assert rule.latency.min_ms == 10.0
    assert rule.latency.max_ms == 20.0


def test_latency_preset_unknown_lists_valid_names():
    with pytest.raises(ValueError, match="unknown latency preset"):
        from_dict({"latency": {"preset": "nosuchservice"}})
    with pytest.raises(ValueError, match="dynamodb"):
        from_dict({"latency": {"preset": "nosuchservice"}})


def test_latency_preset_every_name_resolves():
    for name in LATENCY_PRESETS:
        rule = from_dict({"latency": {"preset": name}})
        assert rule.latency is not None
        assert rule.latency.mean_ms > 0
        assert rule.latency.sample() >= 0


def test_latency_preset_to_dict_and_describe():
    rule = from_dict(
        {"service": "dynamodb", "latency": {"preset": "dynamodb"}}
    )
    d = to_dict(rule)
    assert d["latency"]["preset"] == "dynamodb"

    engine = RuleEngine()
    engine.set_rules(
        [{"service": "dynamodb", "latency": {"preset": "dynamodb"}}]
    )
    ctx = RequestContext(service="dynamodb", operation="ListTables")
    decision = engine.decide(ctx)
    assert decision is not None
    assert decision.latency_ms >= 0
    assert "latency:dynamodb" in describe(decision)
