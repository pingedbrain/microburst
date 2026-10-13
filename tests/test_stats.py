"""GET /_microburst/stats — aggregates: what each fault costs, not just what
fired. /metrics counts; /stats explains (latency distribution, per-rule hit
counts, per-service split, fault-vs-forwarded ratio).
"""

from test_proxy import _control, _ddb, _put_item


def test_stats_shape_when_idle(upstub, microburst_server, aws_env):
    _, proxy = microburst_server(upstub[1].url)
    stats = _control(proxy.url, "GET", "/_microburst/stats")
    assert stats["uptime_s"] >= 0
    assert stats["requests"] == {"total": 0, "faulted": 0, "forwarded": 0}
    assert stats["upstream_latency_ms"] == {"n": 0}
    assert stats["by_rule"] == []
    assert stats["by_service"] == []


def test_stats_counts_faults_and_forwards(upstub, microburst_server, aws_env):
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{"service": "dynamodb", "times": 1,
                "error": {"code": "InternalError", "status": 500}}],
    )
    _put_item(_ddb(proxy.url))  # attempt 1 faulted; the retry forwards
    stats = _control(proxy.url, "GET", "/_microburst/stats")
    assert stats["requests"]["total"] == 2
    assert stats["requests"]["faulted"] == 1
    assert stats["requests"]["forwarded"] == 1

    (entry,) = stats["by_rule"]
    assert entry["fired"] == 1
    assert "error:InternalError" in entry["describe"]

    (svc,) = stats["by_service"]
    assert svc == {"service": "dynamodb", "requests": 2, "faulted": 1}

    lat = stats["upstream_latency_ms"]
    assert lat["n"] == 1
    assert lat["min"] == lat["p50"] == lat["p95"] == lat["max"]
    assert lat["mean"] >= 0


def test_latency_fault_counts_as_faulted_and_forwarded(
    upstub, microburst_server, aws_env
):
    """A latency-then-forward rule lands in both buckets — faulted and
    forwarded are different seams, not a partition."""
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{"service": "dynamodb", "times": 1, "latency": 10}],
    )
    _put_item(_ddb(proxy.url))
    stats = _control(proxy.url, "GET", "/_microburst/stats")
    assert stats["requests"]["faulted"] == 1
    assert stats["requests"]["forwarded"] == 1
    assert stats["upstream_latency_ms"]["n"] == 1


def test_rule_hits_survive_rule_deletion(upstub, microburst_server, aws_env):
    """by_rule aggregates like fault_counts — a deleted rule's hits are
    still accounted for (rule.fired_count dies with the rule)."""
    _, proxy = microburst_server(upstub[1].url)
    _control(proxy.url, "POST", "/_microburst/rules", [
        {"service": "dynamodb", "times": 1,
         "error": {"code": "InternalError", "status": 500}},
    ])
    _put_item(_ddb(proxy.url))  # first attempt faulted, retry forwards
    _control(proxy.url, "DELETE", "/_microburst/rules", [])
    stats = _control(proxy.url, "GET", "/_microburst/stats")
    (entry,) = stats["by_rule"]
    assert entry["fired"] == 1
    assert entry["describe"] == "error:InternalError"


def test_stats_counts_unknown_service(upstub, microburst_server, aws_env):
    """Requests that detect no service still count, bucketed 'unknown'."""
    import urllib.request

    _, proxy = microburst_server(upstub[1].url)
    urllib.request.urlopen(proxy.url + "/no-aws-shape-here")  # forwarded
    stats = _control(proxy.url, "GET", "/_microburst/stats")
    assert stats["requests"]["forwarded"] == 1
    assert stats["by_service"] == [
        {"service": "unknown", "requests": 1, "faulted": 0}
    ]


# -- ProxyStats unit: reservoir bound + percentile math -----------------------

def test_latency_reservoir_is_bounded():
    from microburst.stats import LATENCY_RESERVOIR, ProxyStats

    stats = ProxyStats()
    for i in range(LATENCY_RESERVOIR + 100):
        stats.record_upstream_ms(float(i))
    assert len(stats.latency_ms) == LATENCY_RESERVOIR
    s = stats.latency_summary()
    # n/mean are all-time; min/max describe the recent reservoir window
    assert s["n"] == LATENCY_RESERVOIR + 100
    assert s["min"] == 100.0
    assert s["max"] == float(LATENCY_RESERVOIR + 99)


def test_latency_percentile_math():
    from microburst.stats import ProxyStats

    stats = ProxyStats()
    for ms in (10.0, 20.0, 30.0, 40.0):
        stats.record_upstream_ms(ms)
    assert stats.latency_summary() == {
        "n": 4, "min": 10.0, "p50": 20.0, "p95": 40.0,
        "max": 40.0, "mean": 25.0,
    }
