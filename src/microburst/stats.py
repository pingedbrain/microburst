"""Proxy statistics for ``GET /_microburst/stats``.

``/metrics`` counts; ``/stats`` explains — what each fault costs: per-rule
hit counts, upstream latency distribution, fault-vs-forwarded ratio.

Everything here is a running aggregate or a bounded reservoir, like the
Prometheus counters — nothing per-request is kept, and aggregates survive
both fired-log deque eviction and rule deletion (``rule.fired_count``
dies with its rule; ``by_rule`` doesn't).
"""

from __future__ import annotations

import math
import time
from collections import Counter, deque

# Percentile samples beyond this bound are dropped oldest-first — the
# reservoir describes *recent* upstream behavior; n and mean stay all-time.
LATENCY_RESERVOIR = 2048


class ProxyStats:
    """In-memory stats store. Single asyncio loop → plain counters/dicts,
    no locks (same convention as ``Microburst.fired``)."""

    def __init__(self) -> None:
        self.started_mono = time.monotonic()
        # requests relayed to the upstream seam — includes post-fault
        # relays (latency-then-forward) and cassette replays
        self.forwarded = 0
        # requests per detected service; "unknown" when detection fails
        self.by_service: Counter[str] = Counter()
        # rule id → {"describe": last fired action, "fired": hits}
        self.by_rule: dict[int, dict] = {}
        # upstream time-to-headers samples — see Upstream._relay_* for
        # what one sample covers
        self.latency_ms: deque[float] = deque(maxlen=LATENCY_RESERVOIR)
        self.latency_n = 0
        self.latency_total_ms = 0.0

    def record_request(self, service: str | None) -> None:
        self.by_service[service or "unknown"] += 1

    def record_fault(self, rule_id: int, action: str) -> None:
        """``action`` is the fired log's action string — the last one wins
        as the rule's ``describe`` (sampled values like latency_ms vary)."""
        entry = self.by_rule.setdefault(
            rule_id, {"describe": action, "fired": 0}
        )
        entry["describe"] = action
        entry["fired"] += 1

    def record_forward(self) -> None:
        self.forwarded += 1

    def record_upstream_ms(self, ms: float) -> None:
        self.latency_ms.append(ms)
        self.latency_n += 1
        self.latency_total_ms += ms

    def latency_summary(self) -> dict:
        """``n``/``mean`` are all-time; min/p50/p95/max describe the
        reservoir — the last ``LATENCY_RESERVOIR`` upstream responses."""
        if self.latency_n == 0:
            return {"n": 0}
        samples = sorted(self.latency_ms)
        return {
            "n": self.latency_n,
            "min": round(samples[0], 1),
            "p50": round(_percentile(samples, 0.50), 1),
            "p95": round(_percentile(samples, 0.95), 1),
            "max": round(samples[-1], 1),
            "mean": round(self.latency_total_ms / self.latency_n, 1),
        }


def _percentile(sorted_samples: list[float], q: float) -> float:
    """Nearest-rank percentile over an already-sorted list."""
    idx = math.ceil(q * len(sorted_samples)) - 1
    return sorted_samples[min(max(idx, 0), len(sorted_samples) - 1)]
