"""Fault rules: matchers + effects + presets.

A rule is a match predicate (service / operation / region / resource) plus a
probability plus one or more effects (latency, error, timeout, reset). Rules
evaluate in order; the first matching rule wins per request.
"""

from __future__ import annotations

import itertools
import random
import time
from dataclasses import dataclass, field

from microburst.detect import RequestInfo
from microburst.models import operation_error_names

_ids = itertools.count(1)


@dataclass
class Latency:
    min_ms: float
    max_ms: float

    def sample(self) -> float:
        return random.uniform(self.min_ms, self.max_ms)


@dataclass
class FaultError:
    code: str | None = None      # None → sample from the operation's modeled errors
    status: int | None = None    # None → modeled httpStatusCode, else 400
    message: str | None = None


@dataclass
class Rule:
    service: str | None = None
    operation: str | None = None
    region: str | None = None
    resource: str | None = None
    probability: float = 1.0
    times: int | None = None
    error: FaultError | None = None
    latency: Latency | None = None
    timeout_ms: float | None = None
    reset: bool = False
    id: int = field(default_factory=lambda: next(_ids))
    fired_count: int = 0

    def matches(self, info: RequestInfo) -> bool:
        if self.times is not None and self.fired_count >= self.times:
            return False
        if self.service and self.service != "*" and self.service != info.service:
            return False
        if self.operation and self.operation != info.operation:
            return False
        if self.region and self.region != info.region:
            return False
        return not (self.resource and (info.resource is None or self.resource not in info.resource))


@dataclass
class Decision:
    rule: Rule
    error: FaultError | None
    latency_ms: float
    timeout_ms: float | None
    reset: bool


def from_dict(data: dict) -> Rule:
    error = data.get("error")
    latency = data.get("latency")
    return Rule(
        service=data.get("service"),
        operation=data.get("operation"),
        region=data.get("region"),
        resource=data.get("resource"),
        probability=float(data.get("probability", 1.0)),
        times=data.get("times"),
        error=FaultError(**error) if isinstance(error, dict) else None,
        latency=(
            Latency(float(latency), float(latency))
            if isinstance(latency, (int, float))
            else Latency(float(latency["min"]), float(latency["max"]))
            if isinstance(latency, dict)
            else None
        ),
        timeout_ms=data.get("timeout_ms"),
        reset=bool(data.get("reset", False)),
    )


def to_dict(rule: Rule) -> dict:
    out: dict = {"id": rule.id}
    for attr in ("service", "operation", "region", "resource"):
        value = getattr(rule, attr)
        if value is not None:
            out[attr] = value
    if rule.probability != 1.0:
        out["probability"] = rule.probability
    if rule.times is not None:
        out["times"] = rule.times
    if rule.error:
        out["error"] = {
            k: v
            for k, v in {
                "code": rule.error.code,
                "status": rule.error.status,
                "message": rule.error.message,
            }.items()
            if v is not None
        }
    if rule.latency:
        out["latency"] = {"min": rule.latency.min_ms, "max": rule.latency.max_ms}
    if rule.timeout_ms is not None:
        out["timeout_ms"] = rule.timeout_ms
    if rule.reset:
        out["reset"] = True
    out["fired_count"] = rule.fired_count
    return out


class RuleEngine:
    def __init__(self) -> None:
        self._rules: list[Rule] = []

    @property
    def rules(self) -> list[Rule]:
        return self._rules

    def set_rules(self, data: list[dict]) -> list[Rule]:
        self._rules = [from_dict(item) for item in data]
        return self._rules

    def add_rules(self, data: list[dict]) -> list[Rule]:
        new = [from_dict(item) for item in data]
        self._rules.extend(new)
        return new

    def delete_matching(self, spec: list[dict]) -> int:
        """Remove rules matching each spec (all given fields equal). Empty
        list clears everything."""
        if not spec:
            count = len(self._rules)
            self._rules.clear()
            return count
        removed = 0
        for target in spec:
            keep = []
            for rule in self._rules:
                comparable = to_dict(rule)
                comparable.pop("id")
                comparable.pop("fired_count")
                if all(comparable.get(k) == v for k, v in target.items()):
                    removed += 1
                else:
                    keep.append(rule)
            self._rules = keep
        return removed

    def decide(self, info: RequestInfo) -> Decision | None:
        for rule in self._rules:
            if not rule.matches(info):
                continue
            if random.random() >= rule.probability:
                continue
            rule.fired_count += 1
            error = rule.error
            if error and error.code is None:
                plausible = operation_error_names(info.service or "", info.operation or "")
                if plausible:
                    error = FaultError(
                        code=random.choice(plausible),
                        status=error.status,
                        message=error.message,
                    )
            return Decision(
                rule=rule,
                error=error,
                latency_ms=rule.latency.sample() if rule.latency else 0.0,
                timeout_ms=rule.timeout_ms,
                reset=rule.reset,
            )
        return None


PRESETS: dict[str, dict] = {
    "ddb-throttle": {
        "service": "dynamodb",
        "probability": 0.1,
        "error": {"code": "ProvisionedThroughputExceededException"},
    },
    "flaky-s3": {
        "service": "s3",
        "probability": 0.05,
        "error": {"code": "SlowDown"},
    },
    "slow-lambda": {
        "service": "lambda",
        "operation": "Invoke",
        "latency": {"min": 500, "max": 2000},
    },
    "kms-outage": {
        "service": "kms",
        "probability": 1.0,
        "error": {"code": "KMSInternalException", "status": 503},
    },
    "sqs-backlog": {
        "service": "sqs",
        "latency": {"min": 2000, "max": 5000},
    },
    "regional-failover": {
        "service": "*",
        "probability": 0.5,
        "error": {"code": "ServiceUnavailable", "status": 503},
    },
    "network-jitter": {
        "service": "*",
        "latency": {"min": 50, "max": 300},
    },
    "gateway-storm": {
        "service": "*",
        "probability": 0.2,
        "error": {"code": "InternalError", "status": 500},
    },
}


@dataclass
class FiredEvent:
    ts: float
    rule_id: int
    service: str | None
    operation: str | None
    resource: str | None
    region: str | None
    action: str
    path: str

    def to_dict(self) -> dict:
        return {
            "ts": self.ts,
            "rule_id": self.rule_id,
            "service": self.service,
            "operation": self.operation,
            "resource": self.resource,
            "region": self.region,
            "action": self.action,
            "path": self.path,
        }


def describe(decision: Decision) -> str:
    parts = []
    if decision.latency_ms:
        parts.append(f"latency:{decision.latency_ms:.0f}ms")
    if decision.error:
        parts.append(f"error:{decision.error.code}")
    if decision.timeout_ms:
        parts.append(f"timeout:{decision.timeout_ms:.0f}ms")
    if decision.reset:
        parts.append("reset")
    return "+".join(parts) or "match"


def now() -> float:
    return time.time()
