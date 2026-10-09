"""Fault rules: matchers + effects + presets.

A rule is a match predicate (service / operation / region / resource) plus a
probability plus one or more effects (latency, error, timeout, reset). Rules
evaluate in order; the first matching rule wins per request.
"""

from __future__ import annotations

import base64
import hashlib
import itertools
import json
import random
import time
import xml.etree.ElementTree as ET
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl

import jmespath

from microburst.core.context import BODY_UNSET, RequestContext
from microburst.models import operation_error_names

if TYPE_CHECKING:
    from microburst.forward import ResponseFault

_ids = itertools.count(1)


@dataclass
class Latency:
    """Latency distribution. `uniform` needs min/max; `gaussian` mean/stddev
    (optionally clamped to min/max); `spike` is mostly baseline (min/max)
    with occasional spike_ms hits at spike_p probability."""
    dist: str = "uniform"
    min_ms: float = 0.0
    max_ms: float = 0.0
    mean_ms: float = 0.0
    stddev_ms: float = 0.0
    spike_ms: float = 0.0
    spike_p: float = 0.05

    def sample(self) -> float:
        if self.dist == "gaussian":
            v = random.gauss(self.mean_ms, self.stddev_ms)
            if self.max_ms > self.min_ms:
                v = min(max(v, self.min_ms), self.max_ms)
            return max(v, 0.0)
        if self.dist == "spike":
            if random.random() < self.spike_p:
                return self.spike_ms or self.max_ms
            return random.uniform(self.min_ms, self.max_ms)
        return random.uniform(self.min_ms, self.max_ms)


@dataclass
class FaultError:
    code: str | None = None      # None → sample from the operation's modeled errors
    status: int | None = None    # None → modeled httpStatusCode, else 400
    message: str | None = None
    fields: dict | None = None   # extra error-shape members, rendered per protocol


@dataclass
class Rule:
    service: str | None = None
    operation: str | None = None
    region: str | None = None
    resource: str | None = None
    headers: dict[str, str] | None = None
    body: str | None = None          # jmespath expression — truthy = match
    rate_count: int | None = None    # at most N fires per rate_window
    rate_window: float | None = None  # seconds
    sequence: tuple[int, int] | None = None  # (fail N, pass M), repeating
    probability: float = 1.0
    deterministic: bool = False
    times: int | None = None
    error: FaultError | None = None
    latency: Latency | None = None
    timeout_ms: float | None = None
    reset: bool = False
    response: dict | None = None   # post-forward response mutation spec
    ttl_s: float | None = None     # rule expires N seconds after creation
    active_at: float | None = None # epoch — rule starts matching then
    until: float | None = None     # epoch — rule stops matching then
    id: int = field(default_factory=lambda: next(_ids))
    fired_count: int = 0
    created_ts: float = field(default_factory=time.time)
    _body_expr: Any = field(default=None, repr=False)  # jmespath Parser
    _fired_ts: deque = field(default_factory=deque, repr=False)
    _seq_pos: int = field(default=0, repr=False)

    def matches(self, info: RequestContext) -> bool:
        if self.times is not None and self.fired_count >= self.times:
            return False
        now = time.time()
        if self.ttl_s is not None and now - self.created_ts >= self.ttl_s:
            return False
        if self.active_at is not None and now < self.active_at:
            return False
        if self.until is not None and now >= self.until:
            return False
        if self.service and self.service != "*" and self.service != info.service:
            return False
        if self.operation and self.operation != info.operation:
            return False
        if self.region and self.region != info.region:
            return False
        if self.resource and (info.resource is None or self.resource not in info.resource):
            return False
        if self.headers:
            for name, needle in self.headers.items():
                value = _header(info.headers, name)
                # "" needle = presence check; otherwise substring — same
                # semantics as the resource matcher
                if value is None or needle not in value:
                    return False
        if self.body is not None:
            parsed = _body_value(info)
            if parsed is None or not self._body_expr.search(parsed):
                return False
        if self.rate_count is not None:
            now = time.time()
            while self._fired_ts and now - self._fired_ts[0] >= self.rate_window:
                self._fired_ts.popleft()
            if len(self._fired_ts) >= self.rate_count:
                return False
        if self.sequence is not None:
            # every request that gets this far consumes a sequence slot,
            # whether it lands in the fail or pass phase
            fail, total = self.sequence[0], self.sequence[0] + self.sequence[1]
            pos = self._seq_pos
            self._seq_pos = (self._seq_pos + 1) % total
            if pos >= fail:
                return False
        return True


@dataclass
class Decision:
    rule: Rule
    error: FaultError | None
    latency_ms: float
    timeout_ms: float | None
    reset: bool
    response_fault: ResponseFault | None = None  # built at fire time


def _ts_or_none(value) -> float | None:
    """Epoch seconds, an ISO-8601 string, or a datetime (yaml.safe_load
    already converts timestamp-looking scalars). Naive values resolve in
    the host's local timezone; a trailing ``Z`` means UTC."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError(f"invalid timestamp {value!r}")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            pass
        iso = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            return datetime.fromisoformat(iso).timestamp()
        except ValueError as e:
            raise ValueError(f"invalid timestamp {value!r}") from e
    raise ValueError(f"invalid timestamp {value!r}")


def _latency_from_dict(data: dict) -> Latency:
    dist = data.get("dist", "uniform")
    num = {k: float(v) for k, v in data.items() if k != "dist"}
    if dist == "gaussian":
        return Latency(
            dist="gaussian",
            mean_ms=num.get("mean", 0.0),
            stddev_ms=num.get("stddev", 0.0),
            min_ms=num.get("min", 0.0),
            max_ms=num.get("max", 0.0),
        )
    if dist == "spike":
        return Latency(
            dist="spike",
            min_ms=num.get("min", 0.0),
            max_ms=num.get("max", 0.0),
            spike_ms=num.get("spike_ms", 0.0),
            spike_p=num.get("spike_p", 0.05),
        )
    return Latency(min_ms=num.get("min", 0.0), max_ms=num.get("max", 0.0))


def from_dict(data: dict) -> Rule:
    error = data.get("error")
    latency = data.get("latency")
    rate = data.get("rate")
    sequence = data.get("sequence")
    rule = Rule(
        service=data.get("service"),
        operation=data.get("operation"),
        region=data.get("region"),
        resource=data.get("resource"),
        headers=(
            {str(k): str(v) for k, v in data["headers"].items()}
            if isinstance(data.get("headers"), dict)
            else None
        ),
        body=data.get("body"),
        rate_count=int(rate["count"]) if isinstance(rate, dict) else None,
        rate_window=float(rate.get("window_s", 60)) if isinstance(rate, dict) else None,
        sequence=(
            (int(sequence["fail"]), int(sequence["pass"]))
            if isinstance(sequence, dict)
            else (int(sequence[0]), int(sequence[1]))
            if isinstance(sequence, (list, tuple)) and len(sequence) == 2
            else None
        ),
        probability=float(data.get("probability", 1.0)),
        deterministic=bool(data.get("deterministic", False)),
        times=data.get("times"),
        error=FaultError(**error) if isinstance(error, dict) else None,
        latency=(
            Latency(min_ms=float(latency), max_ms=float(latency))
            if isinstance(latency, (int, float))
            else _latency_from_dict(latency)
            if isinstance(latency, dict)
            else None
        ),
        timeout_ms=data.get("timeout_ms"),
        reset=bool(data.get("reset", False)),
        response=data.get("response") if isinstance(data.get("response"), dict) else None,
        ttl_s=float(data["ttl_s"]) if data.get("ttl_s") is not None else None,
        active_at=_ts_or_none(data.get("active_at")),
        until=_ts_or_none(data.get("until")),
    )
    if rule.body is not None:
        try:
            rule._body_expr = jmespath.compile(rule.body)
        except Exception as e:  # any compile failure → bad rule
            raise ValueError(f"invalid body jmespath {rule.body!r}: {e}") from e
    if (
        rule.error is not None
        and rule.error.fields is not None
        and not isinstance(rule.error.fields, dict)
    ):
        raise ValueError("error.fields must be a mapping of member → value")
    return rule


def to_dict(rule: Rule) -> dict:
    out: dict = {"id": rule.id}
    for attr in ("service", "operation", "region", "resource"):
        value = getattr(rule, attr)
        if value is not None:
            out[attr] = value
    if rule.headers:
        out["headers"] = rule.headers
    if rule.body is not None:
        out["body"] = rule.body
    if rule.rate_count is not None:
        out["rate"] = {"count": rule.rate_count, "window_s": rule.rate_window}
    if rule.sequence is not None:
        out["sequence"] = {"fail": rule.sequence[0], "pass": rule.sequence[1]}
    if rule.probability != 1.0:
        out["probability"] = rule.probability
    if rule.deterministic:
        out["deterministic"] = True
    if rule.times is not None:
        out["times"] = rule.times
    if rule.error:
        out["error"] = {
            k: v
            for k, v in {
                "code": rule.error.code,
                "status": rule.error.status,
                "message": rule.error.message,
                "fields": rule.error.fields,
            }.items()
            if v is not None
        }
    if rule.latency:
        lat = rule.latency
        d: dict = {"dist": lat.dist}
        if lat.dist == "gaussian":
            d.update(mean=lat.mean_ms, stddev=lat.stddev_ms)
        elif lat.dist == "spike":
            d.update(spike_ms=lat.spike_ms, spike_p=lat.spike_p)
        d.update(min=lat.min_ms, max=lat.max_ms)
        out["latency"] = d
    if rule.timeout_ms is not None:
        out["timeout_ms"] = rule.timeout_ms
    if rule.reset:
        out["reset"] = True
    if rule.response:
        out["response"] = rule.response
    if rule.ttl_s is not None:
        out["ttl_s"] = rule.ttl_s
        remaining = rule.ttl_s - (time.time() - rule.created_ts)
        out["ttl_remaining_s"] = max(0.0, round(remaining, 1))
    if rule.active_at is not None:
        out["active_at"] = rule.active_at
        out["starts_in_s"] = max(0.0, round(rule.active_at - time.time(), 1))
    if rule.until is not None:
        out["until"] = rule.until
        out["ends_in_s"] = max(0.0, round(rule.until - time.time(), 1))
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

    def decide(self, info: RequestContext) -> Decision | None:
        for rule in self._rules:
            if not rule.matches(info):
                continue
            # deterministic: the draw is a hash of the request identity, so
            # the same resource always lands on the same side of p — "this
            # bucket always fails", reproducible without RNG seeds. Bonus
            # property: failure tiers nest (resources failing at p=0.1 are a
            # subset of those failing at p=0.5).
            draw = _draw(info) if rule.deterministic else random.random()
            if draw >= rule.probability:
                continue
            rule.fired_count += 1
            if rule.rate_count is not None:
                rule._fired_ts.append(time.time())
            error = rule.error
            if error and error.code is None:
                plausible = operation_error_names(info.service or "", info.operation or "")
                if plausible:
                    error = FaultError(
                        code=random.choice(plausible),
                        status=error.status,
                        message=error.message,
                        fields=error.fields,
                    )
            return Decision(
                rule=rule,
                error=error,
                latency_ms=rule.latency.sample() if rule.latency else 0.0,
                timeout_ms=rule.timeout_ms,
                reset=rule.reset,
                response_fault=_response_fault(rule.response),
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
    # SigV4 front-layer faults — statuses pinned because rest protocols
    # default to 503 while auth errors are 400/403 on the real wire.
    "expired-token": {
        "service": "*",
        "error": {
            "code": "ExpiredTokenException",
            "status": 400,
            "message": "The security token included in the request is expired",
        },
    },
    "clock-skew": {
        "service": "*",
        "error": {
            "code": "RequestTimeTooSkewed",
            "status": 403,
            "message": "The difference between the request time and the "
            "current time is too large.",
        },
    },
    "bad-signature": {
        "service": "*",
        "error": {
            "code": "SignatureDoesNotMatch",
            "status": 403,
            "message": "The request signature we calculated does not match "
            "the signature you provided. Check your key and signing method.",
        },
    },
}



def _body_value(info: RequestContext):
    """Parse the request body for jmespath — JSON first, then form-encoded
    (query-protocol services post Action&Param= form bodies), then XML
    (rest-xml: S3 tagging/ACL/lifecycle payloads). Cached on the context
    so a rule chain parses at most once."""
    cached = info._parsed_body
    if cached is not BODY_UNSET:
        return cached
    parsed = None
    if info.body:
        try:
            parsed = json.loads(info.body)
        except (ValueError, UnicodeDecodeError):
            stripped = info.body.lstrip()
            if stripped.startswith(b"<"):
                parsed = _xml_to_dict(info.body)
            else:
                try:
                    parsed = dict(
                        parse_qsl(info.body.decode("utf-8", "replace"))
                    )
                except Exception:  # noqa: BLE001 — opaque bodies don't match
                    parsed = None
    info._parsed_body = parsed
    return parsed


def _xml_to_dict(body: bytes):
    """rest-xml bodies → nested dict for jmespath. Repeated siblings become
    lists; namespaces are stripped. Returns None on unparseable XML."""
    def strip(tag: str) -> str:
        return tag.rsplit("}", 1)[-1]

    def node(el):
        children = list(el)
        if not children:
            return el.text or ""
        out: dict = {}
        for child in children:
            key = strip(child.tag)
            val = node(child)
            if key in out:
                if not isinstance(out[key], list):
                    out[key] = [out[key]]
                out[key].append(val)
            else:
                out[key] = val
        return out

    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return None
    return {strip(root.tag): node(root)}


def _header(headers, name: str) -> str | None:
    """Case-insensitive header lookup — works on CIMultiDict and plain dicts."""
    lname = name.lower()
    for k, v in headers.items():
        if k.lower() == lname:
            return v
    return None


def _draw(info: RequestContext) -> float:
    """Stable pseudo-random draw in [0, 1) keyed on request identity."""
    key = "|".join(
        str(v) for v in (info.service, info.operation, info.resource, info.access_key)
    )
    digest = hashlib.sha256(key.encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


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


def _response_fault(spec: dict | None) -> ResponseFault | None:
    """Build a fresh ResponseFault per fire — resolve() mutates it in place."""
    if spec is None:
        return None
    from microburst.forward import ResponseFault

    return ResponseFault(
        truncate_bytes=_int_or_none(spec.get("truncate_bytes")),
        truncate_frac=_float_or_none(spec.get("truncate_frac")),
        abort_bytes=_int_or_none(spec.get("abort_bytes")),
        abort_frac=_float_or_none(spec.get("abort_frac")),
        corrupt_bytes=int(spec.get("corrupt_bytes", 0)),
        bandwidth_kbps=_float_or_none(spec.get("bandwidth_kbps")),
        event_error_code=_event_error(spec).get("code"),
        event_error_message=_event_error(spec).get("message"),
        event_error_after=int(_event_error(spec).get("after_frames", 3)),
        event_mutations=_event_mutations(spec),
        set_headers=(
            {str(k): str(v) for k, v in spec["set_headers"].items()}
            if isinstance(spec.get("set_headers"), dict)
            else {}
        ),
        strip_headers=(
            tuple(str(h) for h in spec["strip_headers"])
            if isinstance(spec.get("strip_headers"), list)
            else ()
        ),
    )


def _event_error(spec: dict) -> dict:
    ee = spec.get("event_error")
    return ee if isinstance(ee, dict) else {}


def _payload_bytes(v) -> bytes | None:
    if v is None:
        return None
    if isinstance(v, bytes):
        return v
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":")).encode()
    return str(v).encode()


def _event_mutations(spec: dict) -> tuple:
    """``response.event_frames`` — frame-level event-stream surgery.

    Each entry is ``{at: <frame index>, <action>}`` where the action is
    one of: ``inject`` (``{event_type, message_type, payload}`` — emits a
    new frame before that index), ``error`` (``{code, message}`` —
    terminal error frame), ``drop``, ``payload``/``payload_b64``
    (replace the frame's payload, headers kept, CRCs recomputed),
    ``corrupt_payload``, ``bad_crc``, or ``cut`` (emit a fraction of the
    frame then end the stream).
    """
    entries = spec.get("event_frames")
    if not isinstance(entries, list):
        return ()
    from microburst.eventstream import (
        Mutation,
        build_error_frame,
        build_message,
    )

    out = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        at = int(entry.get("at", 0))
        inject = entry.get("inject")
        inject_frame = None
        if isinstance(inject, dict):
            inject_frame = build_message(
                {
                    ":message-type": str(inject.get("message_type", "event")),
                    ":event-type": str(inject.get("event_type", "fault")),
                },
                _payload_bytes(inject.get("payload")) or b"",
            )
        error = entry.get("error")
        error_frame = None
        if isinstance(error, dict) and error.get("code"):
            error_frame = build_error_frame(
                str(error["code"]), str(error.get("message") or error["code"])
            )
        payload = _payload_bytes(entry.get("payload"))
        if payload is None and entry.get("payload_b64"):
            payload = base64.b64decode(entry["payload_b64"])
        out.append(
            Mutation(
                at=at,
                inject=inject_frame,
                error=error_frame,
                cut=_float_or_none(entry.get("cut")),
                drop=bool(entry.get("drop")),
                payload=payload,
                corrupt_payload=bool(entry.get("corrupt_payload")),
                bad_crc=bool(entry.get("bad_crc")),
            )
        )
    return tuple(out)


def _int_or_none(v):
    return int(v) if v is not None else None


def _float_or_none(v):
    return float(v) if v is not None else None


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
    rf = decision.response_fault
    if rf is not None:
        if rf.truncate_bytes is not None or rf.truncate_frac is not None:
            parts.append("response:truncate")
        if rf.abort_bytes is not None or rf.abort_frac is not None:
            parts.append("response:abort")
        if rf.corrupt_bytes:
            parts.append(f"response:corrupt:{rf.corrupt_bytes}B")
        if rf.bandwidth_kbps:
            parts.append(f"response:{rf.bandwidth_kbps:.0f}kbps")
        if rf.event_error_code:
            parts.append(
                f"response:event_error:{rf.event_error_code}"
                f"@{rf.event_error_after}"
            )
        if rf.event_mutations:
            parts.append(f"response:event_frames:{len(rf.event_mutations)}")
    return "+".join(parts) or "match"


def now() -> float:
    return time.time()
