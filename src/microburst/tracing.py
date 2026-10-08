"""Optional OpenTelemetry tracing — zero cost when the API isn't installed.

If ``opentelemetry-api`` is importable, every injected fault is wrapped in
a span (``microburst.fault``) carrying the rule id, service, operation and
action. With no SDK configured the API returns a no-op tracer, so the
overhead is one attribute check per fault.
"""

from __future__ import annotations

from contextlib import nullcontext

try:
    from opentelemetry import trace  # pyright: ignore[reportMissingImports]

    _tracer = trace.get_tracer("microburst")
except ImportError:  # pragma: no cover - depends on optional extra
    _tracer = None


def fault_span(rule_id: int, service, operation, action: str):
    """Context manager: an OTel span if available, else a no-op."""
    if _tracer is None:
        return nullcontext()
    return _tracer.start_as_current_span(
        "microburst.fault",
        attributes={
            "microburst.rule_id": rule_id,
            "microburst.service": service or "unknown",
            "microburst.operation": operation or "unknown",
            "microburst.action": action,
        },
    )
