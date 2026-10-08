"""RequestContext — the object that travels through the pipeline.

Holds the raw request facts plus everything detection resolved. Everything
downstream (rule matching, effects, the fired log) reads from this instead
of re-inspecting the aiohttp request — new matchers and detectors extend
what lands here without changing pipeline signatures.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from microburst.rules import Decision


@dataclass
class RequestContext:
    # Raw request facts
    method: str = "GET"
    path: str = "/"
    query: Mapping[str, Any] = field(default_factory=dict)
    headers: Mapping[str, Any] = field(default_factory=dict)
    body: bytes | None = None

    # Detection results
    service: str | None = None
    operation: str | None = None
    region: str | None = None
    resource: str | None = None
    access_key: str | None = None
    # Wire protocol actually observed on the request. Differs from the
    # service model's declared protocol for migrated services — e.g.
    # CloudWatch's model says smithy-rpc-v2-cbor but boto3 sends
    # query-compatible JSON (x-amzn-query-mode). Fault responses must
    # match the request's protocol, not the model's.
    protocol: str | None = None
    query_compat: bool = False

    # Rule decision (filled by the engine before effects run)
    decision: Decision | None = None


# Backwards-compatible name used throughout the pre-0.1 codebase.
RequestInfo = RequestContext
