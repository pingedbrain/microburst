"""Request detection: resolve service/operation/region/resource.

Mirrors how AWS itself — and MiniStack's router — identify a request:
SigV4 credential scope names the service and region; ``X-Amz-Target`` or
the query-protocol ``Action`` parameter names the operation; REST services
are matched on method + requestUri patterns from the botocore model.

To add a detection strategy (e.g. presigned URLs, host-prefix routing),
extend the chain here — detectors compose, they don't replace each other.
"""

from __future__ import annotations

from collections.abc import Mapping

from microburst.core.context import RequestContext, RequestInfo
from microburst.detection.operation import resolve_operation
from microburst.detection.resource import resource_hint
from microburst.detection.rest import match_rest_operation, rest_operations
from microburst.detection.scope import parse_credential_scope

__all__ = [
    "RequestContext",
    "RequestInfo",
    "detect",
    "match_rest_operation",
    "parse_credential_scope",
    "rest_operations",
    "should_buffer",
]

# Operations that carry large payloads in either direction. For these the
# body is streamed through untouched — detection never needs them.
_STREAMING_OPS = {
    ("s3", "PutObject"),
    ("s3", "GetObject"),
    ("s3", "UploadPart"),
    ("lambda", "Invoke"),  # response payloads can be large; body still small
}

_BUFFER_LIMIT = 4 * 1024 * 1024


def should_buffer(
    service: str | None,
    operation: str | None,
    content_length: int | None,
) -> bool:
    """Whether the body is needed for detection/rule matching."""
    if (service, operation) in _STREAMING_OPS:
        return False
    if content_length is None:
        return True
    return content_length <= _BUFFER_LIMIT


def detect(
    headers: Mapping[str, str],
    method: str,
    path: str,
    query: Mapping[str, str],
    body: bytes | None,
) -> RequestContext:
    service, region, access_key = parse_credential_scope(headers)
    ctx = RequestContext(
        method=method,
        path=path,
        query=query,
        headers=headers,
        body=body,
        service=service,
        region=region,
        access_key=access_key,
    )
    ctx.operation = resolve_operation(service, headers, method, path, query, body)
    ctx.resource = resource_hint(service, ctx.operation, path, body)
    return ctx
