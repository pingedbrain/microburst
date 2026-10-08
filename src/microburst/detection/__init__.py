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
from microburst.detection.host import parse_host, virtual_label
from microburst.detection.operation import parse_rpcv2_path, resolve_operation
from microburst.detection.resource import resource_hint
from microburst.detection.rest import match_rest_operation, rest_operations
from microburst.detection.scope import parse_credential_scope
from microburst.models import get_protocol, service_for_target_prefix

__all__ = [
    "RequestContext",
    "RequestInfo",
    "detect",
    "match_rest_operation",
    "parse_credential_scope",
    "parse_rpcv2_path",
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

# Services whose real endpoint embeds a deployment stage as the first
# path segment — `{api-id}.execute-api.{region}.amazonaws.com/{stage}` —
# so the modeled route (e.g. `/@connections/{connectionId}`) sits one
# segment deeper. If detection with the full path fails, retry stripped.
_STAGE_PREFIX_SERVICES = {"apigatewaymanagementapi"}


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


def _wire_protocol(headers: Mapping[str, str], service: str | None) -> str | None:
    """The protocol the request actually speaks — which may differ from the
    service model's declared protocol for migrated services."""
    content_type = headers.get("Content-Type", "")
    if (
        headers.get("smithy-protocol") == "rpc-v2-cbor"
        or content_type.startswith("application/cbor")
        or headers.get("Accept", "").startswith("application/cbor")
    ):
        return "smithy-rpc-v2-cbor"
    if "x-amzn-query-mode" in headers or "X-Amz-Target" in headers:
        return "json"
    return get_protocol(service)


def _target_prefix(headers: Mapping[str, str], path: str) -> str | None:
    """The service-identifying prefix carried by rpc-style requests."""
    rpcv2 = parse_rpcv2_path(path)
    if rpcv2:
        return rpcv2[0]
    target = headers.get("X-Amz-Target", "")
    if "." in target:
        return target.rsplit(".", 1)[0]
    return None


def detect(
    headers: Mapping[str, str],
    method: str,
    path: str,
    query: Mapping[str, str],
    body: bytes | None,
) -> RequestContext:
    service, region, access_key = parse_credential_scope(headers, query)
    # The target prefix resolves service *exactly* — it disambiguates the
    # shared SigV4 scopes (dynamodb vs dynamodbstreams, events vs
    # eventbridgev2) and identifies requests whose scope didn't resolve.
    service = service_for_target_prefix(_target_prefix(headers, path) or "") or service

    # Host fills what credentials don't provide: service for unsigned
    # requests, region, and the virtual-hosted resource label
    # (bucket.s3.…, {accountId}.s3-control.…).
    host = headers.get("Host", "")
    host_service, host_region, host_label = parse_host(host)
    # The host is the addressed service — it wins over the SigV4 scope,
    # which deliberately aliases (s3control signs as "s3").
    service = host_service or service
    region = region or host_region
    label = host_label or virtual_label(host, service)

    # For S3 virtual-hosted style the bucket lives in the host — prepend it
    # so REST route matching sees the modeled /{Bucket}/{Key} shape.
    match_path = path
    if label and service == "s3":
        match_path = f"/{label}" + ("" if path == "/" else path)

    ctx = RequestContext(
        method=method,
        path=path,
        query=query,
        headers=headers,
        body=body,
        service=service,
        region=region,
        access_key=access_key,
        protocol=_wire_protocol(headers, service),
        query_compat="x-amzn-query-mode" in headers,
    )
    ctx.operation = resolve_operation(
        service, headers, method, match_path, query, body
    )
    if (
        ctx.operation is None
        and service in _STAGE_PREFIX_SERVICES
        and match_path.count("/") >= 2
    ):
        ctx.operation = resolve_operation(
            service,
            headers,
            method,
            "/" + match_path.lstrip("/").split("/", 1)[1],
            query,
            body,
        )
    ctx.resource = (
        resource_hint(service, ctx.operation, match_path, body) or label
    )
    return ctx
