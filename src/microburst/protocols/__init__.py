"""Error serialization registry per AWS wire protocol.

The SDKs classify failures by the *error code parsed from the body* (plus
status code), not the status alone. To make injected faults exercise the
real retry machinery — throttling backoff, adaptive rate limiting, modeled
retryable exceptions — the body must be shaped the way the service actually
shapes it.

Renderers self-register via ``@register_serializer``. Importing this
package imports every serializer module, so the registry is always full.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable
from xml.sax.saxutils import escape

from microburst.models import (
    error_http_status,
    get_protocol,
    is_query_compat_service,
    query_error_namespace,
)

__all__ = ["get_serializer", "register_serializer", "render_error"]

# render(code, message, request_id, service, fields=None, resource=None)
#   -> (headers, body)
_SERIALIZERS: dict[str, Callable] = {}

_JSON_VERSION_RE = re.compile(r"x-amz-json-1\.(\d)")


def register_serializer(*protocol_names: str) -> Callable:
    def deco(fn: Callable) -> Callable:
        for name in protocol_names:
            _SERIALIZERS[name] = fn
        return fn

    return deco


def get_serializer(protocol: str | None) -> Callable | None:
    return _SERIALIZERS.get(protocol or "")


def xml_members(fields: dict | None, sep: str = "") -> str:
    """Error-shape members as XML elements: dicts nest, lists repeat the
    parent tag, scalars are escaped text. ``sep`` joins top-level
    members (e.g. ``"\\n    "`` to pretty-print one member per line)."""
    if not fields:
        return ""
    return sep.join(_xml_member(k, v) for k, v in fields.items())


def _xml_member(name: str, value) -> str:
    if isinstance(value, dict):
        inner = "".join(_xml_member(k, v) for k, v in value.items())
        return f"<{name}>{inner}</{name}>"
    if isinstance(value, (list, tuple)):
        return "".join(_xml_member(name, v) for v in value)
    if value is None:
        text = ""
    elif isinstance(value, bool):
        text = "true" if value else "false"
    else:
        text = str(value)
    return f"<{name}>{escape(text)}</{name}>"


# Importing the package populates the registry.
from microburst.protocols import (
    cbor,  # noqa: F401
    json_rpc,  # noqa: F401
    query,  # noqa: F401
    rest_json,  # noqa: F401
    rest_xml,  # noqa: F401
)

_DEFAULT_RENDERER = _SERIALIZERS["json"]


def _request_id() -> str:
    return str(uuid.uuid4())


def render_error(
    service: str | None,
    code: str,
    message: str = "",
    status: int | None = None,
    protocol: str | None = None,
    query_compat: bool = False,
    request_ct: str | None = None,
    fields: dict | None = None,
    resource: str | None = None,
) -> tuple[int, dict[str, str], bytes]:
    """Serialize an AWS-looking error. Returns (status, headers, body).

    ``protocol`` is the wire protocol observed on the request — it wins over
    the service model's declared protocol (migrated services like CloudWatch
    accept query-compatible JSON even though the model says rpc-v2-cbor).
    ``query_compat`` adds the ``x-amzn-query-error`` header AWS sends to
    query-compatible clients, which drives the SDK's parsed error code.
    ``request_ct`` is the request's own Content-Type — for json services the
    observed ``x-amz-json-1.x`` version wins over the model's ``jsonVersion``.
    ``fields`` are extra error-shape members rendered protocol-natively
    (json members, XML elements); ``resource`` feeds wire-standard fields
    like S3's ``Resource``.
    """
    protocol = protocol or (get_protocol(service) if service else None)

    # The query-compat header code may arrive namespaced
    # (``AWS.SimpleQueueService.NonExistentQueue``); the body and status
    # lookups always use the bare code.
    ns = query_error_namespace(service)
    bare_code = code
    if ns and code.startswith(f"{ns}."):
        bare_code = code[len(ns) + 1:]

    if status is None:
        if service is None:
            status = 503
        else:
            # RPC-style services conventionally serve client faults at 400;
            # rest protocols lean on 5xx for transient errors.
            default = (
                400
                if protocol in ("json", "query", "ec2", "smithy-rpc-v2-cbor")
                else 503
            )
            status = error_http_status(
                service, bare_code, default=default, protocol=protocol
            )
    if not message:
        message = code

    renderer = get_serializer(protocol) or _DEFAULT_RENDERER
    headers, body = renderer(
        bare_code, message, _request_id(), service,
        fields=fields, resource=resource,
    )
    if protocol == "json" and request_ct:
        m = _JSON_VERSION_RE.search(request_ct)
        if m:
            headers["Content-Type"] = f"application/x-amz-json-1.{m.group(1)}"
    if query_compat or (
        protocol == "json" and is_query_compat_service(service)
    ):
        qcode = f"{ns}.{bare_code}" if ns else bare_code
        headers["x-amzn-query-error"] = f"{qcode};Sender"
    return status, headers, body
