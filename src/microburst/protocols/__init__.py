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

import uuid
from collections.abc import Callable

from microburst.models import error_http_status, get_protocol

__all__ = ["get_serializer", "register_serializer", "render_error"]

# render(code, message, request_id) -> (headers, body)
_SERIALIZERS: dict[str, Callable] = {}


def register_serializer(*protocol_names: str) -> Callable:
    def deco(fn: Callable) -> Callable:
        for name in protocol_names:
            _SERIALIZERS[name] = fn
        return fn

    return deco


def get_serializer(protocol: str | None) -> Callable | None:
    return _SERIALIZERS.get(protocol or "")


# Importing the package populates the registry.
from microburst.protocols import (
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
) -> tuple[int, dict[str, str], bytes]:
    """Serialize an AWS-looking error. Returns (status, headers, body)."""
    protocol = get_protocol(service) if service else None
    if status is None:
        if service is None:
            status = 503
        else:
            # json/query services conventionally serve client faults at 400;
            # rest protocols lean on 5xx for transient errors.
            default = 400 if protocol in ("json", "query", "ec2") else 503
            status = error_http_status(service, code, default=default)
    if not message:
        message = code

    renderer = get_serializer(protocol) or _DEFAULT_RENDERER
    headers, body = renderer(code, message, _request_id())
    return status, headers, body
