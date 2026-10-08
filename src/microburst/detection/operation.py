"""Operation name resolution.

Order mirrors how AWS routing works: ``X-Amz-Target`` for JSON-RPC services,
query-protocol ``Action`` (kept as a general fallback — some services
migrated their model to ``json`` while clients still speak query), then
REST path matching against the service model.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from urllib.parse import parse_qsl

from microburst.detection.rest import match_rest_operation
from microburst.models import get_protocol

# rpc-v2-cbor services address ops as /service/{targetPrefix}/operation/{Op}.
_RPCV2_PATH = re.compile(r"^/service/([^/]+)/operation/([^/]+)$")


def parse_rpcv2_path(path: str) -> tuple[str, str] | None:
    """(targetPrefix, operation) from an rpc-v2-cbor URL path, or None."""
    m = _RPCV2_PATH.match(path)
    if m:
        return m.group(1), m.group(2)
    return None


def resolve_operation(
    service: str | None,
    headers: Mapping[str, str],
    method: str,
    path: str,
    query: Mapping[str, str],
    body: bytes | None,
) -> str | None:
    rpcv2 = parse_rpcv2_path(path)
    if rpcv2:
        return rpcv2[1]

    if service is None:
        return None

    target = headers.get("X-Amz-Target", "")
    if "." in target:
        return target.rsplit(".", 1)[-1]

    action = query.get("Action")
    if action:
        return action
    if body and b"Action=" in body:
        params = dict(parse_qsl(body.decode("utf-8", "replace")))
        action = params.get("Action")
        if action:
            return action

    protocol = get_protocol(service)
    if protocol in ("rest-xml", "rest-json"):
        return match_rest_operation(service, method, path, query, headers)

    return None
