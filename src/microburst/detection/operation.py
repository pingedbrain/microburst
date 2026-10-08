"""Operation name resolution.

Order mirrors how AWS routing works: ``X-Amz-Target`` for JSON-RPC services,
query-protocol ``Action`` (kept as a general fallback — some services
migrated their model to ``json`` while clients still speak query), then
REST path matching against the service model.
"""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import parse_qsl

from microburst.detection.rest import match_rest_operation
from microburst.models import get_protocol


def resolve_operation(
    service: str | None,
    headers: Mapping[str, str],
    method: str,
    path: str,
    query: Mapping[str, str],
    body: bytes | None,
) -> str | None:
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
