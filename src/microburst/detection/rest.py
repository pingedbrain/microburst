"""REST-protocol operation matching.

Several operations share the same method+path (S3 ``PutObject`` vs
``CopyObject``); disambiguation happens at match time via literal query
markers, required headers, and required body keys declared in the botocore
service model.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping

from microburst.models import get_service_model

_REST_OP_CACHE: dict[str, list[dict]] = {}


def uri_to_regex(uri: str) -> re.Pattern:
    """Convert a Smithy requestUri (``/x/{Label}/{Greedy+}``) to a regex."""
    out = ""
    i = 0
    for match in re.finditer(r"\{[A-Za-z0-9_]+\+?\}", uri):
        out += re.escape(uri[i:match.start()])
        out += ".*" if match.group().endswith("+}") else "[^/]+"
        i = match.end()
    out += re.escape(uri[i:])
    return re.compile(f"^{out}$")


def rest_operations(service: str) -> list[dict]:
    """Compile candidate operations for a REST-protocol service."""
    if service in _REST_OP_CACHE:
        return _REST_OP_CACHE[service]
    entries: list[dict] = []
    model = get_service_model(service)
    if model is not None:
        for op_name in model.operation_names:
            op = model.operation_model(op_name)
            http = op.http
            uri = http.get("requestUri", "/")
            path_uri, _, literal_query = uri.partition("?")
            query_markers = {
                part.split("=", 1)[0] for part in literal_query.split("&") if part
            }
            required_headers = set()
            required_body_keys: set[str] = set()
            body_keys: set[str] = set()
            has_payload = False
            payload_name = None
            payload_keys: set[str] = set()
            payload_required: set[str] = set()
            input_shape = getattr(op, "input_shape", None)
            if input_shape is not None:
                payload_member = input_shape.serialization.get("payload")
                for name, member in input_shape.members.items():
                    serialization = member.serialization
                    if (
                        serialization.get("location") == "header"
                        and name in input_shape.required_members
                    ):
                        required_headers.add(
                            serialization.get("locationName", name).lower()
                        )
                    elif not serialization.get("location"):
                        if name == payload_member:
                            # The body itself is this member. A structure
                            # payload carries the member's wire name as the
                            # XML root element — and on rest-json its inner
                            # members ARE the JSON body keys.
                            has_payload = True
                            if member.type_name == "structure":
                                payload_name = serialization.get(
                                    "locationName", name
                                )
                                for inner_name, inner in member.members.items():
                                    inner_wire = inner.serialization.get(
                                        "locationName", inner_name
                                    )
                                    payload_keys.add(inner_wire)
                                    if inner_name in member.required_members:
                                        payload_required.add(inner_wire)
                            continue
                        wire_name = serialization.get("locationName", name)
                        body_keys.add(wire_name)
                        if name in input_shape.required_members:
                            required_body_keys.add(wire_name)
            entries.append(
                {
                    "method": http.get("method", "GET"),
                    "regex": uri_to_regex(path_uri),
                    "op": op_name,
                    "query_markers": query_markers,
                    "required_headers": required_headers,
                    "required_body_keys": required_body_keys,
                    "body_keys": body_keys,
                    "has_payload": has_payload,
                    "payload_name": payload_name,
                    "payload_keys": payload_keys,
                    "payload_required": payload_required,
                }
            )
    _REST_OP_CACHE[service] = entries
    return entries


_XML_ROOT = re.compile(rb"<\?[^>]*\?>|<!--.*?-->|<([A-Za-z0-9_:-]+)")


def structured_body_keys(
    body: bytes | None,
) -> tuple[frozenset[str], str] | None:
    """(key names, kind) carried by a body — kind is ``json``, ``xml`` or
    ``raw``.

    ``None`` when there is no body at all (no signal). For XML the single
    key is the root element's local name — which is exactly how rest-xml
    models name the structure payload member. ``raw`` covers blob bodies
    and unparseable content (empty key set).
    """
    if not body:
        return None
    stripped = body.lstrip()
    if stripped[:1] == b"{":
        try:
            obj = json.loads(body)
        except ValueError:
            return frozenset(), "raw"
        keys = frozenset(obj) if isinstance(obj, dict) else frozenset()
        return keys, "json"
    if stripped[:1] == b"<":
        for match in _XML_ROOT.finditer(stripped):
            if match.group(1):
                root = match.group(1).decode().rsplit(":", 1)[-1]
                return frozenset({root}), "xml"
    return frozenset(), "raw"


def match_rest_operation(
    service: str,
    method: str,
    path: str,
    query: Mapping[str, str],
    headers: Mapping[str, str],
    body: bytes | None = None,
) -> str | None:
    candidates = []
    lower_headers = {k.lower() for k in headers}
    entries = [
        entry
        for entry in rest_operations(service)
        if entry["method"] == method and entry["regex"].match(path)
    ]
    if not entries:
        return None
    # Body structure only earns its parse cost when it can break a tie —
    # i.e. some candidate declares body keys or expects a raw payload.
    needs_body = any(
        entry["required_body_keys"]
        or entry["body_keys"]
        or entry["has_payload"]
        for entry in entries
    )
    parsed = structured_body_keys(body) if needs_body else None
    for entry in entries:
        score = 0
        if entry["query_markers"]:
            if entry["query_markers"] <= set(query):
                score += 2 * len(entry["query_markers"])
            else:
                score -= 10
        for h in entry["required_headers"]:
            score += 2 if h in lower_headers else -10
        if parsed is not None:
            body_keys, kind = parsed
            if kind == "xml" and entry["payload_name"]:
                # rest-xml structure payloads serialize as a root element
                # named after the payload member.
                score += 3 if entry["payload_name"] in body_keys else -10
                required = entry["required_body_keys"]
                all_keys = entry["body_keys"]
            else:
                required = entry["required_body_keys"] | entry["payload_required"]
                all_keys = entry["body_keys"] | entry["payload_keys"]
            score += 2 * len(required & body_keys)
            score -= 10 * len(required - body_keys)
            score += len((all_keys & body_keys) - required)
            # A non-empty body with no keys at all is a raw payload —
            # favor payload ops (structure-demanding ops already lost
            # points on missing required keys).
            if not body_keys and body and entry["has_payload"]:
                score += 2
        candidates.append((score, entry["op"]))
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]
