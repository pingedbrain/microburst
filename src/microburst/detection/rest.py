"""REST-protocol operation matching.

Several operations share the same method+path (S3 ``PutObject`` vs
``CopyObject``); disambiguation happens at match time via literal query
markers and required headers declared in the botocore service model.
"""

from __future__ import annotations

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
            input_shape = getattr(op, "input_shape", None)
            if input_shape is not None:
                for name, member in input_shape.members.items():
                    if (
                        member.serialization.get("location") == "header"
                        and name in input_shape.required_members
                    ):
                        required_headers.add(
                            member.serialization.get("locationName", name).lower()
                        )
            entries.append(
                {
                    "method": http.get("method", "GET"),
                    "regex": uri_to_regex(path_uri),
                    "op": op_name,
                    "query_markers": query_markers,
                    "required_headers": required_headers,
                }
            )
    _REST_OP_CACHE[service] = entries
    return entries


def match_rest_operation(
    service: str,
    method: str,
    path: str,
    query: Mapping[str, str],
    headers: Mapping[str, str],
) -> str | None:
    candidates = []
    lower_headers = {k.lower() for k in headers}
    for entry in rest_operations(service):
        if entry["method"] != method or not entry["regex"].match(path):
            continue
        score = 0
        if entry["query_markers"]:
            if entry["query_markers"] <= set(query):
                score += 2 * len(entry["query_markers"])
            else:
                score -= 10
        for h in entry["required_headers"]:
            score += 2 if h in lower_headers else -10
        candidates.append((score, entry["op"]))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]
