"""REST collision sweep — synthesize a request for every operation and
check the matcher resolves it to itself.

S3 alone has 36 ops on ``GET /{Bucket}``; chime services stack up to 5 on a
route. For every operation this module builds the minimal request that
op's model declares — literal query markers, required querystring/header
members, required body keys (or an XML root / raw payload) — and records
what :func:`match_rest_operation` resolves. Ops that don't self-resolve
are genuinely ambiguous given only model signals; the committed snapshot
(`fidelity/rest_sweep.json`) is the regression guard.
"""

from __future__ import annotations

import json
import re

from microburst.detection.rest import match_rest_operation, rest_operations
from microburst.models import get_service_model

_REST_PROTOCOLS = ("rest-json", "rest-xml")

_LABEL = re.compile(r"\{[A-Za-z0-9_]+\+?\}")


def _sample_path(uri: str) -> str:
    """Fill ``{Label}`` with a value and ``{Greedy+}`` with two segments."""
    def repl(match: re.Match) -> str:
        return "a/b" if match.group().endswith("+}") else "x"

    return _LABEL.sub(repl, uri)


def synthesize_request(service: str, op_name: str) -> dict | None:
    """Minimal request the operation's model declares, as matcher inputs:
    ``{method, path, query, headers, body}``. ``None`` if unmodeled.
    """
    model = get_service_model(service)
    if model is None:
        return None
    try:
        op = model.operation_model(op_name)
    except Exception:  # noqa: BLE001
        return None
    http = op.http
    uri = http.get("requestUri", "/")
    path_uri, _, literal_query = uri.partition("?")

    query = []
    for part in literal_query.split("&"):
        if part:
            k, _, v = part.partition("=")
            query.append((k, v or "x"))
    headers: list[str] = []
    required_body: set[str] = set()
    payload_name: str | None = None
    payload_kind: str | None = None

    input_shape = getattr(op, "input_shape", None)
    if input_shape is not None:
        payload_member = input_shape.serialization.get("payload")
        required = set(input_shape.required_members)
        for name, member in input_shape.members.items():
            ser = member.serialization
            wire = ser.get("locationName", name)
            if name not in required:
                continue
            location = ser.get("location")
            if location == "querystring":
                query.append((wire, "x"))
            elif location == "header":
                headers.append(wire.lower())
            elif not location:
                if name == payload_member:
                    payload_name = wire
                    payload_kind = member.type_name
                else:
                    required_body.add(wire)

    body: bytes | None = None
    if payload_name:
        if payload_kind == "structure" and model.protocol == "rest-xml":
            body = f"<{payload_name}/>".encode()
        elif payload_kind == "structure":
            body = b"{}"
        else:
            body = b"x"  # blob/stream payload — raw bytes
    elif required_body:
        body = json.dumps({k: "x" for k in required_body}).encode()

    return {
        "method": http.get("method", "GET"),
        "path": _sample_path(path_uri),
        "query": dict(query),
        "headers": {h: "x" for h in headers},
        "body": body,
    }


def sweep_service(service: str) -> dict[str, str | None]:
    """``{operation: resolved_operation}`` for every op of a REST service."""
    out: dict[str, str | None] = {}
    for entry in rest_operations(service):
        req = synthesize_request(service, entry["op"])
        if req is None:
            out[entry["op"]] = None
            continue
        out[entry["op"]] = match_rest_operation(
            service,
            req["method"],
            req["path"],
            req["query"],
            req["headers"],
            req["body"],
        )
    return out


def sweep_all() -> dict[str, dict[str, str | None]]:
    """Sweep every service whose model declares a REST protocol."""
    from botocore.session import Session

    session = Session()
    out: dict[str, dict[str, str | None]] = {}
    for service in sorted(session.get_available_services()):
        model = get_service_model(service)
        if model is not None and model.protocol in _REST_PROTOCOLS:
            out[service] = sweep_service(service)
    return out
