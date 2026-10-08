"""Request detection: resolve (service, operation, region, resource) from an
incoming AWS API request.

Mirrors how AWS itself — and MiniStack's router — identify a request:
SigV4 credential scope names the service and region; ``X-Amz-Target`` or the
query-protocol ``Action`` parameter names the operation; REST services are
matched on method + requestUri patterns from the botocore service model.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from urllib.parse import parse_qsl

from microburst.models import get_protocol, get_service_model, service_for_scope

_CRED_RE = re.compile(
    r"Credential=(?P<key>[^/,]+)/(?P<date>\d{8})/(?P<region>[^/]+)/"
    r"(?P<service>[^/]+)/aws4_request"
)

# Operations that carry large payloads in either direction. For these the body
# is streamed through untouched — detection never needs them.
_STREAMING_OPS = {
    ("s3", "PutObject"),
    ("s3", "GetObject"),
    ("s3", "UploadPart"),
    ("lambda", "Invoke"),  # response payloads can be large; body still small
}


@dataclass
class RequestInfo:
    service: str | None
    operation: str | None
    region: str | None
    resource: str | None
    access_key: str | None = None


def _parse_credential_scope(headers) -> tuple[str | None, str | None, str | None]:
    auth = headers.get("Authorization", "")
    match = _CRED_RE.search(auth)
    if not match:
        return None, None, None
    return (
        service_for_scope(match.group("service")),
        match.group("region"),
        match.group("key"),
    )


_REST_OP_CACHE: dict[str, list[dict]] = {}


def _uri_to_regex(uri: str) -> re.Pattern:
    """Convert a Smithy requestUri (``/x/{Label}/{Greedy+}``) to a regex."""
    out = ""
    i = 0
    for match in re.finditer(r"\{[A-Za-z0-9_]+\+?\}", uri):
        out += re.escape(uri[i:match.start()])
        out += ".*" if match.group().endswith("+}") else "[^/]+"
        i = match.end()
    out += re.escape(uri[i:])
    return re.compile(f"^{out}$")


def _rest_operations(service: str) -> list[dict]:
    """Compile candidate operations for a REST-protocol service.

    Several operations share the same method+path (S3 ``PutObject`` vs
    ``CopyObject``); disambiguation happens at match time via query-string
    markers and headers declared in the service model.
    """
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
                    "regex": _uri_to_regex(path_uri),
                    "op": op_name,
                    "query_markers": query_markers,
                    "required_headers": required_headers,
                }
            )
    _REST_OP_CACHE[service] = entries
    return entries


def _match_rest_operation(service: str, method: str, path: str, query,
                          headers) -> str | None:
    candidates = []
    for entry in _rest_operations(service):
        if entry["method"] != method or not entry["regex"].match(path):
            continue
        score = 0
        if entry["query_markers"]:
            if entry["query_markers"] <= set(query):
                score += 2 * len(entry["query_markers"])
            else:
                score -= 10
        lower_headers = {k.lower() for k in headers}
        for h in entry["required_headers"]:
            score += 2 if h in lower_headers else -10
        candidates.append((score, entry["op"]))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def _operation_for(service: str | None, headers, method: str, path: str,
                   query: dict, body: bytes | None) -> str | None:
    if service is None:
        return None

    target = headers.get("X-Amz-Target", "")
    if "." in target:
        return target.rsplit(".", 1)[-1]

    # Query-protocol style `Action` — some services migrated models to `json`
    # but clients may still speak query; check both query string and body.
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
        return _match_rest_operation(service, method, path, query, headers)

    return None


def _resource_hint(service: str | None, operation: str | None, path: str,
                   body: bytes | None) -> str | None:
    if service is None:
        return None
    if service == "s3":
        segments = [s for s in path.split("/") if s]
        return segments[0] if segments else None
    if body:
        if service == "sqs":
            segments = [s for s in path.split("/") if s]
            return segments[-1] if segments else None
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            return None
        if service == "dynamodb":
            return payload.get("TableName")
        for key in ("TopicArn", "TargetArn", "QueueUrl", "FunctionName",
                    "Name", "StackName", "Bucket"):
            if isinstance(payload.get(key), str):
                return payload[key].rsplit(":", 1)[-1].rsplit("/", 1)[-1]
    return None


def should_buffer(service: str | None, operation: str | None,
                  content_length: int | None) -> bool:
    """Whether the body is needed for detection/rule matching."""
    if (service, operation) in _STREAMING_OPS:
        return False
    if content_length is None:
        return True
    return content_length <= 4 * 1024 * 1024


def detect(headers, method: str, path: str, query: dict,
           body: bytes | None) -> RequestInfo:
    service, region, access_key = _parse_credential_scope(headers)
    operation = _operation_for(service, headers, method, path, query, body)
    resource = _resource_hint(service, operation, path, body)
    return RequestInfo(
        service=service,
        operation=operation,
        region=region,
        resource=resource,
        access_key=access_key,
    )
