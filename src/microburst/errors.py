"""Error serialization per AWS wire protocol.

The SDKs classify failures by the *error code parsed from the body* (plus
status code), not the status alone. To make injected faults exercise the real
retry machinery — throttling backoff, adaptive rate limiting, modeled
retryable exceptions — the body must be shaped the way the service actually
shapes it.
"""

from __future__ import annotations

import json
import uuid
from xml.sax.saxutils import escape

from microburst.models import error_http_status, get_protocol


def _request_id() -> str:
    return str(uuid.uuid4())


def _json_body(code: str, message: str) -> bytes:
    return json.dumps({"__type": code, "message": message}).encode()


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

    rid = _request_id()

    if protocol == "json":
        headers = {
            "Content-Type": "application/x-amz-json-1.0",
            "x-amzn-RequestId": rid,
            "x-amzn-ErrorType": code,
        }
        return status, headers, _json_body(code, message)

    if protocol in ("query", "ec2"):
        headers = {
            "Content-Type": "text/xml",
            "x-amzn-RequestId": rid,
        }
        body = (
            "<ErrorResponse>"
            "<Error>"
            f"<Code>{escape(code)}</Code>"
            f"<Message>{escape(message)}</Message>"
            "<Type>Sender</Type>"
            "</Error>"
            f"<RequestId>{rid}</RequestId>"
            "</ErrorResponse>"
        )
        return status, headers, body.encode()

    if protocol == "rest-xml":
        headers = {
            "Content-Type": "application/xml",
            "x-amz-request-id": rid,
            "x-amz-id-2": uuid.uuid4().hex * 2,
        }
        body = (
            "<Error>"
            f"<Code>{escape(code)}</Code>"
            f"<Message>{escape(message)}</Message>"
            f"<RequestId>{rid}</RequestId>"
            "</Error>"
        )
        return status, headers, body.encode()

    if protocol == "rest-json":
        headers = {
            "Content-Type": "application/json",
            "x-amzn-RequestId": rid,
            "x-amzn-errortype": code,
        }
        return status, headers, json.dumps({"message": message}).encode()

    # Unknown service/protocol: closest AWS-looking generic envelope.
    headers = {
        "Content-Type": "application/x-amz-json-1.0",
        "x-amzn-RequestId": rid,
        "x-amzn-ErrorType": code,
    }
    return status, headers, _json_body(code, message)
