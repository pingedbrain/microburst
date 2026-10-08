"""AWS `rest-json` protocol (API Gateway-family services)."""

from __future__ import annotations

import json
from typing import Any

from microburst.models import error_shape
from microburst.protocols import register_serializer

# Rest-json services whose errors carry the code in the *body* rather than
# x-amzn-ErrorType — glacier sends {"code", "message", "type"} (verified
# live). Most rest-json services use the header.
_BODY_CODE = {"glacier"}

# Wire Content-Type overrides — rest-json is conventionally
# ``application/json``; captures show sesv2 speaks ``x-amz-json-1.1``.
_CONTENT_TYPE = {"sesv2": "application/x-amz-json-1.1"}


@register_serializer("rest-json")
def render(
    code: str, message: str, request_id: str, service: str | None = None
) -> tuple[dict[str, str], bytes]:
    # AWS carries the code in the x-amzn-ErrorType header, not the body —
    # the body holds the error shape's members (Lambda: {"Type","Message"},
    # API Gateway: {"message"}). Body-code services like Glacier are the
    # exception (verified live).
    if service in _BODY_CODE:
        return {
            "Content-Type": "application/json",
            "x-amzn-RequestId": request_id,
        }, json.dumps({"code": code, "message": message, "type": "Client"}).encode()

    headers = {
        "Content-Type": _CONTENT_TYPE.get(service or "", "application/json"),
        "x-amzn-RequestId": request_id,
        "x-amzn-ErrorType": code,
    }
    fields: dict[str, Any] = {}
    shape = error_shape(service, code) if service else None
    members = getattr(shape, "members", None) if shape is not None else None
    if members:
        for name in members:
            if name.lower() == "message":
                fields[name] = message
            elif name.lower() == "type":
                fields[name] = "User"
        if not fields:
            fields["message"] = message
    else:
        fields["message"] = message
    return headers, json.dumps(fields).encode()
