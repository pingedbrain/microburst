"""AWS `rest-json` protocol (API Gateway-family services)."""

from __future__ import annotations

import json

from microburst.protocols import register_serializer


@register_serializer("rest-json")
def render(code: str, message: str, request_id: str) -> tuple[dict[str, str], bytes]:
    headers = {
        "Content-Type": "application/json",
        "x-amzn-RequestId": request_id,
        "x-amzn-errortype": code,
    }
    return headers, json.dumps({"message": message}).encode()
