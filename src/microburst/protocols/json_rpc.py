"""AWS `json` protocol (DynamoDB, KMS, and query-migrated services like SQS)."""

from __future__ import annotations

import json

from microburst.protocols import register_serializer


@register_serializer("json")
def render(code: str, message: str, request_id: str) -> tuple[dict[str, str], bytes]:
    headers = {
        "Content-Type": "application/x-amz-json-1.0",
        "x-amzn-RequestId": request_id,
        "x-amzn-ErrorType": code,
    }
    body = json.dumps({"__type": code, "message": message}).encode()
    return headers, body
