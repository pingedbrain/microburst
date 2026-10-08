"""AWS `json` protocol (DynamoDB, KMS, and query-migrated services like SQS)."""

from __future__ import annotations

import json

from microburst.models import (
    error_message_member,
    service_metadata,
)
from microburst.protocols import register_serializer

# Verified ``__type`` prefixes from live-AWS captures — most json services
# send the bare code; coral-stack services namespace it. Grows with captures.
_TYPE_PREFIX = {
    "dynamodb": "com.amazonaws.dynamodb.v20120810#",
    "sqs": "com.amazonaws.sqs#",
}


@register_serializer("json")
def render(
    code: str, message: str, request_id: str, service: str | None = None
) -> tuple[dict[str, str], bytes]:
    version = service_metadata(service).get("jsonVersion", "1.0")
    headers = {
        "Content-Type": f"application/x-amz-json-{version}",
        "x-amzn-RequestId": request_id,
    }
    prefix = _TYPE_PREFIX.get(service or "", "")
    member = error_message_member(service, code) if service else "message"
    body = json.dumps({"__type": f"{prefix}{code}", member: message}).encode()
    return headers, body
