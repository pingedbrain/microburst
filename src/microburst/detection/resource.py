"""Best-effort resource hints (table, bucket, queue) for rule matching.

Detection is intentionally partial: resource fields vary per operation and
not every request carries one. A hint is better than nothing — matchers
treat a missing resource as "does not match", not as an error.
"""

from __future__ import annotations

import json


def resource_hint(
    service: str | None,
    operation: str | None,
    path: str,
    body: bytes | None,
) -> str | None:
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
