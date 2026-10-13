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
    "cloudwatch": "com.amazonaws.cloudwatch.v2010_08_01#",
}

# Codes raised by the auth/validate front layer — before the request reaches
# the service — come back namespaced under the coral runtime instead of the
# service's own namespace (or bare code). Real AWS observation: the sfn
# capture carries ``com.amazon.coral.service#AccessDeniedException``; the
# same namespacing is documented for credential-validation failures
# (ExpiredToken, UnrecognizedClient, …) across json services.
_CORAL_LAYER_PREFIX = "com.amazon.coral.service#"
_CORAL_LAYER_CODES = frozenset({
    "AccessDeniedException",
    "ExpiredTokenException",
    "InvalidClientTokenId",
    "InvalidSignatureException",
    "MissingAuthenticationTokenException",
    "UnrecognizedClientException",
})


@register_serializer("json")
def render(
    code: str,
    message: str,
    request_id: str,
    service: str | None = None,
    fields: dict | None = None,
    resource: str | None = None,
) -> tuple[dict[str, str], bytes]:
    version = service_metadata(service).get("jsonVersion", "1.0")
    headers = {
        "Content-Type": f"application/x-amz-json-{version}",
        "x-amzn-RequestId": request_id,
    }
    prefix = (
        _CORAL_LAYER_PREFIX
        if code in _CORAL_LAYER_CODES
        else _TYPE_PREFIX.get(service or "", "")
    )
    # Coral front-layer errors aren't in the service model, so
    # error_message_member can't see them — the capture shows they carry
    # capital ``Message`` (sfn: {"__type":"com.amazon.coral.service#…",
    # "Message":"…"}), unlike service-layer ``message``.
    member = (
        "Message"
        if code in _CORAL_LAYER_CODES
        else (error_message_member(service, code) if service else "message")
    )
    payload = {"__type": f"{prefix}{code}", member: message}
    # Real AWS observation (us-east-1, 9 ops probed): athena
    # InvalidRequestException always carries a *semantic* duplicate of
    # the failure in ``AthenaErrorCode`` + ``ErrorCode`` — INVALID_INPUT
    # for missing workgroups/catalogs, NAMED_QUERY_NOT_FOUND,
    # QUERY_EXECUTION_NOT_FOUND, etc. MetadataException carries neither.
    # INVALID_INPUT is the generic default; fields can override with a
    # more specific reason.
    if service == "athena" and code == "InvalidRequestException":
        payload.setdefault("AthenaErrorCode", "INVALID_INPUT")
        payload.setdefault("ErrorCode", "INVALID_INPUT")
    if fields:
        payload.update(fields)
    body = json.dumps(payload).encode()
    return headers, body
