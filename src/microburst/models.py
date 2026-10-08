"""Service model access via botocore.

Wraps botocore's loader to resolve AWS service models (protocol, operations,
error shapes) by the names requests actually carry: SigV4 credential scope and
endpoint prefixes. Everything is lazy and cached.
"""

from __future__ import annotations

import botocore.session

# SigV4 credential scopes that differ from the botocore service name.
_SCOPE_ALIASES = {
    "monitoring": "cloudwatch",
    "email": "ses",
    "iotdata": "iot-data",
    "iot-jobs-data": "iot-jobs-data",
    "iotwireless": "iotwireless",
    "elasticfilesystem": "elasticfilesystem",
    "api.ecr": "ecr",
    "aoss": "opensearchserverless",
    "cloudfront-keyvaluestore": "cloudfront-keyvaluestore",
    "cognito-idp": "cognito-idp",
    "cognito-identity": "cognito-identity",
}

_session = botocore.session.get_session()
_model_cache: dict[str, object] = {}


def service_for_scope(scope: str) -> str:
    """Map a SigV4 credential scope to a botocore service name."""
    return _SCOPE_ALIASES.get(scope, scope)


def get_service_model(service: str):
    """Return the botocore ServiceModel for a service name, or None."""
    if service in _model_cache:
        return _model_cache[service]
    try:
        model = _session.get_service_model(service)
    except Exception:  # noqa: BLE001 — unknown service names / broken data loaders
        model = None
    _model_cache[service] = model
    return model


def get_protocol(service: str) -> str | None:
    model = get_service_model(service)
    if model is None:
        return None
    return model.protocol


def error_shape(service: str, code: str):
    """Find the shape for an error code/name in a service model, or None.

    Accepts the bare shape name or a Smithy-style ``prefix#Name``.
    """
    model = get_service_model(service)
    if model is None:
        return None
    name = code.rsplit("#", 1)[-1]
    shape = _shape_or_none(model, name)
    if shape is not None:
        return shape
    for candidate in model.shape_names:
        if candidate == name or candidate.rsplit("#", 1)[-1] == name:
            return _shape_or_none(model, candidate)
    return None


def _shape_or_none(model, name: str):
    try:
        return model.shape_for(name)
    except Exception:  # noqa: BLE001 — unmodeled codes raise assorted errors
        return None


# AWS's real status for common codes when the service model doesn't carry an
# ``httpStatusCode`` trait (most don't). Verified behavior that matters: the
# SDK's retry decision depends on the status — S3 SlowDown at 400 is treated
# as a terminal client error, at 503 it retries.
_KNOWN_STATUS = {
    # throttling family (botocore's retryable throttled codes)
    "Throttling": 400,
    "ThrottlingException": 400,
    "ThrottledException": 400,
    "RequestThrottledException": 400,
    "RequestThrottled": 400,
    "EC2ThrottledException": 400,
    "TooManyRequestsException": 429,
    "ProvisionedThroughputExceededException": 400,
    "TransactionInProgressException": 400,
    "RequestLimitExceeded": 503,
    "BandwidthLimitExceeded": 400,
    "LimitExceededException": 400,
    "PriorRequestNotComplete": 400,
    "SlowDown": 503,
    # transient server-side
    "InternalError": 500,
    "InternalServerError": 500,
    "InternalFailure": 500,
    "InternalServiceError": 500,
    "ServiceUnavailable": 503,
    "ServiceUnavailableException": 503,
    "ServiceUnavailableError": 503,
    "RequestTimeout": 408,
    "RequestTimeoutException": 408,
    # common terminal errors — keep these non-retryable
    "AccessDeniedException": 403,
    "AccessDenied": 403,
    "InvalidAccessKeyId": 403,
    "SignatureDoesNotMatch": 403,
    "ExpiredTokenException": 400,
    "UnauthorizedException": 401,
    "NotAuthorizedException": 400,
    "ValidationException": 400,
    "ValidationError": 400,
    "InvalidParameterValue": 400,
    "InvalidParameterException": 400,
    "InvalidParameterValueException": 400,
    "MissingParameter": 400,
    "NoSuchBucket": 404,
    "NoSuchKey": 404,
    "NotFound": 404,
    "NotFoundException": 404,
    "ResourceNotFoundException": 404,
    "ResourceNotFound": 404,
    "ResourceInUseException": 400,
    "ResourceAlreadyExistsException": 400,
    "ConditionalCheckFailedException": 400,
    "InvalidClientTokenId": 403,
}


def _modeled_status(service: str, name: str) -> int | None:
    """httpStatusCode from the raw service model, when modeled."""
    try:
        loader = _session.get_component("data_loader")
        model = loader.load_service_model(service, "service-2")
        shape = model.get("shapes", {}).get(name)
        if shape:
            status = shape.get("error", {}).get("httpStatusCode")
            return status if isinstance(status, int) else None
    except Exception:  # noqa: BLE001 — service model may be absent/partial
        return None
    return None


def error_http_status(service: str, code: str, default: int = 503) -> int:
    """HTTP status for an error code.

    Order: modeled ``httpStatusCode`` (rare but authoritative when present) →
    curated AWS-observed map → ``default`` (callers pick per protocol; AWS
    json/query services conventionally serve client errors at 400).
    """
    name = code.rsplit("#", 1)[-1]
    status = _modeled_status(service, name)
    if status is not None:
        return status
    return _KNOWN_STATUS.get(name, default)


def operation_error_names(service: str, operation: str) -> list[str]:
    """Error shape names modeled for an operation (for plausible sampling)."""
    model = get_service_model(service)
    if model is None:
        return []
    try:
        op = model.operation_model(operation)
    except Exception:  # noqa: BLE001 — OperationNotFoundError and friends
        return []
    return [shape.name for shape in op.error_shapes]
