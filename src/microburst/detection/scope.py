"""SigV4/SigV4A credential-scope parsing.

The Authorization header's Credential field is the most reliable service +
region signal: it is what AWS itself routes on. Presigned URLs carry the
same scope in the ``X-Amz-Credential`` query parameter (bare, without the
``Credential=`` prefix). SigV4A uses the same shape — algorithm becomes
``AWS4-ECDSA-P256-SHA256`` and region becomes ``*``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from microburst.models import service_for_scope

CREDENTIAL_RE = re.compile(
    r"Credential=(?P<key>[^/,]+)/(?P<date>\d{8})/(?P<region>[^/]+)/"
    r"(?P<service>[^/]+)/aws4_request"
)

# Bare scope as carried by presigned URLs:
# X-Amz-Credential=AKID/20240101/us-east-1/s3/aws4_request
SCOPE_RE = re.compile(
    r"^(?P<key>[^/]+)/(?P<date>\d{8})/(?P<region>[^/]+)/"
    r"(?P<service>[^/]+)/aws4_request$"
)


def parse_credential_scope(
    headers: Mapping[str, str],
    query: Mapping[str, str] | None = None,
) -> tuple[str | None, str | None, str | None]:
    """Return (service, region, access_key) from signing material.

    Order: Authorization header (SigV4/SigV4A), then the presigned-URL
    ``X-Amz-Credential`` query parameter.
    """
    auth = headers.get("Authorization", "")
    match = CREDENTIAL_RE.search(auth)
    if not match and query:
        # Presigned URLs: AWS SDKs emit X-Amz-Credential; some generators
        # (older SDKs, third-party tools) lowercase the parameter name.
        cred = query.get("X-Amz-Credential") or query.get("x-amz-credential")
        if cred:
            match = SCOPE_RE.match(cred)
    if not match:
        return None, None, None
    return (
        service_for_scope(match.group("service")),
        match.group("region"),
        match.group("key"),
    )
