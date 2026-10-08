"""SigV4 credential-scope parsing.

The Authorization header's Credential field is the most reliable service +
region signal: it is what AWS itself routes on. Presigned URLs use
X-Amz-Credential in the query string instead — handled separately when we
add presigned support.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from microburst.models import service_for_scope

CREDENTIAL_RE = re.compile(
    r"Credential=(?P<key>[^/,]+)/(?P<date>\d{8})/(?P<region>[^/]+)/"
    r"(?P<service>[^/]+)/aws4_request"
)


def parse_credential_scope(
    headers: Mapping[str, str],
) -> tuple[str | None, str | None, str | None]:
    """Return (service, region, access_key) from the Authorization header."""
    auth = headers.get("Authorization", "")
    match = CREDENTIAL_RE.search(auth)
    if not match:
        return None, None, None
    return (
        service_for_scope(match.group("service")),
        match.group("region"),
        match.group("key"),
    )
