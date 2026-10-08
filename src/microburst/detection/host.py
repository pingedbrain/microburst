"""Host-header detection: virtual-hosted style + region extraction.

Virtual-hosted addressing puts the resource in the host —
``bucket.s3.us-east-1.amazonaws.com``, ``{accountId}.s3-control…``, or the
emulator flavors ``bucket.s3.localhost.localstack.cloud``. When it fires,
the label is prepended to the request path so REST route matching sees
``/{Bucket}/{Key}`` as modeled. The host is also the only service/region
signal for unsigned requests (or ones the proxy can't parse credentials
from), and corroborates them when present.
"""

from __future__ import annotations

import re
from functools import cache

# host suffixes to strip before looking for the endpoint prefix —
# dualstack variants and AWS/amazonaws domain tails
_REGION = re.compile(r"^[a-z0-9-]+-\d+$")

# endpointPrefix aliases that appear on the wire but aren't model prefixes
_PREFIX_ALIAS = {
    "s3-accelerate": "s3",
    "s3-accelerate.dualstack": "s3",
    "s3-control": "s3control",
    "queue": "sqs",  # legacy global SQS endpoint queue.amazonaws.com
    "data.iot": "iot-data",  # classic endpoint; the model has data-ats.iot
    "appsync-api": "appsync",  # GraphQL invoke endpoint {api}.appsync-api.…
    "lambda-url": "lambda",  # Function URLs {url-id}.lambda-url.….on.aws
}

# Prefix-shaped labels that aren't modeled prefixes but map to a service.
# Directory buckets: {bucket}--{az}--x-s3.s3express-{az}.{region}.amazonaws.com
# sign with the `s3express` scope, which botocore doesn't model as a service.
_PREFIX_PATTERNS = (
    (re.compile(r"^s3express-[a-z0-9-]+$"), "s3"),
    # ATS data-plane endpoints: {endpoint-name}-ats.iot.{region}.amazonaws.com
    # — the `iot` label alone would misread them as the control plane
    (re.compile(r"^[a-z0-9-]+-ats\.iot$"), "iot-data"),
)

# Host-derived service is only trusted on AWS-shaped or emulator-shaped
# domains — a random `logs.datadog.com` shouldn't classify as AWS logs.
_TRUSTED_TAILS = (
    ".amazonaws.com",
    ".amazonaws.com.cn",
    ".c2s.ic.gov",
    ".on.aws",   # Lambda Function URLs, ECR OCI pull-through, etc.
    ".api.aws",  # api.aws service endpoints (e.g. *.controlcatalog.api.aws)
    ".localstack.cloud",
    ".localhost",
)


def _trusted_host(host: str) -> bool:
    name = host.split(":", 1)[0].lower()
    return name == "localhost" or name.endswith(_TRUSTED_TAILS)


@cache
def _endpoint_prefix_map() -> dict[str, str]:
    """endpointPrefix → service, from every service model's metadata."""
    from botocore.session import Session

    from microburst.models import service_metadata

    out: dict[str, str] = {}
    for svc in Session().get_available_services():
        ep = service_metadata(svc).get("endpointPrefix")
        if isinstance(ep, str):
            out.setdefault(ep, svc)
    return out


def parse_host(host: str) -> tuple[str | None, str | None, str | None]:
    """(service, region, virtual_label) from a Host header.

    ``virtual_label`` is the resource carried in the host (the bucket for
    ``bucket.s3.…``) — ``None`` for path-style/general endpoints.
    """
    if not host:
        return None, None, None
    parts = host.split(":", 1)[0].lower().split(".")
    if len(parts) < 2:
        return None, None, None

    prefixes = _endpoint_prefix_map()
    # longest prefix first: data.iot spans two labels
    for size in range(min(3, len(parts)), 0, -1):
        for i in range(len(parts) - size + 1):
            candidate = ".".join(parts[i : i + size])
            service = prefixes.get(candidate) or _PREFIX_ALIAS.get(candidate)
            if service is None:
                service = next(
                    (svc for pat, svc in _PREFIX_PATTERNS if pat.match(candidate)),
                    None,
                )
            if service is None:
                continue
            label = ".".join(parts[:i]) or None
            # s3-accelerate/dualstack tokens after the prefix are ignored;
            # the first region-shaped token wins
            region = next(
                (
                    tok
                    for tok in parts[i + size :]
                    if _REGION.match(tok)
                ),
                None,
            )
            if not _trusted_host(host):
                return None, region, label
            return service, region, label
    return None, None, None


def virtual_label(host: str, service: str | None) -> str | None:
    """The leading host label when ``service`` virtual-hosts resources
    (S3-style ``bucket.<anything>``). Works even when the host carries no
    endpoint prefix — e.g. ``bucket.localhost:9999`` under virtual
    addressing against the proxy itself."""
    if not host or service != "s3":
        return None
    first = host.split(":", 1)[0].lower().split(".", 1)[0]
    if first in ("", "localhost", "s3") or first.replace(".", "").isdigit():
        return None
    if _REGION.match(first):
        return None
    return first
