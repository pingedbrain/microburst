"""AWS `rest-xml` protocol (S3, CloudFront) — XML Error envelope."""

from __future__ import annotations

import uuid
from xml.sax.saxutils import escape

from microburst.protocols import register_serializer

# Live-capture verified: Route53 serves text/xml and the generic
# x-amzn-RequestId; S3-style services use application/xml plus the
# x-amz-request-id/x-amz-id-2 pair. Default is the S3 family.
_TEXT_XML_SERVICES = {"route53"}


@register_serializer("rest-xml")
def render(
    code: str, message: str, request_id: str, service: str | None = None
) -> tuple[dict[str, str], bytes]:
    if service in _TEXT_XML_SERVICES:
        headers = {
            "Content-Type": "text/xml",
            "x-amzn-RequestId": request_id,
        }
    else:
        headers = {
            "Content-Type": "application/xml",
            "x-amz-request-id": request_id,
            "x-amz-id-2": uuid.uuid4().hex * 2,
        }
    body = (
        "<Error>"
        f"<Code>{escape(code)}</Code>"
        f"<Message>{escape(message)}</Message>"
        f"<RequestId>{request_id}</RequestId>"
        "</Error>"
    )
    return headers, body.encode()
