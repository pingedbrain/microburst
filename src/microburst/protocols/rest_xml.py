"""AWS `rest-xml` protocol (S3, CloudFront) — XML Error envelope."""

from __future__ import annotations

import uuid
from xml.sax.saxutils import escape

from microburst.protocols import register_serializer


@register_serializer("rest-xml")
def render(code: str, message: str, request_id: str) -> tuple[dict[str, str], bytes]:
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
