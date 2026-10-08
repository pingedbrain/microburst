"""AWS `query` and `ec2` protocols — XML ErrorResponse envelope."""

from __future__ import annotations

from xml.sax.saxutils import escape

from microburst.protocols import register_serializer


@register_serializer("query", "ec2")
def render(
    code: str, message: str, request_id: str, service: str | None = None
) -> tuple[dict[str, str], bytes]:
    headers = {
        "Content-Type": "text/xml",
        "x-amzn-RequestId": request_id,
    }
    body = (
        "<ErrorResponse>"
        "<Error>"
        f"<Code>{escape(code)}</Code>"
        f"<Message>{escape(message)}</Message>"
        "<Type>Sender</Type>"
        "</Error>"
        f"<RequestId>{request_id}</RequestId>"
        "</ErrorResponse>"
    )
    return headers, body.encode()
