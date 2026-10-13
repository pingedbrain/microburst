"""AWS `rest-xml` protocol (S3, CloudFront) — XML Error envelope."""

from __future__ import annotations

import uuid
from xml.sax.saxutils import escape

from microburst.models import service_metadata
from microburst.protocols import register_serializer, xml_members

# Live-capture verified: Route53 serves text/xml, the generic
# x-amzn-RequestId, and a query-style ``ErrorResponse`` envelope wrapped in
# the service's xml namespace — not S3's flat ``<Error>``. CloudFront is
# in the same family per AWS docs (unverified by capture). S3-family
# services use application/xml plus the x-amz-request-id/x-amz-id-2 pair
# and the flat envelope. Default is the S3 family.
_ERROR_RESPONSE_SERVICES = {"route53", "cloudfront"}


@register_serializer("rest-xml")
def render(
    code: str,
    message: str,
    request_id: str,
    service: str | None = None,
    fields: dict | None = None,
    resource: str | None = None,
) -> tuple[dict[str, str], bytes]:
    if service in _ERROR_RESPONSE_SERVICES:
        headers = {
            "Content-Type": "text/xml",
            "x-amzn-RequestId": request_id,
        }
        # botocore doesn't carry xmlNamespace for these services — the wire
        # namespace is https://{endpointPrefix}.amazonaws.com/doc/{apiVersion}/
        # (verified against the route53 capture).
        meta = service_metadata(service)
        xmlns = (
            f' xmlns="https://{meta["endpointPrefix"]}.amazonaws.com'
            f'/doc/{meta["apiVersion"]}/"'
            if meta.get("endpointPrefix") and meta.get("apiVersion")
            else ""
        )
        body = (
            '<?xml version="1.0"?>\n'
            f"<ErrorResponse{xmlns}>"
            "<Error>"
            "<Type>Sender</Type>"
            f"<Code>{escape(code)}</Code>"
            f"<Message>{escape(message)}</Message>"
            f"{xml_members(fields)}"
            "</Error>"
            f"<RequestId>{request_id}</RequestId>"
            "</ErrorResponse>"
        )
        return headers, body.encode()

    host_id = uuid.uuid4().hex * 2
    headers = {
        "Content-Type": "application/xml",
        "x-amz-request-id": request_id,
        "x-amz-id-2": host_id,
    }
    # Real S3 errors carry the requested path plus the same host id the
    # x-amz-id-2 header reports (AWS documentation; consistent with the
    # committed captures' headers).
    extra = f"<Resource>{escape(resource)}</Resource>" if resource else ""
    body = (
        "<Error>"
        f"<Code>{escape(code)}</Code>"
        f"<Message>{escape(message)}</Message>"
        f"{extra}{xml_members(fields)}"
        f"<RequestId>{request_id}</RequestId>"
        f"<HostId>{host_id}</HostId>"
        "</Error>"
    )
    return headers, body.encode()
