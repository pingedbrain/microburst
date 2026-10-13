"""AWS `query` and `ec2` protocols — XML error envelopes.

Live-capture verified shapes (fidelity/captures/*):

- query services (cfn, iam, rds, elbv2, …) send a pretty-printed
  ``ErrorResponse`` carrying the model's ``xmlNamespace``, with ``Error``
  members ordered Type, Code, Message.
- ec2 sends ``<?xml?><Response><Errors><Error>`` — no Type, no xmlns,
  ``RequestID`` (capital D) — and ``text/xml;charset=UTF-8``.
"""

from __future__ import annotations

from xml.sax.saxutils import escape

from microburst.models import service_metadata
from microburst.protocols import register_serializer, xml_members


@register_serializer("query")
def render(
    code: str,
    message: str,
    request_id: str,
    service: str | None = None,
    fields: dict | None = None,
    resource: str | None = None,
) -> tuple[dict[str, str], bytes]:
    headers = {
        "Content-Type": "text/xml",
        "x-amzn-RequestId": request_id,
    }
    xmlns = service_metadata(service).get("xmlNamespace", "")
    xmlns_attr = f' xmlns="{escape(xmlns)}"' if xmlns else ""
    fields_xml = xml_members(fields, sep="\n    ")
    fields_xml = f"    {fields_xml}\n" if fields_xml else ""
    body = (
        f"<ErrorResponse{xmlns_attr}>\n"
        "  <Error>\n"
        "    <Type>Sender</Type>\n"
        f"    <Code>{escape(code)}</Code>\n"
        f"    <Message>{escape(message)}</Message>\n"
        f"{fields_xml}"
        "  </Error>\n"
        f"  <RequestId>{request_id}</RequestId>\n"
        "</ErrorResponse>\n"
    )
    return headers, body.encode()


@register_serializer("ec2")
def render_ec2(
    code: str,
    message: str,
    request_id: str,
    service: str | None = None,
    fields: dict | None = None,
    resource: str | None = None,
) -> tuple[dict[str, str], bytes]:
    headers = {
        "Content-Type": "text/xml;charset=UTF-8",
        "x-amzn-RequestId": request_id,
    }
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<Response><Errors><Error>"
        f"<Code>{escape(code)}</Code>"
        f"<Message>{escape(message)}</Message>"
        f"{xml_members(fields)}"
        "</Error></Errors>"
        f"<RequestID>{request_id}</RequestID>"
        "</Response>"
    )
    return headers, body.encode()
