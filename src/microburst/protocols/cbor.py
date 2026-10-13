"""AWS `smithy-rpc-v2-cbor` protocol (CloudWatch, GameLift, EventBridge v2).

Errors are CBOR maps ``{"__type": code, "message": msg}`` — the SDK parser
reads ``__type`` from the decoded body plus ``x-amzn-requestid`` header.
The encoder below only covers what an error envelope needs: a flat map of
text strings (definite-length encoding, major types 5 + 3).
"""

from __future__ import annotations

from microburst.protocols import register_serializer


def _text(out: bytearray, s: str) -> None:
    b = s.encode()
    n = len(b)
    if n < 24:
        out.append(0x60 | n)
    elif n < 256:
        out += bytes((0x78, n))
    elif n < 65536:
        out += bytes((0x79,)) + n.to_bytes(2, "big")
    else:
        out += bytes((0x7A,)) + n.to_bytes(4, "big")
    out += b


def encode_map_str(pairs: dict[str, str]) -> bytes:
    """Minimal CBOR encoder for a flat string map (error envelopes)."""
    out = bytearray()
    n = len(pairs)
    assert n < 24, "error envelopes never exceed 23 pairs"
    out.append(0xA0 | n)  # map(n)
    for k, v in pairs.items():
        _text(out, k)
        _text(out, v)
    return bytes(out)


@register_serializer("smithy-rpc-v2-cbor")
def render(
    code: str,
    message: str,
    request_id: str,
    service: str | None = None,
    fields: dict | None = None,
    resource: str | None = None,
) -> tuple[dict[str, str], bytes]:
    headers = {
        "smithy-protocol": "rpc-v2-cbor",
        "Content-Type": "application/cbor",
        "x-amzn-requestid": request_id,
    }
    pairs = {"__type": code, "message": message}
    if fields:
        pairs.update({k: "" if v is None else str(v) for k, v in fields.items()})
    body = encode_map_str(pairs)
    return headers, body
