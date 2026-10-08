"""Serializer contract.

A renderer turns (code, message, request_id) into (headers, body) in one
AWS wire protocol. Renderers are registered per protocol name via
``@register_serializer`` in ``microburst.protocols`` — adding a protocol
means adding a file here and registering it; nothing else changes.
"""

from __future__ import annotations

from collections.abc import Callable

# render(code, message, request_id) -> (headers, body)
Renderer = Callable[[str, str, str], "tuple[dict[str, str], bytes]"]
