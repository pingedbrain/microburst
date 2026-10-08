"""Cassette record/replay: capture upstream responses, replay them later.

``--record DIR`` writes every proxied upstream response to
``DIR/<sha256>.json``, keyed by ``sha256(method + " " + path_qs + body)`` —
headers are deliberately excluded so signatures and timestamps don't
perturb the key (a cassette replays across runs).

``--replay DIR`` serves responses from the cassette without touching the
upstream — while rules still inject faults on top, which is the point:
deterministic real traffic underneath, chaos on top.

Body bytes are always part of the key — JSON-protocol services all POST
to ``/``, so the path alone doesn't discriminate.
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import AsyncIterator
from pathlib import Path


class Cassette:
    def __init__(self, directory: str | Path, mode: str):
        if mode not in ("record", "replay"):
            raise ValueError(f"bad cassette mode: {mode}")
        self.dir = Path(directory)
        self.mode = mode
        if mode == "record":
            self.dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def key_for(method: str, path_qs: str, body: bytes | None) -> str:
        h = hashlib.sha256()
        h.update(method.encode())
        h.update(b" ")
        h.update(path_qs.encode())
        h.update(b"\n")
        h.update(body or b"")
        return h.hexdigest()

    def _path(self, key: str) -> Path:
        return self.dir / f"{key}.json"

    def get(self, key: str) -> dict | None:
        path = self._path(key)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return None

    def put(self, key: str, entry: dict) -> None:
        self._path(key).write_text(json.dumps(entry, indent=2))

    def tee(
        self,
        chunks: AsyncIterator[bytes],
        key: str,
        status: int,
        headers,
    ) -> AsyncIterator[bytes]:
        """Pass chunks through, accumulating a recording written at EOF."""

        async def gen() -> AsyncIterator[bytes]:
            buf = bytearray()
            async for chunk in chunks:
                buf.extend(chunk)
                yield chunk
            self.put(key, {
                "status": status,
                "headers": dict(headers),
                "body_b64": base64.b64encode(bytes(buf)).decode(),
            })

        return gen()


def entry_response_body(entry: dict) -> bytes:
    return base64.b64decode(entry.get("body_b64", ""))
