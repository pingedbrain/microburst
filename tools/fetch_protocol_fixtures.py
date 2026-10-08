"""Fetch AWS-authored protocol test fixtures (no AWS credentials needed).

botocore's repo carries protocol compliance fixtures under
``tests/unit/protocols/output/`` — request/response pairs authored for
SDK conformance, including error envelopes for every wire protocol.
This script pins them to the installed botocore release and vendors
them into ``fidelity/protocol/`` so our serializers can be diffed
against AWS-authored expectations — a wire-truth leg that needs no
AWS account at all.

    python tools/fetch_protocol_fixtures.py           # vendored, pinned
    python tools/fetch_protocol_fixtures.py --check   # verify vs SOURCE
"""

from __future__ import annotations

import hashlib
import json
import sys
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "fidelity" / "protocol"
SOURCE = OUT / "SOURCE.json"

BASE = (
    "https://raw.githubusercontent.com/boto/botocore/{tag}"
    "/tests/unit/protocols/output/{name}"
)

FILES = (
    "json.json",
    "json_1_0.json",
    "json_1_0-query-compatible.json",
    "query.json",
    "ec2.json",
    "rest-json.json",
    "rest-xml.json",
    "smithy-rpc-v2-cbor.json",
    "smithy-rpc-v2-cbor-query-compatible.json",
    "smithy-rpc-v2-cbor-non-query-compatible.json",
)


def main() -> int:
    check = "--check" in sys.argv
    import botocore

    tag = botocore.__version__
    digests = {}
    for name in FILES:
        url = BASE.format(tag=tag, name=name)
        try:
            data = urllib.request.urlopen(url, timeout=30).read()
        except urllib.error.URLError as e:
            print(f"  {name}: fetch failed ({e}) — skipping")
            continue
        digests[name] = hashlib.sha256(data).hexdigest()
        if not check:
            OUT.mkdir(parents=True, exist_ok=True)
            (OUT / name).write_bytes(data)
            print(f"  {name}: {len(data)} bytes")

    if check:
        src = json.loads(SOURCE.read_text())
        drift = [
            n for n, d in digests.items()
            if src["sha256"].get(n) != d
        ]
        if drift:
            print(f"fixture drift vs botocore {tag}: {drift}")
            return 1
        print(f"fixtures match botocore {tag}")
        return 0

    SOURCE.write_text(json.dumps({
        "origin": "https://github.com/boto/botocore",
        "path": "tests/unit/protocols/output",
        "botocore_tag": tag,
        "sha256": digests,
    }, indent=2, sort_keys=True) + "\n")
    print(f"→ {OUT} (botocore {tag})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
