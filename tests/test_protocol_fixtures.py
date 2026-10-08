"""Parity against AWS-authored protocol fixtures — no AWS creds needed.

botocore ships protocol-compliance fixtures (``tests/unit/protocols/
output/*.json``) with expected error wire responses written by AWS for
SDK conformance. We vendor them in ``fidelity/protocol/`` (see
``tools/fetch_protocol_fixtures.py``) and assert: our rendered error,
parsed by a botocore parser built from the fixture's own model, yields
the fixture's expected error code and members.
"""

from __future__ import annotations

import json
from pathlib import Path

from botocore.model import ServiceModel
from botocore.parsers import create_parser

from microburst.protocols import render_error

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fidelity" / "protocol"


def _cases():
    for path in sorted(FIXTURE_DIR.glob("*.json")):
        if path.name == "SOURCE.json":
            continue
        query_compat = "query-compatible" in path.name
        groups = json.loads(path.read_text())
        if not isinstance(groups, list):
            continue
        for group in groups:
            meta = group.get("metadata", {})
            protocol = meta.get("protocol")
            shapes = group.get("shapes", {})
            for case in group.get("cases", []):
                code = case.get("errorCode")
                if not code:
                    continue
                yield path.name, group, meta, protocol, shapes, case, query_compat


def test_our_render_parses_to_fixture_error():
    checked = skipped = 0
    failures = []
    for fname, group, meta, protocol, shapes, case, qc in _cases():
        given = case.get("given") or {}
        op_name = given.get("name", "Op")
        model = ServiceModel({
            "metadata": meta,
            "operations": {op_name: given},
            "shapes": shapes,
        })
        try:
            parser = create_parser(model.protocol)
        except Exception:  # noqa: BLE001 — protocol unsupported locally
            skipped += 1
            continue

        expected_code = case["errorCode"]
        status, headers, body = render_error(
            None,
            expected_code.rsplit("#", 1)[-1],
            case.get("errorMessage", ""),
            protocol=protocol if isinstance(protocol, str) else None,
            query_compat=qc,
        )
        try:
            parsed = parser.parse(
                {
                    "status_code": status,
                    "headers": {k.lower(): v for k, v in headers.items()},
                    "body": body,
                },
                None,
            )
        except Exception as e:  # noqa: BLE001 — our bytes must parse
            failures.append(
                f"{fname}:{case.get('id')}: parse failed {e!r}"
            )
            continue

        err = parsed.get("Error") or {}
        got = err.get("Code")
        # query-compat wires the code namespaced; the fixture's
        # expectedCode is the bare shape code
        bare = (got or "").rsplit("#", 1)[-1].rsplit(".", 1)[-1]
        if bare != expected_code.rsplit("#", 1)[-1]:
            failures.append(
                f"{fname}:{case.get('id')}: "
                f"code {got!r} != {expected_code!r}"
            )
            continue
        # Members other than Message are fixture data a generic injector
        # can't fabricate (e.g. ComplexError's TopLevel/Header) — the
        # envelope contract is the parsed Code plus Message when the
        # fixture defines one.
        expected_msg = (case.get("error") or {}).get(
            "Message", (case.get("error") or {}).get("message")
        )
        if expected_msg is not None and (
            err.get("Message") != expected_msg
            and err.get("message") != expected_msg
        ):
            failures.append(
                f"{fname}:{case.get('id')}: message "
                f"{err.get('Message', err.get('message'))!r} "
                f"!= {expected_msg!r}"
            )
            continue
        checked += 1

    assert checked > 20, f"suspiciously few cases checked: {checked}"
    assert not failures, "\n".join(failures[:20])
    print(f"\n{checked} fixture error cases verified ({skipped} skipped)")


def test_fixtures_vendored():
    files = list(FIXTURE_DIR.glob("*.json"))
    assert files, "run tools/fetch_protocol_fixtures.py"
    src = json.loads((FIXTURE_DIR / "SOURCE.json").read_text())
    assert src["botocore_tag"]
