"""Regenerate the REST collision-sweep snapshot.

Runs every rest-json/rest-xml operation through the matcher with a
synthesized minimal request and writes ``fidelity/rest_sweep.json`` —
the regression guard the sweep test compares against, plus a console
summary of ambiguous routes.

    python tools/rest_sweep.py
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

SNAPSHOT = Path(__file__).resolve().parent.parent / "fidelity" / "rest_sweep.json"


def main() -> int:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
    from microburst.detection.sweep import sweep_all

    results = sweep_all()
    SNAPSHOT.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")

    n_ops = sum(len(v) for v in results.values())
    unresolved = {
        svc: {op: got for op, got in ops.items() if got != op}
        for svc, ops in results.items()
    }
    unresolved = {svc: ops for svc, ops in unresolved.items() if ops}
    n_unresolved = sum(len(v) for v in unresolved.values())

    print(f"{n_ops} ops swept across {len(results)} REST services")
    print(f"{n_unresolved} ops do not self-resolve → {SNAPSHOT}")
    by_route: dict[str, list[str]] = defaultdict(list)
    for svc, ops in sorted(unresolved.items()):
        for op, got in sorted(ops.items()):
            print(f"  {svc}: {op} → {got or 'none'}")
        by_route[svc].extend(ops)
    return 0


if __name__ == "__main__":
    sys.exit(main())
