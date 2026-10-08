"""Repo-facing wrapper: `microburst fidelity` writing into ./fidelity/.

The real implementation ships in the wheel — see ``microburst.fidelity``.
This script exists so repo history/docs keep working:

    python tools/live_fidelity.py capture --services dynamodb
    python tools/live_fidelity.py report
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from microburst.fidelity import fidelity_main

if __name__ == "__main__":
    args = sys.argv[1:]
    # historical flag name: capture --services a,b
    sys.exit(fidelity_main(args))
