"""Every chaos.yml shipped in examples/ must parse into valid rules —
guards the docs against DSL drift."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from microburst import rules

EXAMPLES = Path(__file__).parent.parent / "examples"
CONFIGS = sorted(
    p for p in EXAMPLES.rglob("*.yml") if p.name != "docker-compose.yml"
)


@pytest.mark.parametrize("path", CONFIGS, ids=lambda p: p.parent.name or p.name)
def test_example_config_parses(path):
    cfg = yaml.safe_load(path.read_text())
    parsed = [rules.from_dict(r) for r in cfg["rules"]]
    assert parsed, f"{path}: empty ruleset"
    for rule in parsed:
        effects = [
            rule.error, rule.latency, rule.timeout_ms, rule.reset,
            rule.response, rule.request, rule.partial_rows,
        ]
        assert any(e is not None and e is not False for e in effects), (
            f"{path}: rule has matchers but no fault effect"
        )
