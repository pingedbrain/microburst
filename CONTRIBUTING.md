# Contributing

## Setup

```bash
uv venv && uv pip install -e ".[test]"
pytest
ruff check src/ tests/
```

## Conventions

- Error shapes, operation metadata and protocols come from **botocore service
  models** — don't hand-maintain parallel tables if the model can answer.
- Wire formats must match what AWS SDKs parse. When changing error
  serialization, verify against a real `boto3` client (see `tests/`).
- Keep matchers deterministic and observable: every decision should show up
  in `/_microburst/fired` or not happen at all.
- The control API is intentionally unauthenticated (local dev tool). Don't
  add features that make it safe to expose — document instead.
