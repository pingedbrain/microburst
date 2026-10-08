# AGENTS.md

Guidance for AI coding agents working in this repository.

## What this project is

microburst is an **AWS failure injection proxy**. It sits between an app and
any AWS endpoint (emulator or real), detects the service/operation/region of
each request, and returns protocol-correct fault responses that exercise
real SDK retry behavior. It is a developer tool — the control API is
unauthenticated by design and must never be exposed publicly.

## Layout

- `src/microburst/models.py` — botocore service-model access: protocols,
  operation/error shapes, `httpStatusCode` lookup, curated status map.
- `src/microburst/detect.py` — request → (service, operation, region,
  resource) detection. SigV4 scope → `X-Amz-Target` → `Action=` → REST path.
- `src/microburst/errors.py` — error serialization per AWS protocol.
- `src/microburst/rules.py` — rule matching engine + presets.
- `src/microburst/proxy.py` — aiohttp server: forwarding, effects, control
  API, optional SigV4 re-signing.
- `src/microburst/cli.py` — entry point and YAML config loading.
- `tests/` — pytest, self-contained (fixture spins a stub upstream; no
  external emulator needed).

## Non-negotiable invariants

- **Error fidelity is the product.** An injected fault must be
  indistinguishable from the real AWS failure to the SDK — same error code
  in the body, same HTTP status, same envelope for the protocol. A generic
  `429` or malformed body is a bug, not a simplification.
- **Status codes matter for retry classification.** e.g. S3 `SlowDown` is
  terminal at 400 but retried at 503; `ProvisionedThroughputExceededException`
  retries at 400. Prefer `httpStatusCode` from the service model, then the
  curated map in `models.py`, then a protocol-aware default.
- **Authority order for AWS facts:** botocore service model > AWS docs >
  observed emulator behavior. Label which one you used in code comments and
  commit messages. Never claim "validated against real AWS" unless it was.
- REST operation matching is ambiguous by nature (e.g. S3 `PutObject` vs
  `CopyObject` share method+path) — disambiguate via query markers and
  modeled headers, not first-match ordering.

## Conventions

- Dependencies stay minimal: aiohttp, PyYAML, botocore. New deps need a
  reason in the PR description.
- Every fired fault is observable — if a code path can affect a request, it
  must be visible in `/_microburst/fired` or it shouldn't affect the request.
- Broad `except Exception` is acceptable only at defensive proxy boundaries
  and must carry a `# noqa: BLE001` with a one-line justification.

## Working

```bash
uv pip install -e ".[test]"
pytest                      # full suite, self-contained
ruff check src/ tests/      # lint gate — CI enforces this
python demo.py              # end-to-end demo (needs MiniStack on :4566)
```

CI runs pytest on Python 3.10–3.13 and blocks merge. Releases publish to
PyPI via trusted publishing (`.github/workflows/release.yml`); bump
`version` in `pyproject.toml` and cut a GitHub release.

## This repo adopts Apache Magpie

`.apache-magpie.lock` records the recommended skill floor;
`.apache-magpie-overrides/` holds project-specific skill overrides. Local
Magpie state belongs in `.apache-magpie-local/` (gitignored).
