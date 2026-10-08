# AGENTS.md

Guidance for AI coding agents working in this repository.

## What this project is

microburst is an **AWS failure injection proxy**. It sits between an app and
any AWS endpoint (emulator or real), detects the service/operation/region of
each request, and returns protocol-correct fault responses that exercise
real SDK retry behavior. It is a developer tool — the control API is
unauthenticated by design and must never be exposed publicly.

## Layout

The pipeline is `detect → decide → effect-or-forward`, with registries as
extension seams:

- `src/microburst/core/context.py` — `RequestContext`: the object that
  travels the pipeline (raw request + detected fields + decision).
- `src/microburst/core/pipeline.py` — `Microburst` orchestrator: rule
  engine, upstream client, fired log, request handlers.
- `src/microburst/detection/` — scope / operation / rest / resource
  detectors; `__init__` exposes `detect()` and `should_buffer()`.
- `src/microburst/protocols/` — error-serializer registry
  (`@register_serializer` per protocol: json, query+ec2, rest-xml,
  rest-json). `render_error()` is the facade.
- `src/microburst/effects/` — composable stages in fixed order
  (latency → reset → timeout → error), one file per effect.
- `src/microburst/rules.py` — rule matching engine + presets.
- `src/microburst/forward.py` — upstream relay + SigV4 re-signing
  (data plane only, no decisions).
- `src/microburst/control.py` — `/_microburst/*` control plane.
- `src/microburst/models.py` — botocore service-model access.
- `src/microburst/app.py` / `cli.py` — app wiring + entry point.
- `tests/` — pytest, self-contained (stub upstream fixture; no external
  emulator needed).
- `ROADMAP.md` — planned extensions, organized by seam.

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
