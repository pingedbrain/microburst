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
  rest-json, smithy-rpc-v2-cbor). `render_error()` is the facade.
- `src/microburst/effects/` — composable stages in fixed order
  (latency → reset → timeout → error), one file per effect.
- `src/microburst/rules.py` — rule matching engine + presets.
- `src/microburst/forward.py` — upstream relay + SigV4 re-signing
  (data plane only, no decisions).
- `src/microburst/pg/` — PostgreSQL wire mode: a sibling transport, not
  the HTTP pipeline. `proto.py` frame codec, `errors.py` ErrorResponse
  renderer, `detect.py` SQL verb detection, `server.py` asyncio TCP
  proxy (`--protocol postgres`, control API on `--control-port`).
- `src/microburst/redis/` — Redis wire mode: same sibling-transport
  shape as pg/. `proto.py` RESP2+RESP3 codec, `errors.py` `-CODE`
  rendering, `detect.py` verb/first-key extraction, `server.py` asyncio
  TCP proxy (`--protocol redis`) with MULTI-aware error skipping and
  pub/sub push-mode passthrough.
- `src/microburst/mysql/` — MySQL wire mode: same sibling-transport
  shape as pg/. `proto.py` 3B-LE+seq packet codec (multi-packet
  reassembly, greeting/handshake-response parsers), `errors.py`
  ERR_Packet renderer (errno↔SQLSTATE defaults), `detect.py` command
  + SQL detection (reuses pg's `sql_facts`), `server.py` asyncio TCP
  proxy (`--protocol mysql`) with tx-aware error skipping, prepared
  stmt-id→SQL tracking, auth passthrough, TLS-capability stripping.
- `src/microburst/tcp/` — generic byte-stream wire mode
  (`--protocol tcp`) for protocols with no dedicated module: duplex
  frame/chunk pumps, transport faults only (no `error:` renderer —
  `respond:` is the escape hatch).
- `src/microburst/grpc/` — gRPC wire mode over h2c (cleartext HTTP/2,
  `--protocol grpc`): same sibling-transport shape. `proto.py` h2
  helpers + gRPC status-code map / trailer builders; `server.py` runs
  two `h2` state machines bridged per stream — trailers-only and
  mid-stream (`partial_messages`) error injection, RST_STREAM aborts,
  stalls with real flow-control backpressure (inbound DATA is acked
  only once forwarded). No TLS — `grpcs://` is rejected.
- `src/microburst/transports.py` — `TRANSPORTS` registry: the seam
  `cli.py` dispatches `--protocol` through (name → `run_*` path,
  default ports, upstream schemes, extra option keys).
- `src/microburst/framing.py` — defensive message-boundary parsing:
  `FramedStream` for framed upload bodies (eventstream preludes, gRPC
  length prefixes) used by `request:` faults; `GenericFramer` +
  `parse_framing` for tcp mode's user-declared framing
  (length-prefix / delimiter / fixed).
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
- **The wire protocol is the observed protocol, not the model's.** Migrated
  services (CloudWatch etc.) declare `smithy-rpc-v2-cbor` but clients like
  boto3 send query-compatible JSON (`x-amzn-query-mode` + `X-Amz-Target` +
  JSON body). `RequestContext.protocol` records what the request actually
  speaks — fault responses must match that, never the model alone.
- **Authority order for AWS facts:** botocore service model > AWS docs >
  observed emulator behavior. Label which one you used in code comments and
  commit messages. Never claim "validated against real AWS" unless it was.
- REST operation matching is ambiguous by nature (e.g. S3 `PutObject` vs
  `CopyObject` share method+path) — disambiguate via query markers and
  modeled headers, not first-match ordering.

## TCP transport contract

`--protocol` dispatches through `TRANSPORTS` in `transports.py`. A TCP
sibling transport is:

- a `TRANSPORTS` entry: name, `run` path (`"pkg.mod:func"`), default
  listen port, default upstream + accepted URL schemes, optional extra
  config keys cli forwards as kwargs;
- `run_<name>(host, port, upstream_host, upstream_port, control_port,
  rules, watch_config=None, **options) -> int` — blocks until shutdown;
- a package with codec / detector / handler modules (pg/, redis/ are
  the reference; tcp/ shows the protocol-blind variant);
- a `service:` name stamped on every `RequestContext`/`FiredEvent` so
  `service:` matching and `/fired` filtering work;
- a `*Proxy` state object exposing the surface `control.py` reads:
  `engine`/`fired`/`fault_counts`/`stats`/`requests_seen`/`_listeners`/
  `upstream`/`subscribe_fired`/`handle_control` — `pg.PgProxy` is the
  canonical shape.

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
PyPI via trusted publishing (`.github/workflows/release.yml`); versions are
tag-derived (`hatch-vcs`) — update CHANGELOG, tag `vX.Y.Z`, cut a GitHub
release (see CONTRIBUTING.md).

## This repo adopts Apache Magpie

`.apache-magpie.lock` records the recommended skill floor;
`.apache-magpie-overrides/` holds project-specific skill overrides. Local
Magpie state belongs in `.apache-magpie-local/` (gitignored).
