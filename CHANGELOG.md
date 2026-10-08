# Changelog

All notable changes to this project will be documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[SemVer](https://semver.org/).

## [0.2.0] - 2026-10-08

Full protocol coverage (436/436 modeled services), a grown-up rule DSL,
response-side faults, and an observability surface.

### Added

- **`smithy-rpc-v2-cbor` protocol** — CBOR encoder + serializer covers the
  18 rpc-v2 services (cloudwatch, gamelift, eventbridgev2, ...). Detection
  honors the *observed* wire protocol, including `x-amzn-query-mode`
  query-compatible JSON with `x-amzn-query-error` response headers — so
  migrated services like CloudWatch get errors their client actually parses.
- **Signing-scope aliases** — 50+ SigV4 signing names empirically resolved
  from real botocore clients (`monitoring`→cloudwatch, `states`→stepfunctions,
  `execute-api`→apigatewaymanagementapi, ...) plus `X-Amz-Target`/CBOR-path
  target-prefix disambiguation for the 40 shared scopes
  (dynamodb vs dynamodbstreams, events vs eventbridgev2, s3 vs s3control).
- **Presigned URL + SigV4A detection** — `X-Amz-Credential` in the query
  string is parsed like the Authorization header; SigV4A scopes (region
  `*`) work out of the box.
- **Rule matchers** — `headers:` (case-insensitive, `""` = presence),
  `body:` (jmespath over JSON, form-encoded, and XML rest-xml payloads).
- **Rule cadence** — `rate: {count, window_s}` rolling window,
  `sequence: {fail, pass}` repeating patterns, `deterministic: true`
  (request-identity hash draw — same resource always lands on the same
  side of p, nested failure tiers), `ttl_s` rule expiration.
- **Response-side faults** — `response:` block mutates upstream responses
  while streaming: `truncate_frac|truncate_bytes` (valid envelope, short
  body), `abort_frac|abort_bytes` (mid-stream connection death),
  `corrupt_bytes` (200 OK, wrong payload), `bandwidth_kbps` (throttled
  body stream).
- **Latency distributions** — `uniform` (default), `gaussian`
  (mean/stddev, optional clamps), `spike` (baseline + occasional spikes).
- **Control plane** — `GET /_microburst/fired/stream` (SSE live tail,
  bounded per-consumer queues, keepalives), `GET /_microburst/fired`
  filters (`service`/`operation`/`rule_id`/`limit`), and
  `GET /_microburst/metrics` (Prometheus exposition; counters survive
  fired-log eviction).
- **Optional OTel spans** — `pip install microburst[otel]` emits
  `microburst.fault` spans per injected fault; no-op when absent.
- **`microburst --version`** — tag-driven versioning via hatch-vcs; the
  git tag is the single source of truth.

### Changed

- **Architecture** — `proxy.py` god-object split into a layered pipeline:
  `detection/` (scope/operation/rest/resource), `protocols/` (registry of
  error serializers — add a protocol = add one file), `effects/`
  (composable stages), `forward.py` (data plane), `control.py` (control
  plane), `RequestContext` as the request-scoped state object.
- **Testing** — 27 → 538 tests. New `test_fidelity.py` harness renders
  every service's error and parses it with botocore's own protocol
  parser: `Error.Code` round-trips for all 436 modeled services.

## [0.1.0] - 2026-10-07

Initial public release.

### Added

- Protocol-aware fault injection proxy for AWS: detects service, operation,
  region and resource per request (SigV4 credential scope, `X-Amz-Target`,
  `Action=`, REST path patterns from botocore service models).
- Wire-correct error serialization for `json`, `query`, `ec2`, `rest-xml`
  and `rest-json` protocols, with HTTP statuses resolved from the service
  model or a curated AWS-observed map.
- Rule engine: match on service/operation/region/resource, probability,
  `times` (fire N times then pass), error/latency/timeout/reset effects.
- Fault presets (`ddb-throttle`, `flaky-s3`, `slow-lambda`, `kms-outage`,
  `sqs-backlog`, `regional-failover`, `network-jitter`, `gateway-storm`).
- Control API under `/_microburst/*` (rules CRUD, fired-fault log, presets).
- Optional SigV4 re-signing for real `amazonaws.com` upstreams.
- CLI (`microburst`), YAML config, `demo.py` (orders pipeline → MiniStack).

[0.2.0]: https://github.com/pingedbrain/microburst/releases/tag/v0.2.0
[0.1.0]: https://github.com/pingedbrain/microburst/releases/tag/v0.1.0
