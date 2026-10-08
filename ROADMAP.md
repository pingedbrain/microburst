# Roadmap

Organized by extension seam — the architecture is built so each item is
"add a file in the right package" rather than "touch everything".
`[size]` is a rough effort hint; `[good-first-issue]` marks items that are
self-contained and need no deep context.

## Current coverage (measured, botocore 1.40)

436 modeled services:

| Protocol | Services | Status |
|---|---|---|
| `json` | 137 | ✅ X-Amz-Target + JSON envelope |
| `query` | 17 | ✅ `Action=` + XML envelope |
| `ec2` | 1 | ✅ (shares query serializer) |
| `rest-json` | 259 | ✅ path matching + JSON envelope |
| `rest-xml` | 4 | ✅ path matching + XML envelope (s3, cloudfront, route53, s3control) |
| `smithy-rpc-v2-cbor` | 18 | ✅ CBOR error envelope + `/service/{tp}/operation/{op}` detection — *but note*: migrated clients (e.g. boto3 CloudWatch) actually speak **query-compatible JSON** (`x-amzn-query-mode`), which is handled via wire-protocol detection, not the model's declared protocol |

Service detection: SigV4 scope covers everything signed. Operation detection:
json/query/ec2 resolve by name; REST resolves by method+path with
query-marker + required-header disambiguation.

## Detection (`detection/`)

- ~~rpc-v2-cbor support~~ ✅ + ~~scope aliases~~ ✅ (50 derived empirically
  from resolved boto3 signing names + 7 curated ambiguous picks +
  `_TARGET_PREFIXES` for exact service resolution)
- Wire-protocol detection ✅ — the observed request protocol wins over the
  model's declared protocol (query-compat JSON for migrated services).
  Remaining: true CBOR clients (non-botocore SDKs) still get CBOR errors.
- ~~Presigned URL detection~~ ✅ — `X-Amz-Credential` in the query string
  resolves service/region/key like the Authorization header.
- ~~SigV4A~~ ✅ — same credential shape parses; region shows as `*`.
- ~~Body-based REST disambiguation~~ ✅ — required/declared body members
  score candidates: JSON top-level keys, rest-xml root element vs
  structure-payload wire name (resolves the S3 `PutBucketX` family even
  without query markers), raw bodies favor payload ops. Genuinely
  ambiguous empty-body GETs (e.g. chime `DescribeChannel*` variants)
  still resolve to first stable match — no modeled signal exists.
- Host-prefix / virtual-hosted style detection (S3 `bucket.s3…`,
  `queue.amazonaws.com` style hosts) for upstreams that route on Host.

## Protocols (`protocols/`)

- ~~`rpc-v2-cbor` serializer~~ ✅ — flat-map CBOR encoder +
  `smithy-protocol`/`x-amzn-requestid` headers.
- ~~Query-compat error header~~ ✅ — `x-amzn-query-error: Code;Sender` sent
  when the request carries `x-amzn-query-mode` (matches real AWS behavior).
- `aws-json-1.1` variant detection + envelope differences. `[good-first-issue]`
- `__type` namespacing: emit `prefix#Code` vs bare `Code` where the service
  expects it. `[size:S]`
- Protocol-specific fields in error bodies (S3 `Resource`, `HostId`;
  DynamoDB `ItemCollectionMetrics` style extras). `[size:M]`

## Effects (`effects/`)

- ~~Corrupt body~~ ✅ — `response: {corrupt_bytes: N}` flips N bytes in the
  buffered body (200 OK, same length, wrong payload).
- ~~Truncated response~~ ✅ — `response: {truncate_frac|truncate_bytes}` —
  Content-Length stripped, valid envelope, body cut short.
- ~~Mid-stream abort~~ ✅ — `response: {abort_frac|abort_bytes}` — partial
  body, then the connection dies (client sees incomplete read).
- ~~Bandwidth shaping~~ ✅ — `response: {bandwidth_kbps}` paces the body
  stream at KiB/s.
- ~~Latency distributions (gaussian, spike)~~ ✅ — `latency: {dist: gaussian,
  mean, stddev, min?, max?}` and `{dist: spike, min, max, spike_ms, spike_p}`;
  uniform stays the default.

## Rules (`rules.py`)

- ~~Body matchers~~ ✅ — `body: <jmespath>` evaluated on JSON/form-encoded
  bodies (truthy = match), compiled+validated at rule load.
- ~~Rate-based rules~~ ✅ — `rate: {count, window_s}` rolling-window cap.
- ~~Sequences~~ ✅ — `sequence: {fail, pass}` repeating pattern per rule.
- ~~Header matchers~~ ✅ — `headers: {name: substring}` (AND'd, `""` =
  presence check, same semantics as `resource`).
- ~~Nested body matchers for XML payloads~~ ✅ — rest-xml bodies (S3
  Tagging/ACL/Lifecycle) parse to dicts for jmespath.
- ~~Rule TTL/expiration~~ ✅ — `ttl_s`, with `ttl_remaining_s` surfaced in
  GET /rules.
- ~~Per-resource deterministic flakiness~~ ✅ — `deterministic: true` hashes
  the request identity; same resource always lands on the same side of p,
  and failure tiers nest monotonically.

## Control plane (`control.py`)

- ~~Live fired-event stream~~ ✅ — `GET /_microburst/fired/stream` SSE,
  bounded per-consumer queues, keepalives.
- ~~Fired log filters~~ ✅ — `?service=&operation=&rule_id=` on GET /fired
  (time-range still open).
- ~~`/metrics` Prometheus endpoint~~ ✅ — requests/faults by (service,
  operation, action) + rules gauge. Counters survive fired-log deque
  eviction.
- ~~OTel span emission per injected fault~~ ✅ — `microburst.fault` spans
  via optional `opentelemetry-api` (`pip install microburst[otel]`);
  no-op when absent.

## Data plane (`forward.py`)

- ~~HTTP/2 upstream support~~ ✅ — `--http2` swaps the upstream client to
  httpx with HTTP/2 (negotiates via ALPN on https upstreams; cleartext
  stays h1 — httpx doesn't do h2c). Downstream stays HTTP/1.1 (boto3
  doesn't speak h2 anyway).
- ~~Response-side faults~~ ✅ — `response:` block (truncate/abort/corrupt/
  bandwidth) mutates the upstream response while streaming.
- Keep-alive / connection pooling fidelity checks against real AWS.

## Fidelity & verification

- **Fidelity harness** — ~~headless phase done~~ ✅: `test_fidelity.py`
  renders every service's error and parses it with botocore's own
  protocol parser — `Error.Code` round-trips for all 436 services
  (437 checks). Open: live diff against real AWS — run the same call
  against AWS + microburst, diff envelopes, publish a per-service
  fidelity report.
  The strongest moat: evidence-grade correctness claims. `[size:L]`
- ~~Error-shape fuzzing~~ ✅ — `test_every_modeled_error_roundtrips`
  iterates every unique modeled error wire code per service (~90k shapes
  deduped), asserts `Error.Code` round-trips through botocore's parser AND
  that status matches the modeled `httpStatusCode`.
- Multi-SDK matrix — boto3, aws-sdk-js-v3, aws-sdk-java, aws-sdk-go v2.
  Same wire format, different header quirks. `[size:M]`
- REST collision sweep — run every ambiguous method+path pair through the
  matcher (S3's 9 routes, chime families) and snapshot expected ops. `[size:S]`

## Ecosystem

- ~~Docker image~~ ✅ — multi-stage `Dockerfile` (slim, wheel build),
  published to GHCR on release by `docker.yml`. ~~Compose example~~ ✅ —
  `examples/docker-compose.yml` wires microburst + MiniStack.
- ~~GitHub Action~~ ✅ — `action.yml` composite action: `uses:
  pingedbrain/microburst@vX` runs the container, waits for health, sets
  `AWS_ENDPOINT_URL`. Optional `config:` mounts a rules file.
- MiniStack-native integration — `/_ministack/chaos`-compatible API so the
  same faults work without a separate proxy hop.
- ~~TUI dashboard~~ ✅ — `microburst dashboard [--connect URL]` (rich,
  `microburst[tui]` extra): rules table + live fired stream via SSE.
  `[size:L]`

## Explicitly out of scope (for now)

- TCP-level chaos (Toxiproxy does it — we're the layer above).
- Infrastructure faults (kill instances, network partitions — that's FIS).
- Anything that requires MITM/TLS interception.
