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
- **Body-based REST disambiguation** — measured: 14/263 REST services have
  method+path collisions (S3: 113 ops in 9 routes; chime*: ~60 ops; plus
  glacier/qbusiness/sso-oidc pairs). Query markers + required headers
  already resolve the S3 subresource family (`?acl`, `?tagging`…); what
  remains is body-driven (`TagResource` vs `UntagResource`, chime POST
  families). `[size:M]`
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

- Corrupt body — malformed JSON/XML that exercises SDK deserialization,
  not just error handling. `[good-first-issue]`
- Truncated response — valid envelope, body cut short. `[size:M]`
- Mid-stream abort — send headers + partial body, then die. `[size:M]`
- Bandwidth shaping — throttle streamed response bytes/sec. `[size:M]`
- Latency distributions (gaussian, spike) instead of uniform. `[size:S]`

## Rules (`rules.py`)

- Body matchers — jmespath/JSONPath expressions on the request payload.
  `[size:M]` (big contributor unlock)
- Header matchers. `[good-first-issue]`
- Rate-based rules — N faults per window, not just probability. `[size:M]`
- Sequences — fail N, pass M, repeat (a chaos script per rule). `[size:M]`
- Rule TTL/expiration. `[size:S]`
- ~~Per-resource deterministic flakiness~~ ✅ — `deterministic: true` hashes
  the request identity; same resource always lands on the same side of p,
  and failure tiers nest monotonically.

## Control plane (`control.py`)

- Live fired-event stream — SSE or WebSocket tail of `/_microburst/fired`.
  `[size:M]` (would make the demo and dashboards live)
- `/metrics` Prometheus endpoint. `[good-first-issue]`
- Fired log filters (service/operation/rule_id/time range). `[size:S]`
- OTel span emission per injected fault. `[size:L]`

## Data plane (`forward.py`)

- HTTP/2 upstream support.
- Response-side faults — mutate the *upstream* response (strip fields,
  inject latency mid-stream) rather than only replacing it. `[size:L]`
- Keep-alive / connection pooling fidelity checks against real AWS.

## Fidelity & verification

- **Fidelity harness** — run the same call against real AWS + microburst,
  diff the envelopes byte-for-byte, publish a per-service fidelity report.
  The strongest moat: evidence-grade correctness claims. `[size:L]`
- Error-shape fuzzing — iterate every modeled exception of every operation
  and assert each parses to the right code in the SDK. `[size:M]`
- Multi-SDK matrix — boto3, aws-sdk-js-v3, aws-sdk-java, aws-sdk-go v2.
  Same wire format, different header quirks. `[size:M]`
- REST collision sweep — run every ambiguous method+path pair through the
  matcher (S3's 9 routes, chime families) and snapshot expected ops. `[size:S]`

## Ecosystem

- Docker image + compose examples. `[good-first-issue]`
- GitHub Action for CI pipelines (service container + preset flag). `[size:M]`
- MiniStack-native integration — `/_ministack/chaos`-compatible API so the
  same faults work without a separate proxy hop.
- TUI dashboard — live rules + fired stream (leverages the SSE endpoint).
  `[size:L]`

## Explicitly out of scope (for now)

- TCP-level chaos (Toxiproxy does it — we're the layer above).
- Infrastructure faults (kill instances, network partitions — that's FIS).
- Anything that requires MITM/TLS interception.
