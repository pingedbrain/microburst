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
- ~~Host-prefix / virtual-hosted detection~~ ✅ — `detection/host.py`
  parses the Host header: `bucket.s3.…` (and emulator
  `bucket.s3.localhost…`/`bucket.localhost`) prepend the label so REST
  matching sees `/{Bucket}/{Key}`; `{accountId}.s3-control…` resolves the
  s3control service (it signs as `s3`); host fills service/region for
  unsigned requests on AWS-shaped or emulator-shaped domains only —
  arbitrary `api.logs.example.com` hosts stay undetected.

## Protocols (`protocols/`)

- ~~`rpc-v2-cbor` serializer~~ ✅ — flat-map CBOR encoder +
  `smithy-protocol`/`x-amzn-requestid` headers.
- ~~Query-compat error header~~ ✅ — `x-amzn-query-error: Code;Sender` sent
  when the request carries `x-amzn-query-mode` (matches real AWS behavior).
- ~~`aws-json-1.x` variant detection~~ ✅ — the request's observed
  `x-amz-json-1.x` Content-Type wins over the model's `jsonVersion`
  (`render_error(request_ct=...)`), same rule as `ctx.protocol`.
- ~~`__type` namespacing~~ ✅ — per-service `prefix#Code` vs bare `Code`
  verified against live captures (`com.amazonaws.dynamodb.v20120810#`,
  `com.amazonaws.sqs#`, `com.amazonaws.cloudwatch.v2010_08_01#`); front-layer
  auth codes (`ExpiredTokenException`, `UnrecognizedClientException`, …)
  get `com.amazon.coral.service#` — observed on the sfn capture. The map
  grows as captures cover more services.
- ~~Protocol-specific fields in error bodies~~ ✅ — `error.fields` merges
  arbitrary members per protocol (json/rest-json body, XML elements on
  query/ec2/rest-xml, CBOR map); S3-family errors auto-carry `Resource`,
  `RequestId`, and `HostId` matching `x-amz-id-2`; route53/cloudfront emit
  the `ErrorResponse`+xmlns envelope verified on the route53 capture.
- ~~ec2 envelope~~ ✅ — real AWS sends `<?xml?><Response><Errors><Error>` +
  `<RequestID>` (capital D, no `<Type>`) with `text/xml;charset=UTF-8`;
  we emitted the query `ErrorResponse` shape. Fixed + structural
  regression coverage. (real AWS capture)
- ~~query `xmlns`~~ ✅ — cfn/iam/rds/elbv2 captures carry
  `<ErrorResponse xmlns="…">` from the model's `xmlNamespace` with
  pretty-printed Type, Code, Message ordering. (real AWS capture)
- ~~Coral-layer `Message`~~ ✅ — front-layer auth errors carry capital
  `Message`, not service-layer `message` (sfn capture).
- ~~rest-json RequestID members~~ ✅ — error-shape members named
  `RequestID`/`RequestIdentifier` are filled from the request id
  (pinpoint capture).
- ~~athena error-code taxonomy~~ ✅ — resolved with real AWS probing
  (us-east-1, 9 ops): `InvalidRequestException` always carries semantic
  `AthenaErrorCode`+`ErrorCode` (`INVALID_INPUT`,
  `NAMED_QUERY_NOT_FOUND`, `QUERY_EXECUTION_NOT_FOUND`…); `MetadataException`
  carries neither. Default `INVALID_INPUT` emitted, overridable via
  `error.fields`.

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
- ~~SigV4 fault presets~~ ✅ — `expired-token` (400),
  `clock-skew` (`RequestTimeTooSkewed` 403), `bad-signature`
  (`SignatureDoesNotMatch` 403) — statuses pinned since rest protocols
  default to 503 while auth errors are 400/403 on the wire.
- ~~Response header mutation~~ ✅ — `response: {set_headers:,
  strip_headers:}` — wrong Content-Type on a 200, stripped `x-amz-*`
  headers: exercises SDK parse failure paths body corruption doesn't
  reach. Applied after built-in mutations so explicit intent wins.
- ~~Request-side faults~~ ✅ — `request: {slow_upload: {rate_kbps},
  cut_upload: {after_bytes|after_frac}}` faults the client→proxy
  upload: paced reads stall the SDK's write path; `cut_upload` resets
  the client connection after N consumed bytes without forwarding (a
  true mid-upload reset for streaming uploads — S3 PutObject/UploadPart;
  pre-buffered bodies cut on the read path instead).
- ~~Service latency presets~~ ✅ — `latency: {preset: dynamodb}` resolves
  a measured per-service baseline (real AWS us-east-1 probing, 2026-02:
  service-side residual = p50 − ~155ms network floor, gaussian with
  inferred stddev). `tools/latency_probe.py` refreshes the data.
- ~~gRPC / event-stream request faults~~ ✅ — `cut_upload.after_messages`
  resets the client connection after N complete frames and
  `corrupt_upload.at_message` poisons message N's checksum/length for
  the upstream's parser. Covers `application/vnd.amazon.eventstream`
  and `application/grpc*` bodies; non-framed types no-op with a
  fired-event note, malformed framing falls back to byte thresholds.

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
- ~~Scheduled activation windows~~ ✅ — `active_at`/`until` (epoch,
  ISO-8601, or YAML datetime) bound when a rule starts/stops matching;
  `starts_in_s`/`ends_in_s` surfaced in `GET /rules`.

## Control plane (`control.py`)

- ~~Live fired-event stream~~ ✅ — `GET /_microburst/fired/stream` SSE,
  bounded per-consumer queues, keepalives.
- ~~Fired log filters~~ ✅ — `?service=&operation=&rule_id=` and time-range
  `?since=&until=` (epoch seconds or ISO-8601) on GET /fired.
- ~~`/metrics` Prometheus endpoint~~ ✅ — requests/faults by (service,
  operation, action) + rules gauge. Counters survive fired-log deque
  eviction.
- ~~OTel span emission per injected fault~~ ✅ — `microburst.fault` spans
  via optional `opentelemetry-api` (`pip install microburst[otel]`);
  no-op when absent.
- ~~Rules file hot-reload~~ ✅ — `--watch` polls the `--config` file's
  mtime and replaces the ruleset on change; a broken file keeps the
  previous rules (logged, not fatal).
- ~~Stats endpoint~~ ✅ — `GET /_microburst/stats`: the fired log says
  *what* fired; stats say how much each fault costs — per-rule hit counts
  (survive rule deletion), upstream time-to-headers percentiles over a
  bounded reservoir, per-service split, fault-vs-forwarded totals.
  `/metrics` counts; `/stats` explains.

## Data plane (`forward.py`)

- ~~HTTP/2 upstream support~~ ✅ — `--http2` swaps the upstream client to
  httpx with HTTP/2 (negotiates via ALPN on https upstreams; cleartext
  stays h1 — httpx doesn't do h2c). Downstream stays HTTP/1.1 (boto3
  doesn't speak h2 anyway).
- ~~Response-side faults~~ ✅ — `response:` block (truncate/abort/corrupt/
  bandwidth) mutates the upstream response while streaming.
- ~~Event-stream faults~~ ✅ — `response.event_error` splices a
  well-formed `:error` frame (valid CRCs) into
  `application/vnd.amazon.eventstream` bodies after N frames — mid-stream
  errors for Kinesis SubscribeToShard, S3 Select, etc.
- ~~Keep-alive / connection pooling fidelity~~ ✅ — `test_keepalive.py`:
  pooled clients reuse one upstream connection through the proxy, injected
  errors keep the downstream socket alive (no `Connection: close`), and
  `reset` still kills. Live captures show AWS itself varies per service
  (keep-alive on dynamodb/lambda, close on ec2/secretsmanager) — the
  proxy preserves rather than imposes semantics.

## Fidelity & verification

- **Fidelity harness** — ✅ both phases: `test_fidelity.py` renders every
  service's error and parses it with botocore's own protocol parser —
  `Error.Code` round-trips for all 436 services (~870 checks across the
  fuzz sweep). **Live-AWS diff** ✅: `tools/live_fidelity.py` captures raw
  wire responses from real AWS (`capture`) and diffs them against
  `render_error` (`report`) — `fidelity/REPORT.md` publishes the result
  (13/13 probes match status + parsed code + Content-Type). It already
  caught real divergences: json services serve unmodeled client errors at
  400 not the name-guessed 404, `x-amz-json-1.1` content types,
  `com.amazonaws.*` `__type` prefixes, rest-json `x-amzn-ErrorType`
  headers, SQS's `AWS.SimpleQueueService.*` query-compat namespace,
  Route53's `text/xml`, and empty-body HEAD errors. Extend `PROBES` and
  re-run `capture` to grow coverage.
- ~~Error-shape fuzzing~~ ✅ — `test_every_modeled_error_roundtrips`
  iterates every unique modeled error wire code per service (~90k shapes
  deduped), asserts `Error.Code` round-trips through botocore's parser AND
  that status matches the modeled `httpStatusCode`.
- ~~Multi-SDK matrix~~ ✅ — `tools/sdk-matrix/` runs boto3, aws-sdk-js-v3,
  aws-sdk-go-v2, and aws-sdk-java-v2 against a live microburst with
  `p=1.0` rules and verifies parsed error code, status, and retry
  attempts (measured both client-side and via the fired log). 16/16
  cells pass in CI — including the namespaced `AWS.SimpleQueueService.*`
  code all four SDKs read from `x-amzn-query-error`. It also found a
  real detection bug (JS S3 sends `HEAD /bucket/` — greedy `{Key+}` was
  swallowing the empty key segment).
- ~~REST collision sweep~~ ✅ — `detection/sweep.py` synthesizes the minimal
  request each of the ~10.4k REST ops declares and asserts the matcher
  resolves it back to itself; `fidelity/rest_sweep.json` is the committed
  snapshot, `test_rest_sweep.py` the regression guard. 10,403 ops swept;
  only 5 remain unresolvable — genuine AWS aliases (identical literal
  routes like `GetBucketLifecycle`/`GetBucketLifecycleConfiguration`, and
  `ListBuckets`/`ListDirectoryBuckets` which AWS separates by host). The
  sweep drove three real matcher fixes: query-marker *values* discriminate
  (`?operation=create` vs `suspend`), required querystring members score
  (S3 `partNumber`/`uploadId`), and route specificity breaks ties when a
  greedy `{Label+}` swallows literal sibling segments.
- ~~Structural envelope conformance~~ ✅ — `conform`/`report` now diff a
  body signature on top of status/code/CT: namespace-qualified XML
  element paths + `<?xml` presence for XML, top-level keys + `__type`
  namespace prefix for JSON. Immediately caught four real divergences
  (ec2 `Response/Errors` envelope, missing query `xmlns`, coral
  `Message` casing, pinpoint `RequestID`, athena `AthenaErrorCode`) —
  all fixed; report is 28/28.
- ~~`fidelity diff`~~ ✅ — `microburst fidelity diff A B` compares any two
  capture sets (same fields as conform, neutral labels, writes `DIFF.md`
  into B). Verified: us-west-2 vs ap-southeast-2 AWS captures are
  shape-identical 28/28.
- ~~SDK matrix expansion~~ ✅ — aws-sdk-rust (smithy `Intercept` attempt
  counting, `=` pins) and AWSSDK-v4 .NET (`DelegatingHandler` attempt
  counting) cells; 20/20 pass with zero serializer changes. SDK-specific
  HEAD-error parse documented (rust → `null`, .NET → `ServiceUnavailable`).
- ~~**Happy-path captures**~~ ✅ — `SUCCESS_PROBES` (25 read-only
  list/describe calls covering all six wire families) capture real 2xx
  responses tagged `"kind": "success"` (`__ok`-suffixed when the op name
  collides with an error probe). `conform`/`diff` compare them on
  status + Content-Type + envelope shape — success bodies carry no error
  code — and `report` skips them with a note. The gate parametrizes
  error captures only. Goldens will be refreshed into
  `fidelity/captures/` on the next real-AWS capture run.
- ~~**Fidelity regression gate in CI**~~ ✅ — `test_fidelity_gate.py`
  runs `check_capture` (the pure comparison extracted from `report`)
  over every committed capture and asserts status + parsed
  `Error.Code` + Content-Type + envelope shape all match; the count is
  derived from `fidelity/captures/` with a `>= 28` floor so deletions
  fail loudly. pytest coverage makes it automatic — `ci.yml` already
  runs the suite.
- ~~**Conformance CI vs other emulators**~~ ✅ — weekly
  `emulator-conformance` workflow captures the probe set against a
  LocalStack service container, `conform`s it against the committed AWS
  goldens, and publishes `CONFORM.md` + captures as a run artifact and
  step summary. Informational, not a merge gate; extending to more
  emulators is a matrix addition.

## Ecosystem

- ~~Docker image~~ ✅ — multi-stage `Dockerfile` (slim, wheel build),
  published to GHCR on release by `docker.yml`. ~~Compose example~~ ✅ —
  `examples/docker-compose.yml` wires microburst + MiniStack.
- ~~GitHub Action~~ ✅ — `action.yml` composite action: `uses:
  pingedbrain/microburst@vX` runs the container, waits for health, sets
  `AWS_ENDPOINT_URL`. Optional `config:` mounts a rules file.
- MiniStack-native integration — `/_ministack/chaos`-compatible API so the
  same faults work without a separate proxy hop.
- **Protocol-agnostic core + PostgreSQL wire** — issue #6. ~~Phase 0/1
  (MVP)~~ ✅: PG landed as a *sibling transport* (`src/microburst/pg/`),
  not a pipeline refactor — shared rule engine/fired log/control API,
  `--protocol postgres` TCP proxy with startup faults (`53300` FATAL),
  simple + extended-protocol query faults, tx-status-aware ERROR
  injection, `partial_rows` mid-ResultSet aborts, latency/reset/timeout.
  What remains: COPY sub-protocol interception, `25P02`-class
  in-transaction emulation (needs deeper upstream tx tracking), named
  prepared-statement lifecycle (Close/deallocate), replication
  (`walsender`) protocol, gRPC trailers via the same sibling-transport
  seam. `[size:L → M remaining]`
- ~~Redis wire mode~~ ✅ (MVP) — `src/microburst/redis/`, the third
  sibling transport: RESP2+RESP3 frame codec, `-CODE` error renderer
  (`MOVED`/`ASK` redirect tails compose from `error.fields.slot`/
  `target`), `operation:`=verb / `resource:`=first-key / `args:`=regex
  matching, `cut_reply: {after_bytes}` mid-reply aborts, MULTI-aware
  error skipping (`skipped: in-multi`), pub/sub+MONITOR push-mode
  passthrough. What remains: RESP3 `!` blob-error synthesis, real
  RESP2↔RESP3 translation, TLS (`rediss://`), subscribe-mode fault
  resumption (interleave faults into the push stream). `[size:M]`
- ~~MySQL wire mode~~ ✅ (MVP) — `src/microburst/mysql/`, a fourth
  dedicated sibling transport (`--protocol mysql`, default listen
  13306, upstream `mysql`/`mariadb` schemes): 3B-LE+seq packet codec
  with multi-packet reassembly, ERR_Packet renderer with
  errno↔SQLSTATE defaulting (`error.errno`/`error.code`,
  1105/`HY000` fallback), `operation:` = verb/`stmt_*`/`com_*`/
  `startup`, per-connection prepared-statement id→SQL tracking so
  `sql:` matches `COM_STMT_EXECUTE`, tx-aware non-fatal error
  skipping (`skipped: in-transaction`), FATAL close-after-ERR,
  `partial_rows` + `cut_reply`, startup ERR-as-first-packet refusals
  (the real 1040/1129 shape), auth passthrough with
  TLS/compression capability stripping, multi-statement relay
  (`SERVER_MORE_RESULTS_EXISTS`-aware). What remains: the
  post-handshake-response refusal shape (per-user limits, `1045`),
  `COM_STMT_FETCH` cursor interception and replication/binlog
  streams (currently spliced passthrough), TLS termination, LOAD
  DATA INFILE fault injection. `[size:M]`
- ~~TUI dashboard~~ ✅ — `microburst dashboard [--connect URL]` (rich,
  `microburst[tui]` extra): rules table + live fired stream via SSE.
  `[size:L]`
- ~~Generic TCP transport + registry~~ ✅ — `src/microburst/transports.py`
  formalizes the sibling-transport seam (`TRANSPORTS` registry: name →
  `run_*` path, default ports, upstream schemes, extra option keys;
  `cli.py` dispatches every `--protocol` through it) and
  `src/microburst/tcp/` is the protocol-blind fourth sibling: duplex
  frame/chunk pumps, `--framing`/`framing:` user-declared segmentation
  (length-prefix / delimiter / fixed), `service: tcp` +
  `payload:` byte-regex matching, `operation:` = `c2s:frame`/`s2c:frame`/
  `conn`, transport faults only (`latency`/`reset`/`timeout`/
  `cut_upload`/`cut_reply`/`corrupt`/`respond` — `error:` deliberately
  has no renderer). What remains: a `slow_upload`-style rate limiter on
  the stream, per-frame `corrupt.at_message`, TLS termination.
  `[size:M]`
- ~~Cassette record/replay~~ ✅ — `--record DIR` captures upstream
  responses keyed by method+path+body; `--replay DIR` serves them
  upstream-free while rules still inject faults.
- ~~`microburst fidelity` subcommand~~ ✅ — the live-AWS diff harness
  ships in the wheel (`capture`/`report`, `--dir`); `tools/live_fidelity.py`
  is a thin repo wrapper. Extended since: `capture --endpoint-url` +
  `conform` diff any AWS-compatible endpoint against the committed goldens
  (found 21/28 divergences on first run against a real emulator);
  `snapshot` + a weekly zero-credentials workflow watch botocore for model
  drift; captures carry AWS request-id provenance; `fidelity/protocol/`
  vendors AWS-authored protocol fixtures parsed back through botocore's
  own models.
- ~~Runnable examples~~ ✅ — `examples/` ships seven self-contained
  failure scenarios (throttled writes, timeout cascade, poison queue,
  S3 SlowDown, stream cuts, expired token, generic HTTP) with configs
  kept parseable by a test.
- ~~Scripted end-to-end demo~~ ✅ — `demo.py` (endpoints via
  `MINISTACK_URL`/`MICROBURST_URL`) + a real recorded run embedded in
  the README: 31 orders, 11 SDK-absorbed throttles, fired-log summary.

## Explicitly out of scope (for now)

- ~~TCP-level chaos~~ — partially landed: `--protocol tcp` covers
  transport faults for protocols without a dedicated module; what stays
  out is protocol *semantics* (per-operation errors still need a
  dedicated transport like pg/redis).
- Infrastructure faults (kill instances, network partitions — that's FIS).
- Anything that requires MITM/TLS interception.
