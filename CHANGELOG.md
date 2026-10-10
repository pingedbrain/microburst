# Changelog

All notable changes to this project will be documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[SemVer](https://semver.org/).

## [Unreleased]

### Added

- **gRPC wire mode** — `microburst --protocol grpc --port 15051
  --upstream localhost:50051` runs an h2c (cleartext HTTP/2) proxy for
  gRPC endpoints, sharing the rules engine, fired log, control API
  (`--control-port`, default 9999), metrics and stats. New package
  `src/microburst/grpc/` (thin helpers over `h2.connection.H2Connection`
  — two state machines bridging client and upstream streams — plus an
  asyncio connection handler); `h2>=4` is now a direct dependency.
  Rule mapping: `service: grpc`, `operation:` = the lowercased full
  `:path` (`/pkg.Svc/Method` → `pkg.svc/method`), `resource:`/`args:`/
  `payload:` = the raw `:path`. `error: {code, message}` renders the
  trailers-only response (HTTP 200 + `grpc-status` + `grpc-message` +
  END_STREAM — the real early-error shape; codes accept names or
  numbers, `error.status` is ignored with a note). New rule key
  `partial_messages: N` relays N real upstream messages then lands the
  configured trailers (mid-stream error) or RST_STREAM when no `error:`
  is set. Effects: `latency`, `timeout`/`timeout_ms` (`timeout: true`
  parks the call — inbound DATA stays unacknowledged for real
  flow-control backpressure — and the client's own deadline fires),
  `reset` (RST_STREAM INTERNAL_ERROR, per-RPC abort),
  `cut_reply`/`cut_upload` `{after_bytes|after_messages}` (TCP-level
  mid-stream death). Client RST propagates upstream; upstream
  RST/GOAWAY propagates downstream. h2c only — no TLS (`grpcs://`
  rejected); non-gRPC h2 streams proxy with `error:` skipped
  (`skipped: non-grpc content-type`). New example: `examples/14-grpc`.
- **MySQL wire mode** — `microburst --protocol mysql --port 13306
  --upstream localhost:3306` runs a TCP proxy speaking the MySQL
  client/server protocol (MySQL and MariaDB upstreams), sharing the
  rules engine, fired log, control API (own listener,
  `--control-port`, default 9999), metrics and stats. New package
  `src/microburst/mysql/` (3-byte-LE + sequence-id packet codec with
  multi-packet reassembly, ERR_Packet renderer, command/SQL detector
  reusing pg's verb lexer, asyncio connection handler). Rule mapping:
  `service: mysql`, `operation:` = SQL verb for `COM_QUERY` /
  `stmt_*` for prepared-statement commands / `com_*` for the rest /
  `startup` for the connect handshake, `resource:` = table-ish token
  (database name for `com_init_db`), `sql:` = case-insensitive regex
  on query text — applies to `COM_STMT_PREPARE` text and to
  `COM_STMT_EXECUTE` via per-connection statement-id→SQL tracking.
  `error:` renders a real ERR_Packet (`0xFF errno '#' sqlstate
  message`): new `error.errno` key fills the SQLSTATE from a curated
  map, `error.code` is read as the SQLSTATE and fills the errno;
  defaults are `1105`/`HY000` (`ER_UNKNOWN_ERROR`). `severity: FATAL`
  closes after the ERR — the only error class safe inside a
  transaction; non-fatal errors inject only while idle
  (`SERVER_STATUS_IN_TRANS` tracked from relayed status flags), else
  the rule skips with `skipped: in-transaction`. Startup faults send
  the ERR as the very first packet instead of a greeting (the real
  1040/1129 refusal shape — upstream untouched). Effects: `latency`,
  `timeout`/`timeout_ms`, `reset`, `partial_rows` (N real row packets
  then TCP-abort), `cut_reply: {after_bytes|after_messages}`. TLS
  refused (`CLIENT_SSL`/`CLIENT_COMPRESS` stripped from the relayed
  greeting; an unsolicited SSLRequest gets a real `1043 Bad
  handshake` ERR + close); auth fully passthrough including
  auth-switch/more-data rounds; LOCAL INFILE sub-dialog relayed;
  multi-statement replies relay all sub-resultsets
  (`SERVER_MORE_RESULTS_EXISTS`-aware); `COM_STMT_FETCH` and the
  replication/binlog command family splice to passthrough. New
  example: `examples/13-mysql`.
- **Redis wire mode** — `microburst --protocol redis --port 16379
  --upstream localhost:6379` runs a TCP proxy speaking RESP (RESP2 +
  RESP3 framing), sharing the rules engine, fired log, control API
  (own listener, `--control-port`, default 9999), metrics and stats.
  New package `src/microburst/redis/` (command/reply frame codec,
  `-CODE` error renderer, command verb/key detector, asyncio connection
  handler). Rule mapping: `service: redis`, `operation:` = command verb,
  `resource:` = best-effort first key, `args:` = case-insensitive regex
  on decoded command text, `error: {code, message}` where `code`
  carries the whole post-`-` line (cluster redirects keep their
  `slot host:port` tail — or compose it via `error: {code: MOVED,
  fields: {slot, target}}`), `cut_reply: {after_bytes: N}` (N bytes of
  the real reply, then TCP-abort mid-frame), `timeout: true`.
  Fidelity semantics: `error:` is never injected inside MULTI — the
  rule skips and the fired note says `skipped: in-multi`; latency,
  reset and timeout still apply mid-transaction. Pub/sub and MONITOR
  are passthrough after subscription (push-mode socket splice); RESP3
  `>` push and `|` attribute frames relay mid-reply without counting
  as the command's reply. New example: `examples/11-redis`.
- **Transport registry** — `src/microburst/transports.py` formalizes
  the sibling-transport seam: a `TRANSPORTS` map (name → `run_*` path,
  default listen/upstream ports, accepted upstream URL schemes, extra
  option keys) that `cli.py` dispatches `--protocol` through. The
  per-protocol if/elif collapsed into one shared TCP-mode branch —
  `http|postgres|redis|tcp` all resolve via the same lookup, and a new
  wire transport is a registry entry + a sibling package implementing
  the documented `run_<name>(host, port, upstream_host, upstream_port,
  control_port, rules, watch_config=None, **options)` contract (see
  AGENTS.md).
- **Generic TCP mode** — `microburst --protocol tcp --upstream
  host:port [--port N] [--framing spec]` runs a byte-stream proxy for
  protocols with no dedicated module (MySQL/Kafka/Cassandra/Mongo
  wire), sharing the rules engine, fired log, control API and stats.
  Optional user-declared framing — `length-prefix` (`size`, `offset`,
  `endian`, `includes_self`, `adjust`), `delimiter` (`bytes`), `fixed`
  (`size`) — via `--framing "kind:k=v,..."` or a config `framing:`
  mapping; default is unframed. New package `src/microburst/tcp/`
  (duplex frame/chunk pumps, per-unit decisions, armed stream faults).
  Rule mapping: `service: tcp`, `operation:` = `c2s:frame`/`s2c:frame`/
  `conn`, `payload:` = case-insensitive latin-1 regex on unit bytes
  (also works in pg/redis via sql/args fallback). Effects: `latency`,
  `timeout`/`timeout_ms`, `reset`, `cut_upload`/`cut_reply`
  `{after_bytes|after_messages}`, `corrupt {at_bytes, bit: flip}`
  (absolute c2s stream offset), `respond {data|hex|base64, then:
  forward|close|hold}` (synthetic client reply, unit never forwarded).
  Honest scope: `error:` has no renderer in tcp mode — the rule fires
  with `note: error has no renderer in tcp mode` and forwards; no TLS;
  malformed framing latches the parser off and falls back to verbatim
  passthrough (noted in `/fired`). New example: `examples/12-tcp`.

## [0.9.0] - 2026-10-09

### Added

- **PostgreSQL wire mode** — `microburst --protocol postgres --port
  15432 --upstream localhost:5432` runs a TCP proxy speaking the PG v3
  wire protocol, sharing the rules engine, fired log, control API
  (own listener, `--control-port`, default 9999), metrics and stats.
  New package `src/microburst/pg/` (frame codec, ErrorResponse renderer,
  SQL verb detector, asyncio connection handler). Rule mapping:
  `service: postgres`, `operation:` = SQL verb or `startup`, `sql:` =
  case-insensitive regex on query text, `error: {sqlstate, severity,
  message}`, `partial_rows: N` (N real DataRows then TCP-abort),
  `timeout: true` (hang until client disconnect). Fidelity semantics:
  `FATAL`/`PANIC` severity sends the ErrorResponse then closes like real
  PG; plain `ERROR` is injected only while the session is idle — inside
  a transaction the rule skips and the fired note says
  `skipped: in-transaction`. Auth is passthrough (SCRAM intact),
  `SSLRequest`/`GSSENCRequest` are refused `N`, extended-protocol
  batches (`Parse…Sync`, named statements included) are decided at batch
  granularity. New example: `examples/10-postgres`.
- **Message-boundary request faults** — `request:` learns two framed-body
  options. `cut_upload.after_messages: N` parses framed uploads
  (`application/vnd.amazon.eventstream` preludes, `application/grpc*`
  length prefixes) and resets the client connection after the Nth
  complete message — the SDK sees the link die *between* frames.
  `corrupt_upload.at_message: N` forwards N-1 messages verbatim, then
  poisons message N (flipped eventstream message-CRC byte;
  nonsense-huge gRPC length prefix) on the upstream send path — the
  upstream's parser rejects it and its error reaches the still-connected
  client. Non-framed Content-Types are a no-op; malformed framing stops
  message counting and falls back to byte thresholds. Outcomes surface
  on the fired event's new `note` field (frame counts, fallback
  reasons). New example: `examples/09-eventstream-cut`.
- **`GET /_microburst/stats`** — aggregates that `/metrics` can't express:
  uptime, request totals (`total` / `faulted` / `forwarded` — a
  latency-then-forward fault lands in both buckets), upstream latency
  percentiles over a bounded reservoir (last 2048 relays, measured
  send → response-headers), per-rule hit counts that survive rule
  deletion, and a per-service request/fault split.
- **Happy-path fidelity captures** — `SUCCESS_PROBES` (25 read-only
  list/describe ops across all six wire families) capture real 2xx
  responses tagged `"kind": "success"`. `conform`/`diff` compare them on
  status + Content-Type + envelope shape; `report` skips them
  (microburst forwards success bodies, never renders). 25 real-AWS
  success goldens committed, content-scrubbed with structure preserved.
- **Fidelity regression gate** — `test_fidelity_gate.py` runs
  `check_capture` over every committed error capture (status, parsed
  `Error.Code`, Content-Type, envelope shape) inside pytest — serializer
  drift from a golden fails CI automatically.
- **Emulator conformance CI** — weekly `emulator-conformance` workflow
  captures the probe set against LocalStack (pinned 4.14.0 — `latest`
  now requires an auth token) and publishes the `conform` score as a
  step summary + artifact. Informational, never a merge gate.

## [0.8.0] - 2026-10-09

### Added

- **Measured service latency presets** — `latency: {preset: <service>}`
  resolves a per-service baseline from real AWS probing (read-only ops,
  us-east-1, 2026-02, n=8): each preset encodes the service-side residual
  (p50 minus the ~155ms host→region network floor) as a gaussian `mean`;
  `stddev = max(5, 0.35·mean)` is a labeled inference, not a measurement.
  25 presets (`dynamodb`, `s3`, `sqs`, `sns`, `lambda`, `kinesis`, `iam`,
  `ec2`, `cloudformation`, `ssm`, `secretsmanager`, `sts`, `logs`,
  `firehose`, `events`, `stepfunctions`, `kms`, `athena`, `route53`,
  `cloudfront`, `glacier`, `wafv2`, `elbv2`, `apigateway`, `pinpoint`);
  explicit keys in the same block override preset values. New tool:
  `tools/latency_probe.py` re-measures the data against a real account.
- **Request-side faults** — `request:` rule block faults the
  client→proxy upload: `slow_upload: {rate_kbps}` paces the proxy's read
  of the request body (backpressures the SDK's write path on streaming
  uploads; on bodies pre-buffered for detection the pacing shifts to the
  upstream send), and `cut_upload: {after_bytes|after_frac}` resets the
  client connection after N consumed bytes without forwarding — a true
  mid-upload ECONNRESET on streaming uploads, a read-path reset on
  buffered ones. New example: `examples/08-upload-cut`.
- **Fired-log time range** — `GET /_microburst/fired` accepts `?since=` /
  `?until=` (epoch seconds or ISO-8601) alongside the field filters.
- **`fidelity capture --region`** — probe any AWS region; probe kwargs
  embed region-bearing ARNs, which rebind to the target region. The goldens
  were re-verified identical across all 17 enabled regions of a real
  account — only service availability differs, never the wire shape.
- **`error.fields`** — extra error-shape members on injected errors,
  rendered per protocol (json members, `<Error>` children on query/ec2,
  flat elements on rest-xml, CBOR map entries). S3-family errors now also
  carry the wire-standard `Resource`, `RequestId`, and `HostId` (matching
  the `x-amz-id-2` header).
- **Envelope-shape conformance** — `fidelity report` and `fidelity conform`
  now diff a structural body signature in addition to status/code/CT:
  namespace-qualified XML element paths + `<?xml` presence, and JSON
  top-level keys + `__type` namespace prefix. Catches wire-shape drift
  botocore's parser tolerates (wrong envelope root, missing `xmlns`,
  `RequestID` vs `RequestId`).
- **`fidelity diff`** — compare any two capture sets directly
  (`microburst fidelity diff A B`), same fields as `conform` with neutral
  labels; writes `DIFF.md` into B.
- **SigV4 fault presets** — `expired-token` (400), `clock-skew`
  (`RequestTimeTooSkewed` 403), `bad-signature` (`SignatureDoesNotMatch`
  403): auth-layer failures as one-word presets.
- **Response header mutation** — `response: {set_headers:,
  strip_headers:}` rewrites or drops response headers (wrong
  Content-Type on a 200, stripped `x-amz-*`) — exercises SDK parse paths
  body corruption can't reach.
- **Scheduled rule windows** — `active_at`/`until` (epoch, ISO-8601, or
  YAML datetime) bound when a rule starts and stops matching;
  `starts_in_s`/`ends_in_s` appear in `GET /rules`.
- **Rules hot-reload** — `--watch` reloads the `--config` file's rules
  on every save; a broken file keeps the previous ruleset.
- **Athena semantic error fields** — `InvalidRequestException` now
  carries `AthenaErrorCode`+`ErrorCode` (default `INVALID_INPUT`,
  overridable via `error.fields`) — real AWS probing showed the fields
  are always present on that exception. Fidelity report is now **28/28**.

### Fixed

- **ec2 error envelope** — ec2-protocol errors now render the real AWS
  shape (`<?xml version="1.0" encoding="UTF-8"?>` +
  `<Response><Errors><Error>` with `<RequestID>` and no `<Type>`, at
  `text/xml;charset=UTF-8`) instead of the query `ErrorResponse` shape
  (real AWS capture).
- **query `xmlns` + member order** — query-protocol errors carry the
  model's `xmlNamespace` on `ErrorResponse` and AWS's pretty-printed
  Type, Code, Message member order (verified on cfn/iam/rds/elbv2
  captures).
- **Coral-layer `Message` casing** — front-layer auth errors emit capital
  `Message` (sfn capture), not the service-layer `message`.
- **rest-json RequestID members** — error-shape members named
  `RequestID`/`RequestIdentifier` are filled from the request id
  (pinpoint capture). Athena's unmodeled `ErrorCode`/`AthenaErrorCode`
  taxonomy remains a documented divergence.
- **Coral front-layer `__type` prefix** — auth-layer codes
  (`ExpiredTokenException`, `UnrecognizedClientException`,
  `InvalidClientTokenId`, `AccessDeniedException`,
  `MissingAuthenticationTokenException`, `InvalidSignatureException`) now
  render `com.amazon.coral.service#Code` on json services, matching the
  real-AWS capture, instead of the service namespace.
- **Observed `x-amz-json-1.x` version wins** — a request whose Content-Type
  pins a json protocol version gets that version echoed back, not the
  model's `jsonVersion` (same rule as observed `ctx.protocol`).
- **Timeout envelope matches the wire protocol** — `timeout_ms` responses
  now honor the observed protocol, query-compat flag, and request
  Content-Type instead of always emitting the default json envelope.
- **Route53/CloudFront error envelope** — text-xml rest-xml services now
  emit `<?xml?><ErrorResponse xmlns="…"><Error><Type>Sender</Type>…
  <RequestId/></ErrorResponse>` (verified against the route53 capture)
  instead of S3's flat `<Error>`.

## [0.7.0] - 2026-10-08

### Added

- **Runnable failure-scenario examples** — `examples/` now holds seven
  self-contained scenarios (throttled writes, timeout-vs-retry-budget,
  poison queue, S3 SlowDown uploads, mid-stream event cuts, expired
  token, generic HTTP dependency), each with a config + app script, and
  a test that keeps every shipped config parseable.
- **Emulator conformance** — `microburst fidelity capture --endpoint-url
  <emulator>` runs the same probes against any AWS-compatible endpoint,
  and `microburst fidelity conform --emu DIR --aws DIR` diffs them
  against the committed real-AWS goldens on the fields SDKs read:
  status, parsed `Error.Code`, Content-Type. Emulators can gate on
  byte-level AWS similarity with zero AWS credentials — first run
  against MiniStack found 21 divergences (content-type drift, codes
  that don't parse, missing ops).

## [0.6.0] - 2026-10-08

### Added

- **Credential-free fidelity freshness** — `microburst fidelity snapshot`
  digests the fidelity-relevant model surface (protocol metadata,
  routes, error codes/`httpStatusCode`, error members) for all ~436
  services into `fidelity/models_snapshot.json`; a weekly `model-drift`
  workflow regenerates it against the latest botocore and opens an
  issue on drift — the signal to re-capture. No AWS creds in CI.
- **AWS-authored protocol fixtures** — `tools/fetch_protocol_fixtures.py`
  vendors botocore's conformance fixtures (error wire responses per
  protocol) into `fidelity/protocol/`; a test parses our rendered
  errors with each fixture's own model and asserts the expected
  `Error.Code`/members.
- **Capture provenance** — every capture now records AWS request ids,
  region, and botocore version so reviewers can verify real wire
  provenance (account ids still redacted). `fidelity
  backfill-provenance` upgrades old captures in place.

## [0.5.1] - 2026-10-08

### Added

- **Event-stream frame surgery** — `response.event_frames` takes a list
  of mutations applied at upstream frame indices: `drop`, `inject`
  (custom `:event-type` + payload), `payload`/`payload_b64` (replace the
  payload, CRCs recomputed so SDKs parse it as real), `corrupt_payload`,
  `bad_crc` (broken message CRC → SDK checksum error), `cut` (emit a
  fraction of the frame, then EOF), and `error` (terminal `:error`
  frame — the same behavior `event_error` provides as shorthand).
- **Virtual-hosted detection beyond S3** — host parsing now covers
  `appsync-api` invoke endpoints, Lambda Function URLs (`*.on.aws`),
  classic `data.iot` and `{endpoint}-ats.iot` (IoT data plane, not the
  control plane), S3 Express directory buckets (`s3express-*` labels;
  they sign with the unmodeled `s3express` scope), and `*.api.aws`
  service endpoints. `apigatewaymanagementapi` requests strip the
  deployment-stage path segment before route matching — real
  `execute-api` endpoints embed it (`/{stage}/@connections/{id}`).

## [0.5.0] - 2026-10-08

### Added

- **Cassette record/replay** — `--record DIR` captures every upstream
  response keyed by `sha256(method + path + query + body)`;
  `--replay DIR` serves them without contacting upstream while rules keep
  injecting faults on top. Deterministic real traffic, chaos on top.
- **Event-stream faults** — `response.event_error` splices a well-formed
  `:error` frame (valid CRCs, `:error-code`/`:error-message` headers)
  into `application/vnd.amazon.eventstream` responses after N frames —
  mid-stream errors for Kinesis SubscribeToShard, S3 Select, etc.
- **`microburst fidelity` subcommand** — the live-AWS diff harness now
  ships in the wheel: `fidelity capture` (needs boto3 + AWS credentials)
  and `fidelity report`, writing to `--dir` (default `./fidelity`).
- **SDK matrix runs in CI** — a `sdk-matrix` job exercises boto3,
  aws-sdk-js-v3, aws-sdk-go-v2, and aws-sdk-java-v2 against a live
  microburst on every push.

- **Multi-SDK matrix** — `tools/sdk-matrix/` runs real boto3,
  aws-sdk-js-v3, aws-sdk-go-v2, and aws-sdk-java-v2 clients against a
  live microburst and verifies the *parsed* error code, HTTP status,
  and retry attempts (measured client-side and via the fired log —
  both must agree). 16/16 cells pass in CI.
- **Expanded live-AWS probes** — 28 wire captures (was 13), adding
  Kinesis, StepFunctions, Cognito, Athena, Route53Resolver, WAFv2,
  CloudWatch (query-compat JSON on the wire), EventBridge, Glacier, SESv2,
  Pinpoint, AppSync, ELBv2, RDS, CloudFormation. The diff now also checks
  Content-Type.

### Fixed

- REST matching: `{Label+}` greedy routes no longer match an empty
  segment — JS S3's `HEAD /bucket/` was detected as `HeadObject` instead
  of `HeadBucket` (found by the SDK matrix). Bucket-level trailing slashes
  fall back to the bucket route; keys that genuinely end in `/` still
  match.
- `__type` prefix for CloudWatch (`com.amazonaws.cloudwatch.v2010_08_01#`),
  rest-json body-code flavor for Glacier (`{"code","message","type"}`,
  no `x-amzn-ErrorType`), and `x-amz-json-1.1` Content-Type for SESv2 —
  all verified against live captures.

## [0.4.0] - 2026-10-08

Live-AWS-verified fidelity. Everything in this release was corrected
against real AWS wire captures (`fidelity/`), not model inference.

### Added

- **Live-AWS fidelity diff** — `tools/live_fidelity.py` captures raw error
  responses from real AWS (botocore transport wrap — real TLS bytes, no
  proxy) via read-only probes against nonexistent resources, then diffs
  status / parsed `Error.Code` / Content-Type / request-id placement
  against `render_error`. `fidelity/REPORT.md` commits the result:
  13/13 probes match. Account IDs are redacted from captures.
- **REST collision sweep** — `detection/sweep.py` synthesizes the minimal
  request each of ~10.4k REST ops declares and asserts the matcher
  resolves it back; `fidelity/rest_sweep.json` is the committed snapshot
  (`tools/rest_sweep.py` regenerates). 5 ops remain unresolvable — all
  genuine AWS aliases with identical literal routes.
- **Host / virtual-hosted detection** — `bucket.s3.…` and emulator
  `bucket.s3.localhost…`/`bucket.localhost` addressing: the label is
  prepended for route matching and becomes the resource hint;
  `{accountId}.s3-control…` resolves s3control despite its `s3` signing
  scope; host fills service/region on unsigned requests for AWS-shaped
  domains only.
- **Keep-alive fidelity tests** — pooled clients reuse one upstream
  connection through the proxy; injected errors keep the downstream
  socket alive; `reset` still hard-kills.

### Fixed (all verified against live AWS captures)

- json/cbor services serve unmodeled client errors at **400** per the
  awsJson spec — name-guessing had produced 404s.
- `Content-Type` honors the model's `jsonVersion` (`x-amz-json-1.1` for
  KMS, Logs, SecretsManager).
- `__type` is namespaced where AWS namespaces it
  (`com.amazonaws.dynamodb.v20120810#`, `com.amazonaws.sqs#`).
- Error message member follows the shape (`message` vs `Message`).
- rest-json carries the code in `x-amzn-ErrorType`; the body holds the
  error shape's members (`{"Type":"User","Message":…}`).
- Query-compat services emit `x-amzn-query-error: AWS.<ns>.<code>;Sender`
  automatically when the model declares `awsQueryCompatible`
  (`AWS.SimpleQueueService` verified live).
- Route53 serves `text/xml` + `x-amzn-RequestId` (not the S3-style pair).
- HEAD requests return empty error bodies (HTTP semantics — S3 404s).
- Numeric error codes (`"404"`) resolve to that HTTP status.
- REST matcher: query-marker *values* discriminate (`?operation=create`
  vs `suspend`), required querystring members score (S3 `partNumber`/
  `uploadId`), route specificity breaks ties when a greedy `{Label+}`
  swallows literal sibling segments.

## [0.3.0] - 2026-10-08

Dashboard, Docker, and HTTP/2.

### Added

- **`microburst dashboard`** — live TUI (rich, `microburst[tui]` extra):
  rules table + fired-event stream over SSE, reconnecting reader.
- **Docker image** — multi-stage slim `Dockerfile`; `docker.yml` publishes
  `ghcr.io/pingedbrain/microburst:{version,latest}` on release.
- **`--http2`** — upstream transport swaps to httpx with HTTP/2
  (`microburst[h2]` extra; ALPN on https upstreams, cleartext stays h1).

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

[0.3.0]: https://github.com/pingedbrain/microburst/releases/tag/v0.3.0
[0.2.0]: https://github.com/pingedbrain/microburst/releases/tag/v0.2.0
[0.1.0]: https://github.com/pingedbrain/microburst/releases/tag/v0.1.0
