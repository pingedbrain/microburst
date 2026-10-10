<p align="center">
  <img src="assets/mascot.jpeg" alt="Nimbus, the microburst mascot" width="300">
</p>

<h1 align="center">microburst</h1>

<p align="center">
  <a href="https://pingedbrain.github.io/microburst/">site</a> ·
  <a href="https://pypi.org/project/microburst/">pypi</a> ·
  <a href="https://github.com/pingedbrain/microburst/releases">releases</a>
</p>

<p align="center">
  <strong>AWS failure injection that your SDK actually believes.</strong><br>
  Throttling, latency, timeouts and resets — in the exact wire format AWS uses,<br>
  so retry, backoff and circuit-breaker code gets exercised for real.
</p>

<p align="center">
  <a href="https://github.com/pingedbrain/microburst/actions/workflows/ci.yml"><img src="https://github.com/pingedbrain/microburst/actions/workflows/ci.yml/badge.svg" alt="ci"></a>
  <a href="https://pypi.org/project/microburst/"><img src="https://img.shields.io/pypi/v/microburst?v=1" alt="PyPI"></a>
  <img src="https://img.shields.io/pypi/pyversions/microburst?v=1" alt="Python">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License: MIT"></a>
</p>

---

## The problem

Your app handles `ThrottlingException` and retries with backoff — or so you
hope. The only way to know is to make AWS actually throttle you, and until
now your options were:

| Option | Catch |
|---|---|
| Generic proxies (Toxiproxy et al.) | Protocol-blind. They can drop bytes, but they can't return a `ProvisionedThroughputExceededException` in the envelope your SDK parses — so **they never exercise retry logic**. |
| Mocks | You test your `except` block, not the SDK. Backoff, jitter, retry budgets: all untested. |
| Managed chaos services | Operate at infrastructure level (kill instances), not API semantics — and not against your local emulator. |
| Chaos features inside emulators | You have to adopt *their* whole emulator, sometimes on a paid tier. |

**microburst is the missing piece:** a standalone, protocol-aware proxy that
works against *any* AWS-compatible endpoint — MiniStack, moto, or real AWS —
and returns failures the SDK can't tell apart from the real thing.

## Why "protocol-aware" matters

SDKs decide whether to retry by **parsing the error code out of the
response body**, and whether that code is retryable depends on the status
too. S3 `SlowDown` at HTTP 400 is a terminal client error; at 503 the SDK
backs off and retries. Get the shape wrong and you're testing a failure AWS
never produces.

microburst reads the **botocore service models** — the same definitions the
SDK uses — so injected errors carry the right code, the right XML/JSON
envelope, and the right status.

## Install

```bash
pip install microburst        # or: uvx microburst
pip install microburst[tui]   # + TUI dashboard (rich)
pip install microburst[h2]    # + HTTP/2 upstream transport (httpx)
pip install microburst[otel]  # + OpenTelemetry fault spans
```

Docker:

```bash
docker run --rm -p 9999:9999 ghcr.io/pingedbrain/microburst:latest \
  --upstream http://host.docker.internal:4566
```

Docker Compose (microburst + MiniStack wired together):

```bash
docker compose -f examples/docker-compose.yml up
```

Runnable failure scenarios — throttled writers, timeout vs retry-budget,
poison queues, stream cuts, mid-upload resets, generic HTTP deps — live in
[`examples/`](examples/README.md).

## GitHub Action

Drop fault injection into any workflow — microburst runs as a step
container and exports `AWS_ENDPOINT_URL` for you:

```yaml
jobs:
  chaos-tests:
    runs-on: ubuntu-latest
    services:
      ministack:
        image: ministackorg/ministack:latest
        ports: ["4566:4566"]
    steps:
      - uses: actions/checkout@v4
      - uses: pingedbrain/microburst@v0.3.0
        with:
          upstream: http://localhost:4566
          config: .github/chaos.yml   # optional rules file
      - run: pytest                 # AWS_ENDPOINT_URL already set
```

## Quickstart

```bash
microburst --upstream http://localhost:4566   # your emulator, e.g. MiniStack
```

```bash
export AWS_ENDPOINT_URL=http://localhost:9999
python your_app.py        # all AWS calls now flow through microburst
```

Inject throttling at runtime:

```bash
curl -X PATCH localhost:9999/_microburst/rules -d '[
  {"service": "dynamodb", "probability": 0.3,
   "error": {"code": "ProvisionedThroughputExceededException"}}
]'
```

Or fire a preset:

```bash
curl -X POST localhost:9999/_microburst/presets/ddb-throttle
```

Then watch **exactly what fired** — chaos you can audit:

```bash
curl localhost:9999/_microburst/fired
# → [{"rule": "...", "service": "dynamodb", "operation": "PutItem",
#     "action": "error:ProvisionedThroughputExceededException", ...}]
```

Or watch it live in the terminal:

```bash
microburst dashboard            # TUI: rules + live fault stream (needs [tui])
```

## Rules

```yaml
- service: dynamodb          # SigV4 credential-scope name, "*" for all
  operation: PutItem         # optional; resolved per AWS protocol
  region: us-east-1          # optional
  resource: orders           # substring of table/bucket/queue/…
  headers:                   # optional; all must match (substring)
    x-amz-acl: public-read   # e.g. only canned-ACL puts
    x-amz-copy-source: ""    # "" = presence check (e.g. CopyObject)
  body: "TableName == 'orders'"        # jmespath on JSON/form body — truthy = match
  rate: {count: 5, window_s: 60}       # at most N fires per rolling window
  sequence: {fail: 3, pass: 2}         # fail 3, pass 2, repeat
  probability: 0.5           # default 1.0
  deterministic: true        # hash the request identity — the same resource
                             # always lands on the same side of p (reproducible
                             # "this bucket always fails" without RNG seeds)
  times: 3                   # fire at most N times, then pass through
  ttl_s: 120                 # rule expires N seconds after creation
  active_at: "2026-01-01T00:00:00Z"  # start matching at this time
  until: 1893456000          # stop matching then (epoch or ISO-8601)
  error:
    code: SlowDown           # omit → samples a plausible modeled exception
    status: 503              # omit → modeled/curated AWS status
    message: "slow down"
    fields:                  # extra error-shape members, rendered per
      BucketName: my-bucket  # protocol (json members / XML elements)
  latency: {min: 500, max: 2000}   # ms; or a bare number, or a distribution:
                                   # {dist: gaussian, mean: 500, stddev: 100,
                                   #  min: 100, max: 2000}
                                   # {dist: spike, min: 50, max: 100,
                                   #  spike_ms: 5000, spike_p: 0.05}
                                   # {preset: dynamodb} — measured per-service
                                   #  baseline (see below); explicit keys in the
                                   #  same block override preset values
  timeout_ms: 30000                # hold the connection, then 504
  reset: true                      # abort the TCP connection
  response:                        # post-forward: mutate the upstream response
    truncate_frac: 0.5             # valid envelope, body cut short (or
                                   # truncate_bytes: N)
    abort_frac: 0.3                # send 30%, then kill the connection
                                   # mid-stream (or abort_bytes: N)
    corrupt_bytes: 16              # flip N bytes — 200 OK, wrong payload
    bandwidth_kbps: 64             # cap downstream throughput
    event_error:                   # eventstream (Kinesis SubscribeToShard,
      code: ThrottlingException    #  S3 Select): splice a well-formed
      message: "slowed mid-stream" #  :error frame mid-stream — terminal
      after_frames: 3              #  for the stream, after N real frames
    event_frames:                  # frame-level eventstream surgery:
      - at: 1                      #  `at` counts upstream frames (0-based)
        drop: true                 #  drop that frame
      - at: 3
        inject:                    #  emit a custom frame before index 3
          event_type: Stats
          payload: '{"BytesScanned": 42}'   # str, dict, or payload_b64
      - at: 4
        payload: '{"rew": 1}'      #  replace payload, CRCs recomputed
      - at: 5
        bad_crc: true              #  broken CRC → SDK checksum error
      - at: 6
        cut: 0.5                   #  emit half the frame, then EOF
      - at: 8
        error: {code: ThrottlingException}  # terminal :error frame
    set_headers:                   # mutate response headers — wrong CT on
      Content-Type: text/plain     # a 200, added x-amz-*, etc.
    strip_headers: [ETag]          # drop response headers entirely
  request:                         # pre-forward: fault the client→proxy upload
    slow_upload: {rate_kbps: 8}    # read the client body at ≤8 KiB/s — stalls
                                   # the SDK's write path (exercises write/
                                   # socket timeouts); the body still
                                   # arrives whole upstream
    cut_upload: {after_bytes: 1024}  # after N bytes of the upload are
                                   # consumed, reset the client connection —
                                   # ECONNRESET mid-PUT, nothing forwarded
                                   # (or after_frac: 0.5, or
                                   #  after_messages: N — framed bodies only:
                                   #  count messages and reset on a frame
                                   #  boundary, eventstream / grpc*)
    corrupt_upload: {at_message: 3}  # framed bodies: forward N-1 messages
                                   # verbatim, poison message N's
                                   # checksum/length so the UPSTREAM's
                                   # parser rejects it — the client stays
                                   # connected for the upstream's error
```

`times: 1` is the sleeper feature — *"fail exactly once, then let the retry
succeed"* verifies your retry path end-to-end instead of just proving errors
surface.

`latency: {preset: <service>}` picks a measured per-service baseline
(`dynamodb`, `s3`, `sqs`, `sns`, `lambda`, `kinesis`, `iam`, `ec2`,
`cloudformation`, `ssm`, `secretsmanager`, `sts`, `logs`, `firehose`,
`events`, `stepfunctions`, `kms`, `athena`, `route53`, `cloudfront`,
`glacier`, `wafv2`, `elbv2`, `apigateway`, `pinpoint`). The numbers are a
**real AWS observation** — read-only ops probed against live `us-east-1`
in 2026-02, n=8 per service — encoded as each service's *service-side
residual*: observed p50 minus the ~155ms host→region network floor,
because your own network isn't microburst's to simulate. The gaussian
`stddev` (`max(5, 0.35·mean)`) is a modeling **inference**, not a
measurement — service-side variance isn't recoverable through network
noise. Treat presets as order-of-magnitude baselines, not SLAs; refresh
them with `tools/latency_probe.py` (real AWS only).

Fault ordering: a fired rule applies `request:` faults first — the upload
happens before any response can exist, so a `cut_upload` link dies before
an `error:` envelope could be sent — then latency → reset → timeout →
error, then the forward, then `response:` mutations on the way back.

One fidelity note on `request:` faults: microburst buffers request bodies
when detection needs them (non-streaming ops ≤4 MiB, `body:` matchers,
cassette mode). A buffered upload has already finished on the client's
side, so `slow_upload` can't stall its write — the pacing moves to the
upstream send — and `cut_upload` lands on the client's read path (same
observable as `reset: true`). Streaming uploads (S3 `PutObject`,
`UploadPart`, payloads >4 MiB) get the real thing: write-path
backpressure and a true mid-upload reset.

`after_messages` / `corrupt_upload` need a framed Content-Type:
`application/vnd.amazon.eventstream` (prelude-parsed frames) or
`application/grpc*` (5-byte length prefix). On any other type they're
no-ops — the request forwards and the fired event's `note` field says
so (`content-type not a framed stream`). Frame parsing is defensive:
a malformed prelude/prefix stops message counting, falls back to the
byte thresholds (which still apply if configured), and forwards
verbatim on `corrupt_upload`; every degraded outcome lands in `note`
with `frames_seen` detail.

### Presets

`ddb-throttle` · `flaky-s3` · `slow-lambda` · `kms-outage` · `sqs-backlog` ·
`regional-failover` · `network-jitter` · `gateway-storm` · `expired-token` ·
`clock-skew` · `bad-signature`

## Control API

| Method | Path | Effect |
|---|---|---|
| GET | `/_microburst/health` | upstream, rule count, requests seen |
| GET · POST · PATCH · DELETE | `/_microburst/rules` | list / replace / append / clear rules |
| GET · DELETE | `/_microburst/fired` | fault audit log / clear it — GET filters: `?service=&operation=&rule_id=&limit=&since=&until=` (epoch or ISO-8601) |
| GET | `/_microburst/fired/stream` | live SSE tail — every fault as it fires |
| GET | `/_microburst/metrics` | Prometheus exposition: `microburst_requests_total`, `microburst_faults_total{service,operation,action}`, `microburst_rules_active` |
| GET | `/_microburst/stats` | aggregates — uptime, `requests{total,faulted,forwarded}`, upstream time-to-headers percentiles (`min/p50/p95/max/mean` over the last 2048 relays), per-rule hit counts, per-service request/fault split |
| GET · POST | `/_microburst/presets` & `/{name}` | list / activate presets |

Load rules at startup with `microburst --config chaos.yml`
(see `examples/chaos.yml`); add `--watch` to hot-reload the file on every
save — the file replaces the whole ruleset each reload.

## PostgreSQL wire mode

microburst also speaks the **PostgreSQL v3 wire protocol** — a TCP proxy
that injects SQLSTATE-correct `ErrorResponse`s and connection faults
between your app and any Postgres server. PG is a sibling transport, not
a second use of the HTTP pipeline: the same rules engine, fired log,
control API, and stats serve both modes.

```bash
microburst --protocol postgres --port 15432 --upstream localhost:5432
# control API is its own HTTP listener (default :9999, --control-port)

psql "host=localhost port=15432 dbname=mydb" -c "select 1"
# → flows through the proxy to localhost:5432
```

Rule matching for pg: `service: postgres`, `operation:` is the lowercased
SQL verb (`select`, `insert`, `begin`, …) or `startup` for the connect
handshake; `resource:` matches a table-ish token; `sql:` is a
case-insensitive regex against the raw query text.

```yaml
# refuse connections like a maxed-out pool
- service: postgres
  operation: startup
  error: {sqlstate: "53300", severity: FATAL, message: "too many connections"}

# serialization failure — psql shows: ERROR:  could not serialize access
- service: postgres
  operation: select
  sql: "into orders"
  error: {sqlstate: "40001", message: "could not serialize access"}

# slow, then canceled — a realistic statement_timeout
- operation: update
  latency: {min: 800, max: 1500}
  error: {sqlstate: "57014", message: "canceling statement due to statement timeout"}

# server dies mid-ResultSet: 2 real rows, then the TCP link aborts
- operation: select
  partial_rows: 2

- operation: select
  timeout: true     # hang until the client gives up
  # or: reset: true  # TCP RST
```

**Severity is the fidelity.** `severity: FATAL` (or `PANIC`) sends the
ErrorResponse then closes the connection — exactly what real PostgreSQL
does — and is the only error class safe to inject inside an open
transaction. A plain `ERROR` is injected **only while the session is
idle** (upstream's last `ReadyForQuery` said `I`): inside a transaction
(`T`/`E`) the rule is skipped and the query forwards untouched, because
an error the upstream never saw would desync client and server tx state.
Skips are recorded on the fired event's `note` (`skipped: in-transaction`).

Startup errors always close the session after the ErrorResponse — a
refused connection has no session to return to. `timeout_ms: N` stalls a
query for N ms; `timeout: true` stalls until the client disconnects.
Extended protocol (`Parse…Sync` batches, including named prepared
statements learned per connection) is decided at batch granularity.
Every fired fault lands in `/_microburst/fired` and `/metrics` with
`service="postgres"`, `operation` = verb, and the first ~80 chars of SQL
in `path`.

**Limitations (honest list):**

- Auth is passthrough only — SCRAM works because credentials are
  relayed uninspected; the proxy never synthesizes `AuthenticationOk`.
- `SSLRequest`/`GSSENCRequest` are refused with `N` (plaintext only).
- COPY streams relay verbatim but are never intercepted.
- No replication protocol (`walsender`/`CopyBothResponse`), no
  `FunctionCall` interception (it forwards + relays), no per-statement
  error replay like `25P02` emulation inside a transaction — injecting
  into an open tx needs upstream tx tracking beyond the status byte.
- Only the first statement of a multi-statement `Query` is classified.

## Redis wire mode

microburst also speaks the **RESP wire protocol** (RESP2 + RESP3
framing) — a TCP proxy that injects `-CODE` error replies and
connection faults between your app and any Redis server. Third sibling
transport: same rules engine, fired log, control API, and stats.

```bash
microburst --protocol redis --port 16379 --upstream localhost:6379
# control API is its own HTTP listener (default :9999, --control-port)

redis-cli -p 16379 GET foo
# → flows through the proxy to localhost:6379
```

Rule matching for redis: `service: redis`, `operation:` is the
lowercased command verb (`get`, `set`, `cluster`, …); `resource:`
matches a best-effort first key (argv[1] for most commands — eval
scripts, `XREAD`'s `STREAMS` token, numkeys-prefixed and
subcommand-style verbs resolve their real key position); `args:` is a
case-insensitive regex against the decoded command text (the `sql:`
matcher, for commands instead of queries).

```yaml
# cluster redirect — slot migration mid-reshard. `code` carries the
# whole post-dash line; clients regex `MOVED <slot> <host:port>`
- service: redis
  operation: get
  error: {code: "MOVED 3999 127.0.0.1:7001"}
# or composed: error: {code: MOVED, fields: {slot: 3999, target: "127.0.0.1:7001"}}

# replica just promoted read-only — writes fail until the client reconnects
- operation: set
  error: {code: READONLY, message: "You can't write against a read only replica."}

# dataset still loading after a restart
- error: {code: LOADING, message: "Redis is loading the dataset in memory"}

# memory pressure on writes only
- args: "^(set|mset|hset|lpush)"
  probability: 0.2
  error: {code: OOM, message: "command not allowed when used memory > 'maxmemory'."}

# server dies mid-bulk: 10 bytes of the real reply, then TCP abort
- operation: get
  cut_reply: {after_bytes: 10}

- operation: get
  timeout: true     # hang until the client gives up
  # or: reset: true  # TCP RST
```

**The MULTI caveat.** `error:` is injected everywhere *except* inside a
MULTI transaction — a `-ERR` the upstream never saw would desync the
queued commands (the client believes the command failed to queue;
upstream still queues it). While `in_multi`, the rule is skipped, the
command forwards, and the fired event's `note` records
`skipped: in-multi`. Latency/reset/timeout still apply mid-MULTI — a
slow or dead link can't diverge upstream state. `EXEC`/`DISCARD`/`RESET`
close the tracked transaction.

Every fired fault lands in `/_microburst/fired` and `/metrics` with
`service="redis"`, `operation` = verb, `resource` = first key, and the
first ~80 chars of the command in `path`.

**Limitations (honest list):**

- Pub/sub and MONITOR are passthrough-only: once a `SUBSCRIBE`/
  `PSUBSCRIBE`/`SSUBSCRIBE`/`MONITOR` forwards, the connection is a
  server-push stream and the proxy splices the sockets — no injection
  while subscribed. RESP3 `>` push and `|` attribute frames arriving
  mid-reply are relayed but never counted as the command's reply.
- No TLS (`rediss://` is rejected), no RESP2↔RESP3 translation — the
  upstream answers in whatever mode the client negotiated via `HELLO`.
- AUTH is just a command: `operation: auth` matches the verb, but the
  proxy never synthesizes `+OK` for credentials upstream didn't check.
- `error:` replies always use the RESP2-style `-` error line (valid in
  RESP3 too); `!` blob-errors are relayed but never synthesized.
- Inline commands are tokenized on whitespace only — quoted strings in
  inline mode aren't unquoted for detection (the raw bytes forward
  untouched either way).

## MySQL wire mode

microburst also speaks the **MySQL client/server protocol** — a TCP
proxy that injects errno+SQLSTATE-correct `ERR_Packet`s and connection
faults between your app and any MySQL or MariaDB server. Fourth sibling
transport: same rules engine, fired log, control API, and stats.

```bash
microburst --protocol mysql --port 13306 --upstream localhost:3306
# control API is its own HTTP listener (default :9999, --control-port)

mysql -h 127.0.0.1 -P 13306 -u app -e "select 1"
# → flows through the proxy to localhost:3306
```

Rule matching for mysql: `service: mysql`, `operation:` is the
lowercased SQL verb for `COM_QUERY` (`select`, `insert`, `begin`, …),
`stmt_*` for extended-protocol commands (`stmt_prepare`,
`stmt_execute`, `stmt_close`, `stmt_reset`, `stmt_fetch`,
`stmt_send_long_data`), `com_*` for the rest (`com_ping`, `com_init_db`,
…), or `startup` for the connect handshake. `resource:` matches a
table-ish token (and the database name for `com_init_db`); `sql:` is a
case-insensitive regex against the query text — it matches
`COM_STMT_PREPARE` text *and* `COM_STMT_EXECUTE`, because the proxy
tracks prepared statement-id → SQL per connection.

Errors carry both halves MySQL drivers classify on:
`error: {errno: 1064}` fills the SQLSTATE from a curated map (`42000`),
`error: {code: "40001"}` reads `code` as the SQLSTATE and fills the
errno (`1213`). Neither given → `1105`/`HY000` (`ER_UNKNOWN_ERROR`).

```yaml
# refuse connections like a maxed-out mysqld — the ERR is the very
# first packet instead of a greeting, then the link closes (the real
# 1040 shape: mysqld can't even create the session)
- service: mysql
  operation: startup
  error: {errno: 1040, code: "08004", message: "Too many connections"}

# syntax error — the errno maps to SQLSTATE 42000 automatically
- operation: select
  error: {errno: 1064, message: "You have an error in your SQL syntax"}

# deadlock — the errno+SQLSTATE pair InnoDB actually sends
- sql: "into orders"
  error: {errno: 1213, code: "40001", message: "Deadlock found when trying to get lock"}

# lock wait timeout
- operation: update
  error: {errno: 1205, message: "Lock wait timeout exceeded"}

# server dies mid-ResultSet: 2 real row packets, then the TCP link aborts
- operation: select
  partial_rows: 2

- operation: select
  timeout: true     # hang until the client gives up
  # or: reset: true  # TCP RST
```

**The transaction rule.** `SERVER_STATUS_IN_TRANS` in the status flags
of every relayed OK/EOF packet tracks whether the session sits inside
an open transaction — pg mode's `Z`-byte analog. A non-fatal `error:`
is injected **only while idle**: inside a transaction the rule is
skipped, the command forwards untouched, and the fired event's `note`
records `skipped: in-transaction` (a real `1213` rolls the whole tx
back — an injected one would claim a rollback upstream never
performed). `severity: FATAL` (or `PANIC`) sends the ERR then closes
the connection — the only error class safe inside a tx, because a dead
connection can't diverge upstream state.

Startup faults fire **before** the greeting: a server that can't create
the session sends the ERR as the very first packet (`1040`/`1129`-class
refusals) — upstream is never touched. Multi-statement replies relay
every sub-resultset (`SERVER_MORE_RESULTS_EXISTS`-aware), though only
the first statement's verb is classified. Every fired fault lands in
`/_microburst/fired` and `/metrics` with `service="mysql"`, `operation`
as above, and the first ~80 chars of SQL (or the command name) in
`path`.

**Limitations (honest list):**

- Auth is passthrough only — the full greeting → handshake-response →
  auth-switch/more-data → OK/ERR exchange relays verbatim, so
  `caching_sha2_password` and `mysql_native_password` both work; the
  proxy never synthesizes auth success.
- No TLS: `CLIENT_SSL` and `CLIENT_COMPRESS` are stripped from the
  relayed greeting (compression would re-frame every later packet) —
  clients fall back to plaintext or refuse client-side, exactly as
  against a mysqld without SSL. A client that sends an SSLRequest
  anyway gets a real `1043 Bad handshake` ERR and a close.
  `mysqls://`-style upstream URLs are rejected.
- `COM_STMT_FETCH` cursor flows and the replication family
  (`COM_BINLOG_DUMP`, `COM_BINLOG_DUMP_GTID`, `COM_REGISTER_SLAVE`,
  `COM_TABLE_DUMP`) are streaming sub-protocols — after forwarding, the
  proxy splices the sockets and stops deciding.
- `error:` can't be injected on commands with no server reply
  (`COM_STMT_CLOSE`, `COM_STMT_SEND_LONG_DATA`) — the rule fires with
  `note: skipped: command has no reply` and the command forwards.
- `error.fields` has no wire slot in ERR_Packet — accepted and ignored.
- A refused connection has two real shapes; only the pre-greeting one
  is synthesized. Refusals arriving *after* the handshake response
  (per-user limits, `1045` access-denied) currently can't be injected.

## Generic TCP mode

For wire protocols with **no dedicated module** — Kafka, Cassandra,
Mongo, or your own — `--protocol tcp` runs a generic
byte-stream proxy. It is a fifth sibling transport: same rules engine,
fired log, control API, and stats — but deliberately protocol-blind.
It injects **transport faults** (latency, resets, timeouts, cut streams,
corrupted bytes, synthetic replies), not protocol-correct errors.

```bash
microburst --protocol tcp --upstream localhost:3306 --port 13306
# --port defaults to upstream-port + 10000 (same convention as
# postgres/redis); control API on :9999, --control-port

nc localhost 13306    # any client — bytes flow to :3306
```

**Framing** is optional and declared once per proxy, either
`--framing "<spec>"` or a `framing:` mapping in the config file. Each
direction gets its own parser:

```yaml
# length-prefix — offset bytes, then a size-byte integer, then payload.
# includes_self: the declared length counts the length field itself
# (Mongo). adjust: extra header bytes after the field.
framing: {kind: length-prefix, size: 4, endian: big}              # Kafka
framing: {kind: length-prefix, size: 3, adjust: 1, endian: little} # MySQL (3B len + 1B seq)
framing: {kind: length-prefix, size: 4, offset: 5, endian: big}   # Cassandra v4 (len @ hdr+5)
framing: {kind: length-prefix, size: 4, endian: little, includes_self: true}  # Mongo
# delimiter — frame = bytes up to and including the delimiter
framing: {kind: delimiter, bytes: "0d0a"}                          # CRLF (hex or "\r\n")
# fixed — every N bytes is a frame
framing: {kind: fixed, size: 64}
```

CLI string form: `--framing "length-prefix:size=3,adjust=1,endian=little"`.
`size` accepts 1, 2, 3, 4, or 8 (3 exists for MySQL/Cassandra-v5).
With no `framing:` the stream is **unframed**: no segmentation at all.

**Decision units.** Framed mode evaluates every complete frame in both
directions — `operation: c2s:frame` on client→server units,
`operation: s2c:frame` on replies. Unframed mode evaluates rules once
per connection (`operation: conn`): connection-level rules match on the
first client→server chunk, and `payload:`/`bytes:` content rules get a
growing 4 KiB stream prefix to find the protocol's identity — once a
rule fires (or the prefix fills) the decision is latched and the rest
of the connection streams with whatever faults armed.

Rule matching: `service: tcp`; `payload:` is a case-insensitive regex on
the unit bytes decoded as latin-1 (byte-identity — `\x00`-`\xff` map
1:1). `payload:` also works in pg/redis modes, matching their
sql/args text.

```yaml
# latency / reset / timeout — both directions, per unit or per conn
- service: tcp
  operation: c2s:frame
  payload: "BEGIN|START TRANSACTION"
  latency: {min: 50, max: 200}

- service: tcp
  operation: conn
  timeout: true            # hang the link until the client gives up

# client→server dies after 64 KiB of uploads post-match
- service: tcp
  operation: c2s:frame
  cut_upload: {after_bytes: 65536}      # or after_messages: N (framed)

# reply stream dies after 3 frames — mid-response RST
- service: tcp
  operation: s2c:frame
  cut_reply: {after_messages: 3}        # or after_bytes: N

# flip the byte at absolute offset 7 of the client→server stream
- service: tcp
  corrupt: {at_bytes: 7, bit: flip}

# hand-crafted reply: answer the client yourself, never forward.
# then: forward (keep relaying) | close | hold (swallow the rest)
- service: tcp
  payload: "PING"
  respond: {data: "+PONG\r\n", then: forward}
  # or {hex: "2b504f4e47"}, {base64: "K1BPTkc="}
```

**Honest limits:**

- `error:` does not apply — there is no protocol error envelope to
  render. A rule that sets it fires the event with `note: error has no
  renderer in tcp mode` and the unit forwards untouched. Use a
  dedicated transport (pg/redis) for real protocol errors, or
  `respond:` to hand-craft one.
- No TLS — the proxy never terminates or starts it.
- No protocol semantics: no operation names, no transaction awareness,
  no per-message-type matching beyond `payload:` on raw bytes.
- Malformed framing latches the parser off — the stream falls back to
  verbatim passthrough and a `malformed … framing — passthrough` note
  lands in `/fired`.
- Armed cut thresholds count bytes/frames relayed *after* the rule
  fires; `corrupt.at_bytes` is an absolute stream offset.
- Unframed mode re-evaluates rules per chunk until one fires —
  `probability:`/`sequence:`/`times:` interact per evaluation, so
  prefer explicit matchers or `deterministic: true` there.
- Every fired fault lands in `/_microburst/fired` with `service="tcp"`,
  `operation` = `c2s:frame`/`s2c:frame`/`conn`, `path` = hex preview of
  the unit's first 32 bytes, and `note` for fallback detail.

## gRPC wire mode

microburst also proxies **gRPC over h2c** — `--protocol grpc` runs a
cleartext-HTTP/2 data plane (two `h2` state machines bridging client
and upstream streams; prior-knowledge h2c on both legs, the common
local-dev setup). It is a sixth sibling transport: same rules engine,
fired log, control API, and stats.

```bash
microburst --protocol grpc --port 15051 --upstream localhost:50051
# control API on :9999, --control-port

grpcurl -plaintext localhost:15051 list        # or point your app at it
```

The decision unit is **one RPC call**, taken when the request HEADERS
arrive: `operation:` is the lowercased full method path
(`/helloworld.Greeter/SayHello` → `helloworld.greeter/sayhello`),
`resource:`/`args:`/`payload:` match the raw `:path`, `service: grpc`.

Where the status lands is the fidelity: gRPC errors ride **trailers**
over HTTP 200 — never a non-200 (that would be a transport failure;
`error.status` is ignored and the event notes it).

```yaml
rules:
  # trailers-only: the shape servers send when a call fails before the
  # handler writes — one HEADERS frame, 200 + grpc-status + END_STREAM
  - service: grpc
    operation: my.pkg.svc/getitem
    error: {code: UNAVAILABLE}       # name or number (14) both work

  # mid-stream: 3 real messages relay, then trailers say the call died
  - operation: my.pkg.svc/watch
    partial_messages: 3
    error: {code: RESOURCE_EXHAUSTED, message: "quota mid-stream"}

  # partial_messages alone → RST_STREAM after N messages
  - operation: my.pkg.svc/tail
    partial_messages: 10

  # the server stalls — nothing answers; the client's own deadline
  # fires and it sees DEADLINE_EXCEEDED client-side
  - operation: my.pkg.svc/slow
    timeout: true                    # or timeout_ms: 2000 to stall-then-serve

  # per-RPC abort: RST_STREAM(INTERNAL_ERROR)
  - operation: my.pkg.svc/method
    reset: true

  # reply stream dies mid-message at TCP level — the link drops
  - operation: my.pkg.svc/export
    cut_reply: {after_bytes: 1024}   # or after_messages
```

- Flow control is coupled: inbound DATA is only acknowledged once
  forwarded, so a `timeout: true` call drains the client's send window
  — exactly what a stalled server looks like.
- Unary, server-streaming, client-streaming and bidi calls all proxy;
  `cut_upload:` cuts the client→server direction the same way.
- A client RST propagates upstream; an upstream RST/GOAWAY propagates
  downstream. `reset:` is always a per-RPC RST_STREAM — connection-wide
  death is `cut_*`'s job.
- Every fired fault lands in `/_microburst/fired` with
  `service="grpc"`, `operation` = method path, `path` = `:path`, and
  `note` for fallbacks (`malformed grpc prefix — fell back to bytes`,
  `skipped: non-grpc content-type`, …).

**Honest limits:**

- **h2c only** — no TLS termination: `grpcs://` upstreams are rejected
  and TLS-expecting clients get a dead link. Use `grpcurl -plaintext`.
- `latency`/`timeout_ms` sleep the connection's read pump while they
  run — on a multiplexed connection other streams wait too (MVP
  tradeoff; per-stream timers are the follow-up).
- h2 extension frames microburst doesn't model — PRIORITY trees,
  PUSH_PROMISE, unknown frames — are dropped, not relayed.
- Non-gRPC h2 traffic (REST-over-h2, WebSockets-over-h2) proxies
  transparently; `error:` skips it since a grpc-status trailer isn't a
  real error shape there — transport faults (latency/reset/timeout/
  cuts) still apply.
- `respond:`/`corrupt:`/`response:`/pg's `partial_rows`,
  `slow_upload`/`corrupt_upload` don't apply in grpc mode — noted in
  the fired event when set.

## Real AWS upstreams

```bash
microburst --upstream https://dynamodb.us-east-1.amazonaws.com
```

Requests are re-signed with your credentials automatically for
`amazonaws.com` upstreams (`--no-resign` to disable). Useful for game days
against staging accounts. Add `--http2` (needs `microburst[h2]`) to talk
HTTP/2 to the upstream — AWS endpoints negotiate it via ALPN.

## Cassettes: record & replay

Record real upstream traffic once, replay it forever — with rules still
injecting faults on top:

```bash
microburst --upstream https://dynamodb.us-east-1.amazonaws.com --record cass/
microburst --replay cass/ --config chaos.yml   # no upstream contact
```

Entries are keyed by `sha256(method + path + query + body)` — headers are
excluded so signatures/timestamps don't matter. Replays are byte-exact
(status, headers, body); response faults and injected errors apply
normally, so a replayed stream is deterministic underneath and chaotic on
top.

## Fidelity vs real AWS

Error envelopes aren't guessed — they're diffed against live AWS captures.
`tools/live_fidelity.py` records raw wire responses from real AWS
(read-only probes against nonexistent resources, credentials from your
profile/env) and diffs them against what `render_error` produces:

```bash
AWS_PROFILE=you microburst fidelity capture   # raw wire captures
microburst fidelity capture --region eu-west-1 --dir eu/  # any region
microburst fidelity report                    # → fidelity/REPORT.md
microburst fidelity snapshot                  # model digests (no creds)

# emulator conformance — same probes, diffed against the AWS goldens
microburst fidelity capture --endpoint-url http://localhost:4566 --dir ms/
microburst fidelity conform --emu ms/ --aws fidelity/   # → CONFORM.md

# compare any two capture sets directly
microburst fidelity diff ms/ other-emulator/            # → DIFF.md
```

The evidence stays fresh without anyone owning AWS credentials:
a weekly `model-drift` workflow regenerates `fidelity/models_snapshot.json`
against the latest botocore (AWS's models are upstream of the wire) and
opens an issue when error shapes, routes, or protocol metadata move —
that's the signal to re-capture. `fidelity/protocol/` vendors AWS's own
protocol-compliance fixtures (from botocore's conformance suite), so the
envelopes are also checked against AWS-authored wire expectations on
every test run.

A second weekly workflow, `emulator-conformance`, re-runs the same probe
set against a LocalStack service container and `conform`s it against the
committed goldens — an informational measurement, never a merge gate. The
score table lands in the run's step summary; `CONFORM.md` plus the raw
captures upload as the `localstack-conformance` run artifact.

The committed report ([fidelity/REPORT.md](fidelity/REPORT.md)) shows
28/28 probes matching AWS on status, parsed `Error.Code`, Content-Type,
**and envelope shape** (XML element paths, `__type` namespacing) —
including the details that matter to SDK retry behavior:
`x-amz-json-1.1` content types, `com.amazonaws.*`-namespaced `__type`,
rest-json `x-amzn-ErrorType` headers, SQS's `AWS.SimpleQueueService.*`
query-compat namespace, Route53's `text/xml`, empty-body HEAD errors, and
Athena's `AthenaErrorCode`/`ErrorCode` semantic fields.

The envelopes are also region-invariant: the same probe set captured in
every enabled region of a real account (17 regions) conforms 28/28 —
the only per-region difference observed is service availability (e.g.
Pinpoint has no endpoint in 5 regions), never the wire shape.

`capture` also runs `SUCCESS_PROBES` — read-only list/describe calls that
return 200 on a fresh account — recording `"kind": "success"` captures
across all six wire families (success ops that collide with an error
probe name land in `__ok`-suffixed files). `report` skips them with a
note (microburst forwards success bodies verbatim — there is nothing to
render), while `conform`/`diff` compare them on status + Content-Type +
envelope shape, which is where emulator success-shape conformance pays
off. The 25 committed success goldens are real AWS captures with
`content_scrubbed` — leaf values replaced by type-shaped placeholders
(resource names, ids, dates) while the element structure stays verbatim.
Caveat: success shapes are data-dependent — an empty `Buckets` list vs
a populated one is a real path-set diff, so emulator conformance on
success bodies reflects both format *and* cardinality.

### Multi-SDK matrix

`tools/sdk-matrix/` runs real SDK clients — **boto3, aws-sdk-js-v3,
aws-sdk-go-v2, aws-sdk-java-v2** — against a live microburst and verifies
what each SDK *parsed*, not what we *sent*: error code, HTTP status, and
retry attempts (measured twice — client-side and via the proxy's fired
log). All 16 scenario cells pass in CI on every push; the README in that
directory documents the one genuine cross-SDK divergence (codeless HEAD
errors: boto3 reports `"503"`, Go `"ServiceUnavailable"`, JS `"Unknown"`,
Java `null` — the same labels they produce against real AWS).

## Caveats

- Downstream is HTTP/1.1 (AWS SDKs don't speak h2 to the client anyway);
  upstream can be HTTP/2 with `--http2`. Event-stream APIs support
  mid-stream frame surgery via `response.event_frames` (drop, repayload,
  corrupt, inject, cut, `:error` — all with valid framing/CRCs unless
  `bad_crc` is the point).
- Bodies > 4 MiB are streamed uninspected (resource matchers won't apply;
  service/operation still do for REST services).
- The control API is unauthenticated — **bind it to localhost only**.
- SigV4A (multi-region) requests parse fine; rules see `region: "*"`.

## Demo

With MiniStack (or any emulator) on `:4566`:

```bash
python demo.py   # orders pipeline → microburst → MiniStack, scripted fault windows
```

You'll see DynamoDB puts retry through injected throttling, SQS publishes
degrade under latency, S3 reads ride out `SlowDown`, and the pipeline
recover when faults clear — plus the fired-fault ledger at the end.
A real run (endpoints configurable via `MINISTACK_URL`/`MICROBURST_URL`):

```text
⚡ FAULT INJECTED → dynamodb PutItem throttles 35%
    2.1s  order-6    put     56ms ↻ retried ×1 (throttled)  publish   2ms  s3 ✓
    6.8s  order-15   put    757ms ↻ retried ×4 (throttled)  publish   2ms  s3 ✓

⚡ FAULT INJECTED → S3 GetObject SlowDown 50%
   17.7s  order-21   put    356ms ↻ retried ×3   publish  2111ms (slow)  s3 ↻ ×1

⚡ FAULTS CLEARED → recovery
   20.4s  order-23   put      2ms  publish      2ms   s3 ✓     2ms

━━━ fired log (what microburst actually did) ━━━
   17× dynamodb:PutItem → error:ProvisionedThroughputExceededException
    3× s3:GetObject → error:SlowDown
    5× sqs:SendMessage → latency:1009–2192ms

━━━ outcome ━━━
  orders processed:      31
  ddb throttled+retried: 11 (SDK absorbed — app never saw an error)
  ddb hard failures:     0
```

## Contributing & community

- [Contributing](CONTRIBUTING.md) · [Code of Conduct](CODE_OF_CONDUCT.md) · [Security](SECURITY.md)
- Writing a new wire transport? The seam is `src/microburst/transports.py`:
  a `TRANSPORTS` entry (name → `run_<name>` path, default ports, upstream
  schemes) plus a sibling package — the contract is documented in
  `AGENTS.md`, and `pg/`, `redis/`, `mysql/`, `tcp/` are the reference
  implementations.
- This repo adopts [Apache Magpie](https://magpie.apache.org/) for
  agent-assisted maintainership (see `.apache-magpie.lock`).

## License

[MIT](LICENSE) — go break your own stuff before production does.
