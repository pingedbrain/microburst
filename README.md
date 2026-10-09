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
poison queues, stream cuts, generic HTTP deps — live in
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
```

`times: 1` is the sleeper feature — *"fail exactly once, then let the retry
succeed"* verifies your retry path end-to-end instead of just proving errors
surface.

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
| GET · POST | `/_microburst/presets` & `/{name}` | list / activate presets |

Load rules at startup with `microburst --config chaos.yml`
(see `examples/chaos.yml`); add `--watch` to hot-reload the file on every
save — the file replaces the whole ruleset each reload.

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

The committed report ([fidelity/REPORT.md](fidelity/REPORT.md)) shows
27/28 probes matching AWS on status, parsed `Error.Code`, Content-Type,
**and envelope shape** (XML element paths, `__type` namespacing) —
including the details that matter to SDK retry behavior:
`x-amz-json-1.1` content types, `com.amazonaws.*`-namespaced `__type`,
rest-json `x-amzn-ErrorType` headers, SQS's `AWS.SimpleQueueService.*`
query-compat namespace, Route53's `text/xml`, and empty-body HEAD errors.
The one documented divergence is Athena's unmodeled `ErrorCode`/
`AthenaErrorCode` taxonomy — the values aren't derivable from the
service model.

The envelopes are also region-invariant: the same probe set captured in
every enabled region of a real account (17 regions) conforms 28/28 —
the only per-region difference observed is service availability (e.g.
Pinpoint has no endpoint in 5 regions), never the wire shape.

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
- This repo adopts [Apache Magpie](https://magpie.apache.org/) for
  agent-assisted maintainership (see `.apache-magpie.lock`).

## License

[MIT](LICENSE) — go break your own stuff before production does.
