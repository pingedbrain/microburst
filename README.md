<p align="center">
  <img src="assets/mascot.jpeg" alt="Nimbus, the microburst mascot" width="300">
</p>

<h1 align="center">microburst</h1>

<p align="center">
  <strong>AWS failure injection that your SDK actually believes.</strong><br>
  Throttling, latency, timeouts and resets — in the exact wire format AWS uses,<br>
  so retry, backoff and circuit-breaker code gets exercised for real.
</p>

<p align="center">
  <a href="https://github.com/pingedbrain/microburst/actions/workflows/ci.yml"><img src="https://github.com/pingedbrain/microburst/actions/workflows/ci.yml/badge.svg" alt="ci"></a>
  <a href="https://pypi.org/project/microburst/"><img src="https://img.shields.io/pypi/v/microburst" alt="PyPI"></a>
  <img src="https://img.shields.io/pypi/pyversions/microburst" alt="Python">
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
  error:
    code: SlowDown           # omit → samples a plausible modeled exception
    status: 503              # omit → modeled/curated AWS status
    message: "slow down"
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
```

`times: 1` is the sleeper feature — *"fail exactly once, then let the retry
succeed"* verifies your retry path end-to-end instead of just proving errors
surface.

### Presets

`ddb-throttle` · `flaky-s3` · `slow-lambda` · `kms-outage` · `sqs-backlog` ·
`regional-failover` · `network-jitter` · `gateway-storm`

## Control API

| Method | Path | Effect |
|---|---|---|
| GET | `/_microburst/health` | upstream, rule count, requests seen |
| GET · POST · PATCH · DELETE | `/_microburst/rules` | list / replace / append / clear rules |
| GET · DELETE | `/_microburst/fired` | fault audit log / clear it — GET filters: `?service=&operation=&rule_id=&limit=` |
| GET | `/_microburst/fired/stream` | live SSE tail — every fault as it fires |
| GET | `/_microburst/metrics` | Prometheus exposition: `microburst_requests_total`, `microburst_faults_total{service,operation,action}`, `microburst_rules_active` |
| GET · POST | `/_microburst/presets` & `/{name}` | list / activate presets |

Load rules at startup with `microburst --config chaos.yml`
(see `examples/chaos.yml`).

## Real AWS upstreams

```bash
microburst --upstream https://dynamodb.us-east-1.amazonaws.com
```

Requests are re-signed with your credentials automatically for
`amazonaws.com` upstreams (`--no-resign` to disable). Useful for game days
against staging accounts.

## Caveats

- HTTP/1.1 data plane; event-stream APIs pass through but per-frame fault
  injection isn't implemented yet.
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

## Contributing & community

- [Contributing](CONTRIBUTING.md) · [Code of Conduct](CODE_OF_CONDUCT.md) · [Security](SECURITY.md)
- This repo adopts [Apache Magpie](https://magpie.apache.org/) for
  agent-assisted maintainership (see `.apache-magpie.lock`).

## License

[MIT](LICENSE) — go break your own stuff before production does.
