# microburst

**AWS failure injection proxy.** Point your SDK at microburst instead of your AWS
endpoint and inject realistic faults — throttling, latency, timeouts,
connection resets — that the SDK treats exactly like real AWS failures.

Works against **any** upstream: MiniStack, moto, LocalStack, or real AWS.

## Why

Generic proxies (Toxiproxy et al.) are protocol-blind: they can cut a
connection or add delay, but they can't return a
`ProvisionedThroughputExceededException` in the shape the SDK parses — so your
retry/backoff/circuit-breaker code never gets exercised for real.

Microburst is **AWS-protocol-aware**:

- detects service, operation, region and resource per request (SigV4
  credential scope, `X-Amz-Target`, `Action=`, REST path patterns from the
  service model)
- serializes errors in the wire format of the right protocol
  (`json` / `query` / `rest-xml` / `rest-json`)
- picks HTTP statuses from the modeled error shape, and can *sample* a
  plausible modeled exception for an operation
- works as a plain HTTP hop — no MITM, no cert install; for real AWS it can
  re-sign requests with your credentials

## Quickstart

```bash
pip install -e .          # or: uvx microburst (once published)
microburst --upstream http://localhost:4566 --port 9999
```

```bash
export AWS_ENDPOINT_URL=http://localhost:9999
python your_app.py        # all AWS calls now flow through microburst
```

Inject a fault at runtime:

```bash
curl -X PATCH localhost:9999/_microburst/rules -d '[
  {"service": "dynamodb", "probability": 0.3,
   "error": {"code": "ProvisionedThroughputExceededException"}}
]'
```

Or use a preset:

```bash
curl -X POST localhost:9999/_microburst/presets/ddb-throttle
curl -X POST localhost:9999/_microburst/presets/network-jitter
```

See what fired (the part that turns blind chaos into a debugging tool):

```bash
curl localhost:9999/_microburst/fired
```

## Rules

```yaml
- service: dynamodb          # sigV4 credential scope name, "*" for all
  operation: PutItem         # optional; resolved per protocol
  region: us-east-1          # optional
  resource: orders           # substring of table/bucket/queue/etc.
  probability: 0.5           # default 1.0
  times: 3                   # fire at most N times total (great for
                             # "fail once, then retry succeeds")
  error:
    code: SlowDown           # any AWS error code; omit → sample from the
    status: 503              #   operation's modeled exceptions
    message: "slow down"
  latency: {min: 500, max: 2000}   # ms; or a bare number
  timeout_ms: 30000               # hold the connection, then 504
  reset: true                     # abort the TCP connection
```

A rule with only `latency` delays the request and still forwards it. The
first matching rule wins.

### Presets

`ddb-throttle` · `flaky-s3` · `slow-lambda` · `kms-outage` · `sqs-backlog` ·
`regional-failover` · `network-jitter` · `gateway-storm`

## Control API

| Method | Path | Effect |
|---|---|---|
| GET | `/_microburst/health` | upstream, rule count, requests seen |
| GET | `/_microburst/rules` | list active rules |
| POST | `/_microburst/rules` | replace all rules |
| PATCH | `/_microburst/rules` | append rules |
| DELETE | `/_microburst/rules` | body `[]` clears all; or list of field matchers |
| GET | `/_microburst/fired?limit=N` | fault events (rule, service, op, action) |
| DELETE | `/_microburst/fired` | clear the log |
| GET | `/_microburst/presets` | list presets |
| POST | `/_microburst/presets/{name}` | activate a preset |

## Config file

```bash
microburst --config examples/chaos.yml
```

See `examples/chaos.yml`.

## Real AWS upstreams

```bash
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
microburst --upstream https://dynamodb.us-east-1.amazonaws.com
```

Re-signing is enabled automatically for `amazonaws.com` upstreams (use
`--no-resign` to disable). Useful for game days against staging accounts —
inject faults into real API traffic without touching app code.

## Caveats

- HTTP/1.1 data plane; streaming/event-stream APIs
  (Kinesis `SubscribeToShard`, S3 Select, Lambda response streaming) pass
  through but fault injection on frames is not implemented yet.
- Bodies > 4 MiB are streamed uninspected (resource-level matchers won't see
  them; service/operation matchers still work for REST services).
- S3 presigned URLs are not specially detected yet.

## License

MIT
