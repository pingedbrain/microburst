# SDK matrix — real SDKs vs microburst

Verifies that actual AWS SDKs (not just botocore) parse and retry
microburst-injected faults the way they parse real AWS errors.

## What it does

`run.py` starts a local microburst with deterministic `p=1.0` error rules
pointed at a dead upstream (every request is injected before forwarding),
then runs each SDK client against it and records, per scenario:

- the error **code the SDK parsed** (`ClientError.Error.Code`,
  `e.name`, `smithy.APIError.ErrorCode()`)
- the **HTTP status** the SDK saw
- **attempt counts** — measured twice, independently: client-side
  (`before-send` / `$metadata.attempts` / finalize middleware) and
  server-side from the proxy's `/_microburst/fired` log. Both must agree.

## Scenarios

| scenario | service | wire protocol | injected | exercises |
|---|---|---|---|---|
| `dynamo-throttle` | dynamodb | json | `ProvisionedThroughputExceededException` @400 | retryable throttle — SDKs must retry |
| `lambda-notfound` | lambda | rest-json | `ResourceNotFoundException` @404 | `x-amzn-ErrorType` parsing — terminal |
| `sqs-querycompat` | sqs | json (query-compat) | `OverLimit` @400 | `x-amzn-query-error` parsing — all SDKs read the namespaced `AWS.SimpleQueueService.*` code |
| `s3-slowdown` | s3 | rest-xml | `SlowDown` @503 | codeless HEAD error — retryable |

### The s3-slowdown divergence (documented, correct)

HEAD errors carry no body — matching real AWS (verified by the live
`head_bucket` capture, which parses to the status code `404`). Each SDK
maps a codeless 503 differently:

- **boto3** → `"503"` (status-as-code)
- **aws-sdk-go-v2** → `"ServiceUnavailable"` (smithy-go generic)
- **aws-sdk-js-v3** → `"Unknown"` (smithy-js generic)

All three retried the 503 — retry semantics agree, only the error *label*
differs, exactly as it does against real AWS.

## Running

```bash
# one command — spawns microburst, runs all detected SDKs, prints a table
python tools/sdk-matrix/run.py          # or --sdk boto3,js-v3

# per-SDK dependencies (once)
cd tools/sdk-matrix && npm install
cd tools/sdk-matrix/clients && go mod download
```

Requires: Python venv with microburst installed, Node ≥18 (SDK v3),
Go ≥1.21 (SDK v2), JDK+Maven (SDK Java v2 — `clients/java/`).
Toolchains absent → those cells are skipped with a warning.
Results land in `results.json`.

CI runs all four SDKs on every push (`sdk-matrix` job in
`.github/workflows/ci.yml`).
