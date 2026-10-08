# Examples — failure modes worth testing before production finds them

Each directory is a self-contained scenario: a `chaos.yml` and a tiny app
that exercises it. All examples assume an AWS-compatible upstream — the
quickest is [MiniStack](https://github.com/ministackorg/ministack) on
`:4566` (`docker compose -f docker-compose.yml up` runs the full pair).

Every example follows the same shape:

```bash
# 1. start the proxy in front of your upstream
microburst -c examples/<scenario>/chaos.yml -u http://localhost:4566

# 2. point your app (or the provided app.py) at the proxy
AWS_ENDPOINT_URL=http://localhost:9999 python examples/<scenario>/app.py

# 3. see exactly what microburst injected
curl http://localhost:9999/_microburst/fired
```

| # | Scenario | The bug it surfaces |
|---|----------|---------------------|
| 01 | [throttled-writes](01-throttled-writes/) | Does your writer actually survive `ProvisionedThroughputExceededException` — or silently drop records? |
| 02 | [timeout-cascade](02-timeout-cascade/) | Your app timeout is shorter than the SDK's retry budget — faults make calls take 5× and your timeout fires first. |
| 03 | [poison-queue](03-poison-queue/) | `ReceiveMessage` intermittently fails — does your consumer crash the loop or keep polling? |
| 04 | [s3-slowdown-uploads](04-s3-slowdown-uploads/) | `SlowDown` + bandwidth throttling on multipart uploads — retry storms on big payloads. |
| 05 | [stream-cut](05-stream-cut/) | Event streams killed mid-frame — does your consumer handle a truncated `SelectObjectContent`? |
| 06 | [expired-token](06-expired-token/) | `ExpiredTokenException` once mid-session — does the app refresh credentials or die? |
| 07 | [http-dependency](07-http-dependency/) | Any HTTP dependency (not just AWS): latency, resets, timeouts — "the payment API got slow". |

Also in this directory:

- `chaos.yml` / `docker-compose.yml` — mixed-rule config + MiniStack pairing.
- `../demo.py` — the scripted end-to-end demo (order pipeline under a
  live fault schedule).

## Why these and not mocks

A hand-written `ClientError` has the shape *you* imagined. microburst's
errors are rendered from botocore models and verified against real AWS
wire captures — the SDK retries, classifies, and fails exactly as it
would against AWS itself. If your code handles these scenarios through
microburst, it handles them against AWS.
