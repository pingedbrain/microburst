# Timeout cascade — the sleeper bug

Your HTTP handler has a 2s timeout. boto3's default retry budget is 4
attempts with exponential backoff — under faults, a single SDK call can
take 10s+. **Your timeout fires while the SDK is still retrying.** From
the caller's view: the request failed. From the SDK's view: it never got
to finish. This is the #1 "it works in staging, dies in prod" pattern.

```bash
microburst -c examples/02-timeout-cascade/chaos.yml -u http://localhost:4566
python examples/02-timeout-cascade/app.py
```

`app.py` wraps an SQS call in a 1.5s deadline while microburst injects
1–3s latency. The loop reports calls that exceeded the deadline vs calls
the SDK completed slowly — the delta is the failure surface your timeout
hides. Fix options: raise the app deadline above the retry budget, or
cap `total_max_attempts`/retry time in `botocore.config.Config`.
