# S3 SlowDown on uploads — retry storms on big payloads

S3 rate-limits per prefix with `SlowDown` (503). On uploads, a retry
re-sends the whole body — a 50MB upload retrying 4× is a bandwidth
storm. This example combines `SlowDown` with `bandwidth_kbps` to cap
throughput: the failure mode real mobile/edge/backup apps hit.

```bash
microburst -c examples/04-s3-slowdown-uploads/chaos.yml -u http://localhost:4566
python examples/04-s3-slowdown-uploads/app.py
```

Watch upload times and retries. Question for your app: is an upload
helper idempotent across retries, and does it backoff between attempts
or hammer the prefix?
