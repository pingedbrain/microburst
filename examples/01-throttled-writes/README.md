# Throttled writes — does your writer survive backpressure?

The most common real-world AWS failure: DynamoDB starts returning
`ProvisionedThroughputExceededException` mid-batch. The SDK retries with
backoff — but if your table is truly saturated, retries exhaust and the
error lands in *your* code. Does it drop the record? Crash the job?

```bash
microburst -c examples/01-throttled-writes/chaos.yml -u http://localhost:4566
python examples/01-throttled-writes/app.py
```

`app.py` provision a table through the proxy and batch-writes 50 items.
Watch the output: most throttled writes are absorbed by SDK retries
(`↻ retried`), some exhaust (`✗ retries exhausted`). Check
`/_microburst/fired` to see exactly which calls got throttled.
