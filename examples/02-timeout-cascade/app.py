#!/usr/bin/env python3
"""App deadline vs SDK latency — needs ministack + microburst with this
example's chaos.yml."""
import os
import time
from collections import Counter

import boto3
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9999")
APP_DEADLINE_S = 1.5  # "my request handler must answer in 1.5s"

sqs = boto3.client(
    "sqs", endpoint_url=ENDPOINT, region_name="us-east-1",
    aws_access_key_id="test", aws_secret_access_key="test",
)
queue_url = sqs.create_queue(QueueName="jobs")["QueueUrl"]

stats = Counter()
for i in range(10):
    t = time.monotonic()
    try:
        sqs.send_message(QueueUrl=queue_url, MessageBody=f"job-{i}")
        ms = (time.monotonic() - t) * 1000
        late = ms > APP_DEADLINE_S * 1000
        stats["late" if late else "ok"] += 1
        print(f"job-{i:<2} {ms:6.0f}ms {'⏰ OVER deadline — caller already gave up' if late else '✓'}")
    except ClientError as e:
        stats["failed"] += 1
        print(f"job-{i:<2} ✗ {e.response['Error']['Code']}")

print(f"\nwithin deadline: {stats['ok']}  over deadline: {stats['late']}  "
      f"failed: {stats['failed']}")
print("The 'over deadline' calls succeeded at the SDK level — but your "
      "handler timed out and the caller saw a failure.")
