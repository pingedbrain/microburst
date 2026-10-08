#!/usr/bin/env python3
"""Queue consumer under intermittent failures — the naive poll loop."""
import os
import time
from collections import Counter

import boto3
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9999")
sqs = boto3.client(
    "sqs", endpoint_url=ENDPOINT, region_name="us-east-1",
    aws_access_key_id="test", aws_secret_access_key="test",
)
url = sqs.create_queue(QueueName="jobs")["QueueUrl"]
for i in range(20):
    sqs.send_message(QueueUrl=url, MessageBody=f"job-{i}")

seen, stats = set(), Counter()
deadline = time.time() + 30
while time.time() < deadline and len(seen) < 20:
    try:
        msgs = sqs.receive_message(
            QueueUrl=url, MaxNumberOfMessages=5, WaitTimeSeconds=1
        ).get("Messages", [])
    except ClientError as e:
        stats["poll_failed"] += 1
        print(f"poll ✗ {e.response['Error']['Code']} — does your loop survive this?")
        continue  # naive: retry immediately — real code should backoff
    for m in msgs:
        seen.add(m["MessageId"])
        try:
            sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
            stats["processed"] += 1
        except ClientError:
            stats["delete_failed"] += 1  # message will redeliver → seen dedupes

print(f"\nunique messages: {len(seen)}  processed: {stats['processed']}  "
      f"poll failures: {stats['poll_failed']}  delete failures: {stats['delete_failed']}")
if stats["delete_failed"]:
    print("delete failures → those messages redelivered; your consumer "
          "must be idempotent (the set dedupe is the fix).")
