#!/usr/bin/env python3
"""Batch writer under throttling — needs ministack on :4566 + microburst
on :9999 with this example's chaos.yml loaded."""
import os
import sys
import time
from collections import Counter

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9999")
ddb = boto3.client(
    "dynamodb", endpoint_url=ENDPOINT, region_name="us-east-1",
    aws_access_key_id="test", aws_secret_access_key="test",
    config=Config(retries={"max_attempts": 4}),
)

try:
    ddb.create_table(
        TableName="events",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
except ClientError as e:
    if e.response["Error"]["Code"] != "ResourceInUseException":
        raise

attempts = Counter()
ddb.meta.events.register(
    "needs-retry.dynamodb.PutItem", lambda **_: attempts.__setitem__("r", attempts["r"] + 1))

stats = Counter()
for i in range(50):
    before = attempts["r"]
    try:
        ddb.put_item(TableName="events", Item={"pk": {"S": f"ev-{i}"}})
        retried = attempts["r"] > before
        stats["retried" if retried else "clean"] += 1
        print(f"ev-{i:<4} {'↻ retried (throttled)' if retried else '✓'}")
    except ClientError as e:
        stats["exhausted"] += 1
        print(f"ev-{i:<4} ✗ {e.response['Error']['Code']} — retries exhausted")
    time.sleep(0.05)

print(f"\nclean: {stats['clean']}  absorbed by retry: {stats['retried']}  "
      f"hard-failed: {stats['exhausted']}")
sys.exit(0)
