#!/usr/bin/env python3
"""Upload loop under SlowDown — needs ministack + microburst."""
import os
import time
from collections import Counter

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9999")
s3 = boto3.client(
    "s3", endpoint_url=ENDPOINT, region_name="us-east-1",
    aws_access_key_id="test", aws_secret_access_key="test",
    config=Config(retries={"max_attempts": 3},
                  s3={"addressing_style": "path"}),
)
try:
    s3.create_bucket(Bucket="backups")
except ClientError:
    pass

attempts = Counter()
s3.meta.events.register(
    "needs-retry.s3.PutObject", lambda **_: attempts.__setitem__("r", attempts["r"] + 1))

stats = Counter()
body = os.urandom(64 * 1024)
for i in range(15):
    before = attempts["r"]
    t = time.monotonic()
    try:
        s3.put_object(Bucket="backups", Key=f"snap-{i}.bin", Body=body)
        ms = (time.monotonic() - t) * 1000
        retried = attempts["r"] > before
        stats["retried" if retried else "clean"] += 1
        print(f"snap-{i:<3} {ms:6.0f}ms {'↻ retried (SlowDown)' if retried else '✓'}")
    except ClientError as e:
        stats["failed"] += 1
        print(f"snap-{i:<3} ✗ {e.response['Error']['Code']}")

print(f"\nclean: {stats['clean']}  retried: {stats['retried']}  failed: {stats['failed']}")
