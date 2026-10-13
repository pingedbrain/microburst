#!/usr/bin/env python3
"""PUTs with the link cut mid-upload — needs ministack + microburst."""
import os
import time
from collections import Counter

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectionClosedError

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9999")
s3 = boto3.client(
    "s3", endpoint_url=ENDPOINT, region_name="us-east-1",
    aws_access_key_id="test", aws_secret_access_key="test",
    config=Config(retries={"max_attempts": 3},
                  s3={"addressing_style": "path"}),
)
try:
    s3.create_bucket(Bucket="uploads")
except ClientError:
    pass

attempts = Counter()
s3.meta.events.register(
    "needs-retry.s3.PutObject",
    lambda **_: attempts.__setitem__("r", attempts["r"] + 1),
)

body = os.urandom(256 * 1024)
for i in range(8):
    before = attempts["r"]
    t = time.monotonic()
    try:
        s3.put_object(Bucket="uploads", Key=f"blob-{i}.bin", Body=body)
        ms = (time.monotonic() - t) * 1000
        retried = attempts["r"] > before
        print(f"blob-{i:<3} {ms:6.0f}ms "
              f"{'↻ retried after mid-upload reset' if retried else '✓'}")
    except ConnectionClosedError:
        print(f"blob-{i:<3} ✗ connection closed mid-upload "
              "(retries exhausted)")
    except ClientError as e:
        print(f"blob-{i:<3} ✗ {e.response['Error']['Code']}")
