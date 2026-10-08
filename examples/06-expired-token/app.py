#!/usr/bin/env python3
"""Every call fails ExpiredToken ×3 then recovers — does the app notice
vs just crash?"""
import os
import time

import boto3
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9999")
sqs = boto3.client(
    "sqs", endpoint_url=ENDPOINT, region_name="us-east-1",
    aws_access_key_id="test", aws_secret_access_key="test",
)

for i in range(6):
    try:
        sqs.list_queues()
        print(f"call {i} ✓")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        print(f"call {i} ✗ {code} — "
              f"{'refresh creds + retry' if code == 'ExpiredTokenException' else 'unexpected'}")
    time.sleep(0.3)
