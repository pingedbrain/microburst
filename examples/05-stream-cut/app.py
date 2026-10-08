#!/usr/bin/env python3
"""S3 Select over a stream that dies mid-flight — needs ministack +
microburst. If ministack doesn't support SelectObjectContent, the
request fails upstream — the event_frames rule only fires on a real
event-stream response."""
import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9999")
s3 = boto3.client(
    "s3", endpoint_url=ENDPOINT, region_name="us-east-1",
    aws_access_key_id="test", aws_secret_access_key="test",
    config=Config(s3={"addressing_style": "path"}),
)
try:
    s3.create_bucket(Bucket="logs")
except ClientError:
    pass
rows = b"".join(f'{{"n": {i}}}\n'.encode() for i in range(100))
s3.put_object(Bucket="logs", Key="events.json", Body=rows)

records = 0
try:
    resp = s3.select_object_content(
        Bucket="logs", Key="events.json",
        Expression="select * from s3object",
        ExpressionType="SQL",
        InputSerialization={"JSON": {"Type": "LINES"}},
        OutputSerialization={"JSON": {}},
    )
    for event in resp["Payload"]:
        if "Records" in event:
            records += event["Records"]["Payload"].count(b"\n")
        elif "End" in event:
            print("stream ended cleanly")
    print(f"records received before stream end: {records}")
except ClientError as e:
    print(f"stream died: {e.response['Error']['Code']} — "
          f"{records} records already consumed, must restart or resume")
