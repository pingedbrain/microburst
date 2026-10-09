#!/usr/bin/env python3
"""PUTs of framed eventstream bodies, cut on a message boundary —
needs ministack + microburst."""
import os
import struct
import time
import zlib
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


def eventstream_frame(i: int, payload: bytes) -> bytes:
    """A real AWS event-stream message: prelude (total_len u32,
    headers_len u32, prelude CRC32) + headers + payload + message CRC32."""
    def header(name: str, val: str) -> bytes:
        n, v = name.encode(), val.encode()
        return (
            bytes([len(n)]) + n
            + b"\x07" + len(v).to_bytes(2, "big") + v
        )

    headers = header(":message-type", "event") + header(
        ":event-type", f"record-{i}"
    )
    total = 8 + 4 + len(headers) + len(payload) + 4
    prelude = struct.pack("!II", total, len(headers))
    prelude += struct.pack("!I", zlib.crc32(prelude) & 0xFFFFFFFF)
    msg = prelude + headers + payload
    return msg + struct.pack("!I", zlib.crc32(msg) & 0xFFFFFFFF)


body = b"".join(
    eventstream_frame(i, os.urandom(4096)) for i in range(8)
)

attempts = Counter()
s3.meta.events.register(
    "needs-retry.s3.PutObject",
    lambda **_: attempts.__setitem__("r", attempts["r"] + 1),
)

for i in range(8):
    before = attempts["r"]
    t = time.monotonic()
    try:
        s3.put_object(
            Bucket="uploads", Key=f"events-{i}.bin", Body=body,
            ContentType="application/vnd.amazon.eventstream",
        )
        ms = (time.monotonic() - t) * 1000
        retried = attempts["r"] > before
        print(f"events-{i:<3} {ms:6.0f}ms "
              f"{'↻ retried after mid-stream reset' if retried else '✓'}")
    except ConnectionClosedError:
        print(f"events-{i:<3} ✗ connection closed mid-upload "
              "(retries exhausted)")
    except ClientError as e:
        print(f"events-{i:<3} ✗ {e.response['Error']['Code']}")
