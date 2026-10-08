"""boto3 baseline client for the SDK matrix.

Calls each scenario op against the microburst endpoint and prints one JSON
line per scenario: {"sdk", "scenario", "code", "status", "attempts"}.
Attempts are counted client-side via botocore's before-send event (fires
per HTTP request, i.e. per retry attempt).
"""

from __future__ import annotations

import json
import os
import sys

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MB_ENDPOINT", "http://127.0.0.1:9999")
CFG = Config(
    retries={"max_attempts": 3, "mode": "standard"},
    s3={"addressing_style": "path"},
    connect_timeout=5,
    read_timeout=5,
)


def _client(service: str):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="matrix",
        aws_secret_access_key="matrix",
        config=CFG,
    )


def run(scenario: str, service: str, method: str, **kwargs) -> dict:
    client = _client(service)
    counter = {"n": 0}

    def count(request, **kw):
        counter["n"] += 1

    client.meta.events.register("before-send", count)
    try:
        getattr(client, method)(**kwargs)
        return {
            "sdk": "boto3", "scenario": scenario, "code": None,
            "status": 200, "attempts": counter["n"], "unexpected": "no error",
        }
    except ClientError as e:
        return {
            "sdk": "boto3", "scenario": scenario,
            "code": e.response["Error"].get("Code"),
            "status": e.response["ResponseMetadata"].get("HTTPStatusCode"),
            "attempts": counter["n"],
        }
    except Exception as e:  # noqa: BLE001 — surface any SDK failure verbatim
        return {
            "sdk": "boto3", "scenario": scenario,
            "code": type(e).__name__, "status": None,
            "attempts": counter["n"], "error": str(e)[:300],
        }


SCENARIOS = {
    "dynamo-throttle": ("dynamodb", "describe_table", {"TableName": "t"}),
    "lambda-notfound": ("lambda", "get_function", {"FunctionName": "f"}),
    "sqs-querycompat": ("sqs", "get_queue_url", {"QueueName": "q"}),
    "s3-slowdown": ("s3", "head_bucket", {"Bucket": "b"}),
}


def main() -> int:
    only = set(sys.argv[1:])
    for name, (svc, method, kw) in SCENARIOS.items():
        if only and name not in only:
            continue
        print(json.dumps(run(name, svc, method, **kw)), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
