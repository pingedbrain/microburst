"""End-to-end: real boto3 clients through microburst against a stub upstream.

The point of these tests is not "the proxy returns an error" — it is that
the *SDK* classifies the injected fault the way it classifies real AWS:
throttling codes retried, terminal codes surfaced, latency observable.
"""

import json
import time
import urllib.request

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError


def _ddb(url, max_attempts=2):
    return boto3.client(
        "dynamodb",
        endpoint_url=url,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(retries={"max_attempts": max_attempts}),
    )


def _put_item(client, table="orders"):
    return client.put_item(TableName=table, Item={"pk": {"S": "x"}})


def _control(url, method, path, payload=None):
    req = urllib.request.Request(
        url + path,
        method=method,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def test_passthrough(upstub, microburst_server, aws_env):
    stub, upstream = upstub
    _, proxy = microburst_server(upstream.url)
    resp = _put_item(_ddb(proxy.url))
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert stub.count() == 1


def test_throttling_error_reaches_sdk(upstub, microburst_server, aws_env):
    stub, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[{
            "service": "dynamodb",
            "error": {"code": "ProvisionedThroughputExceededException"},
        }],
    )
    with pytest.raises(ClientError) as exc:
        _put_item(_ddb(proxy.url, max_attempts=2))
    assert (exc.value.response["Error"]["Code"]
            == "ProvisionedThroughputExceededException")
    # 2 attempts both intercepted — nothing reached upstream
    assert stub.count() == 0


def test_one_shot_fault_then_retry_succeeds(upstub, microburst_server, aws_env):
    """times=1 → SDK retries internally and the call succeeds."""
    stub, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[{
            "service": "dynamodb",
            "times": 1,
            "error": {"code": "ProvisionedThroughputExceededException"},
        }],
    )
    resp = _put_item(_ddb(proxy.url, max_attempts=3))
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    # first attempt faulted, retry went through to upstream
    assert stub.count() == 1


def test_operation_matcher_scopes_the_fault(upstub, microburst_server, aws_env):
    _stub, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[{
            "service": "dynamodb",
            "operation": "GetItem",
            "error": {"code": "InternalServerError", "status": 500},
        }],
    )
    client = _ddb(proxy.url, max_attempts=1)
    _put_item(client)  # unaffected
    with pytest.raises(ClientError) as exc:
        client.get_item(TableName="orders", Key={"pk": {"S": "x"}})
    assert exc.value.response["Error"]["Code"] == "InternalServerError"


def test_resource_matcher_scopes_the_fault(upstub, microburst_server, aws_env):
    _stub, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[{
            "service": "dynamodb",
            "resource": "orders",
            "error": {"code": "ProvisionedThroughputExceededException"},
        }],
    )
    client = _ddb(proxy.url, max_attempts=1)
    with pytest.raises(ClientError):
        _put_item(client, table="orders")
    _put_item(client, table="other-table")  # passes through


def test_latency_effect(upstub, microburst_server, aws_env):
    stub, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "dynamodb", "latency": {"min": 300, "max": 400}}],
    )
    start = time.monotonic()
    _put_item(_ddb(proxy.url, max_attempts=1))
    assert time.monotonic() - start >= 0.29
    assert stub.count() == 1  # latency then forward


def test_sqs_throttle(upstub, microburst_server, aws_env):
    _stub, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "sqs", "error": {"code": "Throttling"}}],
    )
    client = boto3.client(
        "sqs", endpoint_url=proxy.url, region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test",
        config=Config(retries={"max_attempts": 1}),
    )
    with pytest.raises(ClientError) as exc:
        client.send_message(QueueUrl="http://x/123/jobs", MessageBody="hi")
    assert exc.value.response["Error"]["Code"] == "Throttling"


def test_s3_slowdown(upstub, microburst_server, aws_env):
    _stub, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[{"service": "s3", "error": {"code": "SlowDown"}}],
    )
    client = boto3.client(
        "s3", endpoint_url=proxy.url, region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test",
        config=Config(retries={"max_attempts": 1}, s3={"addressing_style": "path"}),
    )
    with pytest.raises(ClientError) as exc:
        client.put_object(Bucket="b", Key="k", Body=b"x")
    assert exc.value.response["Error"]["Code"] == "SlowDown"


def test_control_api_and_fired_log(upstub, microburst_server, aws_env):
    _stub, upstream = upstub
    _, proxy = microburst_server(upstream.url)

    health = _control(proxy.url, "GET", "/_microburst/health")
    assert health["status"] == "ok"
    assert health["upstream"] == upstream.url

    _control(proxy.url, "PATCH", "/_microburst/rules", [{
        "service": "dynamodb",
        "error": {"code": "InternalError", "status": 500},
    }])
    rules = _control(proxy.url, "GET", "/_microburst/rules")
    assert len(rules) == 1

    with pytest.raises(ClientError):
        _put_item(_ddb(proxy.url, max_attempts=1))

    fired = _control(proxy.url, "GET", "/_microburst/fired")
    assert fired
    assert fired[0]["service"] == "dynamodb"
    assert fired[0]["operation"] == "PutItem"
    assert "InternalError" in fired[0]["action"]

    removed = _control(proxy.url, "DELETE", "/_microburst/rules", [])
    assert removed["removed"] == 1
    _put_item(_ddb(proxy.url))  # clean again


def test_preset_endpoint(upstub, microburst_server, aws_env):
    _stub, upstream = upstub
    _, proxy = microburst_server(upstream.url)
    added = _control(proxy.url, "POST", "/_microburst/presets/kms-outage")
    assert added[0]["service"] == "kms"
    presets = _control(proxy.url, "GET", "/_microburst/presets")
    assert "ddb-throttle" in presets


def test_modeled_error_sampling(upstub, microburst_server, aws_env):
    """error without code → picks a plausible modeled exception for the op."""
    _stub, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[{
            "service": "dynamodb",
            "operation": "PutItem",
            "error": {},
        }],
    )
    with pytest.raises(ClientError) as exc:
        _put_item(_ddb(proxy.url, max_attempts=1))
    assert exc.value.response["Error"]["Code"]
