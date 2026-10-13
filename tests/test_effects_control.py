"""Effects, matchers and the control plane — the seams the MVP grew around.

Same contract as test_proxy: what matters is how the SDK and the fired log
experience each fault, not just what the proxy returns.
"""

import time
import urllib.error
import urllib.request

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectionClosedError
from test_proxy import _control, _ddb, _put_item

# -- terminal effects --------------------------------------------------------

def test_reset_aborts_connection(upstub, microburst_server, aws_env):
    """Every attempt dying at the socket eventually surfaces a
    connection error to the caller."""
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{"service": "dynamodb", "reset": True}],
    )
    with pytest.raises(ConnectionClosedError):
        _put_item(_ddb(proxy.url, max_attempts=2))
    # the connection died — nothing reached upstream for any attempt
    assert upstub[0].count() == 0


def test_reset_then_retry_recovers(upstub, microburst_server, aws_env):
    """A single reset is exactly the transient blip the transport
    (urllib3, below botocore's own retry layer) absorbs."""
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{"service": "dynamodb", "times": 1, "reset": True}],
    )
    resp = _put_item(_ddb(proxy.url))  # abort → transport retry → success
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert upstub[0].count() == 1


def test_timeout_returns_504_after_hold(upstub, microburst_server, aws_env):
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{"service": "dynamodb", "times": 1, "timeout_ms": 60}],
    )
    resp = _put_item(_ddb(proxy.url))
    # 504 is retryable: SDK retried and the second attempt passed through
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert upstub[0].count() == 1  # only the retry reached upstream


def test_latency_plus_error_compose(upstub, microburst_server, aws_env):
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{
            "service": "dynamodb", "times": 1,
            "latency": 120,
            "error": {"code": "InternalError"},
        }],
    )
    t = time.monotonic()
    _put_item(_ddb(proxy.url))  # faulted attempt retries; second passes
    assert time.monotonic() - t >= 0.11
    assert upstub[0].count() == 1


# -- matchers -----------------------------------------------------------------

def test_region_mismatch_does_not_fire(upstub, microburst_server, aws_env):
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{"service": "dynamodb", "region": "eu-west-1",
                "error": {"code": "InternalError"}}],
    )
    _put_item(_ddb(proxy.url))  # signed for us-east-1 → no match
    assert upstub[0].count() == 1


def test_resource_substring_match(upstub, microburst_server, aws_env):
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{"service": "dynamodb", "resource": "prod-", "times": 1,
                "error": {"code": "InternalError", "status": 500}}],
    )
    client = _ddb(proxy.url)
    _put_item(client, table="dev-orders")        # no match → passes
    _put_item(client, table="prod-orders")       # fires once
    _put_item(client, table="prod-orders")       # times exhausted → passes
    assert upstub[0].count() == 3


def test_probability_zero_never_fires(upstub, microburst_server, aws_env):
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{"service": "dynamodb", "probability": 0.0,
                "error": {"code": "InternalError"}}],
    )
    for _ in range(5):
        _put_item(_ddb(proxy.url))
    assert upstub[0].count() == 5


def test_sampled_modeled_error(upstub, microburst_server, aws_env):
    """error{} without a code samples an exception modeled for the op."""
    from microburst.models import operation_error_names

    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{"service": "dynamodb", "operation": "PutItem",
                "error": {}}],
    )
    client = _ddb(proxy.url, max_attempts=1)
    codes = set()
    for _ in range(5):
        with pytest.raises(ClientError) as caught:
            _put_item(client)
        codes.add(caught.value.response["Error"]["Code"])
    # every sampled code is one DynamoDB actually declares for PutItem
    modeled = set(operation_error_names("dynamodb", "PutItem"))
    assert modeled  # sanity: the model has errors to sample
    assert codes <= modeled


def test_unknown_service_falls_back_cleanly(upstub, microburst_server, aws_env):
    """A rule for a service botocore doesn't know still serves an envelope."""
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{"service": "notaservice", "times": 1,
                "error": {"code": "Boom", "status": 500}}],
    )
    req = urllib.request.Request(
        proxy.url + "/",
        headers={
            "Authorization": "AWS4-HMAC-SHA256 "
            "Credential=test/20240101/us-east-1/notaservice/aws4_request",
        },
        data=b"{}",
    )
    with pytest.raises(urllib.error.HTTPError) as caught:
        urllib.request.urlopen(req)
    assert caught.value.status == 500


# -- protocols beyond dynamodb ------------------------------------------------

def test_rest_json_error_shape(upstub, microburst_server, aws_env):
    """lambda is rest-json: JSON body + x-amzn-errortype header."""
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{"service": "lambda", "operation": "Invoke", "times": 1,
                "error": {"code": "TooManyRequestsException"}}],
    )
    lam = boto3.client(
        "lambda", endpoint_url=proxy.url, region_name="us-east-1",
        aws_access_key_id="t", aws_secret_access_key="t",
        config=Config(retries={"max_attempts": 2}),
    )
    lam.invoke(FunctionName="f", Payload=b"{}")  # first fires, retry passes
    fired = _control(proxy.url, "GET", "/_microburst/fired")
    assert fired[0]["operation"] == "Invoke"


def test_ec2_query_xml_error(upstub, microburst_server, aws_env):
    """ec2 speaks the ec2 protocol → XML ErrorResponse the SDK parses."""
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{"service": "ec2", "operation": "DescribeInstances",
                "times": 1, "error": {"code": "RequestLimitExceeded"}}],
    )
    ec2 = boto3.client(
        "ec2", endpoint_url=proxy.url, region_name="us-east-1",
        aws_access_key_id="t", aws_secret_access_key="t",
        config=Config(retries={"max_attempts": 2}),
    )
    ec2.describe_instances()  # throttling retried → second attempt passes
    fired = _control(proxy.url, "GET", "/_microburst/fired")
    assert fired[0]["operation"] == "DescribeInstances"


# -- control plane --------------------------------------------------------------

def test_control_crud_cycle(upstub, microburst_server, aws_env):
    _, proxy = microburst_server(upstub[1].url)
    rules = _control(proxy.url, "POST", "/_microburst/rules", [
        {"service": "dynamodb", "error": {"code": "InternalError"}},
        {"service": "s3", "error": {"code": "SlowDown"}},
    ])
    assert len(rules) == 2 and rules[0]["id"] != rules[1]["id"]

    removed = _control(proxy.url, "DELETE", "/_microburst/rules",
                       [{"service": "s3"}])
    assert removed["removed"] == 1
    remaining = _control(proxy.url, "GET", "/_microburst/rules")
    assert len(remaining) == 1 and remaining[0]["service"] == "dynamodb"

    _control(proxy.url, "DELETE", "/_microburst/rules", [])
    assert _control(proxy.url, "GET", "/_microburst/rules") == []


def test_presets_endpoint(upstub, microburst_server, aws_env):
    _, proxy = microburst_server(upstub[1].url)
    presets = _control(proxy.url, "GET", "/_microburst/presets")
    assert "ddb-throttle" in presets
    rules = _control(proxy.url, "POST", "/_microburst/presets/ddb-throttle")
    assert rules[0]["service"] == "dynamodb"
    with pytest.raises(urllib.error.HTTPError) as caught:
        _control(proxy.url, "POST", "/_microburst/presets/nope")
    assert caught.value.status == 404


def test_fired_log_clear(upstub, microburst_server, aws_env):
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{"service": "dynamodb", "times": 2,
                "error": {"code": "InternalError"}}],
    )
    client = _ddb(proxy.url)
    _put_item(client)  # fires, retries, passes
    _put_item(client)  # fires again
    assert len(_control(proxy.url, "GET", "/_microburst/fired")) == 2
    _control(proxy.url, "DELETE", "/_microburst/fired")
    assert _control(proxy.url, "GET", "/_microburst/fired") == []


def test_fired_time_range_filter(upstub, microburst_server, aws_env):
    """?since=/until= bound the audit log by event timestamp — epoch or
    ISO-8601, both inclusive."""
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{"service": "dynamodb", "times": 1,
                "error": {"code": "InternalError"}}],
    )
    _put_item(_ddb(proxy.url))
    ts = _control(proxy.url, "GET", "/_microburst/fired")[0]["ts"]

    def get(p):
        return _control(proxy.url, "GET", p)
    assert len(get(f"/_microburst/fired?since={ts}")) == 1
    assert len(get(f"/_microburst/fired?until={ts}")) == 1
    assert get(f"/_microburst/fired?since={ts + 1}") == []
    assert get(f"/_microburst/fired?until={ts - 1}") == []
    assert len(get(f"/_microburst/fired?since={ts - 1}&until={ts + 1}")) == 1

    iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts + 1))
    assert get(f"/_microburst/fired?since={iso}") == []


def test_fired_time_range_rejects_garbage(upstub, microburst_server, aws_env):
    _, proxy = microburst_server(upstub[1].url)
    with pytest.raises(urllib.error.HTTPError) as caught:
        _control(proxy.url, "GET", "/_microburst/fired?since=soon")
    assert caught.value.status == 400


def test_control_rejects_bad_body(upstub, microburst_server, aws_env):
    _, proxy = microburst_server(upstub[1].url)
    req = urllib.request.Request(
        proxy.url + "/_microburst/rules", data=b"not json", method="POST",
        headers={"Content-Type": "application/json"},
    )
    with pytest.raises(urllib.error.HTTPError) as caught:
        urllib.request.urlopen(req)
    assert caught.value.status == 400
