"""rpc-v2-cbor protocol support + signing-name scope coverage."""

from __future__ import annotations

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from microburst.detection import detect, parse_rpcv2_path
from microburst.models import service_for_scope, service_for_target_prefix
from microburst.protocols import render_error
from microburst.protocols.cbor import encode_map_str

# -- scope aliases --------------------------------------------------------


@pytest.mark.parametrize(
    "scope,service",
    [
        ("monitoring", "cloudwatch"),
        ("elasticfilesystem", "efs"),
        ("states", "stepfunctions"),
        ("execute-api", "apigatewaymanagementapi"),
        ("mobiletargeting", "pinpoint"),
        ("mturk-requester", "mturk"),
        ("elasticloadbalancing", "elbv2"),
        ("lex", "lexv2-runtime"),
        ("timestream", "timestream-write"),
        ("aoss", "opensearchserverless"),
        ("iotdata", "iot-data"),
        # identity: valid service names pass through
        ("dynamodb", "dynamodb"),
        ("s3", "s3"),
        ("lambda", "lambda"),
    ],
)
def test_scope_alias(scope, service):
    assert service_for_scope(scope) == service


def test_target_prefix_resolves_exact_service():
    assert service_for_target_prefix("GraniteServiceVersion20100801") == "cloudwatch"
    assert service_for_target_prefix("DynamoDBStreams_20120810") == "dynamodbstreams"
    assert service_for_target_prefix("AWSEventsV2") == "eventbridgev2"
    assert service_for_target_prefix("AWSEvents") == "events"
    assert service_for_target_prefix("nope") is None


# -- cbor serialization ---------------------------------------------------


def _decode_cbor_map(data: bytes) -> dict:
    """Tiny decoder for flat string maps — mirrors encode_map_str."""

    def text(buf, i):
        ib = buf[i]
        assert ib >> 5 == 3  # text string
        info = ib & 0x1F
        i += 1
        if info < 24:
            n = info
        elif info == 24:
            n = buf[i]
            i += 1
        elif info == 25:
            n = int.from_bytes(buf[i : i + 2], "big")
            i += 2
        else:
            n = int.from_bytes(buf[i : i + 4], "big")
            i += 4
        return buf[i : i + n].decode(), i + n

    assert data[0] >> 5 == 5  # map
    n = data[0] & 0x1F
    out, i = {}, 1
    for _ in range(n):
        k, i = text(data, i)
        v, i = text(data, i)
        out[k] = v
    return out


def test_encode_map_str_roundtrip():
    body = encode_map_str({"__type": "ThrottlingException", "message": "slow down"})
    assert _decode_cbor_map(body) == {
        "__type": "ThrottlingException",
        "message": "slow down",
    }


def test_cbor_error_render():
    status, headers, body = render_error(
        "cloudwatch", "ThrottlingException", "Rate exceeded"
    )
    assert status == 400
    assert headers["smithy-protocol"] == "rpc-v2-cbor"
    assert headers["Content-Type"] == "application/cbor"
    decoded = _decode_cbor_map(body)
    assert decoded["__type"] == "ThrottlingException"


# -- detection ------------------------------------------------------------


def test_parse_rpcv2_path():
    assert parse_rpcv2_path("/service/GraniteServiceVersion20100801/operation/PutMetricData") == (
        "GraniteServiceVersion20100801",
        "PutMetricData",
    )
    assert parse_rpcv2_path("/other/path") is None
    assert parse_rpcv2_path("/") is None


def _sigv4_headers(scope: str) -> dict:
    return {
        "Authorization": (
            "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/"
            f"{scope}/aws4_request, SignedHeaders=host, Signature=abc"
        )
    }


def test_detect_cbor_service_and_operation():
    headers = _sigv4_headers("monitoring")
    headers["smithy-protocol"] = "rpc-v2-cbor"
    ctx = detect(
        headers, "POST",
        "/service/GraniteServiceVersion20100801/operation/PutMetricData",
        {}, None,
    )
    assert ctx.service == "cloudwatch"
    assert ctx.operation == "PutMetricData"
    assert ctx.region == "us-east-1"


def test_detect_target_prefix_disambiguates_shared_scope():
    # dynamodbstreams signs with the "dynamodb" scope; the X-Amz-Target
    # prefix pins the exact service.
    headers = _sigv4_headers("dynamodb")
    headers["X-Amz-Target"] = "DynamoDBStreams_20120810.GetRecords"
    ctx = detect(headers, "POST", "/", {}, None)
    assert ctx.service == "dynamodbstreams"
    assert ctx.operation == "GetRecords"


def test_detect_eventbridgev2_not_plain_events():
    headers = _sigv4_headers("events")
    headers["smithy-protocol"] = "rpc-v2-cbor"
    ctx = detect(
        headers, "POST", "/service/AWSEventsV2/operation/PutEvents", {}, None
    )
    assert ctx.service == "eventbridgev2"


# -- end-to-end: boto3 cloudwatch through the proxy -----------------------


def _cw_client(url, **cfg):
    return boto3.client(
        "cloudwatch", endpoint_url=url, region_name="us-east-1", **cfg
    )


def test_cbor_throttle_injected_and_retried(upstub, microburst_server, aws_env):
    stub, server = upstub
    rules = [
        {
            "service": "cloudwatch",
            "operation": "PutMetricData",
            "error": {"code": "ThrottlingException", "message": "Rate exceeded"},
            "times": 1,
        }
    ]
    sq, mb = microburst_server(server.url, rules)
    client = _cw_client(mb.url)

    client.put_metric_data(
        Namespace="test", MetricData=[{"MetricName": "m", "Value": 1.0}]
    )  # retried internally, then succeeds

    assert stub.count() == 1  # one fault + one passthrough
    fired = list(sq.fired)
    assert len(fired) == 1
    assert fired[0].service == "cloudwatch"
    assert fired[0].operation == "PutMetricData"


def test_cbor_terminal_error_parses_as_clienterror(
    upstub, microburst_server, aws_env
):
    _, server = upstub
    rules = [
        {
            "service": "cloudwatch",
            "operation": "PutMetricData",
            "error": {"code": "AccessDeniedException"},
        }
    ]
    _, mb = microburst_server(server.url, rules)
    client = _cw_client(
        mb.url, config=Config(retries={"total_max_attempts": 1})
    )

    with pytest.raises(ClientError) as exc:
        client.put_metric_data(
            Namespace="test", MetricData=[{"MetricName": "m", "Value": 1.0}]
        )
    assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
