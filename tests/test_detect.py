"""Detection unit tests."""

import json

from microburst.detection import detect

AUTH = (
    "AWS4-HMAC-SHA256 Credential=AKIATEST/20261007/us-east-1/{scope}"
    "/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc"
)


def _auth(scope):
    return {"Authorization": AUTH.format(scope=scope)}


def test_json_protocol_via_target():
    headers = {**_auth("dynamodb"), "X-Amz-Target": "DynamoDB_20120810.PutItem"}
    body = json.dumps({"TableName": "orders", "Item": {}}).encode()
    info = detect(headers, "POST", "/", {}, body)
    assert info.service == "dynamodb"
    assert info.operation == "PutItem"
    assert info.region == "us-east-1"
    assert info.resource == "orders"


def test_query_protocol_action_in_body():
    headers = _auth("sqs")
    body = b"Action=SendMessage&QueueUrl=http%3A%2F%2Fx%2F123%2Fjobs&MessageBody=hi"
    info = detect(headers, "POST", "/", {}, body)
    assert info.service == "sqs"
    assert info.operation == "SendMessage"


def test_query_protocol_action_in_query_string():
    headers = _auth("sns")
    info = detect(headers, "GET", "/", {"Action": "Publish"}, None)
    assert info.service == "sns"
    assert info.operation == "Publish"


def test_scope_alias_monitoring_maps_cloudwatch():
    headers = _auth("monitoring")
    body = b"Action=PutMetricData&Namespace=x"
    info = detect(headers, "POST", "/", {}, body)
    assert info.service == "cloudwatch"
    assert info.operation == "PutMetricData"


def test_s3_operation_from_path():
    headers = _auth("s3")
    info = detect(headers, "PUT", "/mybucket/some/key.txt", {}, None)
    assert info.service == "s3"
    assert info.operation == "PutObject"
    assert info.resource == "mybucket"


def test_s3_get_object_tagging_like_ops():
    headers = _auth("s3")
    info = detect(headers, "GET", "/b/k", {}, None)
    assert info.operation == "GetObject"


def test_lambda_invoke_rest_match():
    headers = _auth("lambda")
    info = detect(
        headers, "POST", "/2015-03-31/functions/fn/invocations", {}, b"{}"
    )
    assert info.service == "lambda"
    assert info.operation == "Invoke"


def test_unsigned_request_no_service():
    info = detect({}, "GET", "/foo", {}, None)
    assert info.service is None
    assert info.operation is None
