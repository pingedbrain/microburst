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


def test_rest_json_body_disambiguates_tag_untag():
    headers = _auth("chime-sdk-identity")
    tag = detect(
        headers, "POST", "/tags", {},
        b'{"ResourceARN":"arn:x","Tags":[{"Key":"a","Value":"b"}]}',
    )
    untag = detect(
        headers, "POST", "/tags", {},
        b'{"ResourceARN":"arn:x","TagKeys":["a"]}',
    )
    assert tag.operation == "TagResource"
    assert untag.operation == "UntagResource"


def test_rest_xml_body_disambiguates_s3_bucket_puts():
    headers = _auth("s3")
    info = detect(
        headers, "PUT", "/mybucket", {},
        b'<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        b"<TagSet/></Tagging>",
    )
    assert info.operation == "PutBucketTagging"


def test_rest_xml_lifecycle_root():
    headers = _auth("s3")
    info = detect(
        headers, "PUT", "/mybucket", {},
        b"<LifecycleConfiguration><Rule/></LifecycleConfiguration>",
    )
    assert info.operation in ("PutBucketLifecycle", "PutBucketLifecycleConfiguration")


def test_rest_raw_body_favors_payload_op():
    # ImportApiKeys needs the ?mode=import marker, but a raw CSV body must
    # never resolve to a structure-demanding op. Real SDKs also send the
    # required `format` query member.
    headers = _auth("apigateway")
    info = detect(
        headers, "POST", "/apikeys",
        {"mode": "import", "format": "csv"}, b"key1,key2"
    )
    assert info.operation == "ImportApiKeys"


def test_rest_json_required_keys_disambiguate_sso_oidc():
    headers = _auth("sso-oidc")
    info = detect(
        headers, "POST", "/token", {},
        b'{"assertion":"a","clientId":"c","grantType":"g",'
        b'"subjectToken":"s","subjectTokenType":"t"}',
    )
    assert info.operation == "CreateTokenWithIAM"


def _s3_auth():
    return {"Authorization": AUTH.format(scope="s3")}


def test_virtual_hosted_s3_get_object():
    headers = {**_s3_auth(), "Host": "mybucket.s3.us-east-1.amazonaws.com"}
    info = detect(headers, "GET", "/photos/cat.jpg", {}, None)
    assert info.service == "s3"
    assert info.operation == "GetObject"
    assert info.resource == "mybucket"


def test_virtual_hosted_s3_bucket_level_op():
    headers = {**_s3_auth(), "Host": "mybucket.s3.us-west-2.amazonaws.com"}
    info = detect(headers, "GET", "/", {"acl": ""}, None)
    assert info.service == "s3"
    assert info.operation == "GetBucketAcl"
    assert info.resource == "mybucket"


def test_virtual_hosted_emulator_host():
    # boto3 virtual addressing against a local endpoint:
    # endpoint_url=localhost + addressing_style=virtual → bucket.localhost
    headers = {**_s3_auth(), "Host": "mybucket.localhost:4566"}
    info = detect(headers, "GET", "/key.txt", {}, None)
    assert info.service == "s3"
    assert info.operation == "GetObject"
    assert info.resource == "mybucket"


def test_path_style_s3_unaffected():
    headers = {**_s3_auth(), "Host": "s3.us-east-1.amazonaws.com"}
    info = detect(headers, "GET", "/mybucket/photos/cat.jpg", {}, None)
    assert info.service == "s3"
    assert info.operation == "GetObject"
    assert info.resource == "mybucket"


def test_s3control_account_host_prefix():
    headers = {
        "Authorization": AUTH.format(scope="s3"),
        "Host": "123456789012.s3-control.us-east-1.amazonaws.com",
    }
    info = detect(
        headers, "GET", "/v20180820/jobs", {}, None
    )
    assert info.service == "s3control"


def test_host_fills_service_for_unsigned_request():
    headers = {"Host": "dynamodb.us-east-1.amazonaws.com"}
    info = detect(headers, "POST", "/", {}, None)
    assert info.service == "dynamodb"
    assert info.region == "us-east-1"


def test_untrusted_host_does_not_fabricate_service():
    headers = {"Host": "api.logs.datadoghq.com"}
    info = detect(headers, "GET", "/x", {}, None)
    assert info.service is None


def test_proxy_host_yields_nothing():
    headers = {**_s3_auth(), "Host": "127.0.0.1:9999"}
    info = detect(headers, "GET", "/b/k", {}, None)
    assert info.service == "s3"
    assert info.region == "us-east-1"
    assert info.resource == "b"


def test_s3_trailing_slash_is_bucket_level_op():
    # aws-sdk-js-v3 sends HEAD /bucket/ for HeadBucket — the greedy {Key+}
    # must not swallow the empty segment and match HeadObject instead.
    headers = {**_s3_auth(), "Host": "s3.us-east-1.amazonaws.com"}
    info = detect(headers, "HEAD", "/mybucket/", {}, None)
    assert info.operation == "HeadBucket"


def test_s3_key_ending_in_slash_still_resolves_object():
    headers = {**_s3_auth(), "Host": "s3.us-east-1.amazonaws.com"}
    info = detect(headers, "GET", "/mybucket/folder/", {}, None)
    assert info.operation == "GetObject"
