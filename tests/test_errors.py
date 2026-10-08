"""Error serialization tests."""

import json
import xml.etree.ElementTree as ET

from microburst.protocols import render_error


def test_json_protocol_dynamodb():
    status, _headers, body = render_error(
        "dynamodb", "ProvisionedThroughputExceededException",
        "Rate exceeded", None,
    )
    assert status == 400  # modeled httpStatusCode
    # Live AWS: json services carry the code in __type only — DynamoDB
    # namespaces it com.amazonaws.dynamodb.v20120810#, no x-amzn-ErrorType.
    payload = json.loads(body)
    assert payload["__type"] == (
        "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException"
    )
    assert payload["message"] == "Rate exceeded"


def test_query_protocol_xml():
    _status, headers, body = render_error(
        "sns", "Throttling", "Rate exceeded", 400,
    )
    assert headers["Content-Type"] == "text/xml"
    root = ET.fromstring(body)
    assert root.findtext("Error/Code") == "Throttling"
    assert root.find("RequestId") is not None


def test_rest_xml_s3():
    status, headers, body = render_error("s3", "SlowDown", "Slow down", 503)
    assert status == 503
    assert "x-amz-request-id" in headers
    assert "x-amz-id-2" in headers
    root = ET.fromstring(body)
    assert root.findtext("Code") == "SlowDown"


def test_rest_json_uses_errortype_header():
    _status, headers, body = render_error(
        "apigateway", "TooManyRequestsException", "", 429,
    )
    # Live AWS sends the camelCase x-amzn-ErrorType on rest-json.
    assert headers["x-amzn-ErrorType"] == "TooManyRequestsException"
    assert json.loads(body)["message"] == "TooManyRequestsException"


def test_modeled_status_used_when_not_given():
    # ProvisionedThroughputExceededException is modeled at 400, not 429.
    status, _, _ = render_error(
        "dynamodb", "ProvisionedThroughputExceededException", "", None
    )
    assert status == 400


def test_unmodeled_throttling_code_gets_retryable_status():
    # SlowDown is not shaped in the s3 model; AWS serves it as 503. A 4xx
    # here would make the SDK treat the fault as a terminal client error.
    status, _, _ = render_error("s3", "SlowDown", "", None)
    assert status == 503


def test_unknown_service_falls_back_generic_json():
    status, _headers, body = render_error("no-such-svc", "InternalError", "", 500)
    assert status == 500
    assert json.loads(body)["__type"] == "InternalError"


def test_json_escaping():
    _, _, body = render_error("dynamodb", "X", 'msg "quoted"', 400)
    assert json.loads(body)["message"] == 'msg "quoted"'
