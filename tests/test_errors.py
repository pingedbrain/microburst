"""Error serialization tests."""

import json
import xml.etree.ElementTree as ET

from microburst.protocols import render_error


def test_json_protocol_dynamodb():
    status, headers, body = render_error(
        "dynamodb", "ProvisionedThroughputExceededException",
        "Rate exceeded", None,
    )
    assert status == 400  # modeled httpStatusCode
    assert headers["x-amzn-ErrorType"] == "ProvisionedThroughputExceededException"
    payload = json.loads(body)
    assert payload["__type"] == "ProvisionedThroughputExceededException"
    assert payload["message"] == "Rate exceeded"


def test_query_protocol_xml():
    _status, headers, body = render_error(
        "sns", "Throttling", "Rate exceeded", 400,
    )
    assert headers["Content-Type"] == "text/xml"
    root = ET.fromstring(body)
    assert root.find("Error/Code").text == "Throttling"
    assert root.find("RequestId") is not None


def test_rest_xml_s3():
    status, headers, body = render_error("s3", "SlowDown", "Slow down", 503)
    assert status == 503
    assert "x-amz-request-id" in headers
    assert "x-amz-id-2" in headers
    root = ET.fromstring(body)
    assert root.find("Code").text == "SlowDown"


def test_rest_json_uses_errortype_header():
    _status, headers, body = render_error(
        "apigateway", "TooManyRequestsException", "", 429,
    )
    assert headers["x-amzn-errortype"] == "TooManyRequestsException"
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
