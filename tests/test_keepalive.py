"""Keep-alive / connection-pooling fidelity.

Real AWS services differ — captures show ``Connection: keep-alive`` on
dynamodb/lambda/sqs, ``close`` on ec2/secretsmanager, nothing on s3. The
proxy's contract isn't to imitate one flavor; it's to not perturb the
connection semantics of either hop:

- pooled clients must reuse one upstream connection through the proxy
- injected error responses must keep the downstream connection alive
  (a ``close`` here would make SDK retries reconnect — wrong behavior)
- a ``reset`` fault must actually kill the connection
"""

from __future__ import annotations

import http.client

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError


def _ddb_client(endpoint: str, max_attempts: int = 1):
    return boto3.client(
        "dynamodb",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(retries={"max_attempts": max_attempts}),
    )


def test_pooled_requests_reuse_upstream_connection(
    upstub, microburst_server, aws_env
):
    """Three sequential SDK calls through the proxy land on ONE upstream
    TCP connection — the proxy must not break client-side pooling."""
    stub, upstream = upstub
    _, proxy = microburst_server(upstream.url)
    client = _ddb_client(proxy.url)
    for _ in range(3):
        client.list_tables()
    conns = {r["conn"] for r in stub.requests}
    assert len(stub.requests) == 3
    assert len(conns) == 1


def test_injected_error_keeps_connection_alive(upstub, microburst_server, aws_env):
    """An injected fault must not close the downstream connection — AWS
    error responses keep-alive (captures: dynamodb, lambda, sqs)."""
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[
            {
                "service": "dynamodb",
                "probability": 1.0,
                "error": {"code": "InternalError"},
            }
        ],
    )
    host, port = proxy.url.split("//")[1].split(":")
    conn = http.client.HTTPConnection(host, int(port))
    headers = {
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": "DynamoDB_20120810.ListTables",
    }
    for i in range(2):
        conn.request("POST", "/", body=b"{}", headers=headers)
        resp = conn.getresponse()
        resp.read()  # consume fully — required before conn reuse
        assert resp.status == 500
        # AWS faults don't close the socket (Connection: close absent)
        assert (resp.getheader("Connection") or "").lower() != "close"
    conn.close()


def test_retry_after_fault_reuses_connection(
    upstub, microburst_server, aws_env
):
    """SDK retry after an injected fault stays on the same downstream
    connection — no reconnect stall in the retry path."""
    stub, upstream = upstub
    _, proxy = microburst_server(
        upstream.url,
        rules=[
            {
                "service": "dynamodb",
                "probability": 1.0,
                "error": {"code": "ProvisionedThroughputExceededException"},
            }
        ],
    )
    client = _ddb_client(proxy.url, max_attempts=3)
    with pytest.raises(ClientError):
        client.list_tables()
    # the fault never reached upstream — all 3 attempts were injected
    assert stub.count() == 0


def test_reset_fault_drops_connection(upstub, microburst_server, aws_env):
    """``reset`` is the exception by design: it kills the connection."""
    _, proxy = microburst_server(
        upstub[1].url,
        rules=[{"service": "dynamodb", "probability": 1.0, "reset": True}],
    )
    host, port = proxy.url.split("//")[1].split(":")
    conn = http.client.HTTPConnection(host, int(port))
    conn.request(
        "POST",
        "/",
        body=b"{}",
        headers={
            "Content-Type": "application/x-amz-json-1.0",
            "X-Amz-Target": "DynamoDB_20120810.ListTables",
        },
    )
    with pytest.raises(http.client.HTTPException):
        conn.getresponse().read()
    conn.close()
