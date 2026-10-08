"""Cassette record/replay: capture upstream responses, replay + inject."""

from __future__ import annotations

import urllib.error
import urllib.request

import pytest
from botocore.exceptions import ClientError
from test_proxy import _ddb, _put_item

from microburst.cassette import Cassette


def test_record_then_replay(upstub, microburst_server, aws_env, tmp_path):
    stub, upstream = upstub
    cass = Cassette(tmp_path / "cass", "record")
    _, proxy = microburst_server(upstream.url, cassette=cass)

    resp = _put_item(_ddb(proxy.url))
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert stub.count() == 1
    entries = list((tmp_path / "cass").glob("*.json"))
    assert len(entries) == 1

    # replay against a dead upstream — proves nothing was forwarded
    replay_cass = Cassette(tmp_path / "cass", "replay")
    _, rproxy = microburst_server("http://127.0.0.1:1", cassette=replay_cass)
    resp2 = _put_item(_ddb(rproxy.url, max_attempts=1))
    assert resp2["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_replay_still_injects_faults(upstub, microburst_server, aws_env, tmp_path):
    stub, upstream = upstub
    cass = Cassette(tmp_path / "cass", "record")
    _, proxy = microburst_server(upstream.url, cassette=cass)
    _put_item(_ddb(proxy.url))
    assert stub.count() == 1

    replay_cass = Cassette(tmp_path / "cass", "replay")
    _, rproxy = microburst_server(
        "http://127.0.0.1:1",
        cassette=replay_cass,
        rules=[{
            "service": "dynamodb",
            "error": {"code": "ThrottlingException"},
        }],
    )
    with pytest.raises(ClientError) as exc:
        _put_item(_ddb(rproxy.url, max_attempts=1))
    assert exc.value.response["Error"]["Code"] == "ThrottlingException"


def test_replay_miss_returns_503(microburst_server, aws_env, tmp_path):
    cass = Cassette(tmp_path / "empty", "replay")
    _, proxy = microburst_server("http://127.0.0.1:1", cassette=cass)
    req = urllib.request.Request(
        f"{proxy.url}/", data=b"{}", method="POST",
        headers={
            "Content-Type": "application/x-amz-json-1.0",
            "X-Amz-Target": "DynamoDB_20120810.PutItem",
        },
    )
    with pytest.raises(urllib.error.HTTPError) as exc:
        urllib.request.urlopen(req)
    assert exc.value.code == 503


def test_cassette_key_ignores_headers():
    k1 = Cassette.key_for("POST", "/x?a=1", b"body")
    k2 = Cassette.key_for("POST", "/x?a=1", b"body")
    k3 = Cassette.key_for("POST", "/x?a=1", b"other")
    assert k1 == k2
    assert k1 != k3
