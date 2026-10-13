"""Unit tests for the grpc proto helpers — code mapping, grpc-message
encoding, header/trailer builders, chunked send_data."""

from __future__ import annotations

from microburst.grpc.proto import (
    decode_message,
    encode_message,
    error_trailers,
    grpc_status,
    headers_dict,
    is_grpc,
    operation_for,
    trailers_only,
)


def test_grpc_status_names():
    assert grpc_status("UNAVAILABLE") == ("14", None)
    assert grpc_status("RESOURCE_EXHAUSTED") == ("8", None)
    assert grpc_status("ok") == ("0", None)          # case-insensitive
    assert grpc_status("unauthenticated") == ("16", None)


def test_grpc_status_numbers():
    assert grpc_status(14) == ("14", None)
    assert grpc_status("8") == ("8", None)
    assert grpc_status(0) == ("0", None)


def test_grpc_status_defaults_and_fallbacks():
    assert grpc_status(None) == ("14", None)         # UNAVAILABLE default
    code, note = grpc_status("BOGUS")
    assert code == "2"
    assert note is not None and "UNKNOWN" in note
    code, note = grpc_status(99)
    assert code == "2"
    assert note is not None and "out of range" in note


def test_encode_message_percent_encoding():
    # spaces and printable ASCII pass through raw
    assert encode_message("rate limit hit") == "rate limit hit"
    # '%' itself must escape
    assert encode_message("100%") == "100%25"
    # control + non-ASCII bytes escape as %HH
    assert encode_message("a\nb") == "a%0Ab"
    assert encode_message("ñ") == "%C3%B1"


def test_decode_message_roundtrip():
    for msg in ("plain text", "100% sure", "tab\there", "ünïcode"):
        assert decode_message(encode_message(msg)) == msg


def test_operation_for():
    assert operation_for("/pkg.Svc/Method") == "pkg.svc/method"
    assert operation_for("/helloworld.Greeter/SayHello") == (
        "helloworld.greeter/sayhello"
    )
    assert operation_for("/bare") == "bare"
    assert operation_for("noslash") == "noslash"


def test_is_grpc():
    assert is_grpc([("content-type", "application/grpc")])
    assert is_grpc([("content-type", "application/grpc+proto")])
    assert is_grpc([("content-type", "application/grpc; charset=utf-8")])
    assert not is_grpc([("content-type", "application/json")])
    assert not is_grpc([("content-type", "text/plain")])
    assert not is_grpc([])


def test_headers_dict_lowercases():
    d = headers_dict([(":Path", "/a/B"), ("X-Thing", "v")])
    assert d == {":path": "/a/B", "x-thing": "v"}


def test_trailers_only_shape():
    hdrs = dict(trailers_only("14", "gone"))
    assert hdrs[":status"] == "200"
    assert hdrs["content-type"] == "application/grpc"
    assert hdrs["grpc-status"] == "14"
    assert hdrs["grpc-message"] == "gone"


def test_error_trailers_shape():
    hdrs = dict(error_trailers("8", "quota"))
    assert ":status" not in hdrs          # trailers, not a fresh response
    assert hdrs["grpc-status"] == "8"
    assert hdrs["grpc-message"] == "quota"


def test_error_trailers_encodes_message():
    hdrs = dict(error_trailers("2", "line\nbreak"))
    assert hdrs["grpc-message"] == "line%0Abreak"


def test_status_numeric_str():
    assert grpc_status("14") == ("14", None)
