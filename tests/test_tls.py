"""TLS termination: ``--tls-cert``/``--tls-key`` listeners and in-band
STARTTLS-style upgrades.

pg and mysql upgrade mid-connection (SSLRequest→'S' / CLIENT_SSL
handshake response); redis, grpc and tcp listen TLS-from-connect; http
becomes an https listener. Test certs are minted with the openssl
binary — self-signed, test-only material (skipped when openssl is
unavailable).
"""

from __future__ import annotations

import json
import shutil
import socket
import ssl
import subprocess
import urllib.request

import pytest
from h2.events import StreamEnded
from test_grpc_server import GrpcClient, GrpcStack, response_summary
from test_mysql_server import (
    MysqlStack,
    _handshake_response,
    mysql_connect,
    read_packet_sync,
)
from test_pg_server import PgStack, pg_query, read_until_z
from test_resp_server import (
    RedisStack,
    command_wire,
    read_reply_sync,
    recv_until_dead,
)
from test_tcp_server import TcpStack, _recv_exact

from microburst.mysql.proto import (
    CLIENT_PROTOCOL_41,
    CLIENT_SSL,
    greeting_capabilities,
    packet_bytes,
)
from microburst.pg.proto import (
    GSSENC_REQUEST_CODE,
    PROTOCOL_V3,
    SSL_REQUEST_CODE,
    startup_packet,
)
from microburst.tls import server_ssl_context


@pytest.fixture(scope="session")
def tls_pair(tmp_path_factory):
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("openssl binary required to mint test certs")
    d = tmp_path_factory.mktemp("tls")
    cert, key = str(d / "cert.pem"), str(d / "key.pem")
    subprocess.run(
        [
            openssl, "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", key, "-out", cert,
            "-days", "2", "-nodes", "-subj", "/CN=localhost",
        ],
        check=True, capture_output=True,
    )
    return cert, key


@pytest.fixture
def stacks():
    """Start a wire-mode stack and guarantee its teardown."""
    started = []

    def _start(stack):
        stack.start()
        started.append(stack)
        return stack

    yield _start
    for stack in started:
        stack.stop()


def _client_ctx() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _wrap(sock: socket.socket) -> ssl.SSLSocket:
    return _client_ctx().wrap_socket(sock, server_hostname="localhost")


def _connect(port: int) -> socket.socket:
    sock = socket.create_connection(("127.0.0.1", port), timeout=10)
    sock.settimeout(10)
    return sock


# --- context + cli validation ------------------------------------------------------

def test_context_requires_the_pair():
    assert server_ssl_context(None, None) is None
    with pytest.raises(ValueError):
        server_ssl_context("cert.pem", None)
    with pytest.raises(ValueError):
        server_ssl_context(None, "key.pem")


def test_context_loads_pair(tls_pair):
    assert server_ssl_context(*tls_pair) is not None


def test_cli_rejects_lone_tls_flag(tls_pair):
    from microburst.cli import main

    cert, key = tls_pair
    assert main([
        "--protocol", "redis", "--upstream", "h:1",
        "--tls-cert", cert,
    ]) == 2
    assert main([
        "--protocol", "redis", "--upstream", "h:1",
        "--tls-key", key,
    ]) == 2


# --- postgres: SSLRequest → 'S' + in-band upgrade ---------------------------------

def test_pg_sslrequest_upgrades_to_tls(tls_pair, stacks):
    stack = stacks(PgStack([], ssl_ctx=server_ssl_context(*tls_pair)))
    sock = _connect(stack.port)
    sock.sendall(startup_packet(SSL_REQUEST_CODE))
    assert sock.recv(1) == b"S"
    tls = _wrap(sock)
    tls.sendall(
        startup_packet(PROTOCOL_V3, {"user": "t", "database": "d"})
    )
    assert read_until_z(tls)[-1][0] == b"Z"
    # queries inside the tunnel round-trip end to end
    msgs = pg_query(tls, "select 1")
    assert msgs[-1] == (b"Z", b"I")
    assert stack.queries == ["select 1"]
    tls.close()


def test_pg_gssenc_still_refused_inside_tls(tls_pair, stacks):
    stack = stacks(PgStack([], ssl_ctx=server_ssl_context(*tls_pair)))
    sock = _connect(stack.port)
    sock.sendall(startup_packet(SSL_REQUEST_CODE))
    assert sock.recv(1) == b"S"
    tls = _wrap(sock)
    # GSSENC has nothing to terminate into — 'N' inside the tunnel
    tls.sendall(startup_packet(GSSENC_REQUEST_CODE))
    assert tls.recv(1) == b"N"
    tls.sendall(
        startup_packet(PROTOCOL_V3, {"user": "t", "database": "d"})
    )
    assert read_until_z(tls)[-1][0] == b"Z"
    tls.close()


# --- mysql: CLIENT_SSL handshake response → in-band upgrade -----------------------

def test_mysql_sslrequest_upgrades_to_tls(tls_pair, stacks):
    stack = stacks(MysqlStack([], ssl_ctx=server_ssl_context(*tls_pair)))
    sock = _connect(stack.port)
    msg = read_packet_sync(sock)
    assert msg is not None
    _seq, greeting = msg
    # with a TLS context the greeting keeps CLIENT_SSL advertised
    assert greeting_capabilities(greeting) & CLIENT_SSL
    # SSLRequest = the short caps head — TLS starts on the next packet
    sock.sendall(
        packet_bytes(
            (CLIENT_SSL | CLIENT_PROTOCOL_41).to_bytes(4, "little"), 1
        )
    )
    tls = _wrap(sock)
    tls.sendall(packet_bytes(_handshake_response(True), 1))
    reply = read_packet_sync(tls)
    assert reply is not None and reply[1][0] == 0x00  # auth OK
    tls.close()


def test_mysql_plaintext_client_still_works_with_tls_ctx(
    tls_pair, stacks
):
    # CLIENT_SSL advertised but the client declines → plain handshake
    stack = stacks(MysqlStack([], ssl_ctx=server_ssl_context(*tls_pair)))
    sock, greeting = mysql_connect(stack.port)
    assert greeting_capabilities(greeting) & CLIENT_SSL
    sock.close()


# --- redis: TLS-from-connect listener ----------------------------------------------

def test_rediss_listener_roundtrip(tls_pair, stacks):
    stack = stacks(RedisStack([], ssl_ctx=server_ssl_context(*tls_pair)))
    tls = _wrap(_connect(stack.port))
    tls.sendall(command_wire("PING"))
    assert read_reply_sync(tls) == (b"+", b"PONG")
    tls.close()


def test_rediss_listener_rejects_plaintext(tls_pair, stacks):
    stack = stacks(RedisStack([], ssl_ctx=server_ssl_context(*tls_pair)))
    sock = _connect(stack.port)
    sock.settimeout(5)
    sock.sendall(command_wire("PING"))
    # a TLS listener answers plaintext with a handshake failure + close
    assert b"PONG" not in recv_until_dead(sock)
    sock.close()


# --- grpc: TLS-from-connect listener ------------------------------------------------

def test_grpcs_listener_passthrough(tls_pair, stacks):
    stack = stacks(
        GrpcStack(ssl_ctx=server_ssl_context(*tls_pair), reply_msgs=1)
    )
    client = GrpcClient(stack.port, tls_ctx=_client_ctx())
    sid = client.call("/pkg.Svc/Method")
    client.pump(stop=lambda e: isinstance(e, StreamEnded))
    summary = response_summary(client, sid)
    assert summary["status"] == "200"
    assert summary["grpc_status"] == "0"
    assert summary["ended"]
    client.close()


# --- tcp: TLS-from-connect listener --------------------------------------------------

def test_tcps_listener_echo(tls_pair, stacks):
    stack = stacks(TcpStack([], ssl_ctx=server_ssl_context(*tls_pair)))
    tls = _wrap(_connect(stack.port))
    tls.sendall(b"ping")
    assert _recv_exact(tls, 6) == b"R:ping"
    tls.close()


# --- http: https listener --------------------------------------------------------------

def test_https_listener_proxies(tls_pair, upstub):
    from conftest import ServerThread

    from microburst.app import make_app
    from microburst.core.pipeline import Microburst

    stub, upstream = upstub
    sq = Microburst(upstream.url, rules=[], resign=False)
    server = ServerThread(
        make_app(sq), ssl_ctx=server_ssl_context(*tls_pair)
    ).start()
    try:
        ctx = _client_ctx()
        with urllib.request.urlopen(
            f"https://127.0.0.1:{server.port}/_microburst/health",
            context=ctx, timeout=5,
        ) as resp:
            assert json.loads(resp.read())["status"] == "ok"
        with urllib.request.urlopen(
            f"https://127.0.0.1:{server.port}/anything",
            context=ctx, timeout=5,
        ) as resp:
            assert resp.status == 200
        assert stub.count() == 1
    finally:
        server.stop()
