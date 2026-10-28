"""TLS termination helpers — user-provided cert/key only.

microburst terminates *client-side* TLS so TLS-required clients can
attach to a plaintext local upstream (the same unencrypted dev workflow
every transport already targets — nothing upstream is re-encrypted).
Cert generation, trust stores and upstream re-encryption are out of
scope: the operator supplies the pair via ``--tls-cert``/``--tls-key``
(or the ``tls_cert:``/``tls_key:`` config keys).
"""

from __future__ import annotations

import asyncio
import ssl


def server_ssl_context(
    cert: str | None, key: str | None
) -> ssl.SSLContext | None:
    """Build a server-side context from the cert/key pair, or None.

    The pair is all-or-nothing — a lone half is a config error, not a
    reason to silently stay plaintext.
    """
    if cert is None and key is None:
        return None
    if not cert or not key:
        raise ValueError(
            "--tls-cert and --tls-key must be given together"
        )
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert, key)
    return ctx


async def upgrade_writer_tls(
    writer: asyncio.StreamWriter, ctx: ssl.SSLContext
) -> None:
    """Wrap an accepted client stream in TLS (STARTTLS-style upgrade).

    pg (SSLRequest→'S') and mysql (CLIENT_SSL handshake-response) upgrade
    mid-connection, but asyncio has no public streams API for a
    server-side upgrade — ``StreamWriter.start_tls`` is client-only.
    Drive ``loop.start_tls`` on the stream's own transport+protocol and
    rebind the writer's transport slot; the reader keeps its buffered
    bytes and every subsequent write is encrypted.
    """
    transport = writer.transport
    new_transport = await asyncio.get_running_loop().start_tls(
        transport,
        writer._protocol,  # pyright: ignore[reportAttributeAccessIssue] private — streams expose no public handle
        ctx,
        server_side=True,
    )
    writer._transport = new_transport  # pyright: ignore[reportAttributeAccessIssue] private — same gap
