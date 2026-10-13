"""gRPC wire protocol — a sibling transport to the HTTP pipeline.

gRPC rides HTTP/2, so this mode is a **h2c (cleartext HTTP/2) proxy**
built on the ``h2`` state machine: one ``H2Connection`` facing the
client (server-side config), one facing the upstream (client-side
config), with streams mapped between them. Like the other transports it
does not travel ``core/pipeline.py`` — it shares the decision plane:
one ``RequestContext`` per RPC call (``service="grpc"``, ``operation``
= the lowercased ``:path``) through the same ``RuleEngine``, fired log,
stats and control API.

- ``proto`` — thin helpers over ``h2.connection.H2Connection``:
  connection factories, the 17 canonical gRPC status codes, the
  ``grpc-message`` percent-encoding, trailers-only / trailers header
  builders, chunked ``send_data``.
- ``server`` — asyncio h2c proxy: prior-knowledge h2 on both legs,
  per-RPC decisions at request HEADERS, trailers-only / mid-stream
  error injection, RST_STREAM aborts, stalls with real flow-control
  backpressure, ``run_grpc`` entry point.
"""
