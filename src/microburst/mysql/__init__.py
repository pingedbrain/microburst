"""MySQL wire protocol — a sibling transport to the HTTP pipeline.

MySQL is a long-lived framed stream (3-byte LE length + 1-byte sequence
id per packet), not one-request → one-response, so it does not travel
``core/pipeline.py``. What the transports share is the decision plane:
this package builds a ``RequestContext`` per command and calls the same
``RuleEngine``, fired log, stats and control API as HTTP/pg/redis mode.

- ``proto`` — packet codec: 3B LE length + seq framing, multi-packet
  reassembly, capability/status constants, greeting + handshake-
  response parsers, reply classification, test-side packet builders
- ``errors`` — ERR_Packet rendering (``0xFF errno '#' sqlstate msg``)
  with the errno↔sqlstate defaults map
- ``detect`` — command names, SQL verb/table extraction (reuses pg's
  ``sql_facts``), per-command reply shapes
- ``server`` — asyncio TCP proxy: greeting relay with TLS/compression
  capability stripping, auth passthrough, per-command fault decisions,
  tx-aware error injection, ResultSet relay (multi-statement aware)
"""
