"""PostgreSQL wire protocol — a sibling transport to the HTTP pipeline.

PG is a long-lived bidirectional framed stream, not one-request →
one-response, so it does not travel ``core/pipeline.py``. What the two
transports share is the decision plane: this package builds a
``RequestContext`` per query unit and calls the same ``RuleEngine``,
fired log, stats and control API as HTTP mode.

- ``proto`` — v3 framing: startup packet + typed frames, both directions
- ``errors`` — ErrorResponse / NoticeResponse rendering
- ``detect`` — SQL verb / table extraction for ``service``/``operation``
- ``server`` — asyncio TCP proxy: startup negotiation, auth passthrough,
  per-query-unit fault decisions, response relay
"""
