"""Redis wire protocol — a third sibling transport after HTTP and pg.

RESP is a long-lived request→reply framed stream, not one-request →
one-response, so it does not travel ``core/pipeline.py``. What the
transports share is the decision plane: this package builds a
``RequestContext`` per command and calls the same ``RuleEngine``,
fired log, stats and control API as HTTP and pg modes.

- ``proto`` — RESP2+RESP3 framing: command frames (inline + multibulk)
  and recursive reply values, both directions
- ``errors`` — ``-CODE message`` rendering, cluster-redirect tails
- ``detect`` — verb / first-key extraction for ``operation``/``resource``
- ``server`` — asyncio TCP proxy: per-command fault decisions, MULTI-
  aware error skipping, push-mode passthrough, response relay
"""
