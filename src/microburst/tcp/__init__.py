"""Generic TCP byte-stream transport — a fourth sibling after HTTP, pg
and redis, covering wire protocols that have no dedicated module.

There is no codec here on purpose: tcp mode knows nothing about MySQL,
Kafka, Cassandra or Mongo semantics. What it shares with the dedicated
transports is the decision plane — each decision unit (a frame, or the
connection in unframed mode) builds a ``RequestContext``
(``service="tcp"``, ``operation`` = ``c2s:frame`` / ``s2c:frame`` /
``conn``, ``payload`` = unit bytes as latin-1) and goes through the same
``RuleEngine``, ``emit_fired`` log, stats and control API.

Honest scope — transport faults only:

- ``error:`` does NOT apply — rendering a protocol-correct error is the
  dedicated module's job. A rule that sets it fires the event with
  ``note: error has no renderer in tcp mode`` and applies nothing.
- ``respond: {data|hex|base64, then}`` is the escape hatch for
  hand-crafted replies; it writes bytes, it doesn't understand them.

- ``framing`` — optional user-declared segmentation
  (``microburst.framing.parse_framing``), applied per direction
- ``server`` — asyncio TCP proxy: duplex frame/chunk pumps, per-unit
  fault decisions, armed stream faults (cuts/corrupt), socket splice
"""
