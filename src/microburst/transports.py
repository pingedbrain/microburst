"""Transport registry — the seam ``cli.py`` dispatches ``--protocol``
through.

An entry describes one data-plane transport. ``http`` is the built-in
pipeline (``run=None`` — cli wires ``Microburst`` + ``make_app``
itself). Every other entry is a **TCP sibling transport**: a lazily
imported ``run_*`` callable owning an asyncio listener, a per-connection
handler, and the same decision plane (``RuleEngine`` / ``emit_fired`` /
control API / stats).

The contract a TCP transport implements (pg/redis are the reference):

- ``run_<name>(host, port, upstream_host, upstream_port, control_port,
  rules, watch_config=None, **options) -> int`` — blocks until shutdown;
  ``options`` are the transport's declared ``options`` keys resolved
  from ``--<flag>`` or the config file.
- A package with codec / detector / handler modules (e.g.
  ``microburst.pg.{proto,detect,server}``).
- A ``service:`` name for rule matching (``postgres``, ``redis``,
  ``tcp``) stamped on every ``RequestContext`` and ``FiredEvent``.
- A ``*Proxy`` state object exposing the surface ``control.py`` reads:
  ``engine`` / ``fired`` / ``fault_counts`` / ``stats`` /
  ``requests_seen`` / ``_listeners`` / ``upstream`` /
  ``subscribe_fired`` / ``handle_control``.

Registry fields:

- ``run`` — ``"module.path:callable"``, imported on first use. ``None``
  marks the built-in HTTP pipeline.
- ``default_port`` — listen port when ``--port``/config are unset;
  ``None`` means derived at runtime (tcp: upstream port + 10000, the
  same convention pg/redis encode statically).
- ``default_upstream`` — raw ``--upstream`` default; ``None`` means the
  flag/config is required.
- ``schemes`` — URL schemes accepted in ``--upstream`` (empty = any);
  ``upstream_port`` — default port inside a bare ``host:port`` parse
  (``None`` = explicit port required).
- ``options`` — extra config keys cli forwards to ``run_*`` as kwargs
  (e.g. tcp's ``framing``).
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass


@dataclass(frozen=True)
class Transport:
    name: str
    help: str
    run: str | None                  # "pkg.mod:func" — None = builtin http
    default_port: int | None
    default_upstream: str | None
    schemes: tuple[str, ...] = ()
    upstream_port: int | None = None
    options: tuple[str, ...] = ()

    def load(self):
        """Import and return the ``run_*`` callable."""
        assert self.run is not None
        module, _, func = self.run.rpartition(":")
        return getattr(importlib.import_module(module), func)


TRANSPORTS: dict[str, Transport] = {
    "http": Transport(
        name="http",
        help="HTTP/1.1 data plane — the AWS failure-injection pipeline "
        "(detection → decide → effect-or-forward, SigV4 re-signing).",
        run=None,
        default_port=9999,
        default_upstream="http://localhost:4566",
    ),
    "postgres": Transport(
        name="postgres",
        help="PostgreSQL v3 wire proxy — SQLSTATE-correct errors, "
        "tx-aware injection, partial_rows.",
        run="microburst.pg.server:run_postgres",
        default_port=15432,
        default_upstream="localhost:5432",
        schemes=("postgres", "postgresql"),
        upstream_port=5432,
    ),
    "redis": Transport(
        name="redis",
        help="RESP2+RESP3 wire proxy — -CODE errors, MULTI-aware "
        "skipping, cut_reply.",
        run="microburst.redis.server:run_redis",
        default_port=16379,
        default_upstream="localhost:6379",
        schemes=("redis",),
        upstream_port=6379,
    ),
    "mysql": Transport(
        name="mysql",
        help="MySQL/MariaDB wire proxy — errno+SQLSTATE ERR_Packet "
        "injection, tx-aware skipping, auth passthrough, partial_rows.",
        run="microburst.mysql.server:run_mysql",
        default_port=13306,
        default_upstream="localhost:3306",
        schemes=("mysql", "mariadb"),
        upstream_port=3306,
    ),
    "tcp": Transport(
        name="tcp",
        help="generic byte-stream proxy for protocols without a "
        "dedicated module — transport faults only (no protocol-correct "
        "errors), optional --framing.",
        run="microburst.tcp.server:run_tcp",
        default_port=None,          # derived: upstream_port + 10000
        default_upstream=None,      # required
        schemes=(),                 # any scheme accepted
        upstream_port=None,         # explicit port required
        options=("framing",),
    ),
}
