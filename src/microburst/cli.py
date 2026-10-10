"""microburst CLI."""

from __future__ import annotations

import argparse
import logging
import sys

from aiohttp import web

from microburst import __version__
from microburst.app import load_config, make_app
from microburst.core.pipeline import Microburst


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="microburst",
        description="AWS failure injection proxy — inject realistic AWS "
        "errors, latency, and connection faults between your app and any "
        "AWS endpoint (MiniStack, moto, or real AWS).",
    )
    parser.add_argument(
        "--version", "-V", action="version", version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "--protocol", choices=["http", "postgres", "redis"], default=None,
        help="data-plane protocol (default: http). 'postgres'/'redis' swap "
        "the proxy listener to a wire-protocol TCP proxy.",
    )
    parser.add_argument(
        "--upstream", "-u", default=None,
        help="upstream endpoint (default: config file or "
        "http://localhost:4566; postgres mode takes host:port, "
        "default localhost:5432; redis mode takes host:port, "
        "default localhost:6379)",
    )
    parser.add_argument(
        "--port", "-p", type=int, default=None,
        help="listen port (default: 9999 http / 15432 postgres / "
        "16379 redis)",
    )
    parser.add_argument(
        "--control-port", type=int, default=None,
        help="control API port in postgres/redis modes (default: 9999; "
        "ignored in http mode where control shares the data port)",
    )
    parser.add_argument(
        "--host", default=None, help="listen host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--config", "-c", default=None,
        help="YAML config file (upstream, port, rules)",
    )
    parser.add_argument(
        "--watch", action="store_true",
        help="reload rules when the --config file changes",
    )
    parser.add_argument(
        "--resign", action="store_true", default=None,
        help="re-sign requests with AWS_* env credentials (auto for "
        "amazonaws.com upstreams)",
    )
    parser.add_argument(
        "--no-resign", action="store_true",
        help="never re-sign, even for amazonaws.com upstreams",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="debug logging",
    )
    parser.add_argument(
        "--http2", action="store_true",
        help="use HTTP/2 for the upstream connection (needs microburst[h2])",
    )
    parser.add_argument(
        "--record", metavar="DIR", default=None,
        help="record upstream responses into DIR (cassette mode)",
    )
    parser.add_argument(
        "--replay", metavar="DIR", default=None,
        help="serve responses from cassette DIR without contacting upstream",
    )
    parser.add_argument(
        "command", nargs="?", choices=["dashboard", "fidelity"], default=None,
        help="'dashboard' launches the TUI (needs microburst[tui]); "
        "'fidelity capture|report' diffs live-AWS wire responses",
    )
    parser.add_argument(
        "--connect", default=None,
        help="microburst instance URL for dashboard mode "
        "(default: http://127.0.0.1:PORT)",
    )
    return parser


def _tcp_upstream(
    raw: str, schemes: tuple[str, ...], default_port: int
) -> tuple[str, int]:
    """Parse a TCP upstream: host[:port] or scheme://host[:port]."""
    from urllib.parse import urlsplit

    if "://" in raw:
        parsed = urlsplit(raw)
        if parsed.scheme not in schemes:
            raise ValueError(
                f"scheme {parsed.scheme!r} is not {'/'.join(schemes)}"
            )
        return parsed.hostname or "localhost", parsed.port or default_port
    host, sep, port = raw.rpartition(":")
    if not sep:
        return raw, default_port
    if not host:
        host = "localhost"
    try:
        return host, int(port)
    except ValueError:
        raise ValueError(f"expected host:port, got {raw!r}") from None


def _pg_upstream(raw: str) -> tuple[str, int]:
    """host[:port] or postgresql://host[:port]."""
    return _tcp_upstream(raw, ("postgres", "postgresql"), 5432)


def _redis_upstream(raw: str) -> tuple[str, int]:
    """host[:port] or redis://host[:port] (no rediss — no TLS)."""
    return _tcp_upstream(raw, ("redis",), 6379)


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "fidelity":
        # `fidelity` owns its own parser — its flags (--services, --dir)
        # don't belong to the proxy CLI surface.
        from microburst.fidelity import fidelity_main

        return fidelity_main(argv[1:])

    args = build_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    config = {}
    if args.config:
        config = load_config(args.config)

    protocol = args.protocol or config.get("protocol") or "http"
    default_port = {"postgres": 15432, "redis": 16379}.get(protocol, 9999)
    port = args.port or config.get("port") or default_port

    if args.command == "dashboard":
        from microburst.dashboard import run_dashboard

        connect = args.connect or f"http://127.0.0.1:{port}"
        return run_dashboard(connect)

    default_upstream = {
        "postgres": "localhost:5432",
        "redis": "localhost:6379",
    }.get(protocol, "http://localhost:4566")
    upstream = args.upstream or config.get("upstream") or default_upstream
    host = args.host or config.get("host") or "127.0.0.1"
    rules = config.get("rules") or []

    if args.watch and not args.config:
        print("--watch needs --config (the file to watch)", file=sys.stderr)
        return 2

    if protocol in ("postgres", "redis"):
        if args.record or args.replay or args.resign or args.http2:
            print(
                "warning: --record/--replay/--resign/--http2 are HTTP-only "
                f"and ignored in {protocol} mode",
                file=sys.stderr,
            )
        parse_upstream = (
            _pg_upstream if protocol == "postgres" else _redis_upstream
        )
        try:
            upstream_host, upstream_port = parse_upstream(upstream)
        except ValueError as e:
            print(f"invalid --upstream for {protocol} mode: {e}", file=sys.stderr)
            return 2
        control_port = (
            args.control_port or config.get("control_port") or 9999
        )
        if protocol == "postgres":
            from microburst.pg.server import run_postgres

            return run_postgres(
                host,
                port,
                upstream_host,
                upstream_port,
                control_port,
                rules,
                watch_config=args.config if args.watch else None,
            )
        from microburst.redis.server import run_redis

        return run_redis(
            host,
            port,
            upstream_host,
            upstream_port,
            control_port,
            rules,
            watch_config=args.config if args.watch else None,
        )

    if args.no_resign:
        resign = False
    elif args.resign:
        resign = True
    else:
        resign = None  # auto

    cassette = None
    if args.record or args.replay:
        from microburst.cassette import Cassette

        cassette = Cassette(
            args.record or args.replay,
            "record" if args.record else "replay",
        )

    microburst = Microburst(
        upstream, rules=rules, resign=resign, http2=args.http2,
        cassette=cassette,
    )
    app = make_app(
        microburst, watch_config=args.config if args.watch else None
    )

    print(f"microburst listening on http://{host}:{port} → {upstream}", flush=True)
    print(f"control API: http://{host}:{port}/_microburst/health", flush=True)
    print(f"point your app: AWS_ENDPOINT_URL=http://{host}:{port}", flush=True)

    web.run_app(app, host=host, port=port, print=None)
    return 0


if __name__ == "__main__":
    sys.exit(main())
