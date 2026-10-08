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
        "--upstream", "-u", default=None,
        help="upstream AWS endpoint (default: config file or http://localhost:4566)",
    )
    parser.add_argument(
        "--port", "-p", type=int, default=None,
        help="listen port (default: 9999)",
    )
    parser.add_argument(
        "--host", default=None, help="listen host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--config", "-c", default=None,
        help="YAML config file (upstream, port, rules)",
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
        "command", nargs="?", choices=["dashboard"], default=None,
        help="'dashboard' launches the TUI (needs microburst[tui])",
    )
    parser.add_argument(
        "--connect", default=None,
        help="microburst instance URL for dashboard mode "
        "(default: http://127.0.0.1:PORT)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    config = {}
    if args.config:
        config = load_config(args.config)

    port = args.port or config.get("port") or 9999

    if args.command == "dashboard":
        from microburst.dashboard import run_dashboard

        connect = args.connect or f"http://127.0.0.1:{port}"
        return run_dashboard(connect)

    upstream = args.upstream or config.get("upstream") or "http://localhost:4566"
    host = args.host or config.get("host") or "127.0.0.1"
    rules = config.get("rules") or []

    if args.no_resign:
        resign = False
    elif args.resign:
        resign = True
    else:
        resign = None  # auto

    microburst = Microburst(
        upstream, rules=rules, resign=resign, http2=args.http2
    )
    app = make_app(microburst)

    print(f"microburst listening on http://{host}:{port} → {upstream}", flush=True)
    print(f"control API: http://{host}:{port}/_microburst/health", flush=True)
    print(f"point your app: AWS_ENDPOINT_URL=http://{host}:{port}", flush=True)

    web.run_app(app, host=host, port=port, print=None)
    return 0


if __name__ == "__main__":
    sys.exit(main())
