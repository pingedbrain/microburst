"""microburst dashboard — live TUI over the control API.

Connects to a running instance: streams `/_microburst/fired/stream`
(SSE) for live faults and polls `/health` + `/rules`. Optional dep —
needs ``pip install microburst[tui]``.
"""

from __future__ import annotations

import json
import queue
import threading
import time
import urllib.request
from collections import deque
from urllib.error import URLError

_POLL_S = 2.0
_EVENTS_KEPT = 200


def _fetch_json(url: str, timeout: float = 3.0):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read())


def sse_events(url: str, out: queue.Queue, stop: threading.Event):
    """Background SSE reader — pushes parsed `data:` payloads to `out`.
    Reconnects on transient failures; keepalive comments are ignored."""
    while not stop.is_set():
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                for raw in resp:
                    if stop.is_set():
                        return
                    line = raw.decode("utf-8", "replace").rstrip("\n")
                    if not line.startswith("data:"):
                        continue
                    try:
                        out.put_nowait(json.loads(line[5:].strip()))
                    except (json.JSONDecodeError, queue.Full):
                        pass
        except (URLError, OSError):
            if not stop.wait(2.0):
                continue
            return


def _rules_table(rules: list[dict]):
    from rich.table import Table

    table = Table(title="rules", expand=True, header_style="bold cyan")
    for col in ("id", "match", "effects", "fired", "ttl"):
        table.add_column(col, overflow="fold")
    for r in rules:
        match = " ".join(
            f"{k}={r[k]}"
            for k in ("service", "operation", "region", "resource")
            if k in r
        ) or "*"
        effects = ", ".join(
            k for k in ("error", "latency", "timeout_ms", "reset", "response")
            if k in r
        )
        ttl = (
            f"{r['ttl_remaining_s']:.0f}s" if "ttl_remaining_s" in r else ""
        )
        table.add_row(
            str(r["id"]), match, effects, str(r.get("fired_count", 0)), ttl
        )
    return table


def _events_table(events):
    from rich.table import Table

    table = Table(title="fired (live)", expand=True, header_style="bold red")
    for col in ("time", "rule", "service", "operation", "action", "resource"):
        table.add_column(col, overflow="fold")
    for e in events:
        ts = time.strftime("%H:%M:%S", time.localtime(e.get("ts", 0)))
        table.add_row(
            ts,
            str(e.get("rule_id", "")),
            e.get("service") or "?",
            e.get("operation") or "?",
            e.get("action", ""),
            str(e.get("resource") or ""),
        )
    return table


def run_dashboard(connect: str) -> int:
    try:
        from rich.console import Console
        from rich.layout import Layout
        from rich.live import Live
        from rich.panel import Panel
    except ImportError:
        print(
            "dashboard needs `pip install microburst[tui]` "
            "(rich is an optional dependency)"
        )
        return 1

    base = connect.rstrip("/")
    console = Console()
    events: deque[dict] = deque(maxlen=_EVENTS_KEPT)
    inbox: queue.Queue = queue.Queue(maxsize=1000)
    stop = threading.Event()
    reader = threading.Thread(
        target=sse_events,
        args=(f"{base}/_microburst/fired/stream", inbox, stop),
        daemon=True,
    )
    reader.start()

    layout = Layout()
    layout.split_column(Layout(name="header", size=3), Layout(name="main"))
    layout["main"].split_row(Layout(name="rules"), Layout(name="events"))

    try:
        with Live(layout, console=console, refresh_per_second=4, screen=True):
            next_poll = 0.0
            health: dict = {}
            rules: list[dict] = []
            while True:
                now = time.monotonic()
                if now >= next_poll:
                    try:
                        health = _fetch_json(f"{base}/_microburst/health")
                        rules = _fetch_json(f"{base}/_microburst/rules")
                    except (URLError, OSError):
                        health = {"status": "unreachable"}
                    next_poll = now + _POLL_S

                while True:
                    try:
                        events.append(inbox.get_nowait())
                    except queue.Empty:
                        break

                layout["header"].update(
                    Panel(
                        f"[bold]microburst[/] · {base} → "
                        f"{health.get('upstream', '?')} · "
                        f"requests: {health.get('requests_seen', 0)} · "
                        f"rules: {len(rules)}",
                        title="dashboard",
                    )
                )
                layout["rules"].update(_rules_table(rules))
                layout["events"].update(_events_table(list(events)[-25:]))
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        reader.join(timeout=2)
    return 0
