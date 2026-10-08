"""microburst dashboard — live TUI over the control API.

Connects to a running instance: streams `/_microburst/fired/stream`
(SSE) for live faults and polls `/health` + `/rules`. Optional dep —
needs ``pip install microburst[tui]``.

Palette follows the mascot: indigo cloud, amber lightning
(#f59e0b), on the terminal's own dark background.
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
_EVENTS_KEPT = 500
_SPARK_BUCKETS = 24          # 24 × 2.5s = one minute of fault-rate history
_SPARK_BUCKET_S = 2.5

# action → (severity color, glyph)
_SEVERITY = {
    "error": ("bold red", "✗"),
    "reset": ("bold bright_red", "⚡"),
    "timeout": ("bold magenta", "◷"),
    "latency": ("yellow", "~"),
    "response": ("bold cyan", "⇣"),
}
_SERVICE_HUE = {
    "dynamodb": "blue", "s3": "green", "sqs": "yellow", "sns": "magenta",
    "lambda": "bright_yellow", "kms": "cyan", "cloudwatch": "bright_blue",
}


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


def _action_style(action: str) -> tuple[str, str]:
    for key, (color, glyph) in _SEVERITY.items():
        if action.startswith(key):
            return color, glyph
    return "white", "·"


def _header(health: dict, base: str, faults: int):
    from rich.columns import Columns
    from rich.text import Text

    from microburst import __version__

    ok = health.get("status") == "ok"
    status = "[bold green]● live" if ok else "[bold red]● unreachable"
    left = Text.assemble(
        (" ⚡ ", "bold #f59e0b"),
        ("microburst", "bold white"),
        (f" {__version__} ", "dim"),
        ("━ ", "dim #6366f1"),
        (f"{base}", "#6366f1"),
        (" → ", "dim"),
        (f"{health.get('upstream', '?')}", "bold #6366f1"),
    )
    right = Text.from_markup(
        f"[bold white]{health.get('requests_seen', 0)} req[/]"
        f"   [bold #f59e0b]{faults} faults[/]   {status}"
    )
    right.justify = "right"
    return Columns([left, right], expand=True)


def _sparkline(fault_ts: list[float]) -> str:
    """Faults per bucket over the last minute — ▁▂▃▄▅▆▇."""
    bars = " ▁▂▃▄▅▆▇"
    now = time.time()
    counts = [0] * _SPARK_BUCKETS
    for ts in fault_ts:
        age = now - ts
        if age < _SPARK_BUCKETS * _SPARK_BUCKET_S:
            counts[int(age // _SPARK_BUCKET_S)] += 1
    counts.reverse()  # oldest → newest
    peak = max(counts) or 1
    return "".join(bars[min(7, round(c / peak * 7))] for c in counts)


def _rules_table(rules: list[dict]):
    from rich import box
    from rich.table import Table
    from rich.text import Text

    table = Table(
        box=box.SIMPLE_HEAD, border_style="#6366f1",
        header_style="bold #f59e0b", expand=True, padding=(0, 1),
    )
    table.add_column("", width=2)
    table.add_column("id", style="dim", width=3)
    table.add_column("match", ratio=2, overflow="fold")
    table.add_column("effect", ratio=2, overflow="fold")
    table.add_column("p", justify="right", width=5)
    table.add_column("fired", justify="right", style="bold", width=5)
    table.add_column("ttl", justify="right", width=6)

    for r in rules:
        expired = "ttl_remaining_s" in r and r["ttl_remaining_s"] <= 0
        dot = Text("●", style="dim strike") if expired else Text("●", style="green")
        match = " ".join(
            f"[#6366f1]{k}[/]={r[k]}"
            for k in ("service", "operation", "region", "resource")
            if r.get(k)
        ) or "[dim]*[/]"
        effects = []
        if "error" in r:
            effects.append(f"[red]{r['error'].get('code', 'sampled')}[/]")
        if "latency" in r:
            lat = r["latency"]
            if lat.get("dist") == "gaussian":
                effects.append(f"[yellow]~{lat['mean']:.0f}ms±{lat['stddev']:.0f}[/]")
            elif lat.get("dist") == "spike":
                effects.append(f"[yellow]spike {lat['spike_ms']:.0f}ms[/]")
            else:
                effects.append(f"[yellow]{lat['min']:.0f}–{lat['max']:.0f}ms[/]")
        if "timeout_ms" in r:
            effects.append(f"[magenta]timeout {r['timeout_ms']:.0f}ms[/]")
        if r.get("reset"):
            effects.append("[bright_red]reset[/]")
        if "response" in r:
            effects.append(f"[cyan]resp:{','.join(r['response'])}[/]")
        p = r.get("probability", 1.0)
        p_txt = f"{p:.2f}" if p < 1.0 else "[dim]1.0[/]"
        ttl = ""
        if "ttl_remaining_s" in r:
            ttl = f"{r['ttl_remaining_s']:.0f}s" if not expired else "[dim]gone[/]"
        table.add_row(
            dot, str(r["id"]), match, " ".join(effects),
            p_txt, str(r.get("fired_count", 0)), ttl,
        )
    return table


def _events_panel(events):
    from rich.align import Align
    from rich.table import Table
    from rich.text import Text

    if not events:
        return Align.center(
            Text.assemble(
                ("\n\n ⚡ \n\n", "bold #f59e0b"),
                ("no faults yet\n", "bold white"),
                ("point your app at the proxy and watch it storm\n\n", "dim"),
                ("AWS_ENDPOINT_URL=http://localhost:9999 python app.py", "#6366f1"),
            ),
            vertical="middle",
        )

    table = Table(
        box=None, expand=True, padding=(0, 1), show_header=False,
    )
    table.add_column("", width=2)
    table.add_column("", width=9, style="dim")
    table.add_column("", width=11)
    table.add_column("", ratio=1, overflow="fold")
    table.add_column("", ratio=2, overflow="fold")
    table.add_column("", width=12, overflow="fold")

    for e in reversed(events):  # newest on top
        action = e.get("action", "")
        color, glyph = _action_style(action)
        svc = e.get("service") or "?"
        svc_color = _SERVICE_HUE.get(svc, "#6366f1")
        ts = time.strftime("%H:%M:%S", time.localtime(e.get("ts", 0)))
        table.add_row(
            Text(glyph, style=color),
            ts,
            Text(svc, style=f"bold {svc_color}"),
            Text(e.get("operation") or "?", style="white"),
            Text(action, style=color),
            Text(str(e.get("resource") or "—"), style="dim"),
        )
    return table


def run_dashboard(connect: str) -> int:
    try:
        from rich import box
        from rich.console import Console
        from rich.layout import Layout
        from rich.live import Live
        from rich.panel import Panel
        from rich.text import Text
    except ImportError:
        print(
            "dashboard needs `pip install microburst[tui]` "
            "(rich is an optional dependency)"
        )
        return 1

    base = connect.rstrip("/")
    console = Console()
    events: deque[dict] = deque(maxlen=_EVENTS_KEPT)
    fault_ts: list[float] = []
    inbox: queue.Queue = queue.Queue(maxsize=1000)
    stop = threading.Event()
    reader = threading.Thread(
        target=sse_events,
        args=(f"{base}/_microburst/fired/stream", inbox, stop),
        daemon=True,
    )
    reader.start()

    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="main"),
        Layout(name="footer", size=3),
    )
    layout["main"].split_row(
        Layout(name="rules", ratio=5), Layout(name="events", ratio=6)
    )

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
                        ev = inbox.get_nowait()
                        events.append(ev)
                        fault_ts.append(ev.get("ts", time.time()))
                    except queue.Empty:
                        break

                layout["header"].update(
                    Panel(
                        _header(health, base, len(fault_ts)),
                        box=box.ROUNDED, border_style="#6366f1",
                        padding=(0, 1),
                    )
                )
                layout["rules"].update(
                    Panel(
                        _rules_table(rules),
                        title="[bold #f59e0b]⚡ rules", box=box.ROUNDED,
                        border_style="#6366f1",
                    )
                )
                layout["events"].update(
                    Panel(
                        _events_panel(list(events)[-30:]),
                        title="[bold red]⚡ fired — live", box=box.ROUNDED,
                        border_style="#6366f1",
                    )
                )
                layout["footer"].update(
                    Panel(
                        Text.assemble(
                            ("fault rate ", "dim"),
                            (f"[{_sparkline(fault_ts)}]", "bold #f59e0b"),
                            ("  last 60s", "dim"),
                            ("    ·    ", "dim"),
                            ("ctrl+c", "bold white"),
                            (" quit", "dim"),
                        ),
                        box=box.ROUNDED, border_style="#6366f1",
                        padding=(0, 1),
                    )
                )
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        reader.join(timeout=2)
    return 0
