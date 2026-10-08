#!/usr/bin/env python3
"""Hammer a local HTTP upstream through microburst — no AWS involved.
Run `microburst -c chaos.yml -u http://localhost:8080` first; this script
serves the upstream itself."""
import http.server
import threading
import time
import urllib.request
from collections import Counter

PROXY = "http://localhost:9999"


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"ok": true}')

    def log_message(self, *a):  # quiet
        pass


server = http.server.HTTPServer(("127.0.0.1", 8080), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
print("upstream on :8080 — hitting it through microburst on :9999\n")

stats = Counter()
for i in range(25):
    t = time.monotonic()
    try:
        with urllib.request.urlopen(PROXY + "/api", timeout=8) as r:
            r.read()
        ms = (time.monotonic() - t) * 1000
        stats["ok"] += 1
        print(f"req {i:<3} ✓ {ms:6.0f}ms{'  (slow)' if ms > 500 else ''}")
    except Exception as e:
        stats["failed"] += 1
        print(f"req {i:<3} ✗ {type(e).__name__}: {e}")
    time.sleep(0.1)

print(f"\nok: {stats['ok']}  failed: {stats['failed']}  "
      f"— does your HTTP client's retry/timeout policy hold up?")
