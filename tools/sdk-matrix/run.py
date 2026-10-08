"""Multi-SDK matrix: verify real SDKs parse and retry microburst faults.

Starts a local microburst with deterministic p=1.0 error rules (upstream is
a dead port — every request is injected before forwarding), then runs each
available SDK client against it. For every scenario we record:

- the error code the SDK *parsed* (ClientError.Code / e.name / APIError)
- the HTTP status the SDK saw
- how many attempts the SDK made — client-side count plus the proxy's
  own fired log as an independent server-side count

Usage:  python tools/sdk-matrix/run.py [--sdk boto3,js-v3,go-v2] [--port N]
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent

SCENARIOS = {
    "dynamo-throttle": {
        "service": "dynamodb", "operation": "DescribeTable",
        "protocol": "json",
        "rule": {"service": "dynamodb", "probability": 1.0,
                 "error": {"code": "ProvisionedThroughputExceededException"}},
        "expect": {"code": "ProvisionedThroughputExceededException",
                   "status": 400, "retried": True},
    },
    "lambda-notfound": {
        "service": "lambda", "operation": "GetFunction",
        "protocol": "rest-json",
        "rule": {"service": "lambda", "probability": 1.0,
                 "error": {"code": "ResourceNotFoundException"}},
        "expect": {"code": "ResourceNotFoundException",
                   "status": 404, "retried": False},
    },
    "sqs-querycompat": {
        "service": "sqs", "operation": "GetQueueUrl",
        "protocol": "json (query-compat)",
        "rule": {"service": "sqs", "probability": 1.0,
                 "error": {"code": "OverLimit"}},
        # query-compat errors may parse namespaced (AWS.SimpleQueueService.X)
        "expect": {"code_suffix": "OverLimit",
                   "status": 400, "retried": False},
    },
    "s3-slowdown": {
        "service": "s3", "operation": "HeadBucket",
        "protocol": "rest-xml",
        "rule": {"service": "s3", "probability": 1.0,
                 "error": {"code": "SlowDown", "status": 503}},
        # HEAD responses carry no body — each SDK maps a codeless 503
        # differently, exactly as it would against real AWS (boto3's
        # status-as-code is verified by the live head_bucket capture).
        "expect": {"status": 503, "retried": True},
        "expect_sdk": {
            "boto3": {"code": "503"},
            "go-v2": {"code": "ServiceUnavailable"},
            "js-v3": {"code": "Unknown"},
        },
    },
}


def _get(url: str) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return r.status, r.read()
    except Exception as e:  # noqa: BLE001 — control-API probe
        code = getattr(e, "code", 0) or 0
        return code, b""


def _req(url: str, method: str, body: object = None) -> tuple[int, bytes]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, r.read()
    except Exception as e:  # noqa: BLE001 — surface body for debugging
        return getattr(e, "code", 0) or 0, b""


def _fired(base: str) -> list[dict]:
    status, raw = _get(f"{base}/_microburst/fired?limit=500")
    if status != 200:
        return []
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return []


def _sdk_clients(
    node: str | None, go: str | None, mvn: str | None,
) -> dict[str, dict]:
    clients = {
        "boto3": {
            "cmd": [sys.executable, str(HERE / "clients" / "py_client.py")],
            "cwd": REPO,
        },
    }
    if node:
        clients["js-v3"] = {
            "cmd": [node, str(HERE / "clients" / "node_client.mjs")],
            "cwd": HERE,
        }
    if go:
        clients["go-v2"] = {
            "cmd": [go, "run", "."],
            "cwd": HERE / "clients",
        }
    if mvn:
        clients["java-v2"] = {
            # no -q: maven writes diagnostics to stdout
            "cmd": [mvn, "-B", "-f", "java", "compile", "exec:java"],
            "cwd": HERE / "clients",
        }
    return clients


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9911)
    ap.add_argument("--sdk", default="boto3,js-v3,go-v2,java-v2")
    ap.add_argument("--out", default=str(HERE / "results.json"))
    args = ap.parse_args()

    base = f"http://127.0.0.1:{args.port}"
    node = shutil.which("node") or next(
        (str(p) for p in sorted(Path.home().glob(".nvm/versions/node/*/bin/node"))
         if p.exists()), None)
    go = shutil.which("go") or next(
        (str(p) for p in sorted(Path.home().glob(".gvm/gos/*/bin/go"))
         if p.exists()), None)
    mvn = shutil.which("mvn")
    clients = _sdk_clients(node, go, mvn)
    want = {s.strip() for s in args.sdk.split(",")}
    missing = want - set(clients)
    if missing:
        print(f"warning: no client for {sorted(missing)} "
              f"(node={'yes' if node else 'no'}, go={'yes' if go else 'no'})")

    mb = subprocess.Popen(
        [sys.executable, "-m", "microburst",
         "--upstream", "http://127.0.0.1:1", "--port", str(args.port)],
        cwd=REPO, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        for _ in range(60):
            if _get(f"{base}/_microburst/health")[0] == 200:
                break
            time.sleep(0.25)
        else:
            print("microburst did not come up")
            return 1

        status, _ = _req(
            f"{base}/_microburst/rules", "POST",
            [s["rule"] for s in SCENARIOS.values()],
        )
        if status != 200:
            print(f"rule POST failed: {status}")
            return 1

        results = []
        for sdk in sorted(want & set(clients)):
            spec = clients[sdk]
            _req(f"{base}/_microburst/fired", "DELETE")
            env = {**os.environ, "MB_ENDPOINT": base}
            proc = subprocess.run(
                spec["cmd"], cwd=spec["cwd"], capture_output=True,
                text=True, timeout=300, env=env, check=False,
            )
            rows = []
            for line in proc.stdout.splitlines():
                line = line.strip()
                if line.startswith("{"):
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
            if not rows:
                print(f"--- {sdk} produced no results "
                      f"(rc={proc.returncode}) ---")
                # maven logs to stdout; most others to stderr — show both
                if proc.stdout.strip():
                    print(proc.stdout[-1500:])
                print(proc.stderr[-1500:])
            fired = _fired(base)
            for row in rows:
                sc = SCENARIOS.get(row.get("scenario", ""), {})
                svc, op = sc.get("service"), sc.get("operation")
                server_attempts = sum(
                    1 for e in fired
                    if e.get("service") == svc and e.get("operation") == op
                )
                exp = {**sc.get("expect", {}),
                       **sc.get("expect_sdk", {}).get(sdk, {})}
                code = row.get("code") or ""
                if "code" in exp:
                    code_ok = code == exp["code"]
                elif "code_suffix" in exp:
                    code_ok = code.endswith(exp["code_suffix"])
                else:
                    code_ok = True
                attempts = row.get("attempts") or server_attempts
                retried = (attempts or server_attempts) > 1
                ok = (
                    code_ok
                    and row.get("status") == exp.get("status")
                    and retried == exp.get("retried", False)
                    and server_attempts == attempts
                )
                results.append({
                    "sdk": row.get("sdk", sdk),
                    "scenario": row.get("scenario"),
                    "protocol": sc.get("protocol"),
                    "parsed_code": code or None,
                    "status": row.get("status"),
                    "attempts_sdk": row.get("attempts"),
                    "attempts_server": server_attempts,
                    "expected": exp,
                    "ok": ok,
                })

        Path(args.out).write_text(json.dumps(results, indent=2))
        print(f"\n{'sdk':<8} {'scenario':<16} {'protocol':<20} "
              f"{'code':<45} {'status':<7} {'att':<4} {'srv':<4} {'ok'}")
        for r in results:
            print(f"{r['sdk']:<8} {r['scenario'] or '?':<16} "
                  f"{r['protocol'] or '?':<20} "
                  f"{r['parsed_code']!s:<45} {r['status']!s:<7} "
                  f"{r['attempts_sdk']!s:<4} {r['attempts_server']:<4} "
                  f"{'PASS' if r['ok'] else 'FAIL'}")
        n_ok = sum(r["ok"] for r in results)
        print(f"\n{n_ok}/{len(results)} matrix cells pass "
              f"→ {args.out}")
        return 0 if n_ok == len(results) and results else 1
    finally:
        mb.terminate()
        try:
            mb.wait(timeout=5)
        except subprocess.TimeoutExpired:
            mb.kill()


if __name__ == "__main__":
    sys.exit(main())
