#!/usr/bin/env python3
"""microburst live demo — an "orders pipeline" talking to MiniStack *through*
microburst while faults get injected on a schedule.

    .venv/bin/python demo.py            # needs MiniStack on :4566

Timeline (all driven live, no pre-baked output):
  t+2s   ddb-throttle preset  → ~10% of PutItem calls throttle; SDK retries
  t+7s   SQS latency rule     → publishes get 1.5–4s slower
  t+12s  S3 SlowDown 30%      → template fetches start failing/retrying
  t+17s  clear all rules      → recovery
  end    fired-log summary    → exactly which rule hit which operation
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import urllib.request
from collections import Counter

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

MINISTACK = os.environ.get("MINISTACK_URL", "http://localhost:4566")
MICROBURST = os.environ.get("MICROBURST_URL", "http://127.0.0.1:9999")

G, Y, R, C, D, X = "\033[92m", "\033[93m", "\033[91m", "\033[96m", "\033[2m", "\033[0m"


def say(msg, color=""):
    print(f"{color}{msg}{X}", flush=True)


def control(method, path, payload=None):
    req = urllib.request.Request(
        MICROBURST + path, method=method,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def wait_for(url, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url + "/_microburst/health", timeout=1)
            return True
        except Exception:
            time.sleep(0.2)
    return False


def main():
    try:
        urllib.request.urlopen(MINISTACK + "/_ministack/health", timeout=2)
    except Exception:
        say("MiniStack no responde en :4566 — levantalo primero.", R)
        return 1

    say(f"\n{C}━━━ microburst demo: orders pipeline → microburst → MiniStack ━━━{X}\n")
    # kill any microburst left over from a crashed run — the port would already
    # be taken and we'd silently drive the zombie's state
    subprocess.run(["pkill", "-f", "python [-]m microburst"], check=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.3)
    proc = subprocess.Popen(
        [sys.executable, "-m", "microburst", "--upstream", MINISTACK, "--port", "9999"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    if not wait_for(MICROBURST):
        say("microburst no levantó.", R)
        proc.terminate()
        return 1
    control("DELETE", "/_microburst/rules", [])
    control("DELETE", "/_microburst/fired")
    say(f"{D}microburst up on :9999 → {MINISTACK}{X}\n")

    client_cfg = Config(retries={"max_attempts": 4})
    ddb = boto3.client("dynamodb", endpoint_url=MICROBURST, region_name="us-east-1",
                       aws_access_key_id="test", aws_secret_access_key="test",
                       config=client_cfg)
    sqs = boto3.client("sqs", endpoint_url=MICROBURST, region_name="us-east-1",
                       aws_access_key_id="test", aws_secret_access_key="test",
                       config=client_cfg)
    s3 = boto3.client("s3", endpoint_url=MICROBURST, region_name="us-east-1",
                      aws_access_key_id="test", aws_secret_access_key="test",
                      config=Config(retries={"max_attempts": 4},
                                    s3={"addressing_style": "path"}))

    # ---- provision through the proxy (pass-through demo) -------------------
    try:
        ddb.create_table(
            TableName="orders",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceInUseException":
            raise
    queue_url = sqs.create_queue(QueueName="order-events")["QueueUrl"]
    try:
        s3.create_bucket(Bucket="order-templates")
    except ClientError:
        pass  # already exists from a previous run
    s3.put_object(Bucket="order-templates", Key="receipt.txt",
                  Body=b"thanks for your order")
    say(f"{D}provisioned through microburst: table 'orders', queue 'order-events',"
        f" bucket 'order-templates'{X}\n")

    # ---- fault schedule ----------------------------------------------------
    def inject_later(delay, fn, label):
        def _fire():
            time.sleep(delay)
            fn()
            say(f"\n{Y}⚡ {label}{X}")
        threading.Thread(target=_fire, daemon=True).start()

    inject_later(
        2,
        lambda: control("PATCH", "/_microburst/rules", [
            {"service": "dynamodb", "operation": "PutItem", "probability": 0.35,
             "error": {"code": "ProvisionedThroughputExceededException"}}]),
        "FAULT INJECTED → dynamodb PutItem throttles 35%",
    )
    inject_later(
        8,
        lambda: control("PATCH", "/_microburst/rules", [
            {"service": "sqs", "latency": {"min": 1000, "max": 2500}}]),
        "FAULT INJECTED → SQS latency 1–2.5s on every call",
    )
    inject_later(
        14,
        lambda: control("PATCH", "/_microburst/rules", [
            {"service": "s3", "operation": "GetObject", "probability": 0.5,
             "error": {"code": "SlowDown"}}]),
        "FAULT INJECTED → S3 GetObject SlowDown 50%",
    )
    inject_later(
        20,
        lambda: control("DELETE", "/_microburst/rules", []),
        "FAULTS CLEARED → recovery",
    )

    # ---- the app: a tiny order pipeline ------------------------------------
    # botocore emits needs-retry.<service>.<op> once per attempt — including
    # the final one — so retries = fires - 1. This is how we *see* the
    # resilience microburst is exercising.
    attempts = Counter()

    def _watch(op_key):
        def _handler(**_kw):
            attempts[op_key] += 1
        return _handler

    ddb.meta.events.register("needs-retry.dynamodb.PutItem", _watch("put"))
    s3.meta.events.register("needs-retry.s3.GetObject", _watch("s3get"))

    say(f"{C}t={X} app loop: write order → publish event → fetch template\n")
    t0 = time.time()
    stats = Counter()
    order_n = 0
    try:
        _run_loop(t0, stats, order_n, ddb, sqs, s3, queue_url, attempts)
    finally:
        proc.terminate()
    return 0


def _run_loop(t0, stats, order_n, ddb, sqs, s3, queue_url, attempts):
    while time.time() - t0 < 24:
        order_n += 1
        order_id = f"order-{order_n}"

        before = attempts["put"]
        t = time.monotonic()
        try:
            ddb.put_item(TableName="orders",
                         Item={"pk": {"S": order_id}})
            put_ms = (time.monotonic() - t) * 1000
            put_retries = attempts["put"] - before - 1
            put_note = (f" {Y}↻ retried ×{put_retries} (throttled){X}"
                        if put_retries else "")
            stats["ddb_retried"] += bool(put_retries)
            put_str = f"put {put_ms:6.0f}ms{put_note}"
        except ClientError as exc:
            stats["ddb_failed"] += 1
            put_str = (f"{R}put ✗ {exc.response['Error']['Code']}"
                       f" (retries exhausted){X}")

        t = time.monotonic()
        sqs.send_message(QueueUrl=queue_url, MessageBody=order_id)
        pub_ms = (time.monotonic() - t) * 1000

        before = attempts["s3get"]
        t = time.monotonic()
        try:
            s3.get_object(Bucket="order-templates", Key="receipt.txt")[
                "Body"].read()
            s3_ms = (time.monotonic() - t) * 1000
            s3_retries = attempts["s3get"] - before - 1
            if s3_retries:
                stats["s3_retried"] += 1
                s3_status = (f"{Y}s3 ↻ retried ×{s3_retries} {s3_ms:5.0f}ms{X}")
            else:
                stats["s3_ok"] += 1
                s3_status = f"{G}s3 ✓ {s3_ms:5.0f}ms{X}"
        except ClientError as exc:
            stats[f's3_{exc.response["Error"]["Code"]}'] += 1
            s3_status = (f"{R}s3 ✗ {exc.response['Error']['Code']}"
                         f" (retries exhausted){X}")

        stats["orders"] += 1
        flag = f"{Y}(slow){X}" if pub_ms > 500 else ""

        say(f"  {D}{time.time()-t0:5.1f}s{X}  {order_id:<10} "
            f"{put_str}  publish {pub_ms:6.0f}ms {flag}"
            f"  {s3_status}")
        time.sleep(0.4)

    # ---- summary -----------------------------------------------------------
    say(f"\n{C}━━━ fired log (what microburst actually did) ━━━{X}")
    fired = control("GET", "/_microburst/fired?limit=500")
    by_action = Counter(f"{e['service']}:{e['operation']} → {e['action']}"
                        for e in fired)
    for line, count in sorted(by_action.items()):
        say(f"  {count:>3}× {line}")

    say(f"\n{C}━━━ outcome ━━━{X}")
    say(f"  orders processed:      {stats['orders']}")
    say(f"  ddb throttled+retried: {stats.get('ddb_retried', 0)} "
        f"{D}(SDK absorbed — app never saw an error){X}")
    say(f"  ddb hard failures:     {stats.get('ddb_failed', 0)}")
    say(f"  s3 retried:            {stats.get('s3_retried', 0)}")
    hard_fails = sum(v for k, v in stats.items()
                     if k.startswith("s3_") and k != "s3_ok"
                     and k != "s3_retried")
    say(f"  s3 hard failures:      {hard_fails} "
        f"{D}(all retries exhausted — the failure your code must handle){X}\n")


if __name__ == "__main__":
    sys.exit(main())
