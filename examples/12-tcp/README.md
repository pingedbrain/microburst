# 12 — Generic TCP mode

The failure mode this surfaces: **does your client survive link-level
weirdness on a protocol microburst doesn't speak?** There is no MySQL
module here — `--protocol tcp` is deliberately protocol-blind and
injects transport faults only: cut streams, corrupted bytes, hung
links, hand-crafted replies. That's what generic proxies can do; what
they can't do is a `-MOVED` or a SQLSTATE `40001` — for those you want
the pg/redis transports.

This demo pretends the upstream is "mysql-ish": length-prefixed frames
(`size: 3` little-endian + `adjust: 1` for the sequence byte — the real
MySQL header shape). Any TCP server works as the upstream.

## Run it

```bash
# 1. something to proxy — a real mysql on :3306, or anything TCP:
#    nc -lk 3306   (a dumb echo is fine for watching faults land)
microburst --protocol tcp -c examples/12-tcp/chaos.yml
#    → data plane on :13306 (upstream 3306 + 10000), control on :9999

# 2. drive traffic through it
mysql -h 127.0.0.1 -P 13306 -u root           # real client, or:
nc 127.0.0.1 13306                            # raw bytes

# 3. see what fired
curl http://localhost:9999/_microburst/fired?service=tcp
```

## What you'll see

- Connections matching `mysql` in their first bytes get 50–400 ms of
  link latency (`operation: conn`, the unframed-style decision on the
  4 KiB prefix).
- Every `c2s:frame` carrying `SELECT … FOR UPDATE`-ish text has a 1-in-5
  chance of dying mid-upload (`cut_upload.after_messages: 2` — two
  client frames through, then RST).
- Reply streams occasionally die after ~4 KiB (`cut_reply.after_bytes`).
- A `payload: PING` frame gets a hand-crafted `\xff\x00\x01\x02` reply
  written straight to the client — `respond:` never forwards the unit.
  Change `then:` to `close`/`hold` to drop the link or blackhole the
  client instead.
- Byte 16 of the client→server stream flips once — inside a real
  protocol that's a corrupted packet header the server's parser has to
  reject.

`error:` rules are absent on purpose: tcp mode has no error renderer
(a rule that sets one fires with `note: error has no renderer in tcp
mode`). Protocol-correct errors need a dedicated transport.
