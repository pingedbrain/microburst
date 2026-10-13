# 11 — Redis wire mode

The failure mode this surfaces: **does your client branch on the error
code, or on "something failed"?** `MOVED`/`ASK` are redirects a cluster
client must follow; `READONLY` means failover just happened and writes
must wait or reroute; `LOADING` and `OOM` are terminal-for-now. Generic
proxies can drop TCP; they can't send a `-MOVED 3999 127.0.0.1:7001`
your client regexes into a redirect.

Doesn't need an AWS emulator — it needs any Redis server on
`localhost:6379` (docker: `docker run -d -p 6379:6379 redis`).

## Run it

```bash
# 1. the proxy — data plane on :16379, control API on :9999
microburst --protocol redis -c examples/11-redis/chaos.yml

# 2. drive traffic through it — plain redis-cli works
redis-cli -p 16379 SET foo bar
redis-cli -p 16379 GET session:42

# 3. see what fired
curl http://localhost:9999/_microburst/fired?service=redis
```

## What you'll see

- `GET session:*` intermittently returns `-MOVED 3999 127.0.0.1:7001` —
  redis-cli prints `(error) MOVED 3999 127.0.0.1:7001`; a cluster client
  would try to follow it.
- Two-of-seven writes fail `-READONLY` — the post-failover shape.
- A few commands per 10s window bounce `-LOADING`.
- Rare write `-OOM`s, and rare `GET`s deliver ~16 bytes of a real reply
  before the connection dies (`cut_reply`) — "Server closed the
  connection" on the client.
- Inside `MULTI…EXEC`, error rules don't fire — the fired event notes
  `skipped: in-multi`; commands queue (`+QUEUED`) exactly like a real
  server that never saw the fault.
