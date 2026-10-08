# Any HTTP dependency — not just AWS

microburst's modeled errors need an AWS request, but the **network
faults work on any HTTP upstream**: latency, timeouts, connection
resets, bandwidth caps. Point it at any internal API, a payment gateway
sandbox, or a local HTTP service.

```bash
# the proxy becomes a chaos reverse-proxy for anything HTTP
microburst -c examples/07-http-dependency/chaos.yml -u http://localhost:8080
python examples/07-http-dependency/app.py    # hits the proxy directly
```

`chaos.yml` has no `service:` matcher — it fires on every request that
reaches the proxy. `app.py` serves a tiny upstream on :8080 (stdlib
`http.server`) and then hammers it through the proxy.

Note: microburst proxies **HTTP only**. A database on its own TCP wire
protocol (Mongo, Postgres, Redis) doesn't pass through an HTTP proxy —
for those, look at toxiproxy. But any HTTP-speaking dependency is fair
game.
