# 14 — gRPC wire mode

The failure mode this surfaces: **does your client code branch on
`status.code`, or on "the call threw"?** `UNAVAILABLE` (14) and
`RESOURCE_EXHAUSTED` (8) are the retryable shapes; `INTERNAL` (13) is
usually not. A generic TCP proxy can drop connections; it can't send a
`grpc-status` trailer your client parses into a typed status — and it
can't leave the *connection* alive while killing just one stream.

Doesn't need an emulator — it needs any gRPC server speaking cleartext
h2c on `localhost:50051` (grpcurl, a local dev server, the canonical
`grpcbin`-style test server all qualify). TLS is out of scope: h2c
only, so clients must use `-plaintext`/`insecure` channels.

## Run it

```bash
# 1. the proxy — h2c data plane on :15051, control API on :9999
microburst --protocol grpc -c examples/14-grpc/chaos.yml

# 2. drive traffic through it — plaintext clients only
grpcurl -plaintext -d '{"id": 1}' localhost:15051 my.pkg.Svc/GetItem
grpcurl -plaintext localhost:15051 my.pkg.Svc/Watch

# 3. see what fired
curl http://localhost:9999/_microburst/fired?service=grpc
```

## What you'll see

- ~1 in 3 `GetItem` calls fails immediately with
  `ERROR: Code: Unavailable` — the proxy answers a *trailers-only*
  response (HTTP 200 + `grpc-status` + `grpc-message` + END_STREAM),
  the exact shape a real server sends when the call dies before the
  handler writes. Upstream is never contacted for those calls.
- `Watch` streams deliver three real messages, then
  `Code: ResourceExhausted` — partial data followed by failed trailers
  is what a server dying mid-`Send` actually looks like.
- Some `SlowLookup` calls hang until your client's deadline fires —
  `Code: DeadlineExceeded` raised client-side, which is honest: the
  proxy parks the call and never acknowledges inbound DATA (real
  flow-control backpressure), indistinguishable from a wedged server.
- Rare calls abort mid-flight with `Code: Internal` — the proxy sends
  RST_STREAM; the connection itself survives for the other calls.

Watch rules change live: `curl -X DELETE localhost:9999/_microburst/rules
-d '[]'` clears them; `--watch` hot-reloads `chaos.yml` on save.
