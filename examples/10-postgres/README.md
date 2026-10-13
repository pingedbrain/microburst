# 10 — PostgreSQL wire mode

The failure mode this surfaces: **does your app classify SQLSTATEs
correctly?** `40001` (serialization failure) and `40P01` (deadlock) are
safe to retry — `23505` (unique violation) is not. Generic proxies can
drop TCP; they can't send an ErrorResponse your driver parses.

Unlike the other examples this one doesn't need an AWS emulator — it
needs any PostgreSQL server on `localhost:5432` (docker:

`docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=x postgres`).

## Run it

```bash
# 1. the proxy — data plane on :15432, control API on :9999
microburst --protocol postgres -c examples/10-postgres/chaos.yml

# 2. drive traffic through it — plain psql works
psql "host=localhost port=15432 dbname=postgres password=x" \
  -c "select 1" -c "insert into orders values (1)"

# 3. see what fired
curl http://localhost:9999/_microburst/fired?service=postgres
```

## What you'll see

- ~1 in 4 connects dies with `FATAL:  too many connections` (SQLSTATE
  `53300`) — the psql connection itself is refused.
- Inserts into `orders` intermittently fail `40001` — but only while the
  session is idle; inside `BEGIN…COMMIT` the rule skips and the fired
  event's note says `skipped: in-transaction`.
- Some selects take ~1s then return `57014` — the
  slow-then-statement-timeout shape.
- Occasionally a result set delivers a few rows and the link just dies
  (`partial_rows`) — "server closed the connection unexpectedly" on the
  client.

Watch rules change live: `curl -X DELETE localhost:9999/_microburst/rules
-d '[]'` clears them; `--watch` hot-reloads `chaos.yml` on save.
