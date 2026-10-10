# 13 — MySQL wire mode

The failure mode this surfaces: **does your app classify the errno, or
on "something failed"?** `1213` (deadlock) and `1205` (lock wait
timeout) are safe to retry — `1062` (duplicate entry) is not. `1040`
at connect time is the pool-exhausted shape your retry loop sees in
production. Generic proxies can drop TCP; they can't send an
`ERR_Packet` your driver parses into `e.errno`/`e.sqlstate`.

Doesn't need an AWS emulator — it needs any MySQL or MariaDB server on
`localhost:3306` (docker:
`docker run -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=x mysql`).

## Run it

```bash
# 1. the proxy — data plane on :13306, control API on :9999
microburst --protocol mysql -c examples/13-mysql/chaos.yml

# 2. drive traffic through it — the plain mysql client works
mysql -h 127.0.0.1 -P 13306 -u root -px -e "select 1" \
  -e "insert into orders values (1)"

# 3. see what fired
curl http://localhost:9999/_microburst/fired?service=mysql
```

## What you'll see

- ~1 in 4 connects dies instantly with
  `ERROR 1040 (08004): Too many connections` — the ERR arrives as the
  *first* packet instead of a greeting, exactly like a maxed-out
  mysqld (the proxy never even dials upstream for these).
- Inserts into `orders` intermittently fail
  `ERROR 1213 (40001): Deadlock found` — but only while the session
  is idle; inside `START TRANSACTION…COMMIT` the rule skips and the
  fired event's note says `skipped: in-transaction` (a real deadlock
  rolls your tx back — faking one mid-tx would desync the session).
- Some selects take ~1s then return `ERROR 1205 (HY000): Lock wait
  timeout exceeded` — the slow-then-lock-timeout shape.
- Occasionally a result set delivers a few row packets and the link
  just dies (`partial_rows`) — "Lost connection to MySQL server
  during query" on the client.
- `COM_STMT_EXECUTE` calls resolve back to their `PREPARE`d SQL — the
  `sql: "into orders"` rule catches writes through ORMs that use
  prepared statements, not just raw `COM_QUERY` text.

Watch rules change live: `curl -X DELETE localhost:9999/_microburst/rules
-d '[]'` clears them; `--watch` hot-reloads `chaos.yml` on save.
