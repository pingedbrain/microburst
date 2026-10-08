# Poison queue — consumer resilience

Queue consumers are where intermittent AWS failures do the most damage:
one unhandled `InternalError` on `ReceiveMessage` kills the poll loop,
and your worker sits idle while messages pile up. Or worse — a
`DeleteMessage` failure makes a processed message come back for seconds.

```bash
microburst -c examples/03-poison-queue/chaos.yml -u http://localhost:4566
python examples/03-poison-queue/app.py
```

The rule fails 25% of `ReceiveMessage` calls with the real
`InternalError` envelope. `app.py` polls in a naive loop — watch which
polls fail and whether your error handling keeps the consumer alive.
