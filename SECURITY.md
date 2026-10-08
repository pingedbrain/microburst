# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅        |

## Reporting a vulnerability

Please **do not** open a public issue for security reports. Use GitHub's
[private vulnerability reporting](../../security/advisories/new) instead.

## Scope notes

- The `/_microburst/*` control API is **unauthenticated by design** — it is a
  local development tool. Never expose it beyond localhost. Treat anything
  that can reach the proxy port as able to inject faults into your traffic.
- `--resign` mode handles real AWS credentials. Signatures are computed in
  memory and never logged or forwarded to third parties; still, only run it
  against upstreams you trust.
