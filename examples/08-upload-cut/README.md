# Upload cut — the link dies mid-PUT

Response faults exercise the SDK's *read* path; `request:` faults
exercise its *write* path. `cut_upload` resets the client→proxy
connection after N bytes of the upload have been consumed — the SDK
sees a `ConnectionClosedError`, which botocore retries like any
transient link failure. Nothing is forwarded upstream for the cut
attempts.

```bash
microburst -c examples/08-upload-cut/chaos.yml -u http://localhost:4566
python examples/08-upload-cut/app.py
```

The rule's `sequence: {fail: 1, pass: 1}` cuts the first attempt of
every `put_object` mid-upload; the SDK's retry lands on the pass slot
and succeeds — the same pattern a flaky network hop produces, now
auditable in `/_microburst/fired`.

`slow_upload` pairs with it when you want stalls instead of cuts:
`request: {slow_upload: {rate_kbps: 8}}` paces the proxy's read of the
client body, exercising write-path and socket timeouts on big uploads.
