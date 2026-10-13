# Event-stream cut — the link dies between frames

`cut_upload.after_bytes` slices mid-body; `cut_upload.after_messages`
slices on a *message boundary*. When the request Content-Type is a
framed stream — `application/vnd.amazon.eventstream` or
`application/grpc*` — microburst parses frame preludes as the upload
arrives and resets the client→proxy connection after the Nth complete
message. The SDK sees the connection die between frames, not inside
one, which is what a real network hop failure looks like to a
bidirectional streaming client (Transcribe, gRPC bidi, S3 Select-style
uploads).

```bash
microburst -c examples/09-eventstream-cut/chaos.yml -u http://localhost:4566
python examples/09-eventstream-cut/app.py
```

The demo PUTs a hand-built eventstream body (real prelude + message
CRCs) with `ContentType="application/vnd.amazon.eventstream"`. The
rule's `sequence: {fail: 1, pass: 1}` cuts attempt 1 after message 4;
the SDK's retry lands on the pass slot and uploads all 8 frames.

Companion fault — `corrupt_upload.at_message`: instead of cutting, it
forwards N-1 messages verbatim then poisons message N (flipped
eventstream message-CRC byte; nonsense-huge gRPC length prefix) so the
*upstream's* parser rejects it. The client connection stays alive and
receives the upstream's error response — a different observable than a
reset: the service saw your bytes and refused them.

Both options are no-ops on non-framed bodies — check
`/_microburst/fired` for the `note` field (`content-type not a framed
stream`, `malformed frame — fell back to byte counting`, frame counts)
when a rule doesn't do what you expected.
