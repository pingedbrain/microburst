# Stream cut — event streams killed mid-frame

`SelectObjectContent`, Kinesis `SubscribeToShard`, and Lambda
`InvokeWithResponseStream` speak AWS's event-stream protocol — binary
frames with CRCs. A connection dying mid-stream is a distinct failure
from a request-level error: does your consumer crash on a truncated
frame, or handle it?

```bash
microburst -c examples/05-stream-cut/chaos.yml -u http://localhost:4566
python examples/05-stream-cut/app.py
```

The rule injects a terminal `InternalError` event frame after 2 good
frames — the same shape AWS sends when a stream fails server-side.
`app.py` runs S3 Select and counts records before the stream dies.
