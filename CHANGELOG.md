# Changelog

All notable changes to this project will be documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[SemVer](https://semver.org/).

## [0.1.0] - 2026-10-07

Initial public release.

### Added

- Protocol-aware fault injection proxy for AWS: detects service, operation,
  region and resource per request (SigV4 credential scope, `X-Amz-Target`,
  `Action=`, REST path patterns from botocore service models).
- Wire-correct error serialization for `json`, `query`, `ec2`, `rest-xml`
  and `rest-json` protocols, with HTTP statuses resolved from the service
  model or a curated AWS-observed map.
- Rule engine: match on service/operation/region/resource, probability,
  `times` (fire N times then pass), error/latency/timeout/reset effects.
- Fault presets (`ddb-throttle`, `flaky-s3`, `slow-lambda`, `kms-outage`,
  `sqs-backlog`, `regional-failover`, `network-jitter`, `gateway-storm`).
- Control API under `/_microburst/*` (rules CRUD, fired-fault log, presets).
- Optional SigV4 re-signing for real `amazonaws.com` upstreams.
- CLI (`microburst`), YAML config, `demo.py` (orders pipeline → MiniStack).

[0.1.0]: https://github.com/pingedbrain/microburst/releases/tag/v0.1.0
