# Contributing

## Setup

```bash
uv venv && uv pip install -e ".[test]"
pytest
ruff check src/ tests/
```

## Conventions

- Error shapes, operation metadata and protocols come from **botocore service
  models** — don't hand-maintain parallel tables if the model can answer.
- Wire formats must match what AWS SDKs parse. When changing error
  serialization, verify against a real `boto3` client (see `tests/`).
- Keep matchers deterministic and observable: every decision should show up
  in `/_microburst/fired` or not happen at all.
- The control API is intentionally unauthenticated (local dev tool). Don't
  add features that make it safe to expose — document instead.

## Refreshing wire evidence

The repo never needs AWS credentials in CI — captures are crowdsourced:

- Have *any* AWS account? `AWS_PROFILE=you microburst fidelity capture`
  and commit the regenerated `fidelity/captures/*.json`. Account ids are
  redacted; each capture carries a `provenance` block (AWS request ids,
  region, botocore version) so reviewers can sanity-check it's real wire.
- A weekly `model-drift` workflow regenerates `fidelity/models_snapshot.json`
  against the latest botocore and opens an issue when the fidelity-relevant
  model surface changes — that's the signal a re-capture is due.
- `fidelity/protocol/` vendors botocore's AWS-authored conformance
  fixtures; refresh with `python tools/fetch_protocol_fixtures.py`
  after bumping botocore (`--check` verifies what's vendored).

## Releasing

Versioning is **tag-driven** (`hatch-vcs`): the git tag is the single source
of truth — there is no version field to bump. Commits after the last tag
build as `X.Y.Z.devN+gHASH`.

Process (maintainer only):

1. Update `CHANGELOG.md` — move items under a new `## [X.Y.Z]` heading.
2. Tag and release:
   ```bash
   git tag vX.Y.Z && git push origin vX.Y.Z
   gh release create vX.Y.Z --notes-file RELEASE_NOTES.md
   ```
   (or write notes in the GitHub UI)
3. The `release.yml` workflow builds from the tag and publishes to PyPI
   via OIDC — the built version equals the tag automatically.

Versioning policy (semver, applied to the **public surface**: CLI flags,
rule schema, and `/_microburst/*` API — internal Python APIs are free to
change until 1.0):

- **patch**: fixes that don't change observable behavior
- **minor**: new fault types, new matchers, new control endpoints, new flags
- **major**: rule schema changes, removed endpoints/flags, renamed options
