# Contributing to offshoot

offshoot is pre-alpha, single-maintainer, and moving fast. That means: ask
before investing a lot of time in something structural (see "what needs an
issue first" below), but small, well-scoped PRs are welcome and get reviewed
quickly.

## Dev setup

You need:

- Go 1.24+
- cgo (offshoot uses `github.com/mattn/go-sqlite3`, which is cgo-backed —
  there's no pure-Go build)
- the `sqlite3` CLI on `PATH` (the test suite shells out to it as a stock
  foreign writer and for `.dump` equivalence checks)
- Linux or macOS — there are no Windows code paths, and none are planned for
  v1

Optional, only needed for the SDK test tier:

- `python3` (Python SDK tests)
- Node 20+ (TypeScript SDK tests)

```
go build -o offshoot ./cmd/offshoot
go vet ./...
```

## Test tiers

There are five, and they cost very different amounts of time. Run the tier
that matches what you touched — don't run torture on a docs typo, and don't
skip it on a capture-engine change.

| Tier | Command | Cost | When required |
|---|---|---|---|
| Unit/integration | `go test ./... -race` | seconds | Always, every PR |
| Torture (kill-9) | `make test-torture` | ~5 minutes | Touching `internal/capture` or `internal/session` flush paths |
| S3 conformance | `make test-s3` | needs a real S3-compatible provider or MinIO running | Touching `internal/store`'s S3 backend or the CAS probe |
| SDKs | `make test-sdks` | needs `python3` + Node 20+ | Touching `sdk/python`, `sdk/typescript`, or the daemon API surface they depend on |
| SDK publish dry-run | `make dry-run-sdks` | needs `python3` (+ `pip install build twine`) and `npm` | Touching either SDK's manifest (`pyproject.toml`, `package.json`, `sdk/VERSION`) or `.github/workflows/publish.yml` — see "Release process" below |

`go test ./... -race` is hermetic and needs only the `sqlite3` CLI — that's
the bar for "does this even work," and CI runs it on every PR. `make test`
runs the same suite without `-race` if you want a faster local loop, but
`-race` is what actually gets checked before merge, so run it before you
push.

`make test-torture` is not optional cosmetics: it's the suite that proves the
capture engine detects every divergence instead of silently absorbing it
(random `kill -9` of both the writer and the capturer, verified with
`sqlite3 .dump` equivalence after every round). If your change touches
`internal/capture` or a flush path in `internal/session`, a PR without a
torture run attached will get sent back for one, not tests we'll take on
faith.

`make test-s3` needs a real provider or a local MinIO — the in-process fake
used by the unit tests proves nothing about a real bucket's conditional-write
behavior, which is what the whole compare-and-swap safety story rests on.

## Code style

- Match the patterns already in the file you're editing over anything you'd
  reach for on a green field. This codebase leans on long doc comments that
  explain *why* (races found, invariants relied on, what breaks if you change
  the order of two calls) — see `internal/capture/engine.go` for the norm.
  Terse code with a paragraph above it explaining the non-obvious part beats
  clever code with no comment.
- Errors are wrapped with context, not just passed through:
  `fmt.Errorf("rebase: checkpoint TRUNCATE: %w", err)` — prefix with the
  operation, wrap with `%w` so callers can still `errors.Is`/`errors.As`.
- No new dependencies without a good reason — the dependency list is
  deliberately short (AWS SDK, `mattn/go-sqlite3`, `superfly/ltx`). Pulling in
  something new for a small feature is a smell; ask in the issue first.
- `go vet ./...` and `gofmt` clean before you push.

## PR expectations

- Tests required for behavioral changes. A PR that changes behavior with no
  test coverage will get asked for one before review continues.
- Torture run required (and mentioned in the PR description — the template
  has a checkbox) for anything touching capture or flush paths, per the table
  above.
- Docs updated in the same PR when behavior changes — README, ROADMAP, or
  code comments, whichever describes what you touched.
- Conventional commits: `feat:`, `fix:`, `test:`, `docs:`, `chore:`, etc.
  Look at `git log` for the norm in this repo.
- **DCO sign-off required.** Every commit needs a `Signed-off-by` trailer —
  use `git commit -s`. This project uses the Developer Certificate of Origin
  instead of a CLA: signing off is your statement that you wrote the patch or
  otherwise have the right to submit it under the project's license
  (Apache-2.0), not a copyright assignment to anyone. Unsigned commits will
  get bounced back for a rebase with `-s`, not silently merged.

## Release process

There are two independent release tracks — the `offshoot` binary and the
two SDKs — with two independent versioning schemes, on purpose: the SDKs
are thin wrappers over a stable daemon wire protocol and can move on their
own cadence, while the binary's `v0.1.x` tags govern the on-disk storage
format. They currently both happen to read `0.1.0` because both are still
at their first pre-release; that's a coincidence, not a coupling.

### Binary releases

Tag `v0.1.<n>` and push it. `.github/workflows/release.yml` builds
Linux/macOS binaries for both architectures, creates a GitHub release, and
publishes a `ghcr.io` Docker image. `workflow_dispatch` builds the same
artifacts under a `dev-<short-sha>` name without tagging, for a
pre-release smoke check.

### SDK releases

**Both SDKs publish together, from one tag, at one version number** —
`sdk-v<version>` (e.g. `sdk-v0.1.0`), never `sdk-py-v...`/`sdk-ts-v...`
separately. This is the simplest scheme for a two-SDK repo where both
clients track the same daemon wire protocol and ship from the same
PR/review cadence; splitting them into independently-versioned tags would
mean two release processes to keep straight for no benefit either SDK
currently needs. Revisit only if one SDK ever needs to ship a fix the other
genuinely doesn't.

**`sdk/VERSION` is the single source of truth for the SDK version number.**
Both manifests spell the version out literally (PyPI and npm both require
a real version string in their own manifest — neither supports pulling it
from an external file at publish time without extra tooling this repo
doesn't carry), so a release bump is three edits kept in lockstep by hand:

1. `sdk/VERSION`
2. `sdk/python/pyproject.toml`'s `[project].version`
3. `sdk/typescript/package.json`'s `version`

`python3 scripts/check_sdk_versions.py` (also `make check-sdk-versions`)
fails loudly, listing every mismatch, if any of the three disagree — this
runs as the first step of `make dry-run-python-sdk` and again in
`.github/workflows/publish.yml`'s `check` job before either publish job
does anything. Tagging `sdk-v<version>` is a fourth place the same number
has to agree; `scripts/check_sdk_tag_version.py <tag>` (run automatically
on a tag push) catches a tag cut against a stale `sdk/VERSION`.

### Publishing gate

`.github/workflows/publish.yml` triggers on `sdk-v*` tags and
`workflow_dispatch`, and always builds + validates both SDKs' real publish
artifacts (sdist/wheel + `twine check` + install-test for PyPI; `npm pack`
+ install-test for npm). It only **uploads** them when the `PUBLISH_ENABLED`
repository variable (Settings > Secrets and variables > Actions >
Variables) is exactly `"true"` — unset or anything else runs the same job
in dry-run mode (build + check + install-test, no upload). The same
build-and-check tier also runs on every PR via `make dry-run-sdks` in
`ci.yml`'s `sdks` job, so a manifest mistake is caught long before a
release tag exists, not discovered the first time `PUBLISH_ENABLED` flips
on.

**Turning it on requires, at minimum:**

- The `offshoot-db` name claimed on PyPI, with Trusted Publishing (OIDC)
  configured for this repo + `publish.yml` + the `pypi` job — no PyPI
  secret is stored in this repo; `pypa/gh-action-pypi-publish` authenticates
  via the job's `id-token: write` permission alone.
- The `@offshoot-db` scope claimed on npm, and registry auth configured:
  today that means an automation token in the `NPM_TOKEN` repository
  secret (used as `NODE_AUTH_TOKEN` for `npm publish --provenance`). npm
  has since shipped OIDC-based Trusted Publishing for GitHub Actions,
  which would remove the need for a stored token the same way PyPI's does
  — adopting it means confirming the npm CLI version on the runner
  supports it and configuring `@offshoot-db/client` as a Trusted Publisher
  on npmjs.com for this exact repo + workflow, then deleting the
  `NODE_AUTH_TOKEN` env var from the `npm` job. Not done yet; `NPM_TOKEN`
  is the working path today.
- `PUBLISH_ENABLED=true` set as a repository variable.

Claiming both names is a manual, one-time, user-side action tracked as a
deliberately-out-of-band item in [ROADMAP.md](ROADMAP.md) and
[docs/status.md](docs/status.md) — not something a PR against this repo
can do on its own.

**`PUBLISH_ENABLED=true` alone is not enough to publish anything.** A real
upload additionally requires the workflow run itself to be an `sdk-v*` tag
push — `workflow_dispatch` (a manual "Run workflow" click) is *always*
dry-run, unconditionally, even with the gate on, because a dispatch run has
no tag to publish and would otherwise burn whatever version `sdk/VERSION`
happens to read on the branch it was run against. Both the `pypi` and `npm`
jobs' actual upload steps check this directly (`startsWith(github.ref,
'refs/tags/sdk-v')`), not just the `check` job's resolved output, so the
guard survives even a bug in that job's own gating logic.

**Optional extra gate: GitHub Environments.** `publish.yml`'s `pypi` and
`npm` jobs each target a GitHub Environment (`pypi-publish`,
`npm-publish` respectively). Both exist with no protection rules the first
time the workflow runs, until a maintainer adds one — configuring required
reviewers on either (repo Settings > Environments > `pypi-publish` /
`npm-publish` > required reviewers) is a one-click way to require a human
approval on top of the tag+`PUBLISH_ENABLED` gate before that job's steps
run at all. Like the two name claims and the `PUBLISH_ENABLED` flip, this is
a manual, user-side action this repo's PRs can't do on their own — worth
doing before the first real release, not required for the pipeline to be
correct without it.

## What's most welcome right now

- **Docs.** The gap between what's implemented and what the design spec
  promises is real (pre-alpha), and closing that gap in writing — an honest
  implemented/deferred matrix, a CLI reference, fixing stale examples — is
  some of the highest-leverage work available and doesn't need prior
  coordination.
- **Provider conformance runs.** `make test-s3` against AWS S3, R2, or Tigris
  and a report of pass/fail (see the provider table in README.md) is
  directly useful and low-risk to contribute.
- **CLI ergonomics.** Better error messages, `--help` text, flag consistency,
  output formatting — open a PR directly, no issue needed first, unless it
  changes a flag's meaning or removes one.

## What needs an issue first

Open an issue (or comment on an existing one) *before* writing code for:

- **Storage format** — anything touching the LTX segment layout, snapshot
  format, ref schema, or layout versioning. This is the hardest thing to
  change once something has written data with it.
- **Capture engine** — `internal/capture`'s lock dance, rebase/takeover
  logic, or the Sink contract. The invariants here (no duplicate `Apply`,
  every divergence detected not absorbed) are load-bearing and easy to
  violate in a way that only shows up under torture.
- **Fencing** — epoch bumps, lease semantics, ref CAS. This is what keeps two
  writers from corrupting a branch; a change here needs a design discussion,
  not just a green test run.

For everything else: if you're not sure, open an issue and ask — that's
cheaper than a rejected PR.
