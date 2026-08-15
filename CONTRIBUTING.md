# Contributing to offshoot

offshoot is at 0.x, single-maintainer, and moving fast. That means: ask
before investing a lot of time in something structural (see "what needs an
issue first" below), but small, well-scoped PRs are welcome and get reviewed
quickly.

## Dev setup

You need:

- Go 1.25+
- cgo (offshoot uses `github.com/mattn/go-sqlite3`, which is cgo-backed —
  there's no pure-Go build)
- the `sqlite3` CLI on `PATH` (the test suite shells out to it as a stock
  foreign writer and for `.dump` equivalence checks)
- Linux or macOS — there are no Windows code paths, and none are planned for
  v1

Optional, only needed for the SDK test tier:

- `python3` (Python SDK tests)
- Node 20+ (TypeScript SDK tests)

Optional, only needed for the pytest fixture plugin's OWN test tier
(`sdk/python/offshoot/pytest_plugin.py` — the `offshoot-db[pytest]` extra;
the base SDK's plain-`unittest` suites above need none of this):

```
pip install -e "sdk/python[pytest]" pytest-xdist
```

```
go build -o offshoot ./cmd/offshoot
go vet ./...
```

## Test tiers

There are six, and they cost very different amounts of time. Run the tier
that matches what you touched — don't run torture on a docs typo, and don't
skip it on a capture-engine change.

| Tier | Command | Cost | When required |
|---|---|---|---|
| Unit/integration | `go test ./... -race` | seconds | Always, every PR |
| Torture (kill-9) | `make test-torture` | ~5 minutes | Touching `internal/capture` or `internal/session` flush paths |
| S3 conformance | `make test-s3` | needs a real S3-compatible provider or MinIO running | Touching `internal/store`'s S3 backend or the CAS probe |
| SDKs | `make test-sdks` | needs `python3` + Node 20+ | Touching `sdk/python`, `sdk/typescript`, or the daemon API surface they depend on |
| pytest fixture plugin | `make test-pytest-plugin` | needs `pip install -e "sdk/python[pytest]" pytest-xdist` | Touching `sdk/python/offshoot/pytest_plugin.py` or its test suite — kept OUT of `test-sdks` on purpose, since that tier proves the base SDK works with no pytest installed at all |
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

## Local CI

`make ci-local` runs `.github/workflows/ci.yml`'s four jobs locally, in
minutes instead of waiting on a runner:

| Target | Mirrors | What it needs |
|---|---|---|
| `make ci-local-host` | the `test` job (ubuntu-only matrix), run on this host | `sqlite3`/`sqldiff` on PATH (dev-setup prerequisite above) |
| `make ci-local-linux` | the `test` job's ubuntu leg, in Docker | Docker; module/build caches live in named volumes so repeat runs are fast |
| `make ci-local-minio` | the `s3-conformance` job | Docker (spins up MinIO, runs `make test-s3`, always tears down) |
| `make ci-local-sdks` | the `sdks` job | `python3`, Node 20+, and optionally `pip install build twine` for the `dry-run-sdks` step (skipped loudly, not silently, if absent) |

`make ci-local` runs all four in sequence and prints a pass/fail-per-job
summary table with timings; a single job is runnable alone too. See
`scripts/ci-local.sh`'s header comment for the job-by-job detail.

**What it does NOT replicate — CI remains the merge gate, this is pre-merge
signal:**

- **GHA runner image drift.** `ci.yml`'s macOS leg broke without a version
  bump on our side because `macos-latest` rolled to a new image
  ("macos-26") that had quietly dropped the `sqlite3` CLI from PATH — see
  `ci.yml`'s "Install sqlite3 CLI (macos)" step comment. `ci-local-host`
  runs on whatever your machine's own Homebrew/PATH state already is; it
  cannot catch a runner image regression like that, only a real
  `macos-latest` run can.
- **Runner pip/HOME layout.** The `sdks` job's pytest-plugin step installs
  into a venv instead of `pip install --break-system-packages` onto system
  Python specifically because, on the actual (non-root, no-sudo) GHA
  runner, that flag falls back to a *user*-site install under `$HOME` —
  and pytest's `pytester` fixture repoints `HOME` for its subprocess runs,
  making pytest invisible to them (`ValueError: Pytest terminal summary
  report not found`). `ci-local-sdks` uses a venv from the start, so it
  can't reproduce the runner-layout failure mode it was written in
  response to — see `ci.yml`'s `sdks` job comment for the full incident.
- **Actions-specific behavior** — `actions/checkout`, `actions/setup-go`,
  caching, `GITHUB_PATH`, etc. — none of it runs locally; `ci-local` calls
  the same underlying commands (`go vet`, `go test`, `make test-s3`, ...)
  directly.

A green `ci-local` is strong local evidence, not a substitute for the real
GitHub Actions run — push and let CI have the final word.

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
own cadence, while the binary's `v0.2.x` tags govern the on-disk storage
format. The two schemes already read differently (binary at `v0.2.x`, SDKs
at `0.1.0`) — that divergence is the design, not drift.

### Binary releases

Tag `v0.2.<n>` (`vX.Y.Z` generally) and push it.
`.github/workflows/release.yml` builds Linux/macOS binaries for both
architectures, creates a GitHub release, and publishes a `ghcr.io` Docker
image. `workflow_dispatch` builds the same artifacts under a
`dev-<short-sha>` name without tagging, for a smoke check before tagging.

On each release, also bump the packaging surfaces: `Formula/offshoot.rb`'s
`url` + `sha256`, and the version chip in
`site/index.html`'s header (the one place the site states the number).

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
- **No org transfer needed — the manifests already match the real repo.**
  `sdk/python/pyproject.toml`'s `[project.urls]`, `sdk/typescript/package.json`'s
  `repository`/`homepage`/`bugs`, and `server.json`'s `repository.url` all
  point at `github.com/sricola/offshoot` — the repo's actual remote today
  (`git remote -v`). `npm publish --provenance` independently verifies that
  `package.json`'s `repository.url` matches the repo the publish is actually
  running from (it builds the provenance attestation from the real GitHub
  OIDC claims), so publishing from `sricola/offshoot` with the manifest
  already pointing at `sricola/offshoot` passes that check with nothing
  further to do here. Only the PyPI/npm name-claims and `PUBLISH_ENABLED`
  above are still outstanding; the package registry names themselves stay
  `offshoot-db` (PyPI) and `@offshoot-db` (npm) — those are unrelated to the
  repo's GitHub identity and are not being renamed.

Claiming both names is a manual, one-time, user-side action tracked as a
deliberately-out-of-band item in [ROADMAP.md](ROADMAP.md) and
[docs/status.md](docs/status.md) — not something a PR against this repo
can do on its own.

**`PUBLISH_ENABLED=true` alone is not enough to publish anything.** A real
upload additionally requires the workflow run itself to be an actual
`sdk-v*` tag **push** — `workflow_dispatch` (a manual "Run workflow" click)
is *always* dry-run, unconditionally, even with the gate on. Note that a
ref check alone (`startsWith(github.ref, 'refs/tags/sdk-v')`) is NOT
sufficient to establish that: GitHub's "Use workflow from" picker lets a
maintainer dispatch a `workflow_dispatch` run against a tag ref, which makes
`github.ref` match `refs/tags/sdk-v*` too, even though nothing was pushed.
Both the `pypi` and `npm` jobs' actual upload steps therefore check
`github.event_name == 'push'` as well (`startsWith(github.ref,
'refs/tags/sdk-v') && github.event_name == 'push'`), not just the ref
pattern and not just the `check` job's resolved output, so the guard
survives both a dispatch-against-a-tag-ref run and a bug in that job's own
gating logic.

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
  promises is real at 0.x, and closing that gap in writing — an honest
  implemented/deferred matrix, a CLI reference, fixing stale examples — is
  some of the highest-leverage work available and doesn't need prior
  coordination.
- **Provider conformance runs.** `make test-s3` against AWS S3
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
