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

There are four, and they cost very different amounts of time. Run the tier
that matches what you touched — don't run torture on a docs typo, and don't
skip it on a capture-engine change.

| Tier | Command | Cost | When required |
|---|---|---|---|
| Unit/integration | `go test ./... -race` | seconds | Always, every PR |
| Torture (kill-9) | `make test-torture` | ~5 minutes | Touching `internal/capture` or `internal/session` flush paths |
| S3 conformance | `make test-s3` | needs a real S3-compatible provider or MinIO running | Touching `internal/store`'s S3 backend or the CAS probe |
| SDKs | `make test-sdks` | needs `python3` + Node 20+ | Touching `sdk/python`, `sdk/typescript`, or the daemon API surface they depend on |

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
