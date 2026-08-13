# How offshoot is tested

offshoot's pitch leans on a durability claim, so this page shows the work:
what the torture harness actually does and its real numbers, the fencing
model that makes concurrent use safe, what the conformance suite proves
about storage backends, and the gates every change passes. Everything here
names the code or workflow that backs it.

## The kill -9 torture harness

The single most load-bearing test in the repo is
`internal/capture/torture_test.go` (`make test-torture`; build tag
`torture`). What one run actually does:

- A **stock `sqlite3` CLI writer** — not a test double, the same binary
  your application uses — runs a four-transaction script (inserts with
  random blobs, random-row updates, insert-selects, deletes) against a
  WAL-mode database, in a loop, for 5 minutes, while offshoot's capture
  engine follows the WAL live.
- In **roughly half of every round** (`rand.Intn(2)`), the writer is
  `SIGKILL`ed after a random 0–200 ms delay — mid-transaction, mid-WAL
  write, wherever the timer lands.
- Every 10th round, the **capture engine itself is bounced** — shut down
  and restarted against the same state directory mid-traffic — exercising
  the resume-vs-rebase decision under real concurrency. The run fails
  unless at least one bounce provably resumed from prior state
  (`Engine.Resumed()`) rather than rebasing from scratch.
- After **every single round**, the replica must converge with the live
  source database: the test compares full `sqlite3 .dump` output of both
  (`replay.Dump`) and fails on any divergence that doesn't resolve within
  15 seconds. Identical dump text, every round, or the run is red.

Real numbers from a run of this revision (macOS arm64, local disk — the
final line the test itself logs):

```
torture_test.go:148: torture complete: 3478 rounds, 347 bounces, 4 aggregate rebases
    across 348 session-starts, 346 resumed cleanly (resumed/bounce ratio: 1.00)
--- PASS: TestTortureWriterKill (300.06s)
```

In words: a 300-second run drove ~3,500 rounds of live SQLite traffic,
killed the writer with `SIGKILL` roughly 1,700 times (half of every
round), bounced the capture engine 347 times mid-traffic — 346 of which
resumed from prior state rather than rebasing — and the replica converged
to dump-identical content after every one of the ~3,500 rounds. Zero
divergence.

This isn't a one-off: the **nightly workflow runs the full torture suite
on Linux every day (skipping automatically when main has not moved
since the previous run), and on macOS on the weekly Sunday leg**
(`.github/workflows/nightly.yml`, `torture` job — macOS runners bill at
10x, so the macOS torture leg moved from daily to weekly; manual
dispatch runs both). This page is the canonical statement of CI cadence —
other docs link here rather than restating it. Beyond CI,
[CONTRIBUTING.md](../CONTRIBUTING.md) requires a torture run — named in
the PR description — for any change touching the capture or flush paths.

What the harness deliberately does *not* prove: it bounces the capturer
through its graceful-shutdown path, not a `SIGKILL` of the capturer
process itself (that case is argued safe in `internal/capture/engine.go`'s
shutdown/resume doc comments but is not exercised by this harness — the
test's own comments say so, and so does this page).

## Fencing and CAS, in two paragraphs

Every lineage — the append-only chain of storage objects behind a branch —
has **exactly one writer at a time**, enforced by a lease with an epoch
that bumps on every acquisition. Every object write lands as a create-only
put under the epoch current when it was written, so a writer that pauses,
loses its lease, and resumes later writes into a dead epoch prefix that no
ref will ever point at: garbage (collected later), never corruption of the
live chain. A fenced session refuses to write at all once it observes the
loss. The full scheme is
[architecture.md's invariants list](architecture.md#invariants);
the lease machinery is `internal/store/lease.go`.

Every branch pointer update — fork, checkpoint, promote, rollback,
destroy — is a **compare-and-swap** naming the exact prior state it
replaces; a losing writer gets a clean, typed error, never a silently
dropped update. This is why offshoot refuses to run at all against storage
that can't provide conditional writes: every store attach runs a CAS
capability probe (`internal/store/probe_test.go` pins it), and backends
that lack CAS — GCS's S3-interop API, notably — are rejected up front
rather than degraded to a weaker guarantee
([faq.md](faq.md#why-not-google-cloud-storage)). Destructive races have
their own guard: `destroy` CAS-writes a transient `Deleting` claim before
doing anything irreversible, closing the check-then-delete window
([reference.md](reference.md#claim-guarded-delete-milestone-4-task-6b)).

## Backend conformance, against real storage

Every storage backend passes one shared conformance suite
(`storetest.RunConformance`), which pins the semantics the fencing model
depends on — CAS behavior, create-only puts, list/delete edge cases:

- **Local filesystem backend**: `internal/store/conformance_local_test.go`,
  on every `go test` run.
- **S3 backend against a fake**: `internal/store/s3_test.go`, on every run.
- **S3 backend against real MinIO**: CI runs the full conformance suite
  plus the CAS probe against a real MinIO server in Docker on every PR and
  every push to main (`.github/workflows/ci.yml`, `s3-conformance` job →
  `make test-s3` → `TestS3RealProvider`,
  `internal/store/s3_integration_test.go`).
- **Real cloud providers**: the nightly workflow has a credentialed
  real-provider job (`.github/workflows/nightly.yml`,
  `real-provider-conformance`) that runs the same suite against an actual
  S3/R2/Tigris bucket when configured. Honest status: S3 support is
  MinIO-verified in CI on every run, and `TestS3RealProvider` — probe,
  full conformance, and the multipart subtest — has passed against a
  real AWS S3 bucket (us-east-1, 2026-08-13). R2/Tigris remain
  same-code-path, not independently run (see
  [stability.md](stability.md#proposed-v10-criteria)).

## The gates every change passes

From `.github/workflows/ci.yml` and the `Makefile`:

- **`go test ./... -count=1 -race`** on ubuntu-latest, every push and PR.
  The race detector is always on in CI, not an occasional extra. macOS
  coverage lives in the nightly workflow instead (`macos-test` job:
  the full `go test ./... -count=1` — note, without `-race` — on
  macos-latest, weekly Sunday leg and manual dispatch; same 10x-billing
  tradeoff as the torture matrix above).
- **`gofmt` and `go vet`** as hard CI steps; `staticcheck` via `make lint`
  (best-effort there only so offline runs don't fail the two gates that
  already passed).
- **No silent skips in CI.** Integration tests that need `sqlite3` or
  `sqldiff` hard-fail in CI when the binary is missing
  (`testutil.RequireExec`, armed by `CI=true`) instead of skipping green —
  a broken install step turns the build red, not quietly smaller.
- **Metrics lint**: `promtool check metrics` (the Prometheus project's own
  linter, pinned version) runs against a real dump of the daemon's metric
  exposition as a separate hard-gated CI job (`metrics-lint`, armed by
  `OFFSHOOT_REQUIRE_PROMTOOL`).
- **SDK gates**: the Python SDK's base suite runs with no pytest installed
  at all (proving the package has no hidden pytest dependency), the pytest
  plugin has its own suite including a real `pytest-xdist` two-worker run,
  and both SDKs' publishable artifacts are built and install-tested on
  every PR (`make test-sdks`, `test-pytest-plugin`, `dry-run-sdks`).

## Review discipline: mutation-verified regression tests

A regression test in this repo is expected to be **verified against the
pre-fix code** — actually run against the buggy revision to confirm it
fails there, then against the fix to confirm it passes — rather than
merely written to look plausible. Recent examples in the log: commit
`2682437` ("fix(store): stop a fenced writer's snapshot from shadowing the
live segment") states "Tests, all three mutation-verified against the
pre-fix code" and documents what each asserted pre-fix;
`internal/ops/compact_test.go` documents the exact mutation its no-op
assertion was verified against. [CONTRIBUTING.md](../CONTRIBUTING.md)
makes the surrounding policy explicit: behavioral changes don't get
reviewed without tests, and capture/flush changes don't merge without a
torture run.

## See also

- [stability.md](stability.md) — the pre-1.0 format contract these tests
  underwrite.
- [architecture.md](architecture.md#testing-strategy) — the design-level
  testing strategy.
- [status.md](status.md) — the per-feature implemented/tested matrix.
- [benchmarks.md](benchmarks.md) — performance numbers (measured, with
  method), as distinct from the correctness evidence here.
