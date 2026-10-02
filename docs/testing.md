# How offshoot is tested

offshoot's pitch leans on a durability claim, so this page shows the work:
what the torture harness actually does and its real numbers, the fencing
model that makes concurrent use safe, what the conformance suite proves
about storage backends, and the gates every change passes. Everything here
names the code or workflow that backs it.

**At a glance:**

| Claim | Evidence | Where it runs |
|---|---|---|
| A `kill -9`'d writer never corrupts the replica | `TestTortureWriterKill`: ~3,500 rounds, the writer `SIGKILL`ed in roughly half of them, dump-identical every round (the capturer is bounced gracefully, never killed) | nightly Linux, weekly macOS (`nightly.yml`) |
| One writer per lineage, always | lease epochs + create-only puts + CAS on every ref; CAS probe refuses stores without it | every `go test`, RustFS on every PR, AWS nightly |
| Storage backends behave identically | `storetest.RunConformance` | local + fake S3 every run; real RustFS every PR; real AWS nightly since 2026-09-25 |
| Parsers of untrusted bytes fail closed, never panic | five Go native fuzz targets (`make fuzz`) | seed corpora every `go test`; 60 s per target nightly (`nightly.yml`) |
| Per-test isolation is cheap and measured | `make bench-isolation`, one pasted run | [benchmarks.md](benchmarks.md#per-test-isolation-primitives-v0212) |
| Branch-heavy agent topologies hold up | `make bench-branchbench`, one pasted run | [benchmarks.md](benchmarks.md#branchbench-topologies-v0212) |
| What you download is what CI built | keyless cosign + SLSA provenance + SBOM on every tag | `release.yml`; verify per [installation](installation.md#verify-what-you-downloaded) |
| Regression tests fail on the bug they fix | mutation-verified against the pre-fix code | review policy, CONTRIBUTING.md |

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
killed the writer with `SIGKILL` in roughly half of the rounds (the test
does not log the exact count), bounced the capture engine 347 times
mid-traffic — 346 of which
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
([faq.md](faq.md#why-no-google-cloud-storage)). Destructive races have
their own guard: `destroy` CAS-writes a transient `Deleting` claim before
doing anything irreversible, closing the check-then-delete window
([reference.md](reference.md#claim-guarded-delete)).

## Backend conformance, against real storage

Every storage backend passes one shared conformance suite
(`storetest.RunConformance`), which pins the semantics the fencing model
depends on — CAS behavior, create-only puts, list/delete edge cases:

- **Local filesystem backend**: `internal/store/conformance_local_test.go`,
  on every `go test` run.
- **S3 backend against a fake**: `internal/store/s3_test.go`, on every run.
- **S3 backend against real RustFS**: CI runs the full conformance suite
  plus the CAS probe against a real RustFS server in Docker on every PR and
  every push to main (`.github/workflows/ci.yml`, `s3-conformance` job →
  `make test-s3` → `TestS3RealProvider`,
  `internal/store/s3_integration_test.go`). RustFS replaced MinIO here in
  v0.2.10 after MinIO withdrew its community images and binaries; it
  passes the identical suite, multipart preconditions included.
- **Real cloud providers**: the nightly workflow has a credentialed
  real-provider job (`.github/workflows/nightly.yml`,
  `real-provider-conformance`, gated on the `NIGHTLY_S3` repo variable —
  set and running green since 2026-09-25) that runs the same suite against
  an actual AWS S3 bucket (us-east-1). Honest status: S3 support is
  RustFS-verified on every push and AWS-verified nightly; MinIO was
  verified through v0.2.9 and is same-code-path but no longer
  re-verified; other S3-compatible providers are same-code-path only (see
  [stability.md](stability.md#proposed-v10-criteria)).

## Fuzzing the parsers that read untrusted bytes

Five Go native fuzz targets cover every parser that reads bytes offshoot
did not just write itself. Each seeds its corpus from the real encoder and
caps an input at 1 MiB; fuzz input never reaches cgo SQLite.

| Target | Package | Property |
|---|---|---|
| `FuzzValidateName` | `internal/store` | never panics; an accepted name round-trips through `RefKey`, stays under the local backend's root, is one path element, and is never `~by-chain` |
| `FuzzDecodeSnapshot` | `internal/ltxio` | `MaterializeChain`/`Materialize` fail closed (no destination, no temp file) or produce a file whose checksum equals the trailer's post-apply checksum |
| `FuzzApplySegments` | `internal/ltxio` | the same for a segment applied with `ApplySegments`, both as raw bytes and as a CRC-valid segment built from the input and checked against an in-memory model of the apply |
| `FuzzReadSidecar` | `internal/ops` | `readSidecar` returns ok=false with a zero record, or a record with a hash that is a fixed point of the `.sum` format |
| `FuzzDecodeRequest` | `internal/daemon` | the request decoder shared by the unix socket and `POST /rpc` never panics or stalls on a stream, and every request it yields is a fixed point of the wire format |

The LTX targets also panic on any execution over 10 s, because Go's fuzzer
has no per-input timeout and would otherwise report a hang as a pass.

- **On every `go test`**: each target's seed corpus runs as an ordinary
  test.
- **On demand**: `make fuzz` runs each target for 30 s (~3 minutes in
  total; `FUZZTIME=2m make fuzz` for longer).
- **Nightly**: the `fuzz` job in `.github/workflows/nightly.yml` runs each
  target for 60 s on the daily leg, fails on any crasher, and uploads
  `testdata/fuzz/**` as the `fuzz-crashers` artifact. To reproduce one, drop
  the file into `internal/<pkg>/testdata/fuzz/<Target>/` and run
  `go test ./internal/<pkg> -run '<Target>/<file>'`.

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
  the LangGraph checkpoint companion compiles and runs a real `StateGraph`
  from its isolated `[test]` extra, and both SDKs' publishable artifacts are
  built and install-tested on every PR (`make test-sdks`,
  `test-pytest-plugin`, `test-python-langgraph`, `dry-run-sdks`).

## Review discipline: mutation-verified regression tests

A regression test in this repo is expected to be **verified against the
pre-fix code** — actually run against the buggy revision to confirm it
fails there, then against the fix to confirm it passes — rather than
merely written to look plausible. Recent examples in the log: commit
`9314382` ("fix(store): stop a fenced writer's snapshot from shadowing the
live segment") states "Tests, all three mutation-verified against the
pre-fix code" and documents what each asserted pre-fix;
`internal/ops/compact_test.go` documents the exact mutation its no-op
assertion was verified against. [CONTRIBUTING.md](../CONTRIBUTING.md)
makes the surrounding policy explicit: behavioral changes don't get
reviewed without tests, and capture/flush changes don't merge without a
torture run.

## Signed releases

Every tagged release since v0.2.11 is signed by the release workflow
itself with cosign's keyless mode, bound to a certificate identity scoped
to `.github/workflows/release.yml` in this repo — there is no
maintainer-held signing key to leak or rotate. Each artifact also carries
SLSA build provenance as a GitHub attestation, tying the binary back to
the exact workflow run and commit that produced it. An SPDX SBOM of the
Go module graph (`offshoot_<tag>.spdx.json`) ships with every release,
with its own matching attestation. The three independent checks and the
exact commands are at
[installation.md's verification section](installation.md#verify-what-you-downloaded);
releases before v0.2.11 carry checksums only.

## What is not proven here

- **Power loss.** `TestTortureCapturerKill` (`internal/capture/torture_capturer_test.go`,
  build tag `torture`, part of `make test-torture` and the nightly
  `torture` job) runs the capture engine in a child process, `SIGKILL`s it
  mid-traffic every round, restarts it on the same state directory and
  replica, and requires the replica's `.dump` to equal the source's after
  a graceful drain — so the capturer's own crash is exercised, not only
  argued. What no harness does is cut power: the kernel is trusted to
  write back what a killed process had issued. A daemon that
  dies — however it dies — loses up to one `-flush-every` interval
  (default 30 s) of committed-but-unflushed writes
  ([limitations](limitations.md#durability-advances-on-flush-and-the-window-is-explicit)).
- **No power-loss test.** Since v0.2.14 every rename into place in the
  local store, the checkouts and the capture state is followed by a
  directory fsync, and `.sum` sidecars are written atomically, so a local
  store is designed to keep what was flushed across power loss; nothing
  pulls the plug to check.
- **macOS runs without the race detector.** The nightly `macos-test` job
  runs the full suite, including the torture harness, on macOS — but
  without `-race`. `-race` coverage is Linux-only, on every push and PR.
- **Only RustFS and AWS S3 are independently verified against real
  storage.** Every other S3-compatible provider is same-code-path only:
  it should pass the identical conformance suite, but it has not itself
  been run against a live instance in CI. MinIO was verified through
  v0.2.9 and is no longer re-verified after upstream withdrew its
  community images.
- **Benchmark numbers are one machine, one run.** The copy-on-write,
  isolation, and BranchBench numbers in [benchmarks.md](benchmarks.md)
  are quantiles within a single run on a single laptop, not a
  median-of-N across runs or a fleet average; repeat runs have reproduced
  completion counts exactly and wall times within a few percent, but
  run-to-run variance is real.

## See also

- [stability.md](stability.md) — the pre-1.0 format contract these tests
  underwrite.
- [architecture.md](architecture.md#testing-strategy) — the design-level
  testing strategy.
- [status.md](status.md) — the per-feature implemented/tested matrix.
- [benchmarks.md](benchmarks.md) — performance numbers (measured, with
  method), as distinct from the correctness evidence here.
