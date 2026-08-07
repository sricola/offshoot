# TestEngineTakeoverExpectedRestartIsNotRebase: CI flake hardening

## Symptom

On GitHub Actions `ubuntu-latest` (2026-08-07T13:44), `TestEngineTakeoverExpectedRestartIsNotRebase`
(`internal/capture/engine_test.go`) failed:

```
engine_test.go:407: expected-restart continuation after a clean takeover must not rebase; Rebased() = 2, want 1
```

The SAFETY behavior under test is correct and untouched: `takeover()`
(`internal/capture/engine.go:1492`) compares frames consumed against
`checkpoint(RESTART)`'s reported log count (`log==consumed`); if a foreign
write landed in the gap between `endRead()` and the checkpoint call, RESTART
folds that write into the main DB unseen, and the engine correctly rebases
rather than silently losing it (this exact race was the point of Plan 1's
review — rebase-on-detection is the fix, not a bug). The test's job is to
assert the OTHER branch: a clean takeover (no foreign write in the gap)
continues without counting a rebase. The flake was the test's own
environmental assumption breaking, not the engine misbehaving.

## Root cause

The old test wrote 70 txns serially (1ms pacing) and then just slept 2s,
hoping the 64-txn takeover threshold would happen to be crossed only after
its own write loop had fully returned. But `afterDrain` (engine.go:757)
fires `takeover()` synchronously, in the same goroutine call, the instant
`captured` reaches 64 — there is no settling tick. With the engine's default
10ms poll ticker running freely in the background, that threshold crossing
can happen on ANY tick, including one that lands while the test's own loop
still has commits left to issue (crossing 64 happens around txn #64 of 70,
i.e. mid-loop under normal timing). When that happens, the test's OWN
still-in-flight writes are indistinguishable from a foreign writer
straddling the `endRead()`→`checkpoint(RESTART)` gap — and the engine
correctly rebases in response, exactly as designed. On a fast, idle machine
that gap is a few microseconds wide and rarely catches one; under loaded CI
(scheduler delays widening the gap, or delaying the ticker itself) it can,
and did.

## Fix shape

Chose shape (a): make the test's writer provably quiescent before the
takeover window, rather than probabilistically hope for it. Two parts,
both required together (`internal/capture/engine_test.go`,
`TestEngineTakeoverExpectedRestartIsNotRebase`):

1. **`Poll: time.Hour`** on the engine's `Options` (same pattern as
   `TestDrainNowCapturesPendingTransaction` and its siblings) — disables the
   free-running ticker for the test's lifetime. The only thing that can now
   invoke `drain`/`afterDrain`/`takeover` is an explicit `DrainNow` call the
   test itself makes.
2. Writes split into a below-threshold phase (60 txns, `DrainNow`'d and
   confirmed complete — captured=60 < 64, no takeover possible) and an
   over-threshold phase (10 more txns, run only after the first `DrainNow`
   returned, then `DrainNow`'d again). Since `DrainNow` is serviced
   synchronously by Run's own goroutine and only returns once fully
   serviced, the second `DrainNow` call is the ONE place `captured` can
   cross 64 and `takeover()` can run — and it does so with the writer
   goroutine already returned, so there is no foreign write anywhere left
   to land in the gap. The verified-clean (`log==consumed`) path is
   therefore guaranteed, not likely.

A trailing `insertTxns(5)` + `drainNow()` (unchanged in spirit from the
original) forces SQLite's lazy WAL-header rewrite to physically land and
drains it, driving the engine through the `wal.ErrWALRestarted` continuation
path the test exists to exercise.

`engine.go` was not touched — this is a test-only change.

## Verification

- **`go test ./internal/capture -run TestEngineTakeoverExpectedRestartIsNotRebase -count=50 -race`**:
  50/50 PASS, `rebased=1` every time (macOS, this checkout).
- **`go test ./internal/capture -count=1 -race`**: full package green.
- **Pre-fix reproduction under artificial CPU constraint**: reproduced
  successfully, twice, in Docker (`golang:1.24-bookworm`, `sqlite3` CLI +
  `gcc` for cgo, `--cpus=0.3` with 3 competing busy-loop processes sharing
  the same cgroup) against the ORIGINAL (main-branch) test file:
  - Run 1: `count=60` → 2 failures, both `Rebased() = 2, want 1` — the exact
    CI signature.
  - Run 2: `count=60` (killed early by the session timebox after ~13
    iterations) → 1 failure already observed, same `rebased=2` signature.
  A lighter constraint (`--cpus=0.5`, 4 busy loops, `count=30`) did NOT
  reproduce it — consistent with the root cause: the race window really is
  microsecond-scale and needs real scheduler pressure to hit.
- **Post-fix reproduction under the SAME artificial constraint**: attempted
  but did not complete within the session timebox — two attempts stalled on
  cgo-compiling the sqlite3 amalgamation under heavy throttling (fixed by
  priming a shared build-cache volume, but the retry was cut off by the
  timebox before finishing). Not claiming a completed hardened-vs-loaded
  head-to-head; the fix is justified by (1) the reasoned root cause matching
  the CI failure exactly, (2) removing the race at its root (no
  free-running ticker → no unprompted `takeover()` call → no possible
  foreign-write-in-gap during the test's own writes), and (3) 50x local
  `-race` plus full-package `-race` passing. Being honest about this gap
  rather than padding it with a rushed/ambiguous container result.

## Commit

Branch `fix/takeover-test-flake`, based on `main` @
`566ac8472d48d6ce068e9b402cc070067dd1419a` (merge of PR #12).
