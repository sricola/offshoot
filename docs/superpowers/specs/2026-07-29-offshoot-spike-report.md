# offshoot capture-spike report

**Verdict:** GO for Plan 2.

## What was proven

- **Foreign-writer capture (stock sqlite3 CLI):** 6859 combined torture
  rounds run today across two harnesses (4691 rounds in `./bin/torture -d
  10m`, 2168 rounds in `make test-torture` / `TestTortureWriterKill`), 2353
  measured writer `SIGKILL`s (counted explicitly by `./bin/torture`;
  `TestTortureWriterKill` applies the identical ~50%-per-round `SIGKILL`
  mechanism concurrently with capturer bounces but does not print a separate
  kill counter, so it is cited by rounds, not folded into the kill count to
  avoid overstating precision), **0 undetected divergences** across both
  runs — neither harness ever hit its `DIVERGED`/`t.Fatalf` divergence path.
  Prior-session evidence (task-5/task-7 reports, same harnesses, same
  machine): 3371 rounds/1 rebase and 2229 rounds/222 bounces/1 aggregate
  rebase, also zero divergence — consistent with today's numbers.
- **Capturer crash:** 216 engine bounces (`make test-torture`, forced fresh
  with `-count=1` — the cached `go test` result from an unmodified prior run
  was discarded as not being today's own measurement), 2 aggregate rebases
  across 217 session-starts, **216/216 bounces resumed cleanly** (`Engine.
  Resumed()` ratio 1.00). "Every miss detected" is proven independently of
  torture scale by the deterministic unit tests
  `TestEngineDetectsMissedWritesAfterCrash`,
  `TestEngineDetectsInPlaceUpdateAfterCleanShutdown`, and
  `TestEngineResumeAppliesNothingBeforeNewWrite` (all PASS in today's
  `make test` run), which construct the exact "wrote while dead" and
  "in-place UPDATE while dead" scenarios and assert a forced, counted rebase
  — silent resume is proven to occur only in the provably-clean case (salts
  unmoved + WAL empty + main-file SHA-256 match).
- **Passive foreign checkpoints survived without rebase:** yes.
  `TestEngineSurvivesForeignPassiveCheckpoint` run today, 15/15 fresh PASS
  under `-race` (`Rebased() == 1` held on every run — the checkpoint issued
  by the test's foreign connection never forced an extra rebase). Consistent
  with task-5's original finding (also 15/15 under `-race`).

## What was NOT proven (deferred to Plan 2+)

- LTX encoding (spike used raw frame replay), S3 backends, fork/branch ops.
- Resume from mid-WAL offsets (spike resumes only at offset 32; else
  rebases).
- Sustained-throughput capture lag under continuous write load (not
  measured; the spike's torture harnesses drive bursty single-writer traffic
  with idle gaps, not a saturated pipeline).
- macOS AND Linux both torture-tested: **macOS only.** All torture evidence
  in this report and in the task-5/task-7 reports was collected on
  macOS/APFS (`Darwin 27.0.0`, arm64, confirmed via `uname -a` at
  report-writing time). Linux is untested — filesystem differences (ext4/
  XFS `fsync`/rename semantics, mtime resolution, WAL locking behavior under
  `flock` vs. macOS's `fcntl`) are unverified and are a Plan 2 prerequisite,
  not an assumption to carry forward silently.
- **Documented residual risk, not closed:** a foreign write folded between
  `shutdown()`'s verified-clean `checkpoint(RESTART)` and its subsequent
  `checkpoint(TRUNCATE)` can silently advance the resume baseline. Narrowed
  to the microseconds between a pre-`TRUNCATE` on-disk WAL parse
  (`walRacedSinceRestart`) and `TRUNCATE` acquiring its own locks, but not
  closed to zero from outside SQLite's pragma interface. Plan 2's LTX sink
  closes this structurally, by comparing cumulative checksums at
  shutdown/resume instead of reasoning about a gap between two separate
  checkpoint calls. See `internal/capture/engine.go`, `shutdown()`'s "KNOWN
  RESIDUAL RISK" doc comment, and task-7-report.md's hardening-pass section
  for the full derivation.

## Surprises / constraints discovered

- The plan's original `Reader.Bind` never seeded the frame checksum from the
  WAL header — a resumed reader would silently drop every subsequent
  transaction. Found by review before it shipped; fixed by seeding the
  checksum on `Bind` (`needSeed`).
- The plan's original checkpoint-takeover algorithm had a silent-loss race:
  frames could be folded into the main DB by `PRAGMA wal_checkpoint(RESTART)`
  in the window between releasing the read lock (`endRead`) and the
  checkpoint call itself, with no detection. Empirically reproduced against
  the unfixed code (2 of 10 concurrent-writer runs failed with permanent
  divergence). Fixed via checkpoint frame-count verification
  (`log == consumed`) plus `expectRestart` continuation semantics so a
  verified-clean takeover's later, lazy WAL reset is treated as a
  continuation rather than a second, spuriously-counted rebase.
- SQLite's `RESTART` checkpoint reset is **lazy**: the on-disk WAL header
  (new random salts, truncated-looking state) is only rewritten by the
  *next* writer's commit, not at checkpoint time. A naive "bind a fresh
  reader immediately after a clean RESTART" design was tried and caused a
  livelock — the fresh reader re-bound to the still-unchanged file and
  re-consumed already-applied frames, spiking the takeover threshold and
  starving the foreign writer of a clean window to make progress.
- Clean-shutdown resume initially re-applied the entire prior session
  through the `Sink` on restart, because `RESTART` doesn't physically
  truncate the WAL file (only a `TRUNCATE` checkpoint does) — old,
  already-applied frames were still present on disk and got re-parsed and
  re-emitted. Masked in testing by `replay.Replica.Apply`'s incidental
  idempotency (unconditional page-number writes), which the planned LTX sink
  will not have. Fixed: `shutdown()` = verified-clean `RESTART` +
  `TRUNCATE` + a SHA-256 whole-file content hash of the main DB, persisted
  in saved state; `tryResume()` requires a clean marker, a provably-empty
  on-disk WAL, and a matching content hash before it will resume. An
  mtime+size fingerprint was tried first and rejected under adversarial
  review: in-place `UPDATE`s of fixed-width rows defeat the size check, and
  coarse filesystem mtime resolution (HFS+/FAT/NFS, 1s granularity) defeats
  the mtime check.
- SQLite deletes the `-wal`/`-shm` files when the last connection to the
  database closes (its own auto-checkpoint-on-close behavior). Both the
  resume design and the test harnesses had to account for this explicitly —
  several tests hold open a foreign `*sql.DB` connection for their duration
  specifically so the capturer's own close is never the database's last
  connection, matching the intended production topology (application holds
  its own connection; capturer is bounced independently).
- The `Sink` interface now documents that `Apply` is never called twice for
  the same transaction (no idempotency requirement) — a contract that only
  became actually true once the clean-shutdown TRUNCATE fix landed, and one
  Plan 2's LTX sink depends on.

## Go/no-go rationale

Zero undetected divergences across 6859 combined torture rounds and 2353
writer kills today (plus consistent zero-divergence results in the two
preceding sessions' torture runs), with every discovered loss mode — the
takeover fold race, the lazy-RESTART livelock, and the clean-shutdown
re-application bug — caught by review or TDD before it could ship silently,
fixed, and then empirically re-verified to stay fixed under sustained
torture (216/216 capturer bounces resumed cleanly today; 15/15 passive
checkpoints survived without rebase). This directly answers the spec's
risk #4 ("capture spike fails: foreign-connection capture proves flaky under
torture → the design pivots"): capture did not prove flaky — every failure
mode found was detectable, not silent, and the one residual gap (the
`RESTART`→`TRUNCATE` shutdown window) is narrowed to microseconds, documented
rather than hidden, and has a structural close already designed into Plan
2's LTX cumulative-checksum sink. Proceed to Plan 2 (storage layer), with
Linux torture validation as an early Plan 2 task rather than a blocking
precondition, since nothing in the surprises list above is macOS-specific in
its root cause (WAL semantics are cross-platform; only the untested
filesystem-level durability primitives are OS-specific).
