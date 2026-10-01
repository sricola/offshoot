# Tier 2 Follow-ups and Code-Scanning Remediation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the four Tier 2 follow-ups ROADMAP.md recorded after v0.2.12, fix the three known flaky tests structurally, and clear every fixable alert on the repo's code-scanning page (all fourteen are OpenSSF Scorecard findings).

**Architecture:** Six independent tasks on one branch. Tasks 1-2 touch `internal/ops` (and `internal/store` for a new `Head`), Task 3 is a package move, Task 4 is test hygiene in `internal/session`, `internal/ops` and `internal/capture`, Task 5 is CI/Dockerfile/SECURITY.md hygiene plus a CodeQL workflow, Task 6 adds Go native fuzz targets and a nightly fuzz leg. No on-bucket format change anywhere.

**Tech Stack:** Go 1.26 (cgo, mattn/go-sqlite3), `golang.org/x/sys/unix`, GitHub Actions (SHA-pinned), OpenSSF Scorecard (validate with `scorecard --local . --checks <Check>`; install with `brew install scorecard` or `go run github.com/ossf/scorecard/v5@latest`), pip-tools for hash-pinned requirements, Go native fuzzing.

**Spec:** This plan was produced on the brainstorming skill's bounded path; the design was presented in chat on 2026-10-01 and approved under the user's standing instruction to decide autonomously. The "Design" section below is that design, verbatim in substance, and is the binding authority for the tasks.

## Design

1. **Cache-backed refresh.** Rollback, Promote and Compact refresh the writable checkout with a full decode via `materializeAt` (ops.go ~1286, ~1515, ~1657). They will resolve the new head's chain once and call `materializeFromChain` (chainid.go:156), the same path `checkout` uses, so a refresh clones from the by-chain cache when it can; then they stamp and refresh the shadow exactly as today. `planSegment` (checkpoint_delta.go:66) gains a `members []store.ChainMember` parameter; `CheckpointWith` resolves the chain once and passes it.
2. **Same-kind checkpoint overwrite detected.** After winning the ref CAS, `CheckpointWith` confirms the object it uploaded is still its own by comparing the etag `PutIf` returned with the object's current etag from a new `Store.Head(key) (etag string, size int64, err error)`. On mismatch it stamps the sidecar with `PostApplyChecksum=0`, drops the shadow, and records a counter. docs/limitations.md's "do not run concurrent at-rest checkpoints" paragraph becomes "detected and recovered: the next checkpoint is a snapshot and no daemon session trusts the overwritten content".
3. **`internal/ops/reflink` → `internal/reflink`.** Seven importers, no behaviour change.
4. **Three flakes fixed structurally.** Flush ref-CAS retry becomes a budget (≥ 8 attempts AND ≥ 4 × RenewEvery of wall time); `TestForceDestroyStillClaimGuards` is split into two deterministic orderings; `TestEngineResumesCleanly` waits for the second engine to signal state loaded before writing.
5. **Scorecard remediation.** Pin pip installs by hash, `npm ci` where a lockfile exists, Dockerfile base images by digest with Dependabot keeping them fresh, absolute URLs in SECURITY.md, a CodeQL workflow, dismiss the solo-maintainer Code-Review alert with a comment, and list CII-Best-Practices (user must register) and Maintained (self-heals at 90 days) in docs/status.md's standing nag.
6. **Fuzzing.** Go native fuzz targets for the parsers that read untrusted bytes (store names, LTX snapshot/segment decode, sidecar JSON, daemon protocol), seeded from the encoders, plus a bounded nightly fuzz leg.

## Global Constraints

- No `LayoutVersion` bump and no on-bucket format change. `Store.Head` is a read-only addition to the store interface; every backend (Local, S3, FakeS3) implements it and the conformance suite covers it.
- Every fast path keeps today's behaviour as its fallback; forced-fallback tests exist where a new fast path is added.
- Checksums are verified, not assumed: a test that refreshes a checkout compares bytes with `Export`.
- Tests are fixed structurally, never by loosening an assertion or adding a sleep.
- Every GitHub Action stays SHA-pinned with a trailing `# vX.Y.Z` comment; Dependabot bumps them.
- Commit trailers: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01Ea74MaoS1XeSTcxxSx2ntJ`.

## Review Focus

1. A refresh on a filesystem that cannot clone (ext4) must still land the checkout via the materialize fallback with identical bytes — Task 1 adds `TestRefreshDegradesWhenReflinkUnsupported`.
2. A `Head` on a key that was never written must return `store.ErrNotFound`, not a zero etag that happens to compare unequal — Task 2's conformance subtest pins it.
3. A same-kind overwrite where the loser's content is byte-identical (etag equal) must not be reported — Task 2's `TestIdenticalOverwriteIsNotFlagged`.
4. Hash-pinned requirements must resolve on CI's interpreter (ubuntu, python 3.12 in publish.yml, the system python3 in ci.yml) — Task 5 verifies by pushing and watching the `sdks` and `test` jobs, not by local resolution alone.
5. A fuzz target that calls into cgo SQLite must bound input size so the nightly leg cannot hang — Task 6 caps inputs at 1 MiB and uses `-fuzztime` per target.

---

### Task 1: Cache-backed refresh for Rollback, Promote, Compact; one Chain resolution per checkpoint

**Files:**
- Modify: `internal/ops/ops.go` (the three `materializeAt(next, headCheckpoint(next), path)` sites in `RollbackWith`, `PromoteWith`, `Compact`; `CheckpointWith`'s `planSegment` call), `internal/ops/checkpoint_delta.go` (`planSegment` signature; remove its own `w.Store.Chain` call at :83), `internal/ops/chainid.go` (a small exported-within-package helper if needed, e.g. `refreshFromChain(db, ref, path) (checksum uint64, chainID string, err error)` that resolves members, calls `materializeFromChain(..., 0o600)`, prunes the by-chain area, and returns what the stamp needs)
- Test: `internal/ops/refresh_cache_test.go` (new), `internal/ops/checkpoint_delta_test.go` (chain-resolution count)

**Interfaces:**
- Consumes: `materializeFromChain(db, lineage string, members []store.ChainMember, dst string, mode os.FileMode) (chainPlacement, error)` where `chainPlacement` carries `kind` ("clone"|"clone+segments"|"materialize"), `checksum`, `chainID`, `hash`; `observeCheckoutSource` test hook; `reflinkUnsupportedForTest`; `writeSum`, `refreshShadow`; `w.Store.Chain(lineage, txid)`.
- Produces: `planSegment(path string, ref store.Ref, members []store.ChainMember, opts CheckpointOptions) (segmentDelta, bool)`; the three refresh sites emit `observeCheckoutSource` kinds like `checkout` does.

- [ ] **Step 1: Failing tests**

```go
// refresh_cache_test.go
func TestRollbackRefreshClonesFromByChainCache(t *testing.T) {
	// seed 8 MiB, Checkpoint "a"; write; Checkpoint "b"; Checkout once (populates the entry for "b"'s chain);
	// observe kinds; RollbackWith(to "a"); expect the refresh's kind to be "clone" or "clone+segments"
	// (the entry for "a"'s chain exists from the first materialize of "a" when "b" was built on it, or is
	// built now — assert kinds[last] != "materialize" on the SECOND rollback to "a" at minimum);
	// checkout bytes == Export("a"); checkoutState == "clean".
}
func TestPromoteRefreshClonesFromByChainCache(t *testing.T)  { /* fork attempt, write, Checkpoint, Checkout main once, Promote → kind not "materialize" on a second promote of an identical head; bytes == Export(attempt head) */ }
func TestCompactRefreshUsesByChainCache(t *testing.T)        { /* Compact → the refresh goes through materializeFromChain (kind observed); bytes unchanged */ }
func TestRefreshDegradesWhenReflinkUnsupported(t *testing.T) { /* reflinkUnsupportedForTest = true; Rollback → kind "materialize", bytes == Export("a"), no ~by-chain dir */ }

// checkpoint_delta_test.go
func TestCheckpointResolvesChainOnce(t *testing.T) {
	// wrap w.Store in a counting store (count Chain-related List calls, or add a test hook observeChainResolve
	// in ops called wherever w.Store.Chain is invoked on the checkpoint path); one segment checkpoint ⇒ exactly 1.
}
```

Run: `go test ./internal/ops -run 'RefreshCloses|RefreshDegrades|ResolvesChainOnce|RefreshUses' -v` → FAIL.

- [ ] **Step 2: Implement.** In each of the three verbs replace

```go
checksum, chain, err := w.materializeAt(next, headCheckpoint(next), path)
```

with a call to a new helper in chainid.go:

```go
// refreshFromChain lands the writable checkout for ref's head the way
// CheckoutProven does: clone from the by-chain cache when the chain (or a
// prefix of it) is cached, materialize otherwise. It returns the post-apply
// checksum and chain ID the caller stamps into the sidecar.
func (w *Workspace) refreshFromChain(db string, ref store.Ref, path string) (uint64, string, error) {
	members, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
	if err != nil { return 0, "", err }
	placed, err := w.materializeFromChain(db, ref.Lineage, members, path, 0o600)
	if err != nil { return 0, "", err }
	if placed.kind != "clone" { w.pruneByChain(db) }
	return placed.checksum, placed.chainID, nil
}
```

Keep the existing `writeSum(...)` + `refreshShadow(path)` after it. Change `planSegment` to take `members` and delete its internal `Chain` call; in `CheckpointWith` resolve `members` once (it may already resolve for `snapshotMayExist`/bound — reuse) and pass them.

- [ ] **Step 3:** `go test ./internal/ops -count=1 -race`; `go vet ./...`; `gofmt -l .` empty.
- [ ] **Step 4: Commit** `"ops: rollback, promote and compact refresh the checkout through the by-chain cache; checkpoint resolves the chain once"`.

---

### Task 2: Detect a same-kind checkpoint overwrite after the CAS

**Files:**
- Modify: `internal/store/store.go` (interface: `Head(key string) (etag string, size int64, err error)`; document: `ErrNotFound` when absent), `internal/store/local.go`, `internal/store/s3.go`, the fake S3 backend (grep `type fakeS3` / `FakeS3`), `internal/store/conformance.go` (subtests `HeadReturnsEtagAndSize`, `HeadMissingIsErrNotFound`, `HeadEtagChangesWhenContentChanges`), `internal/ops/ops.go` (`CheckpointWith`: capture `PutIf`'s returned etag; after the winning `PutRef`, `Head` the object; on etag mismatch stamp `PostApplyChecksum=0`, `dropShadow(path)`, increment a counter), `internal/ops/metrics` wiring (an ops-side observer + `offshoot_checkpoint_overwrite_detected_total` in internal/daemon/metrics.go next to the fork counters, wired in server.go), `docs/limitations.md` (the concurrent-checkpoint paragraph), `docs/status.md` (the known-limitation row becomes shipped-and-tested), `docs/operations.md` (metric row), `ROADMAP.md` (bullet → ✅).
- Test: `internal/ops/checkpoint_overwrite_test.go` (new), `internal/store/store_test.go` or conformance.

**Interfaces:**
- Consumes: `Store.PutIf(key, data, ifMatch) (etag string, err error)` (read its exact signature at store.go — if it does not return the etag today, add the return and update callers), `checkpointAfterQuiesceForTest` hook (Task 3 of the previous plan) to sequence two racers, `dropShadow`, `stampSumWithFingerprint`/`StampSumHashOnly`.
- Produces: `Store.Head`; `ObserveCheckpointOverwrite func()` hook in ops; counter name above.

- [ ] **Step 1: Failing tests**

```go
func TestSameKindOverwriteIsDetectedAndUntrusted(t *testing.T) {
	// seed; two snapshot checkpoints racing via checkpointAfterQuiesceForTest with a WRITE to the checkout
	// between their encodes (so contents differ); the winner's post-CAS Head sees the loser's etag;
	// assert: winner's sidecar PostApplyChecksum == 0, no .shadow file, overwrite counter == 1,
	// next CheckpointWith is Kind "snapshot", and CheckoutAt(head) equals the checkout after that snapshot.
}
func TestIdenticalOverwriteIsNotFlagged(t *testing.T) { /* same race, no write between encodes ⇒ counter 0, checksum kept, shadow present */ }
func TestOverwriteDetectionSkipsWhenHeadUnsupported(t *testing.T) { /* if any backend returns ErrUnsupported from Head (none should), the checkpoint still succeeds and stamps normally — pin the contract: Head errors other than mismatch do not fail the checkpoint; they log */ }
```

Conformance: `Head` of a written key returns the `PutIf` etag and the byte length; missing key → `ErrNotFound`; overwrite with different bytes changes the etag; overwrite with identical bytes keeps it (Local etags are content hashes; S3 etags are MD5 for single-part — assert "changes when content changes", and "equal to PutIf's return").

- [ ] **Step 2: Implement** across the three backends and `CheckpointWith`; stamping on mismatch uses `StampSumHashOnly(path, hash, lineage, epoch, txid, 0, "")` then `dropShadow(path)`.
- [ ] **Step 3:** `go test ./internal/store ./internal/ops ./internal/daemon -count=1 -race`; the S3 conformance test against the fake backend runs in `go test ./internal/store`; `go vet ./...`; `gofmt -l .` empty.
- [ ] **Step 4: Docs** as listed. **Step 5: Commit** `"store, ops: Head returns an object's etag; a checkpoint that wins the CAS over an overwritten object stops trusting its checksum"`.

---

### Task 3: Move `internal/ops/reflink` to `internal/reflink`

**Files:**
- Move: `internal/ops/reflink/*.go` → `internal/reflink/*.go` (`git mv`; package name stays `reflink`).
- Modify import paths in: `cmd/offshoot/main_test.go`, `internal/ltxio/segment.go`, `internal/ops/chainid.go`, `internal/ops/clone_test.go`, `internal/store/local.go`, `internal/session/flush.go`, `internal/session/flush_test.go` (grep `internal/ops/reflink` to confirm the list), `.github/workflows/ci.yml` (`cow-paths` job's package list), `docs/architecture.md` if it names the path, `ROADMAP.md` bullet → ✅.

- [ ] **Step 1:** `git mv internal/ops/reflink internal/reflink`; `grep -rl 'internal/ops/reflink' . | xargs sed -i '' 's#internal/ops/reflink#internal/reflink#g'`; `go build ./... && go vet ./...`.
- [ ] **Step 2:** `go test ./internal/reflink ./internal/ltxio ./internal/ops ./internal/store ./internal/session ./cmd/offshoot -count=1`; confirm `go list -deps ./internal/ltxio | grep internal/ops` prints nothing.
- [ ] **Step 3: Commit** `"refactor: internal/reflink is its own leaf package; ltxio no longer imports from ops"`.

---

### Task 4: Fix the three flaky tests structurally

**Files:**
- Modify: `internal/session/flush.go` (the `attempt < 8` retry bound around :573-600 → a budget), `internal/session/flush_test.go` (no assertion change; add a comment), `internal/ops/destroy_claim_test.go` (`TestForceDestroyStillClaimGuards` → two subtests with injected ordering), `internal/capture/engine_test.go` (`TestEngineResumesCleanly` waits for ready), `internal/capture/engine.go` only if no ready signal exists (add a `Ready() <-chan struct{}` closed once resume state is loaded, or reuse what 5a55fad's concurrent-takeover test uses — read that test first and copy its mechanism).

- [ ] **Step 1: Flush retry budget.** Replace the fixed count with:

```go
// The retry exists for exactly one rival: our own lease heartbeat
// renewing the ref under us. A fixed count is a flake under load (the
// 2026-09-26 TestFlushSurvivesOwnLeaseRenewalRace failures), so the bound
// is a budget: at least flushCASMinAttempts tries AND at least
// flushCASMinRenewals heartbeat periods of wall time.
const flushCASMinAttempts = 8
const flushCASMinRenewals = 4
start := time.Now()
for attempt := 0; attempt < flushCASMinAttempts || time.Since(start) < time.Duration(flushCASMinRenewals)*s.renewEvery; attempt++ { ... }
```

(use the field that holds `RenewEvery` in the session). Test: run `go test ./internal/session -run TestFlushSurvivesOwnLeaseRenewalRace -count=30 -race` while `go test ./internal/ops -count=1` runs in parallel to load the machine; both green.

- [ ] **Step 2: Force-destroy claim test.** Read `TestDestroyClaimGuards` and the ops test hooks (grep `ForTest` in destroy*.go). Split into `t.Run("lease acquired before destroy reads the ref", ...)` — sequence AcquireLease to complete before Destroy's GetRef (hook or explicit ordering) and assert BOTH succeed (force bypasses the live-lease pre-check by design); and `t.Run("claim lands before the lease", ...)` — Destroy's Deleting claim first, then AcquireLease must fail. Run `-count=200` on macOS: 200/200.
- [ ] **Step 3: Engine resume test.** After `go func() { done2 <- e2.Run(ctx2) }()`, wait on the engine's ready signal before the INSERT. Run `-count=40`.
- [ ] **Step 4:** `go test ./internal/session ./internal/ops ./internal/capture -count=1 -race`. **Step 5: Commit** `"test: flush CAS retry is a time budget; force-destroy claim test pins both orderings; engine resume test waits for ready"`.

---

### Task 5: Scorecard remediation (code-scanning alerts 1-10, 13; nag rows for 12, 14)

**Files:**
- Modify: `.github/workflows/publish.yml` (:138 `pip install --upgrade build twine`; :227 `npm install` → `npm ci` since sdk/typescript has a lockfile — verify `sdk/typescript/package-lock.json` exists; :238 the packed-tarball install), `.github/workflows/ci.yml` (:267, :282, :303 pip installs), `Dockerfile` (:10 `golang:1.26-bookworm`, :25 `debian:bookworm-slim` → `image:tag@sha256:<digest>`; get digests with `docker buildx imagetools inspect <image:tag> --format '{{json .Manifest.Digest}}'`), `.github/dependabot.yml` (add `docker` ecosystem, weekly, grouped), `SECURITY.md` (add absolute URLs: `https://github.com/sricola/offshoot/security/advisories/new` for private reporting, `https://github.com/sricola/offshoot/blob/main/docs/installation.md#verify-what-you-downloaded`), `.github/workflows/codeql.yml` (new: `github/codeql-action/{init,analyze}` SHA-pinned, language go, on push main + pull_request + weekly; `permissions: security-events: write`), `docs/status.md` (standing nag: CII-Best-Practices badge registration at https://www.bestpractices.dev is user-gated; Maintained resolves itself when the repo passes 90 days; Code-Review needs a second human reviewer), `docs/ci-recipes.md` or `CONTRIBUTING.md` (how to refresh the hash-pinned requirements).
- Create: `requirements/ci-publish-tooling.txt`, `requirements/ci-pytest-plugin.txt`, `requirements/ci-langgraph.txt` (hash-pinned via `pip-compile --generate-hashes --output-file ... <in-file>` from `.in` files you also commit), each with a header comment naming the command that regenerates it.

**Method for pip:** third-party packages install with `pip install --require-hashes -r requirements/<file>.txt`; the repo's own packages install afterwards with `pip install --no-deps -e sdk/python` (and `-e "sdk/python-langgraph"` without extras, the extras' deps being in the hash file). Validate with `scorecard --local . --checks Pinned-Dependencies --show-details` (install scorecard first) before pushing; if the local `-e` installs are still flagged, keep them (they are not downloads) and record the residual count in the commit message and docs/status.md.

- [ ] **Step 1:** generate the three hash-pinned files on python 3.12 (`python3.12 -m venv /tmp/pt && /tmp/pt/bin/pip install pip-tools && /tmp/pt/bin/pip-compile --generate-hashes ...`); edit the four workflow run blocks; `npm ci` where a lockfile exists; Dockerfile digests; dependabot docker; SECURITY.md URLs; codeql.yml; docs rows.
- [ ] **Step 2:** `scorecard --local . --checks Pinned-Dependencies,Security-Policy,SAST --show-details` → paste the output in the report; Pinned-Dependencies and Security-Policy must score 10 or the residual must be explained; SAST cannot score locally (needs check-runs) — it rises after CodeQL runs on main and on PRs.
- [ ] **Step 3:** `python3 -c 'import yaml; [yaml.safe_load(open(f)) for f in [".github/workflows/publish.yml",".github/workflows/ci.yml",".github/workflows/codeql.yml"]]'`; `docker build --target build -t offshoot-pin-check .` (proves the digest-pinned base pulls); `make dry-run-sdks` locally if python3.12 is available.
- [ ] **Step 4:** Dismiss alert 13 (Code-Review) with `gh api -X PATCH repos/sricola/offshoot/code-scanning/alerts/13 -f state=dismissed -f dismissed_reason="won't fix" -f dismissed_comment="Solo-maintainer project; changes land via PRs (since #61) but no second human reviewer exists. Revisit when a second maintainer joins (GOVERNANCE.md)."`. Leave 12 and 14 open (documented in status.md).
- [ ] **Step 5: Commit** in two commits: `"ci, docker: hash-pinned pip installs, npm ci, digest-pinned base images, Dependabot for Docker; SECURITY.md links; CodeQL workflow"` and `"docs: standing nag rows for the Scorecard checks no commit can close"`.

---

### Task 6: Go native fuzz targets and a nightly fuzz leg

**Files:**
- Create: `internal/store/fuzz_test.go` (`FuzzValidateName`: never panics; accepted names round-trip through key builders and `ValidateName(dir)` rejects `~by-chain`), `internal/ltxio/fuzz_test.go` (`FuzzDecodeSnapshot`, `FuzzApplySegments`: seeds from `EncodeSnapshot`/`EncodeSegment` of a tiny database; mutated bytes must return an error or a correct file, never panic or hang; cap input at 1 MiB with `if len(data) > 1<<20 { t.Skip() }`), `internal/ops/fuzz_test.go` (`FuzzReadSidecar`: arbitrary JSON bytes → `readSidecar` returns ok=false or a record whose fields are consistent; never panics), `internal/daemon/fuzz_test.go` (`FuzzDecodeRequest`: the protocol decoder on arbitrary bytes never panics).
- Modify: `.github/workflows/nightly.yml` (a `fuzz` job on the daily leg: `for t in FuzzValidateName FuzzDecodeSnapshot FuzzApplySegments FuzzReadSidecar FuzzDecodeRequest; do go test ./internal/... -run '^$' -fuzz "^$t\$" -fuzztime 60s; done` — one package per target since `-fuzz` accepts one target per run; `timeout-minutes: 30`), `Makefile` (`fuzz` target running each for 30s), `docs/testing.md` (fuzz section), `CONTRIBUTING.md` test table row.

- [ ] **Step 1:** write the targets with seed corpora from the encoders; `go test ./internal/ltxio -run '^$' -fuzz '^FuzzApplySegments$' -fuzztime 20s` locally for each target; any crash found is fixed in this task (and reported).
- [ ] **Step 2:** nightly job + Makefile + docs; YAML parse.
- [ ] **Step 3: Commit** `"test: native fuzz targets for store names, LTX decode, the sidecar and the daemon protocol; nightly fuzz leg"`.

---

## Self-review notes

- Design coverage: §1 → Task 1; §2 → Task 2; §3 → Task 3; §4 → Task 4; §5 → Task 5; §6 → Task 6.
- Type consistency: `materializeFromChain`'s `chainPlacement` fields (`kind`, `checksum`, `chainID`) are read in Task 1 as they exist on main; `Store.Head`'s signature is fixed in Task 2 and used nowhere else; `planSegment`'s new arity is used only in `CheckpointWith`.
- Review Focus items 1-5 each have a named test in Tasks 1, 2, 2, 5, 6.
