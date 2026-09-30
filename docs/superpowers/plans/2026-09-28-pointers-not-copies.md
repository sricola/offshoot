# Pointers and Deltas, Not Copies — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the per-attempt hot path O(delta): clone checkouts from identical local files, prove a checkout clean without hashing it, write at-rest checkpoints as segments, and rollback/promote through base pointers — with no on-bucket format change and today's behaviour wherever the filesystem cannot clone.

**Architecture:** All changes live in `internal/ops` (plus a strict clone in `internal/ops/reflink` and a segment-onto-file helper in `internal/ltxio`). The `.sum` sidecar grows additive fields (`chain_id`, `size`, `mtime_ns`, `change_counter`, `shadow`). `checkouts-ro` gains a by-chain area. `Checkpoint` gains a segment branch mirroring `internal/session/flush.go`'s decision rules. `RollbackWith`/`PromoteWith` gain `Fork`'s share branch verbatim.

**Tech Stack:** Go 1.26 (cgo, mattn/go-sqlite3), `golang.org/x/sys/unix` (already a dependency: `Clonefile` on darwin, `FICLONE` on linux), the LTX format via `internal/ltxio`.

**Spec:** `docs/superpowers/specs/2026-09-28-pointers-not-copies-design.md` (committed alongside this plan).

## Global Constraints

- **No `LayoutVersion` bump.** Segments on a CLI-written lineage and base pointers on a rollback/promote lineage are both shapes layout v2 already resolves (`store.Chain`, `chainFrom`). Any task that finds it needs a format change stops and reports.
- **Degrade to today.** Every fast path checks `reflink.Clone`'s `ErrUnsupported` (or a missing shadow/sidecar field) and falls back to the existing code path with no behaviour change. Forced-fallback tests exist for each.
- **Sidecar fields are additive**; an old sidecar without them means "no fast path", never an error. The sidecar's `(lineage, epoch, txid)` identity stays load-bearing: a cloned file is stamped with the destination branch's identity, never the source's.
- **Anything that becomes the live writable checkout is written to a temp file and renamed** (`ltxio.finalizeDestination` discipline); never overwrite in place. `checkouts-ro` code never opens, stats through, or renames over the writable checkout, and vice versa (`export.go:104-116`).
- **`rm -rf checkouts-ro` stays safe at all times**; by-chain entries are immutable content keyed by chain identity and are re-created on demand.
- **Checksums are verified, not assumed**: every test that writes a segment or clones a file materializes the result from the store and compares bytes (or `sqlite3 .dump`) against an independently kept copy, and compares the recorded post-apply checksum with `ltxio.ChecksumDatabase`.
- **Benchmark honesty**: Task 5 re-runs the three benchmark targets and pastes each run verbatim; every prose number derives from a pasted table.
- Commit trailers: `Co-Authored-By: Claude <model> <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B`.

---

### Task 1: Clone a checkout from an identical local file

**Files:**
- Modify: `internal/ops/reflink/reflink.go` (add `Clone` + `ErrUnsupported`), `internal/ops/reflink/reflink_test.go`
- Modify: `internal/ops/sidecar.go` (`sumRecord.ChainID`; `StampSum`/`writeSum` carry it), `internal/ops/materialize.go` (`materializeChainAt` returns the chainID), `internal/ops/ops.go` (`CheckoutProven` clone path; `materializeAt` signature), `internal/ops/export.go` (by-chain path helper; `CheckoutAt` miss consults the by-chain area), `internal/ops/rocache.go` (by-chain entries enumerated for usage/eviction)
- Create: `internal/ops/chainid.go` (`chainID(members []store.ChainMember) string`), `internal/ops/clone_test.go`
- Modify: `internal/ltxio/segment.go` (`ApplySegments(startPath string, startChecksum uint64, segments []io.Reader, dstPath string) (txid, checksum uint64, err error)` — the segment-apply half of `MaterializeChain` starting from an existing file), `internal/ltxio/segment_test.go`

**Interfaces:**
- Consumes: `store.Chain(lineage, target) ([]store.ChainMember, error)` (read `ChainMember`'s fields in `internal/store/store.go:444` — use its key), `reflink.cloneFile(dst, src) bool` (platform files), `sidecar.go`'s `sumRecord`, `writeSum`, `StampSum`, `checkoutState`, `fileSum`; `rocache.go`'s `roCacheEntries`, `touchLastUsed`, `EvictROCache`.
- Produces: `reflink.Clone(dst, src string) error` with `var ErrUnsupported = errors.New("reflink: clone unsupported on this filesystem")`; `chainID(members) string` (hex sha256 of member keys joined by "\n", after `store.Chain`'s epoch collapse); `(*Workspace).byChainPath(db, chainID string) string` = `filepath.Join(w.roCacheRoot(), db, "<dir>", chainID+".db")` where `<dir>` is a name `store.ValidateName` rejects (read the charset loop at `store.go:314`; e.g. a name starting with `.` if rejected, else one containing a rejected character) — pick it, assert it in a test (`store.ValidateName(dir) != nil`); `materializeAt(ref, cp, dst) (postApply uint64, chainID string, err error)`; test hooks `var observeCheckoutSource func(kind string)` with kinds `"clone"`, `"clone+segments"`, `"materialize"`, and `var reflinkUnsupportedForTest bool` (forces `Clone` to return `ErrUnsupported`).

- [ ] **Step 1: Failing tests first** (`internal/ops/clone_test.go`)

```go
func TestForkCheckoutsCloneFromByChainCache(t *testing.T) {
	w := newTestWorkspace(t) // reuse the helper other ops tests use (grep "func newTestWorkspace\|func setupWS" in internal/ops/*_test.go)
	seedDB(t, w, "app", 8<<20)                  // ~8 MiB of rows, then Checkpoint "seed"
	var kinds []string
	observeCheckoutSource = func(k string) { kinds = append(kinds, k) }
	defer func() { observeCheckoutSource = nil }()
	want := exportBytes(t, w, "app", "main", "seed")
	for i := 0; i < 5; i++ {
		br := fmt.Sprintf("a%d", i)
		mustFork(t, w, "app", "main", br, "seed")
		path := mustCheckout(t, w, "app", br)
		if got := readFile(t, path); !bytes.Equal(got, want) {
			t.Fatalf("%s: cloned checkout differs from seed export", br)
		}
		if st, _ := checkoutState(path, mustRef(t, w, "app", br)); st != "clean" {
			t.Fatalf("%s: sidecar state %q, want clean", br, st)
		}
	}
	// first child materializes (and populates), the rest clone
	if kinds[0] != "materialize" || strings.Count(strings.Join(kinds, ","), "clone") != 4 {
		t.Fatalf("checkout sources = %v", kinds)
	}
}

func TestByChainEntriesAreEvictedAndRecreated(t *testing.T) { /* fill the by-chain area, EvictROCache(1), assert removed, Checkout again → "materialize" then a later fork → "clone" */ }

func TestClonePathDegradesWhenUnsupported(t *testing.T) { /* reflinkUnsupportedForTest = true; same flow; kinds all "materialize"; no by-chain dir created */ }

func TestDivergedChildClonesPrefixAndAppliesSegments(t *testing.T) { /* fork child, open a session or write+Checkpoint on the child so its head is base chain + 1 segment; remove the child's checkout; Checkout → "clone+segments"; bytes equal an Export of the child head */ }
```

Run: `go test ./internal/ops -run 'Clone|ByChain|Diverged' -v` → FAIL (undefined symbols).

- [ ] **Step 2: `reflink.Clone`**

```go
// Clone makes dst a copy-on-write clone of src, or returns ErrUnsupported
// without creating dst when the platform or filesystem cannot clone.
// Unlike CopyFile it never falls back to a byte copy.
func Clone(dst, src string) error {
	if forceUnsupported() { return ErrUnsupported }
	if cloneFile(dst, src) { return nil }
	_ = os.Remove(dst)
	return ErrUnsupported
}
```

(`forceUnsupported` reads a package-level test hook set via an exported `SetForceUnsupportedForTest(bool)` so `internal/ops` tests can flip it.) Tests: clone on the temp dir succeeds on APFS/btrfs/xfs-reflink and both files read identical; forced → `ErrUnsupported` and no dst; missing src → error that is not `ErrUnsupported`.

- [ ] **Step 3: `ltxio.ApplySegments`** — factor the segment loop out of `MaterializeChain` (`segment.go:336-448`): start from a copy of `startPath` in a temp file, assert the first segment's `PreApplyChecksum == startChecksum`, apply, finalize by rename to `dstPath`, return the final txid and checksum. `MaterializeChain` keeps its behaviour (tests unchanged) and may call the shared inner loop. Test: snapshot + 3 segments via `MaterializeChain` equals snapshot-materialized file + `ApplySegments` of the same 3 segments, byte for byte, same checksum.

- [ ] **Step 4: chainID + sidecar + materialize plumbing** — `chainID` in `chainid.go`; `sumRecord.ChainID string json:"chain_id,omitempty"`; `writeSum`/`StampSum` gain a `chainID string` parameter (update every caller: `Checkpoint`, `CheckoutProven`, `RollbackWith`, `PromoteWith`, `Compact`, the daemon's Close re-stamp in `internal/session` — grep `StampSum(`); `materializeChainAt` computes the chainID from the members it already resolves and returns it.

- [ ] **Step 5: by-chain cache + `CheckoutProven` clone path** — in the stale/missing branch (`ops.go:326-338`): `members := w.Store.Chain(ref.Lineage, cp.TXID)`; `id := chainID(members)`; if `byChainPath(db,id)` exists with a readable `.sum` whose `ChainID == id`: `reflink.Clone(entry, tmp)`; `os.Rename(tmp, path)`; `StampSum(path, entrySum.Hash, ref.Lineage, ref.HeadEpoch, ref.HeadTXID, entrySum.PostApplyChecksum, id)`; `touchLastUsed(entry)`; observe `"clone"`. Else try prefix hits: for k from len(members)-1 down to 1 (only prefixes that end at a snapshot or segment boundary — every prefix of a resolved chain is valid), look up `chainID(members[:k])`; on a hit, clone to tmp, `ltxio.ApplySegments(tmp, entrySum.PostApplyChecksum, readers for members[k:], path)`, stamp, observe `"clone+segments"`, and populate the full-chain entry. Else materialize as today, observe `"materialize"`, then populate: `reflink.Clone(path, tmpInByChainDir)` → rename to `byChainPath` → `os.Chmod(0444)` → write the entry's `.sum` with the same `Hash`/`PostApplyChecksum` the checkout's sidecar just recorded and `ChainID=id`; on `ErrUnsupported` remove any temp and continue silently. `roCacheEntries` enumerates the by-chain directory's `*.db` files as entries (kind field or branch=`<dir>`, checkpoint=chainID) so `ROCacheUsage`/`EvictROCache` cover them; eviction also removes the entry's `.sum`. `CheckoutAt` (`export.go:243-260`): on a miss, resolve the chain and consult the by-chain entry before `materializeAt` (clone into the ro path, 0444); populate on miss the same way.

- [ ] **Step 6:** `go test ./internal/ops -count=1 -race` and `go test ./internal/ltxio ./internal/ops/reflink -count=1` pass; `go test ./internal/session -count=1` passes (Open → settling suppression still holds on a cloned checkout: add `TestOpenOnClonedCheckoutSkipsSettlingFlush` next to the existing settling test, asserting no snapshot object is written on first flush after a cloned open). `go vet ./...`, `gofmt -l`.

- [ ] **Step 7: Commit** `"ops: clone checkouts from an identical by-chain cache entry; strict reflink.Clone; ltxio.ApplySegments"`.

---

### Task 2: Clean-skip and fork-at-head without hashing

**Files:**
- Modify: `internal/ops/sidecar.go` (fields `Size int64 json:"size,omitempty"`, `ModTimeNS int64 json:"mtime_ns,omitempty"`, `ChangeCounter uint32 json:"change_counter,omitempty"`; `stampFingerprint(path)` helper reading `os.Stat` + header bytes 24–27 big-endian via `dbfile.Reader`; `checkoutState` fast path), `internal/ops/sidecar_test.go`
- Test hook: `var observeFileSum func()` called at the top of `fileSum` so tests can count hash calls.

**Interfaces:**
- Consumes: `fileSum`, `checkoutState(path, ref) (string, uint64)`, `dbfile.Reader` (`internal/dbfile`), `quiesce`.
- Produces: unchanged public API; `checkoutState` returns `"clean"` without calling `fileSum` when identity matches and the three fingerprint fields all match the live file; when any field is missing (old sidecar) or differs it hashes as today and, if the hash matches, re-stamps the fingerprint (`StampSum` with the same hash) so the next call is fast.

- [ ] **Step 1: Failing tests**

```go
func TestCleanSkipDoesNotHashWhenFingerprintMatches(t *testing.T) {
	// seed + Checkpoint; Checkout once (materialize, stamps fingerprint);
	// count fileSum calls across a second Checkout → 0; state clean.
}
func TestModifiedCheckoutStillDetectedRollbackJournalAndWAL(t *testing.T) {
	// for journal_mode in (DELETE, WAL): sqlite3 exec an INSERT on the checkout;
	// Checkout → checkoutState "modified" (hash path taken: fileSum calls == 1).
}
func TestTouchedButUnchangedCheckoutHashesAndStaysClean(t *testing.T) {
	// os.Chtimes the file; Checkout → fileSum calls == 1, state clean, sidecar re-stamped so a third Checkout hashes 0 times.
}
func TestOldSidecarWithoutFingerprintHashes(t *testing.T) { /* strip the new fields from the .sum JSON; Checkout hashes once, then fast */ }
```

- [ ] **Step 2: implement**; `BenchmarkCheckoutCleanSkip`'s doc comment (`fork_bench_test.go:209-217`) updated to say the skip is O(1) when the fingerprint matches. `warnIfUncheckpointed` needs no change (it calls `checkoutState`). **Step 3:** `go test ./internal/ops ./internal/session -count=1 -race`. **Step 4: Commit** `"ops: prove a checkout clean from size, mtime and SQLite's change counter; hash only on mismatch"`.

---

### Task 3: At-rest checkpoints as segments against a reflinked shadow

**Files:**
- Modify: `internal/ops/ops.go` (`Checkpoint` → thin wrapper over new `CheckpointWith(db, branch, name string, meta map[string]string, opts CheckpointOptions) (CheckpointResult, error)`), `internal/ops/sidecar.go` (`Shadow bool json:"shadow,omitempty"`; `shadowPath(checkoutPath) = checkoutPath + ".shadow"`; `refreshShadow(path)` / `dropShadow(path)`), `internal/ops/checkpoint_delta.go` (new: `diffPages(cur, shadow string) (pages []ltxio.Page, commit uint32, pageSize uint32, changedFrac float64, post uint64, err error)` computing the changed set and the incremental post checksum from a pre checksum), `internal/ops/checkpoint_delta_test.go`
- Modify: `cmd/offshoot/main.go` (`checkpoint` gains `--snapshot`; prints `checkpoint <name> at txid N (segment, P pages, K KiB)` or `(snapshot, S MiB)`), `internal/mcp` (`offshoot_checkpoint` result gains `kind`), `internal/daemon` untouched (no daemon checkpoint op exists).

**Interfaces:**
- Consumes: `ltxio.EncodeSegment(pageSize, commit uint32, minTXID, maxTXID, pre, post uint64, pages []ltxio.Page, w io.Writer) error`; `ltxio.UpdateChecksum(running uint64, pgno uint32, oldData, newData []byte) uint64` (read its truncation semantics at `segment.go:271-310` and `MaterializeChain`'s handling of `Commit` shrink at `segment.go:336-448`; mirror exactly); `ltxio.LockPgno(pageSize)`; `store.SegmentKey(lineage, epoch, minTXID, maxTXID)`; `internal/session/flush.go`'s `largeSegmentFraction` constant (copy its value into ops with a comment naming the source, or export it from a shared place); `store.Chain` for the segments-since-snapshot count; `reflink.Clone`.
- Produces: `type CheckpointOptions struct{ Snapshot bool }`; `type CheckpointResult struct{ TXID uint64; Kind string /* "snapshot"|"segment" */; Pages int; Bytes int64 }`; `Checkpoint(db, branch, name, meta) (uint64, error)` unchanged in signature.

Decision rule for a segment (all must hold): sidecar readable with `Shadow` true and the shadow file present; sidecar `(Lineage, Epoch, TXID) == (ref.Lineage, ref.HeadEpoch, ref.HeadTXID)`; page size of checkout equals shadow's; `len(w.Store.Chain(ref.Lineage, ref.HeadTXID)) < bound` where `bound = w.SnapshotEvery` or `ForkShareMaxDepth`; `changedFrac < largeSegmentFraction`; `!opts.Snapshot`; `txid := ref.HeadTXID+1 > 1`. Write: `EncodeSegment(pageSize, commit, txid, txid, sidecar.PostApplyChecksum, post, pages)` → `PutIf(SegmentKey(ref.Lineage, ref.Epoch, txid, txid), buf, "")` with the same orphan/overwrite comment block as the snapshot path → same `PutRef` CAS → `writeSum(..., post, chainID)` → `refreshShadow`. Snapshot path unchanged except it now also refreshes the shadow. `CheckoutProven`'s materialize/clone paths, `RollbackWith`/`PromoteWith`/`Compact`'s refresh also `refreshShadow` after stamping; `refreshShadow` on `ErrUnsupported` calls `dropShadow` and stamps `Shadow=false`.

- [ ] **Step 1: Failing tests** (`checkpoint_delta_test.go`)

```go
func TestSecondCheckpointWritesASegment(t *testing.T) {
	// seed 8 MiB, Checkpoint "a" (snapshot); one-row INSERT; res := CheckpointWith(..."b"...)
	// res.Kind == "segment"; a SegmentKey object exists; its size < 64<<10;
	// materialize head via CheckoutAt into a temp dir → bytes equal the checkout;
	// ltxio.TrailerPostApplyChecksum(segment bytes) == ltxio.ChecksumDatabase(checkout).
}
func TestDeltaCheckpointsRoundTripUnderRandomMutations(t *testing.T) {
	// 30 rounds: random INSERT/UPDATE/DELETE batches, every 7th round a VACUUM (shrink), every 11th a large blob insert (growth);
	// each round: copy the quiesced checkout to keep/<n>.db, CheckpointWith "r<n>";
	// afterwards: for every n, CheckoutAt("r<n>") bytes == keep/<n>.db.
}
func TestShadowIdentityMismatchFallsBackToSnapshot(t *testing.T) { /* rewrite the sidecar's txid; CheckpointWith → Kind snapshot */ }
func TestEveryBoundthCheckpointIsASnapshot(t *testing.T) { /* w.SnapshotEvery = 4; 8 tiny checkpoints; kinds = snapshot, segment×3, snapshot, segment×3 */ }
func TestLargeChangeFractionForcesSnapshot(t *testing.T) { /* rewrite most rows; Kind snapshot */ }
func TestSnapshotOptionForces(t *testing.T) { /* opts.Snapshot → snapshot */ }
func TestNoShadowWhenReflinkUnsupported(t *testing.T) { /* forced unsupported; two checkpoints; both snapshot; no .shadow file */ }
func TestForkFromSegmentHeadMaterializes(t *testing.T) { /* fork child at head after 3 segment checkpoints; Checkout child == parent checkout bytes */ }
```

- [ ] **Step 2: implement**; **Step 3:** `go test ./internal/ops -count=1 -race`; `go test ./cmd/offshoot -count=1` (CLI output test for both kinds); `go test ./internal/mcp -count=1`; **Step 4: Commit** `"ops: at-rest checkpoint writes a segment against a reflinked shadow when it can; --snapshot forces a full one"`.

---

### Task 4: Rollback and promote to a kept checkpoint through a base pointer

**Files:**
- Modify: `internal/ops/ops.go` (`RollbackWith`, `PromoteWith`; `RollbackOptions.Materialize bool`, `PromoteOptions.Materialize bool`; results gain `Shared bool`), `internal/ops/rollback_backup_test.go`/`promote_test.go` (or new `repoint_shared_test.go`), `internal/ops/metrics.go` (counters `offshoot_rollback_total{mode="shared"|"materialized"}`, `offshoot_promote_total{mode=...}` next to the fork counters — read how `ObserveFork` is wired to metrics and mirror), `cmd/offshoot/main.go` (`--materialize` on both verbs; output says `(shared)` or `(materialized)`), `internal/daemon/protocol.go`+`server.go` (`materialize` bool on both ops), `sdk/python/offshoot/client.py` + `sdk/typescript/src/client.ts` (`materialize=False`/`materialize?: boolean`), `internal/mcp/tools.go` (no new argument: MCP always takes the default), docs in Task 5.

**Interfaces:**
- Consumes: `Fork`'s share branch verbatim (`ops.go:868-918`): `bound`, `store.NewLineageID`, `EnsureLayoutV2`, `store.BasePointer`, `WriteLineageBase`; `copySnapshotToNewLineage` for the materialize fallback; `safetyFork` unchanged.
- Produces: in `RollbackWith`, replace the block from `lineage, _, err := w.copySnapshotToNewLineage(ref, cp)` through the kept-checkpoint copy loop with: `members := w.Store.Chain(ref.Lineage, cp.TXID)`; if `len(members) < bound && !opts.Materialize`: `lineage := store.NewLineageID(); EnsureLayoutV2(); bp := store.BasePointer{Lineage: ref.Lineage, TXID: cp.TXID}; WriteLineageBase(lineage, bp); base = &bp; kept = every checkpoint with TXID <= cp.TXID, unchanged (epochs as recorded)`; else today's copy path with `base = nil`. Then `next.Lineage, next.Epoch, next.HeadTXID, next.HeadEpoch, next.Checkpoints, next.Base = lineage, 1, txid, 1, kept, base`. On CAS failure in the shared case, best-effort delete the new `base.json`. `PromoteWith` mirrors with `src.Lineage`/`headCheckpoint(src)`. Before writing, grep for any code that locates a checkpoint's object by `(ref.Lineage, c.Epoch, c.TXID)` directly instead of via `store.Chain` — if found, route it through `Chain` (the shared case requires resolution through the base pointer).

- [ ] **Step 1: Failing tests**

```go
func TestRollbackToKeptCheckpointIsShared(t *testing.T) {
	// seed 32 MiB, Checkpoint "a", write more, Checkpoint "b"; storeBytesBefore := dirBytes(store data/)
	// RollbackWith(to "a") → res.Shared true; store bytes grew by < 8 KiB; base.json exists for the new lineage;
	// Checkout == Export of "a"; CheckoutAt("a") still resolves; ref.Base != nil.
}
func TestPromoteIsSharedAndSourceStaysResolvable(t *testing.T) { /* fork attempt, write, Checkpoint, Promote onto main → Shared; main content == attempt head; destroy attempt; main still materializes (base.json outlives the ref) */ }
func TestGCAfterSharedRollbackReclaimsTheAbandonedFuture(t *testing.T) {
	// after rollback to "a": destroy the -pre-rollback safety fork (force), run GC with zero grace;
	// objects of the old lineage with txid > a.TXID are gone; objects <= a.TXID remain; Checkout still equals "a".
}
func TestCompactAfterSharedRollbackDetaches(t *testing.T) { /* Compact → ref.Base nil, self-contained snapshot, content unchanged */ }
func TestRepeatedRollbacksHitTheDepthFloor(t *testing.T) { /* w.SnapshotEvery = 3; chain of forks/rollbacks until Chain length reaches 3; next rollback → Shared false */ }
func TestMaterializeOptionForcesCopy(t *testing.T) { /* opts.Materialize → Shared false, base.json absent */ }
```

- [ ] **Step 2: implement** (ops, CLI flags, daemon op fields, SDK params + one test each in `sdk/python/tests/test_client.py` and `sdk/typescript/test/client.test.ts` asserting the wire field, metrics). **Step 3:** `go test ./internal/... ./cmd/... -count=1 -race`; `make test-python-sdk PYTHON=/opt/homebrew/bin/python3.14`; `make test-ts-sdk`. **Step 4: Commit** `"ops: rollback and promote to a kept checkpoint through a base pointer; --materialize keeps the copy"`.

---

### Task 5: Measure, then document

**Files:**
- Modify: `docs/benchmarks.md` (re-pasted runs: the CoW/fork section's `make bench` tables, "Per-test isolation primitives", "BranchBench topologies" — each replaced by one new run with machine/date line; a short "What changed in v0.2.12" paragraph per section quoting before/after from the two pastes), `docs/limitations.md` (the six bullets at lines ~205-224 rewritten to the new behaviour, each with its remaining caveat: non-reflink filesystems, the O(size) read in delta checkpoints, first materialization of a seed), `ROADMAP.md` (the "Promote/rollback (and compact) on sharing — deferred" bullet → shipped for rollback/promote; compact stays), `docs/architecture.md`, `docs/concepts.md`, `docs/reference.md`, `docs/operations.md`, `docs/faq.md` (every sentence saying rollback/promote "materialize a full copy" — grep `materialize a full copy\|full copy` — updated; `--snapshot` and `--materialize` documented in reference.md), `docs/status.md` (four rows, shipped-and-tested, test names, "v0.2.12 (unreleased)"), `CHANGELOG.md` (Unreleased → Added/Changed), `sdk/*/README.md` (materialize option).
- Verify: `grep -rn "materialize a full copy" docs README.md ROADMAP.md` prints only historical CHANGELOG lines; `go -C site/gen run .`; `make check-plugin`.
- [ ] **Step 1:** run `make bench` (note: which targets — `bench`, `bench-cow`), `make bench-isolation`, `make bench-branchbench`; paste. **Step 2:** docs. **Step 3: Commit** `"docs: pointers and deltas — benchmarks re-run, limitations rewritten, status and changelog"`.

---

## Self-review notes

- Spec coverage: §1 → Task 1 (incl. prefix clones and Session.Open), §2 → Task 2, §3 → Task 3, §4 → Task 4, verification → Task 5.
- Type consistency: `materializeAt` returns `(uint64, string, error)` from Task 1 onward — Tasks 3 and 4 call it with the new arity; `StampSum`/`writeSum` carry `chainID` from Task 1; Task 3's `refreshShadow` is called from the same sites Task 1 stamps.
- Honesty: every fast path has a forced-fallback test; every byte claim in docs comes from a re-pasted run.
