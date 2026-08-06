# Offshoot Milestone 2: Safe Defaults for Agents

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An unattended agent writing through offshoot cannot silently lose hours of work (background flush), leak branches forever (MCP TTLs), or take the degraded path by default (MCP rides the daemon; fast-path fork) — with the fork cost measured before and after, and enough observability to answer "which branch is behind and why" from `status`.

**Architecture:** Background flush is a per-session ticker calling the existing `Flush("")` with failures surfaced in status rather than killing the session. MCP gains TTL plumbing and a thin daemon-client path so agent checkpoints ride live capture when a daemon is up. Fork gets a benchmarked fast path: when the source checkpoint's chain is exactly one snapshot, the child lineage's seed is a byte-identical object copy (reflink on local stores, server-side CopyObject on S3) instead of materialize-and-re-encode. Checkout learns to skip re-materializing a clean, current checkout — fixing both session-open latency and the documented `dbfile` descriptor stranding.

**Tech Stack:** Go 1.24+, cgo; `golang.org/x/sys` is EXPLICITLY PERMITTED as a new dependency for `unix.Clonefile`/`unix.IoctlFileClone` (decision recorded here; it is the canonical syscall extension, not a third-party dep). Nothing else new.

## Global Constraints

- Module `github.com/offshoot-db/offshoot`; Go 1.24+; cgo (mattn); Linux/macOS only; **no new dependencies except `golang.org/x/sys`** (see above)
- **A branch must always be materializable to exactly the bytes that were flushed** — the fast-path fork must produce byte-identical results to the slow path, proven by test
- Auto-flush must never weaken flush semantics: it calls the same `Flush` path (target-based drain, `ErrDrainIncomplete` loud, ErrCAS-gated cleanup); a failed auto-flush surfaces in `session status`, does not kill the session, and the next tick retries
- Any change under `internal/capture` or `internal/session`'s flush path requires a `make test-torture` run with numbers reported before the task completes
- POSIX lock hazard (see `internal/dbfile`): never open-and-close a raw descriptor on a database path a live in-process SQLite connection may hold; route reads through `dbfile` or prove the path cannot be live
- Every existing test must keep passing unmodified (the `SnapshotEvery: 1` style compatibility escape used in Plan 7 is available only with controller sign-off)
- Wire-protocol additions are backward compatible: new optional request fields only; existing SDKs must keep working unchanged against the new daemon
- CHANGELOG.md gains entries under [Unreleased] per task; version stays in the 0.1.x series
- Commit messages: conventional commits, ending with the repo's session trailers

## File Structure

```
internal/ops/ops.go              (modify) Checkout clean-skip; fast-path fork seed
internal/ops/reflink/reflink.go  reflink/clonefile with copy fallback (build-tagged)
internal/ops/reflink/reflink_test.go
internal/ops/fork_bench_test.go  benchmark suite (testing.B, -short aware)
internal/store/store.go          (modify) CopyObject in the Backend interface
internal/store/local.go          (modify) local CopyObject via reflink pkg
internal/store/s3.go             (modify) server-side CopyObject
internal/store/fake in storetest (modify) fake backend CopyObject
internal/session/session.go      (modify) Options.FlushEvery + flush loop + lag/last-flush state
internal/session/flush.go        (modify) record last-flush time/error
internal/capture/engine.go       (modify) Engine.Lag() — WAL bytes pending beyond consumed
internal/daemon/protocol.go      (modify) SessionInfo gains observability fields
internal/daemon/server.go        (modify) serve wires FlushEvery; status enrichment
internal/mcp/tools.go            (modify) fork ttl arg; daemon-backed checkpoint/fork/checkout
internal/mcp/server.go           (modify) plumb daemon socket option
cmd/offshoot/main.go             (modify) serve -flush-every; mcp -socket -default-ttl; status columns
docs/benchmarks.md               measured fork/flush numbers, method, machine
docs/status.md                   (modify) flip rows this plan ships
README.md                        (modify) daemon flags; MCP daemon mode; fork cost
CHANGELOG.md                     (modify) per task
```

---

### Task 1: Checkout skips re-materializing a clean, current checkout

**Files:**
- Modify: `internal/ops/ops.go` (`Checkout`, ~line 217)
- Test: `internal/ops/ops_test.go` (append)

**Interfaces:**
- Consumes: `checkoutState(path, ref)` (returns "clean"/"modified"/"stale"/"unknown"), `materializeAt`, `writeSum`
- Produces: unchanged signature `Checkout(db, branch) (string, error)`; new observable behavior: a checkout whose state is "clean" for the CURRENT ref head is returned as-is without re-materialization.

Why: every `session.Open` re-materializes via temp+rename, which both costs O(size) and strands a `dbfile` descriptor per open (documented in internal/dbfile). A clean checkout at the current head is byte-correct already.

- [ ] **Step 1: Failing test.** Append to `internal/ops/ops_test.go`:

```go
func TestCheckoutSkipsRematerializeWhenCleanAndCurrent(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	p1, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	st1, err := os.Stat(p1)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	st2, err := os.Stat(p2)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(st1, st2) {
		t.Fatal("a clean, current checkout must not be re-materialized (same inode expected)")
	}
	// A checkpoint advances the head; the next checkout MUST re-materialize.
	mustExecSQL(t, p2, "CREATE TABLE t (v);")
	if _, err := w.Checkpoint("app", "main", "cp1"); err != nil {
		t.Fatal(err)
	}
	// After checkpoint the checkout is clean at the new head — still same inode.
	p3, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	st3, _ := os.Stat(p3)
	if !os.SameFile(st2, st3) {
		t.Fatal("checkpoint leaves the checkout clean at the new head; still no re-materialize")
	}
	// A modified checkout must still be refused/warned per existing semantics,
	// and a stale one (ref moved elsewhere) must re-materialize: fork a branch,
	// destroy+recreate content via rollback to force staleness on a copy.
	// (Assert the "stale" path by mutating the ref's HeadTXID via a second
	// checkpoint from a DIFFERENT checkout dir is not constructible here;
	// instead verify the sum-sidecar mismatch path: corrupt the .sum sidecar
	// and expect re-materialization.)
	sum := p3 + ".sum" // verify the actual sidecar naming in checkout code; adjust if different
	if _, err := os.Stat(sum); err == nil {
		if err := os.WriteFile(sum, []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
		p4, err := w.Checkout("app", "main")
		if err != nil {
			t.Fatal(err)
		}
		st4, _ := os.Stat(p4)
		if os.SameFile(st3, st4) {
			t.Fatal("an unverifiable checkout (bad sidecar) must be re-materialized")
		}
	}
}
```

The implementer must read `checkoutState` and the sidecar naming first and adapt the test's sidecar path to reality (the assertion logic stands).

- [ ] **Step 2: Run to verify it fails** (`go test ./internal/ops -run SkipsRematerialize -v`) — fails on the SameFile assertion today because Checkout always re-materializes.
- [ ] **Step 3: Implement.** In `Checkout`, after loading the ref and before `materializeAt`: if `checkoutState(path, ref) == "clean"` for the current head (the state function already compares the sidecar's lineage+txid — read it; extend its result or add a head-currency check if it only proves cleanliness against the RECORDED txid, not the ref's), return the existing path. Keep the "modified" warning path exactly as-is. Safety note for the implementer: `checkoutState` calls `fileSum` which routes through `dbfile` (Plan: fix/capture-stray-fd-lock-drop) — no new lock hazard, but confirm by reading.
- [ ] **Step 4: Full package + session integration.** `go test ./internal/ops ./internal/session -count=1 -race` — session tests exercise Checkout via Open heavily; they must pass unmodified.
- [ ] **Step 5: Commit** `feat: checkout returns a clean, current materialization instead of rebuilding it` + CHANGELOG entry.

---

### Task 2: Background flush

**Files:**
- Modify: `internal/session/session.go` (Options + loop + state), `internal/session/flush.go` (stamp last-flush), `internal/daemon/server.go` (wire from serve), `cmd/offshoot/main.go` (`serve -flush-every`)
- Test: `internal/session/flush_test.go` (append), `internal/daemon/lifecycle_test.go` (append)

**Interfaces:**
- Consumes: `Session.Flush(name string) (uint64, error)` (existing; flushMu-serialized)
- Produces:

```go
// Options gains:
//   FlushEvery time.Duration // >0: flush automatically at this cadence; 0: manual only (default)
// Session gains (read under s.mu):
//   LastFlush()  (t time.Time, txid uint64, ok bool)  // last SUCCESSFUL flush (auto or manual)
//   LastFlushErr() error                              // most recent auto-flush failure, nil after a success
// serve gains: -flush-every duration (default 30s; 0 disables) applied to every session it opens.
```

Semantics (write into doc comments — they are the contract):
- **Idle short-circuit (PM-review blocking amendment):** a tick with nothing pending — capture caught up AND no committed frames since the last successful flush — does NOTHING: no object write, no ref write, no `flushesSinceSnapshot` advance. Without this, an idle session under 30s cadence uploads a full snapshot every SnapshotEvery ticks (~8 min) forever — millions of pointless S3 PUTs. A test must assert zero store writes across several idle ticks (count Backend puts via the storetest instrumentation or a counting wrapper). The cheapest correct signal for "nothing pending": track whether the sink applied any frames since the last successful flush (the session already observes every Apply under replicaMu); do not invent a new engine query for this task.
- The loop calls `s.Flush("")`. A failure records `lastFlushErr` and retries next tick; it never terminates the session (a fenced/fatal session already stops itself via `Err()` — the loop exits when the session is done).
- A tick that fires while a manual flush holds `flushMu` just blocks briefly (flushMu serializes); no tick pile-up (use a ticker, skip missed ticks).
- `LastFlushErr` clears on any subsequent successful flush, manual or auto.
- Library default stays 0 (explicit); the DAEMON default is 30s — safe-by-default lives at the daemon boundary, not in the library primitive.

- [ ] **Step 1: Failing tests.** Append to `internal/session/flush_test.go`:

```go
func TestAutoFlushShipsWritesWithoutManualFlush(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", FlushEvery: 150 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	mustExec(t, s.CheckoutPath(), "CREATE TABLE t (v); INSERT INTO t VALUES ('x');")
	// No manual flush. Within a few ticks the write must be durable.
	deadline := time.Now().Add(10 * time.Second)
	for {
		ref, _, err := w.Store.GetRef("app", "main")
		if err != nil {
			t.Fatal(err)
		}
		if ref.HeadTXID > 1 { // creation snapshot is txid 1; adapt to the actual baseline txid observed
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("auto-flush never shipped the committed write")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, _, ok := s.LastFlush(); !ok {
		t.Fatal("LastFlush must report the successful auto-flush")
	}
	if err := s.LastFlushErr(); err != nil {
		t.Fatalf("LastFlushErr = %v, want nil", err)
	}
}

func TestAutoFlushFailureSurfacesAndRecovers(t *testing.T) {
	// Use the store-fault injection pattern this package's existing tests use
	// for transient PutRef failures (find the failing-backend wrapper in the
	// session or storetest helpers; wrap the backend to fail N puts). Assert:
	// (1) LastFlushErr() becomes non-nil after a failing tick;
	// (2) the session is NOT dead (Err() nil, checkout usable);
	// (3) once the backend heals, a later tick succeeds and LastFlushErr()
	//     returns nil again.
}
```

Write the second test as complete code against the actual fault-injection helper found in the package (there is one — Plan 5's flush tests injected PutRef failures; reuse it, do not invent a new mechanism). Also append a daemon test to `internal/daemon/lifecycle_test.go`: start a server whose serve path sets FlushEvery=100ms, open a session via the protocol, write via sqlite3, poll `status` until `durable_txid` advances with NO flush op sent.

- [ ] **Step 2: Verify fail.** Compilation failures on Options.FlushEvery/LastFlush.
- [ ] **Step 3: Implement.** Ticker goroutine started in `Open` when FlushEvery > 0, joined in `Close` (respect the existing goroutine-join discipline — read how renewLoop is started/stopped and mirror it exactly, including the fenced/terminal path). Flush already knows success; add the stamp where PutRef confirms (both snapshot and segment paths — single ref-write site). `serve` flag plumbed through the daemon's session-open call site.
- [ ] **Step 4: Verify + torture.** `go test ./internal/session ./internal/daemon -count=1 -race` then full suite, then `make test-torture` (session flush path changed) — report numbers.
- [ ] **Step 5: Commit** `feat: background flush — the daemon ships work on a cadence by default` + CHANGELOG.

---

### Task 3: MCP forks carry TTLs

**Files:**
- Modify: `internal/mcp/tools.go` (fork tool schema + handler), `internal/mcp/server.go` + `cmd/offshoot/main.go` (`mcp -default-ttl`, default "24h")
- Test: `internal/mcp/tools_test.go` (append)

**Interfaces:**
- Consumes: `ops.Fork(db, src, new, at string, ttl time.Duration)`; `ops.Touch`
- Produces: fork tool input gains optional `ttl` (string, Go duration; the description tells the model: branches expire this long after their last activity unless promoted or touched; "none" for immortal). `NewOffshootTools` (or its config) gains a DefaultTTL applied when the arg is absent. `-default-ttl 0`/`"none"` disables the default.

Rules: an explicit `ttl` arg always wins; `"none"` yields no TTL; an unparseable ttl returns a tool error naming the accepted forms. The fork tool's Description must state the default behavior so the model can reason about it (e.g. "forked branches expire 24h after last activity by default; pass ttl:"none" to keep one indefinitely"). **PM amendments:** the fork tool's RESPONSE text echoes the applied TTL and computed expiry time (so it lands in the agent transcript and the model can reason about it later); the Description and README note that TTL REAPING requires a running janitor (`offshoot serve`) — a daemonless MCP setup sweeps expired branches only when `offshoot gc` runs.

- [ ] **Step 1: Failing tests** (append to tools_test.go, following its existing call-tool test harness): (a) fork with `ttl:"2h"` → branch's ref has TTL "2h0m0s"; (b) fork without ttl under DefaultTTL=24h → TTL "24h0m0s"; (c) fork with `ttl:"none"` → TTL "" even under a default; (d) `ttl:"bananas"` → tool error mentioning duration; (e) schema advertises the ttl property (extend the existing generic schema-conformance test if one covers property types).
- [ ] **Step 2-4:** Red → implement → green; `go test ./internal/mcp -count=1 -race` + full suite.
- [ ] **Step 5: Commit** `feat: MCP forks expire by default — agent-created branches stop leaking` + CHANGELOG + flip the docs/status.md row.

---

### Task 4: MCP rides the daemon when one is up

**Files:**
- Modify: `internal/mcp/tools.go`, `internal/mcp/server.go`, `cmd/offshoot/main.go` (`mcp -socket`, default: the same default socket path serve uses)
- Test: `internal/mcp/daemon_test.go` (new)

**Interfaces:**
- Consumes: `daemon.Call(socket, daemon.Request) (daemon.Response, error)` (read internal/daemon/client.go for the exact helper), daemon ops `open/flush/status/branches/fork`
- Produces: behavior only — same seven tools, no schema changes beyond Task 3.

Semantics (doc-comment contract):
- At each tool call (not at startup — the daemon may start later), if the daemon socket answers `status`, the tool takes the daemon path; otherwise it falls back to at-rest exactly as today. Detection failures are silent fallbacks, not errors.
- `checkpoint` on a branch with an open daemon session → daemon `flush` op with the checkpoint name (live capture, no quiesce, no lease collision). No open session → at-rest as today.
- `fork` → daemon `fork` op (which flushes an open source session first). No daemon → at-rest.
- `checkout` on a branch with an open session → return the SESSION's checkout path (the live file the agent should write), replacing today's "no MCP tool currently opens a session" caveat with an accurate description. No session → at-rest materialization as today.
- All other tools stay at-rest (they are ref-level operations; the daemon adds nothing).

- [ ] **Step 1: Failing tests** in new `internal/mcp/daemon_test.go`: start a real daemon server on a temp socket (reuse internal/daemon's test scaffolding via its exported surface — if the scaffolding isn't importable across packages, start the daemon like the SDK tests do: build/run is too heavy, so instead use `daemon.NewServer` directly; it's the same module). Cover: (a) checkpoint against an open session captures an UNFLUSHED write (write via sqlite3 to the session path, call the MCP checkpoint tool, then materialize at that checkpoint at-rest and count rows); (b) fork via MCP from the open session contains the row; (c) checkout returns the session's path while open; (d) with NO daemon (bad socket), every tool behaves exactly as the existing at-rest tests expect (spot-check checkpoint+fork against the existing harness).
- [ ] **Step 2-4:** Red → implement → green; `go test ./internal/mcp ./internal/daemon -count=1 -race` + full suite.
- [ ] **Step 5: Commit** `feat: MCP rides a running daemon — live capture for agent checkpoints` + CHANGELOG + status.md row + README's MCP section gains the one-paragraph daemon-mode note.

---

### Task 5: Fork benchmarks — measure before optimizing

**Files:**
- Create: `internal/ops/fork_bench_test.go`, `docs/benchmarks.md`
- Modify: `Makefile` (`bench` target)

**Interfaces:** none new. Benchmarks must run against BOTH the current slow path and (after Task 6) the fast path with the same harness, so structure them around a size table.

- [ ] **Step 1: Write the benchmarks.** `testing.B` benchmarks: `BenchmarkForkAtHead/size=64MB|512MB` (default) and `size=4GB` guarded behind `-timeout` awareness + `testing.Short()` skip; each iteration: seeded db of N pages (build once per size in a setup, checkpointed), then `b.ResetTimer`, fork to a unique branch, `b.StopTimer`, destroy the fork. Report `b.SetBytes(dbSize)` so ns/op and MB/s both land. Also `BenchmarkCheckoutCleanSkip` (Task 1's win) and `BenchmarkSessionOpen`. Makefile: `bench: go test ./internal/ops -bench Fork -benchmem -run '^$' -count=3`.
- [ ] **Step 2: Run and record.** Run `make bench` on this machine; put the numbers, machine description, method, and date in `docs/benchmarks.md` with the honest framing: local-store numbers, darwin/arm64; linux container numbers if docker is available (run it, don't guess); S3-path benchmarks live behind `make bench-s3` against MinIO (write the target; run it if docker available). State plainly in the doc which paths are O(size) today and that Task 6's fast path targets the single-snapshot case.
- [ ] **Step 3: Commit** `test: fork and checkout benchmarks, with measured baselines` + CHANGELOG.

---

### Task 6: Fast-path fork — byte-identical object copy for single-snapshot chains

**PM-review split (execute as 6a then 6b, separate commits; 6b is timeboxed):**
- **6a:** the reflink package, local-backend `CopyObject`, and the fast path — with S3 explicitly taking the existing slow path (its CopyObject returns a sentinel `ErrCopyUnsupported` and the fork code falls back). Ships the local/eval-harness win on its own.
- **6b:** S3 server-side CopyObject, gated: objects **>5GB fall back to the slow path** (S3's single-request CopyObject limit is 5GB — a hard error otherwise). Conformance subtest added to the MinIO-gated suite. If 6b exceeds its timebox in review cycles, it defers to a follow-up with a status.md row; 6a stands alone.
- Applicability window stated in docs/benchmarks.md: the fast path fires only when the checkpoint's chain is exactly one snapshot (true for at-rest seed+checkpoint eval flows; false once a daemon session has flushed segments past the last snapshot). The spec's "~40ms" figure appears ONLY as the target the measurement checks, never as a claim.

**Files:**
- Create: `internal/ops/reflink/reflink.go`, `internal/ops/reflink/reflink_test.go` (package works standalone: `CopyFile(dst, src string) (cloned bool, err error)` — FICLONE ioctl on linux, clonefile(2) on darwin, silent fallback to a plain copy; build-tagged files `reflink_linux.go`/`reflink_darwin.go` + portable fallback)
- Modify: `internal/store/store.go` (Backend interface + `CopyObject(dst, src string) error`), `internal/store/local.go` (reflink-backed), `internal/store/s3.go` (server-side CopyObject), the storetest fake, `internal/ops/ops.go` (`copySnapshotToNewLineage` fast path)
- Test: `internal/ops/ops_test.go` + `internal/store` conformance additions

**Interfaces:**
- Produces:

```go
// package reflink
// CopyFile copies src to dst, using a filesystem clone (reflink/clonefile)
// when the platform and filesystem support it; cloned reports whether a
// clone happened. dst is created O_EXCL-equivalent via temp+rename by the
// CALLER's conventions — CopyFile itself writes dst directly.
func CopyFile(dst, src string) (cloned bool, err error)

// store.Backend gains:
//   CopyObject(dst, src string) error   // byte-identical copy; ErrNotFound if src absent
```

Fast-path rule in `copySnapshotToNewLineage` (read it first — it currently materializes the chain and re-encodes): when the source checkpoint's chain (via `store.Chain`) is EXACTLY ONE member and that member is a snapshot, the child's seed is `Backend.CopyObject(childSnapshotKey, thatMemberKey)` — the LTX object is lineage-agnostic (the lineage lives in the KEY, verify by reading ltxio's encode: nothing in the object names the lineage; if anything does, STOP and report). Otherwise the existing materialize+re-encode path runs unchanged. The fast path must verify the copy the same way the slow path's output is trusted: after CopyObject, the child chain must resolve (`store.Chain`) — checksum verification happens at first materialization as everywhere else.

- [ ] **Step 1: Failing tests.** (a) reflink package unit tests: copy correctness at several sizes incl. empty and >1 page, cloned flag true on APFS (skip-loudly if the temp filesystem doesn't support clones — probe by attempting one), fallback correctness when forced (export a test hook or build the fallback as a separate function tested directly). (b) Backend conformance: CopyObject round-trip + ErrNotFound, added WHERE THE EXISTING backend conformance tests live (find them in internal/store / storetest — both local, fake, and the MinIO-gated S3 suite must gain the same subtest). (c) ops-level equivalence: create a db with content, checkpoint, fork via the fast path, fork the SAME source with the fast path artificially disabled (export an internal knob `forkSlowPath` test-only variable or pass through a testing hook — pick the least invasive; document it), and assert both children materialize to byte-identical `.dump` output AND the fast-path child's object is byte-identical to the source snapshot object (Backend.Get both, compare). (d) fork past a segment (chain length > 1) still takes the slow path (assert via the knob/counters).
- [ ] **Step 2-3:** Red → implement → green, `go test ./internal/ops ./internal/store -count=1 -race` + full suite; run the MinIO conformance locally via docker if available (the CI job covers it regardless).
- [ ] **Step 4: Re-benchmark.** `make bench` again; update docs/benchmarks.md with before/after table. The fast path should be near-constant-time locally on APFS; say what it is on ext4-without-reflink (falls back to plain copy — still one object copy instead of decode+encode).
- [ ] **Step 5: Commit** `feat: fork copies the snapshot object when the chain allows — reflink locally, CopyObject on S3` + CHANGELOG + status.md.

---

### Task 7: Observability — status answers "which branch is behind and why"

**Files:**
- Modify: `internal/capture/engine.go` (`Lag()`), `internal/session/session.go` (expose), `internal/daemon/protocol.go` (SessionInfo fields), `internal/daemon/server.go` (opStatus), `cmd/offshoot/main.go` (`session status` output)
- Test: `internal/daemon/lifecycle_test.go` (append), `internal/capture/engine_test.go` (append)

**Interfaces:**
- Produces:

```go
// Engine gains (safe for concurrent use; read under the engine's existing mutex discipline):
//   Lag() (bytes int64)      // WAL bytes committed by writers but not yet applied to the replica
// SessionInfo gains:
//   DurableAge  string `json:"durable_age,omitempty"`   // time since last successful flush, "" if never
//   LastFlushAt string `json:"last_flush_at,omitempty"` // RFC3339
//   FlushError  string `json:"flush_error,omitempty"`   // most recent auto-flush failure
//   CaptureLag  int64  `json:"capture_lag_bytes"`
```

Also: one structured log line (key=value to stderr, matching the janitor's existing style) on every session state transition — opened, flushed (auto|manual, txid), flush-failed (error), fenced (cause), closed. Grep the daemon for existing prints and unify the format rather than inventing a second one.

- [ ] **Step 1: Failing tests.** Engine: after writing N committed bytes with the sink paused (the DrainNow tests show how to construct a pending backlog — reuse that setup), `Lag()` returns > 0; after drain, 0. Daemon: `status` for a session with an unflushed write reports CaptureLag > 0 or DurableAge growing (whichever is deterministic — prefer asserting LastFlushAt set after a flush and FlushError set under the Task-2 fault-injection). Engine changes mean torture re-runs (constraint).
- [ ] **Step 2-4:** Red → implement → green; `go test ./internal/capture ./internal/session ./internal/daemon -count=1 -race`; full suite; `make test-torture` with numbers (engine touched).
- [ ] **Step 5: Commit** `feat: session status reports durable age, flush errors, and capture lag` + CHANGELOG.

---

### Task 8: Documentation and the honesty ledger

**Files:**
- Modify: `README.md`, `docs/status.md`, `docs/reference.md`, `CHANGELOG.md`

**Interfaces:** none.

- [ ] **Step 1:** README: daemon section documents `-flush-every` (with the default and the data-loss window it bounds), MCP daemon mode, fork fast path in the flush-cost section (with a link to docs/benchmarks.md); the flush-cost section also documents the busy-session cadence interaction (30s ticks × SnapshotEvery=16 ⇒ a full snapshot every ~8 minutes of continuous writing — the idle short-circuit means idle sessions write nothing); resource-behavior paragraph (per-session disk/FD costs, the dbfile retention story in user terms, no budgets yet — Milestone 4). reference.md: new flags. status.md: flip every row this plan shipped; add rows for what it deliberately did not (async fork-point upload — deferred with reason: it introduces a `pending` chain state whose crash-recovery surface deserves its own plan; per-session FlushEvery wire field — YAGNI until a consumer asks). **PM amendment:** ROADMAP.md + status.md wording corrected to what M2 actually delivers — "MCP rides an EXISTING daemon session"; MCP-initiated session open/close moves to Milestone 3 where the pytest fixture owns session lifecycle (MCP-opened sessions with no lifecycle owner would recreate the leak class this milestone kills). README's MCP section states plainly that the good path requires a harness-opened session.
- [ ] **Step 2:** Run every command shown in changed docs verbatim (the repo's standard).
- [ ] **Step 3:** Commit `docs: milestone 2 — safe defaults documented honestly` + CHANGELOG `[0.1.1]` section drafted (release happens after merge).

---

## Self-Review (performed at plan-writing time)

1. **Roadmap coverage:** background flush (T2), MCP TTL (T3), MCP-rides-daemon (T4), benchmark-then-optimize fork (T5+T6), observability first half (T7), resource docs (T8), plus the dbfile follow-up (T1). Deliberately deferred WITH stated reasons in status.md: async fork-point upload, per-session flush-interval wire field. The 4GB benchmark is opt-in (time), stated in T5.
2. **Placeholder scan:** T2's second test and T4's tests are outlined with their assertion contracts and the instruction to reuse named existing harnesses (fault-injection helper, daemon scaffolding) — bounded elisions per this repo's established pattern; no TBDs.
3. **Type consistency:** `Options.FlushEvery`/`LastFlush()`/`LastFlushErr()` consistent across T2/T7/T8; `reflink.CopyFile(dst, src) (bool, error)` and `Backend.CopyObject(dst, src) error` consistent across T6's sub-parts; SessionInfo fields consistent between T7's protocol and CLI steps; MCP `-default-ttl`/`-socket` consistent between T3/T4 and T8's reference update.
