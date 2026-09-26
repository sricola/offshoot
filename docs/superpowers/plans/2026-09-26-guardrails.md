# Guardrails — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the "an agent cannot silently destroy the branch of record" pitch true by default: a `protect`/`unprotect` verb, an MCP server that refuses `force` on protected branches unless an operator opts in, a `<branch>-pre-rollback` safety fork so rollback is undoable like promote, `compact` that keeps checkpoints, and a daemonless `offshoot mcp` that reaps expired forks itself.

**Architecture:** All policy lives in `internal/ops` (a shared `safetyFork` helper generalized from promote's, `SetProtected`, checkpoint-preserving `Compact`) and is exposed through the CLI, the daemon (the existing `no_backup`/`backup_ttl` request fields now apply to `rollback` too), both SDKs, and MCP. The MCP server gains two operator knobs (`-allow-force`, `-reap-every`) and a background reaper that runs only when no daemon is up. The walkthrough is re-driven so its transcript shows the guardrail holding and a human promoting from the CLI after reviewing `offshoot_diff`.

**Tech Stack:** Go 1.26 (`internal/ops`, `internal/daemon`, `internal/mcp`, `cmd/offshoot`), Python/TypeScript SDKs, bash driver script for the walkthrough.

**Spec:** `reports/offshoot next features roadmap.md`, Tier 1 item "Weeks 4-6: guardrails that make the safe-writes pitch true". Product decisions (PM + staff engineer): (1) `force` through MCP is a real hole today (an agent can pass it), and the incident ledger says prompt-level rules fail, so the default flips to refuse with an opt-in `-allow-force` for harnesses that deliberately want agent-driven promotion; (2) `rollback` discards everything after the checkpoint with no undo, so it gets the same rolling TTL'd safety fork promote got (`<branch>-pre-rollback`), and stays force-free because it is now reversible; (3) `compact` wiping checkpoints is silent data loss the tests already worry about, so it copies them forward rollback-style and documents the cost; (4) TTLs reap nothing without a janitor today, so `offshoot mcp` sweeps on a timer when no daemon is up, using the existing CAS-claimed `Reap` (GC stays with `offshoot gc`/the daemon); (5) `protect`/`unprotect` are CLI verbs (plus ops), not MCP tools: an agent must not be able to lift the flag.

## Global Constraints

- Go directive `1.26.0`; gofmt clean; `go vet ./...` clean; `go test ./... -count=1` green.
- Commit trailers: `Co-Authored-By: Claude <committing model> <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B`.
- Package publication deferred: no PyPI/npm install text.
- Existing MCP tools' names and argument names unchanged; descriptions and prose results may change only where a task says so (the force refusal text, rollback's backup sentence). Every existing behavior test that passes `force: true` through MCP must be adjusted to opt in with `SetAllowForce(true)`, never deleted.
- Safety forks follow promote's contract exactly: marker meta `offshoot.pre-<verb>=<branch>`, TTL always set (default 24h), one rolling slot per branch, replaced only when the marker matches, refused if the previous one has a live lease.
- Doc sample outputs labelled real are re-captured from real runs.

---

### Task 1: `protect` / `unprotect` (ops + CLI)

**Files:**
- Create: `internal/ops/protect.go`
- Modify: `cmd/offshoot/main.go` (usage block near line 55-80; command switch)
- Test: `internal/ops/protect_test.go`, `cmd/offshoot/protect_test.go`

**Interfaces:**
- Produces: `func (w *Workspace) SetProtected(db, branch string, protected bool) (store.Ref, error)` — CAS-retried like `Touch` (`internal/ops/touch.go`), refuses a branch a reaper has claimed (`ref.Reaping`) or a Destroy has claimed (`ref.Deleting`); returns the updated ref. CLI: `offshoot protect <db>[@branch]` and `offshoot unprotect <db>[@branch]`, printing `protected <db>@<branch>` / `unprotected <db>@<branch>`.

- [ ] **Step 1: Write the failing tests**

`internal/ops/protect_test.go`:

```go
package ops

import (
	"strings"
	"testing"
)

// TestSetProtectedFlipsTheFlagAndGatesDestroy: protect a fork, then an
// unforced Destroy refuses and Reap never touches it; unprotect, and
// Destroy proceeds. main starts protected (Create's default) and can be
// unprotected too — the flag is policy, not identity.
func TestSetProtectedFlipsTheFlagAndGatesDestroy(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "keep", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	ref, err := w.SetProtected("app", "keep", true)
	if err != nil {
		t.Fatal(err)
	}
	if !ref.Protected {
		t.Fatalf("returned ref not protected: %+v", ref)
	}
	if err := w.Destroy("app", "keep", false); err == nil || !strings.Contains(err.Error(), "protected") {
		t.Fatalf("unforced destroy of a protected branch must refuse, got %v", err)
	}
	if _, err := w.SetProtected("app", "keep", false); err != nil {
		t.Fatal(err)
	}
	if err := w.Destroy("app", "keep", false); err != nil {
		t.Fatalf("destroy after unprotect: %v", err)
	}
	if _, err := w.SetProtected("app", "main", false); err != nil {
		t.Fatal(err)
	}
	got, _, _ := w.Store.GetRef("app", "main")
	if got.Protected {
		t.Fatal("main must be unprotectable")
	}
	if _, err := w.SetProtected("app", "nope", true); err == nil {
		t.Fatal("unknown branch must error")
	}
}
```

`cmd/offshoot/protect_test.go` (copy the store/seed helper pattern from `cmd/offshoot/diff_test.go:60-125`; do not invent new helpers):

```go
// TestProtectCLIRoundTrip: `offshoot protect app@fork` then `offshoot destroy app@fork`
// refuses; `offshoot unprotect app@fork` then destroy succeeds; output lines
// are exactly "protected app@fork" / "unprotected app@fork".
```
Fill the body with the helpers you find; assert the two output strings and the destroy refusal text contains "protected".

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/ops -run TestSetProtected -count=1 && go test ./cmd/offshoot -run TestProtectCLI -count=1`
Expected: compile failure.

- [ ] **Step 3: Implement**

`internal/ops/protect.go`:

```go
package ops

import (
	"fmt"
	"time"

	"github.com/sricola/offshoot/internal/store"
)

// SetProtected sets or clears db@branch's protected flag. A protected
// branch refuses unforced destroy and promote-onto, is never reaped, and —
// through an MCP server without -allow-force — cannot be forced by an
// agent at all. CAS-retried like Touch; refuses a branch a reaper or a
// Destroy has already claimed, since flipping the flag under either would
// race the claim's own outcome.
func (w *Workspace) SetProtected(db, branch string, protected bool) (store.Ref, error) {
	if err := store.ValidateName(db); err != nil {
		return store.Ref{}, err
	}
	if err := store.ValidateName(branch); err != nil {
		return store.Ref{}, err
	}
	for {
		ref, etag, err := w.Store.GetRef(db, branch)
		if err != nil {
			return store.Ref{}, err
		}
		if ref.Reaping || ref.Deleting {
			return store.Ref{}, fmt.Errorf("ops: %s@%s is being removed; too late to change its protection", db, branch)
		}
		if ref.Protected == protected {
			return ref, nil
		}
		ref.Protected = protected
		ref.Touch(time.Now())
		if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
			if errors.Is(err, store.ErrCAS) {
				continue
			}
			return store.Ref{}, err
		}
		return ref, nil
	}
}
```
(add `errors` to imports.) Read `internal/ops/touch.go` first and match its retry shape exactly if it differs (e.g. a bounded retry count); if `Touch` bounds retries, bound these the same way.

CLI (`cmd/offshoot/main.go`): add cases `"protect"` and `"unprotect"` next to `"touch"`: usage `offshoot protect <db>[@branch]` / `offshoot unprotect <db>[@branch]`; parse with `ops.ParseTarget`; call `w.SetProtected`; print `protected %s@%s` / `unprotected %s@%s`. Add two usage lines after the `touch` line: `offshoot protect <db>[@branch]        refuse unforced destroy/promote-onto, never reap; MCP cannot force it without -allow-force` and `offshoot unprotect <db>[@branch]      clear the protected flag`.

- [ ] **Step 4: Run tests, then commit**

Run: `go test ./internal/ops ./cmd/offshoot -count=1 && gofmt -l . && go vet ./...`

```bash
git add internal/ops/protect.go internal/ops/protect_test.go cmd/offshoot/main.go cmd/offshoot/protect_test.go
git commit -m "protect: offshoot protect/unprotect and ops.SetProtected

Co-Authored-By: Claude <model> <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B"
```

---

### Task 2: Rollback safety fork on every surface

**Files:**
- Modify: `internal/ops/ops.go` (generalize `promoteBackup` into `safetyFork`; add `RollbackOptions`, `RollbackResult`, `RollbackWith`; constants)
- Modify: `cmd/offshoot/main.go` (rollback case: `--no-backup`, `--backup-ttl`, print the undo line)
- Modify: `internal/daemon/protocol.go` (doc: `no_backup`/`backup_ttl` now also for rollback; `Response.Backup` also for rollback), `internal/daemon/server.go` (`opRollback`)
- Modify: `sdk/python/offshoot/client.py` (`rollback(db, branch, to, *, backup=True, backup_ttl=None)`), `sdk/typescript/src/client.ts` (`RollbackOptions{noBackup?, backupTtl?}`; `rollback(db, branch, to, opts)`)
- Modify: `internal/mcp/tools.go` (rollback always backs up with `t.defaultTTL`; description sentence; result names the fork)
- Test: `internal/ops/rollback_backup_test.go`, `internal/daemon/lifecycle_test.go`, `internal/mcp/tools_test.go`, both SDK test files

**Interfaces:**
- Produces in ops:
  ```go
  const RollbackBackupSuffix = "-pre-rollback"
  const RollbackBackupMetaKey = "offshoot.pre-rollback"
  // DefaultPromoteBackupTTL is reused as the rollback default.
  type RollbackOptions struct{ NoBackup bool; BackupTTL time.Duration }
  type RollbackResult struct{ Path, Backup string }
  func (w *Workspace) RollbackWith(db, branch, to string, opts RollbackOptions) (RollbackResult, error)
  func (w *Workspace) Rollback(db, branch, to string) (string, error) // = RollbackWith defaults; returns Path
  // safetyFork is the shared helper: name = branch+suffix; replaces only a fork whose meta[metaKey]==branch;
  // unforced Destroy of the previous one (a live lease refuses); Fork(db, branch, name, "", ttl, {metaKey: branch}).
  func (w *Workspace) safetyFork(db, branch, suffix, metaKey, verb string, ttl time.Duration) (string, error)
  ```
  `promoteBackup` becomes a one-line call to `safetyFork(db, target, PromoteBackupSuffix, PromoteBackupMetaKey, "promote", ttl)` with identical error texts (the promote tests pin "not a promote safety fork"; keep the verb in the message).
  Rollback's undo: `offshoot promote <db>@<branch>-pre-rollback --onto <branch> [--force]`. Rollback skips the safety fork when `to` would leave the branch identical? No — always mint unless `NoBackup`; simplicity wins. Rollback of a branch that IS a safety fork (`x-pre-rollback`) still mints `x-pre-rollback-pre-rollback` only if the name validates (`maxNameLen` 128); if it does not, the error names `--no-backup`, as promote does.
- Daemon: `opRollback` builds `ops.RollbackOptions{NoBackup: req.NoBackup, BackupTTL: parsed req.BackupTTL}` (same validation as promote), calls `refuseIfClaimed(req.DB, branch+ops.RollbackBackupSuffix)` unless `NoBackup`, returns `Response{OK, Checkout: res.Path, Backup: res.Backup}`.
- MCP: `rollback` uses `RollbackWith(..., RollbackOptions{BackupTTL: t.defaultTTL})`; text gains `; the previous head is kept as <db>@<branch>-pre-rollback (undo: offshoot_promote it back onto <branch>)`; structuredContent gains `"backup"`; the tool description gains one sentence: "The branch's previous head is kept first as a TTL'd safety fork `<branch>-pre-rollback` (one per branch, replaced by the next rollback), so a rollback is undone by promoting that fork back."

- [ ] **Step 1: Write the failing tests**

`internal/ops/rollback_backup_test.go` — mirror `promote_backup_test.go`'s `seedPromotePair`/`branchRows` helpers (they are package-level in `internal/ops` tests; reuse, do not duplicate):

```go
// TestRollbackKeepsSafetyFork: rollback main to v1 after a second
// checkpoint; the pre-rollback head (2 rows) survives as main-pre-rollback
// with the marker, a TTL, shared storage; main is at v1 (1 row); promoting
// the safety fork back restores 2 rows.
func TestRollbackKeepsSafetyFork(t *testing.T) { /* seed: create app, checkout, CREATE TABLE t, insert 1 row, checkpoint v1, insert row, checkpoint v2; RollbackWith(app, main, v1, {}) -> Backup == "main"+RollbackBackupSuffix; branchRows(main)=="1"; branchRows(backup)=="2"; ref.Meta[RollbackBackupMetaKey]=="main"; ref.TTL==DefaultPromoteBackupTTL.String(); ref.Base != nil; PromoteWith(backup -> main, Force:true) restores "2" */ }

// TestRollbackReplacesItsOwnSafetyFork: two rollbacks -> the second replaces the first (different lineage).
// TestRollbackRefusesToReplaceAForeignBranch: a user's "main-pre-rollback" without the marker makes rollback refuse; NoBackup sidesteps.
// TestRollbackCompatWrapperBacksUp: Rollback(db, branch, to) mints the fork by default.
```
Write these four fully (the promote equivalents in `promote_backup_test.go` are the template; adapt names and the `to` checkpoint).

Daemon (`lifecycle_test.go`): `TestRollbackKeepsSafetyForkThroughDaemon` — open a session, flush v1, write, flush v2, close; `Request{Op:"rollback", DB:"app", Branch:"main", Name:"v1", BackupTTL:"3h"}` → `OK`, `Backup == "main-pre-rollback"`, ref TTL `3h0m0s`; `BackupTTL:"-1h"` refused; a session open on `main-pre-rollback` refuses the next rollback (`Contains(r.Error, "close")`) unless `NoBackup`.

MCP (`tools_test.go`): `TestRollbackKeepsSafetyForkAndSaysSo` — fork attempt-1, checkout, checkpoint v1, write, checkpoint v2, call `offshoot_rollback` to v1; text contains `app@attempt-1-pre-rollback`; structuredContent has `backup`; ref TTL follows `newTools(t, 2*time.Hour)`.

SDKs: Python `test_rollback_promote_status` — after the existing rollback assertion, assert `"main-pre-rollback"` in `{b.branch for b in c.branches("rp")}` and that `c.rollback("rp", "main", "cp1", backup=False)` (after destroying the fork) leaves none. TS: same in the "rollback, promote, status" test with `{ noBackup: true }`.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/ops -run 'TestRollback.*SafetyFork|TestRollbackCompat|TestRollbackRefusesToReplace' -count=1`
Expected: compile failure (`RollbackWith` undefined).

- [ ] **Step 3: Implement** per Interfaces. In `Rollback`/`RollbackWith`, mint the safety fork AFTER the checkpoint lookup (`no checkpoint %q` error first) and BEFORE `copySnapshotToNewLineage`; the fork reads the target's ref itself, so `etag` from the earlier `GetRef` stays valid (Fork never writes the source ref) — assert that in a comment as promote does. CLI: `offshoot rollback <db>[@branch] --to <cp> [--no-backup] [--backup-ttl DUR]`; after the existing success line print `kept the previous <db>@<branch> head as <db>@<backup> (expires in <ttl>; undo with: offshoot promote <db>@<backup> --onto <branch> --force)` when a backup was minted (`--force` because rollback targets are often protected `main`).

- [ ] **Step 4: Run everything touched**

Run: `go test ./internal/ops ./internal/daemon ./internal/mcp ./cmd/offshoot -count=1 && make test-python-sdk PYTHON=/opt/homebrew/bin/python3.14 && make test-ts-sdk && gofmt -l . && go vet ./...`

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "rollback: keep the previous head as a TTL'd safety fork, like promote

Co-Authored-By: Claude <model> <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B"
```

---

### Task 3: `compact` preserves checkpoints

**Files:**
- Modify: `internal/ops/ops.go` (`Compact`, doc comment)
- Modify: `internal/ops/compact_cow_test.go:~90-105` (the reset assertion becomes a preservation assertion)
- Test: `internal/ops/compact_test.go`

**Interfaces:**
- `Compact` copies every existing checkpoint's snapshot into the new self-contained lineage exactly as `Rollback` does for its `kept` map (all checkpoints qualify: every `c.TXID <= head`), rewriting each to epoch 1 with `CreatedAt`/`Meta` preserved, and ADDS `"compact": head` to that map (not replacing it). The head snapshot copy already made for the new lineage is reused for any checkpoint whose txid equals head (`done` map, as in Rollback). Cost is documented: one snapshot copy per distinct checkpoint txid.

- [ ] **Step 1: Write the failing test** in `internal/ops/compact_test.go`:

```go
// TestCompactPreservesCheckpoints: a shared fork with checkpoints v1 and
// v2 compacts into a self-contained lineage that still has v1, v2 (their
// CreatedAt/Meta intact, epoch rewritten to 1) plus "compact", and
// rollback to v1 afterwards works from the compacted lineage.
```
Seed: create app, fork child (shared), checkout child, write, checkpoint v1 with meta `{"k":"v"}`, write, checkpoint v2; `Compact`; assert `ref.Base == nil`, checkpoints contain v1, v2, compact; `v1.Meta["k"]=="v"`, `v1.Epoch==1`; `Rollback(app, child, "v1")` succeeds and `branchRows` matches the v1 row count.

Update `compact_cow_test.go`'s "must reset checkpoints to exactly {compact}" assertion to expect the fork's existing `fork` checkpoint preserved alongside `compact`.

- [ ] **Step 2: Run to verify failure**, **Step 3: Implement** (reuse `copySnapshotIntoLineage`; abort and `bestEffortDelete` every copied key on any failure before the ref CAS, as Rollback's `cleanup` does), **Step 4: `go test ./internal/ops -count=1`**, **Step 5: Commit** `"compact: keep every checkpoint instead of resetting to {compact}"` with the trailers.

---

### Task 4: MCP refuses `force` on protected branches unless `-allow-force`

**Files:**
- Modify: `internal/mcp/tools.go` (`OffshootTools.allowForce`, `SetAllowForce`, `promote`/`destroy` gate + description sentences)
- Modify: `cmd/offshoot/main.go` (`offshoot mcp ... [-allow-force]`, usage)
- Modify: `plugin/skills/offshoot/SKILL.md` (one sentence in step 6/7: "a protected branch cannot be forced through MCP unless the operator started `offshoot mcp -allow-force`; ask the human, who can promote from the CLI")
- Test: `internal/mcp/tools_test.go` (adjust the three existing `force: true` promote tests to call `ts.SetAllowForce(true)` first; add the new test)

**Interfaces:**
- `func (t *OffshootTools) SetAllowForce(v bool)`; default false. In `promote` and `destroy`: if `a.Force && !t.allowForce`, read the target/branch ref; if it is protected, return `ErrorResult("%s@%s is protected and this MCP server does not allow force (start it with `offshoot mcp -allow-force` to permit it). Ask the human to promote/destroy from the CLI, or work on a fork.", ...)` BEFORE any mutation; if not protected, `force` is irrelevant and the call proceeds normally with `Force: false`. Descriptions: replace "unless `force` is set" clauses with "unless `force` is set AND the server was started with -allow-force; otherwise the refusal means: ask the human".

- [ ] **Step 1: Write the failing test**

```go
// TestMCPRefusesForceOnProtectedByDefault: with the default server, promote
// and destroy with force:true onto/of protected main are tool errors that
// name -allow-force, and main is untouched; after SetAllowForce(true) the
// same calls proceed. force on an UNprotected branch never needed the flag.
```
Assert: error text contains `-allow-force`; `main`'s ref unchanged (lineage, head); after `SetAllowForce(true)`, promote succeeds and destroy of a protected fork succeeds; `destroy` with `force:true` of an unprotected fork succeeds without the flag.

- [ ] **Step 2-5:** run (failure: `SetAllowForce` undefined), implement, run `go test ./internal/mcp ./cmd/offshoot -count=1`, commit `"mcp: force on a protected branch needs -allow-force; default refuses"`.

---

### Task 5: Daemonless reaping in `offshoot mcp`

**Files:**
- Modify: `internal/mcp/tools.go` (`StartReaper(ctx, every)`, `reapOnce(now) (reaped []string, skipped bool, err error)`)
- Modify: `cmd/offshoot/main.go` (`offshoot mcp ... [-reap-every DURATION|none]`, default `60s`; starts the reaper before serving; log lines to stderr `offshoot mcp: reaped <db@branch>` and one-time `offshoot mcp: reaper skipped: daemon is running`)
- Test: `internal/mcp/tools_test.go`, `internal/mcp/daemon_test.go`

**Interfaces:**
- `reapOnce`: if `t.daemonStatus()` reports a daemon, return `skipped=true` without touching the store (the daemon's janitor owns reaping); else call `t.ws.Reap(now)` then `t.ws.ClearStaleDeleteClaims(now)` (mirror `internal/daemon/janitor.go`'s order and error handling; read it first). `StartReaper` runs `reapOnce` on a `time.Ticker` until ctx is done, logging errors to stderr and never panicking. No GC (documented: `offshoot gc` or the daemon).

- [ ] **Step 1: Tests**

```go
// TestReapOnceReapsExpiredForksWhenNoDaemon: fork with ttl 1ms, sleep 5ms, reapOnce(now) -> reaped contains app@attempt-1, skipped=false; the ref is gone.
// TestReapOnceSkipsWhenDaemonIsUp (daemon_test.go, newDaemonTools): same fork; reapOnce -> skipped=true; the ref still exists.
// TestReapOnceNeverReapsProtected: protected fork with 1ms ttl survives.
```

- [ ] **Step 2-5:** run, implement, `go test ./internal/mcp -count=1`, commit `"mcp: reap expired forks on a timer when no daemon is running"`.

---

### Task 6: Docs, walkthrough re-driven, CHANGELOG

**Files:**
- Modify: `docs/demo/mcp-session-driver.sh` (after the id-9 refusal: add `offshoot_diff` id 10 `{"database":"shop","left":"main","right":"migration-attempt@migrated"}`; then a CLI step `offshoot promote shop@migration-attempt --onto main --force` logged as a `$` line; then destroy id 11, list id 12, SQL confirm — ids renumbered, no `force:true` call at all)
- Modify: `docs/demo/mcp-walkthrough.md` (re-assemble sections 6-7 from the new log: the guardrail holds by default, the agent compares with `offshoot_diff`, the human promotes from the CLI; every JSON block from the new run; `tools/list` re-captured; the honesty block updated)
- Modify: `docs/agents.md` (rewrite "The safety posture": force is now a permission boundary by default; `-allow-force`; `protect`/`unprotect`; rollback undo; daemonless reaping means "A TTL alone reaps nothing" is no longer true for `offshoot mcp` — say what `-reap-every` does and that GC still needs `offshoot gc`/the daemon)
- Modify: `docs/reference.md` (new `protect`/`unprotect` entries; rollback entry with `--no-backup`/`--backup-ttl` and the undo; compact entry: checkpoints preserved and the cost; `offshoot mcp` flags `-allow-force`, `-reap-every`; daemon `rollback` op `no_backup`/`backup_ttl`/`backup`; parity table rows for protect (CLI-only, by design) and rollback backup)
- Modify: `docs/status.md` (rows: protected flag settable; rollback safety fork; compact preserves checkpoints; MCP force gate; MCP reaper; each "v0.2.11 (unreleased)" with test names), `docs/limitations.md` and `docs/operations.md` wherever they say TTLs reap nothing without the daemon, `plugin/skills/offshoot/SKILL.md` (rollback undo sentence; protect), `README.md` (verb table: `protect`; rollback cell), `CHANGELOG.md` (Unreleased: Added/Changed, including the behavior change "MCP refuses force on protected branches by default").
- Verify: `go test ./internal/mcp -count=1 && make check-plugin`; grep docs for "A TTL alone reaps nothing" and "force is an argument the agent can pass" (both must be rewritten).

- [ ] Commit `"docs: guardrails — protect verb, force gate, rollback undo, compact keeps checkpoints, daemonless reaping; walkthrough re-driven"`.

---

## Self-review notes

- Coverage vs spec item: protected flag settable (T1), MCP cannot lift it or force past it by default (T4), rollback undo (T2), compact keeps checkpoints (T3), daemonless reaping (T5), docs (T6). The report's "MCP server flag naming the protected set at startup" is replaced by the existing per-branch `Protected` flag plus the `-allow-force` gate: same effect, no second source of truth.
- Consistency: `RollbackResult.Backup` / daemon `Response.Backup` / MCP `backup` / CLI undo line all name `<branch>-pre-rollback`; `safetyFork` is the single implementation for both verbs.
- Risk: T4 flips a documented default; the walkthrough, SKILL.md, agents.md, and CHANGELOG all say so (T6). Harnesses that relied on agent-driven force must add `-allow-force`; the plugin's `.mcp.json` stays on the safe default.
