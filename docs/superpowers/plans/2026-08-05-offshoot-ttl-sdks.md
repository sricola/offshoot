# Offshoot Plan 8: TTL Reaping, Python/TS SDKs, LangGraph Adapter

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Branches can carry a TTL and get reaped automatically when idle; Python and TypeScript SDKs speak the daemon's lifecycle API; a LangGraph companion forks the database when the agent's thread forks.

**Architecture:** TTL lives on the ref (activity clock stamped by every durable write; reaping is CAS-claimed then delegated to the existing Destroy→tombstone→GC pipeline, so no new deletion machinery). The daemon protocol grows the remaining lifecycle ops (create/fork/destroy/rollback/promote/touch/branches/checkout) with explicit open-session semantics; both SDKs are dependency-free newline-JSON socket clients over that protocol; the LangGraph adapter is a thin companion class in the Python SDK that maps thread ids to branches.

**Tech Stack:** Go 1.24+ (existing), Python 3.10+ stdlib-only, Node 20+ with TypeScript as the only dev dependency.

## Global Constraints

- Module `github.com/offshoot-db/offshoot`; Go 1.24+; cgo (mattn); Linux/macOS only; **no new Go module dependencies**
- Python SDK: **stdlib only** (socket, json, subprocess for tests), Python 3.10+; TypeScript SDK: **zero runtime dependencies**, Node 20+, `typescript`/`@types/node` as devDependencies only
- Ref changes are backward compatible: new fields are optional with `omitempty`, **no schema bump** — a Plan-7 ref decodes unchanged and means "no TTL, never reaped"
- **A branch with a live lease is never reaped**; expiry defers until the lease is released or times out (spec: TTL measured from the last durable write or lease renewal, whichever is later; `offshoot touch` resets it explicitly; creating a child does not extend the parent; branches without a TTL live until destroyed)
- **Protected branches are never reaped**, regardless of TTL
- Reaping must be concurrency-safe: a concurrent `touch` and reap must have exactly one winner (CAS), never a silently lost branch
- Daemon lifecycle ops on a branch with an open session in that daemon: `destroy`/`rollback`/`promote`(target) refuse with a clear error telling the caller to close the session; `fork` from an open source session flushes first, then forks — that is the killer feature (fork a live agent's world)
- Every Plan-1..7 test must keep passing unmodified
- Commit messages: conventional commits, ending with the repo's session trailers (`git log -1 --format=%B` shows the format)

## File Structure

```
internal/store/store.go            (modify) Ref gains TTL/TouchedAt/Reaping + Touch helper
internal/store/lease.go            (modify) ReleaseLease stamps TouchedAt
internal/ops/ops.go                (modify) Checkpoint/Fork/Rollback/Promote stamp TouchedAt; Fork gains ttl param; Status reports TTL
internal/ops/reap.go               Reap: CAS-claim expired branches, Destroy them
internal/ops/reap_test.go
internal/ops/touch.go              Touch: set/clear TTL, stamp the clock
internal/ops/touch_test.go
internal/session/flush.go          (modify) flush stamps TouchedAt on the ref it writes
internal/daemon/protocol.go        (modify) lifecycle ops + BranchInfo
internal/daemon/server.go          (modify) lifecycle handlers + janitor loop
internal/daemon/lifecycle_test.go
cmd/offshoot/main.go               (modify) touch command, fork -ttl, serve -reap-every/-gc-grace, status shows TTL
sdk/python/pyproject.toml          Python package "offshoot-db"
sdk/python/offshoot/__init__.py
sdk/python/offshoot/client.py      socket client + Session
sdk/python/offshoot/langgraph.py   ThreadForks companion
sdk/python/tests/test_client.py
sdk/python/tests/test_langgraph.py
sdk/typescript/package.json        "@offshoot-db/client", zero runtime deps
sdk/typescript/tsconfig.json
sdk/typescript/src/client.ts
sdk/typescript/test/client.test.ts
examples/langgraph-rewind/README.md
examples/langgraph-rewind/agent.py
Makefile                           (modify) test-sdk targets
README.md                          (modify) TTL semantics, SDK quickstarts, integration surface
```

---

### Task 1: TTL fields on the ref, `Touch`, and the CLI `touch` command

**Files:**
- Modify: `internal/store/store.go` (Ref struct, refWire, decodeRef, toWire path)
- Modify: `internal/store/lease.go` (`ReleaseLease`)
- Modify: `internal/ops/ops.go` (`Checkpoint`, `Fork`, `Rollback`, `Promote`, `Status`)
- Modify: `internal/session/flush.go` (the PutRef site)
- Create: `internal/ops/touch.go`, `internal/ops/touch_test.go`
- Modify: `cmd/offshoot/main.go` (new `touch` command; `fork` gains `-ttl`; `status` prints TTL)
- Test: extend `internal/store/store_test.go`

**Interfaces:**
- Consumes: `store.Ref`, `Store.GetRef/PutRef`, `ops.Workspace`
- Produces (later tasks rely on these exact names):

```go
// On store.Ref (new fields, all omitempty, no schema bump):
//   TTL       string `json:"ttl,omitempty"`        // Go duration string, e.g. "2h"; "" = no TTL
//   TouchedAt string `json:"touched_at,omitempty"` // RFC3339Nano UTC; activity clock
//   Reaping   bool   `json:"reaping,omitempty"`    // set by Reap's CAS claim (Task 2)

// Touch stamps the activity clock. Reaping measures TTL from the later of
// this stamp and the lease expiry.
func (r *Ref) Touch(now time.Time)

// package ops
// Touch sets (ttl != nil) or keeps (ttl == nil) the branch's TTL and stamps
// the activity clock. A *ttl of 0 clears the TTL. Refuses a branch mid-reap.
func (w *Workspace) Touch(db, branch string, ttl *time.Duration, now time.Time) (store.Ref, error)

// Fork gains a TTL for the child (0 = none):
func (w *Workspace) Fork(db, srcBranch, newBranch, at string, ttl time.Duration) (uint64, error)

// ops.BranchStatus gains:
//   TTL          string // "" if none
//   TTLRemaining string // "" if none; "expired" when past deadline
```

- [ ] **Step 1: Write the failing tests**

Append to `internal/store/store_test.go`:

```go
func TestRefTTLFieldsRoundTripAndOldRefsDecode(t *testing.T) {
	b := newTestBackend(t) // use this file's existing backend helper
	s := &Store{B: b}
	if err := s.InitManifest(); err != nil {
		t.Fatal(err)
	}
	r := Ref{Schema: 2, Lineage: "lin1", Epoch: 1, HeadTXID: 3, HeadEpoch: 1}
	r.TTL = "2h"
	r.Touch(time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC))
	etag, err := s.PutRef("app", "b", r, "")
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := s.GetRef("app", "b")
	if err != nil {
		t.Fatal(err)
	}
	if got.TTL != "2h" || got.TouchedAt != "2026-08-05T12:00:00Z" {
		t.Fatalf("TTL fields did not round-trip: %+v", got)
	}
	// A ref written without the new fields (a Plan-7 ref) must decode with
	// them empty — no TTL means never reaped.
	old := Ref{Schema: 2, Lineage: "lin2", Epoch: 1, HeadTXID: 1, HeadEpoch: 1}
	if _, err := s.PutRef("app", "old", old, ""); err != nil {
		t.Fatal(err)
	}
	gotOld, _, err := s.GetRef("app", "old")
	if err != nil {
		t.Fatal(err)
	}
	if gotOld.TTL != "" || gotOld.TouchedAt != "" || gotOld.Reaping {
		t.Fatalf("plain ref grew TTL state: %+v", gotOld)
	}
	_ = etag
}
```

Create `internal/ops/touch_test.go`:

```go
package ops

import (
	"testing"
	"time"
)

func TestTouchSetsClearsAndStampsTTL(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	ttl := 90 * time.Minute
	now := time.Now()
	ref, err := w.Touch("app", "main", &ttl, now)
	if err != nil {
		t.Fatal(err)
	}
	if ref.TTL != "1h30m0s" {
		t.Fatalf("TTL = %q, want 1h30m0s", ref.TTL)
	}
	if ref.TouchedAt == "" {
		t.Fatal("Touch must stamp the activity clock")
	}
	// nil ttl = keep the TTL, restamp the clock.
	later := now.Add(time.Minute)
	ref2, err := w.Touch("app", "main", nil, later)
	if err != nil {
		t.Fatal(err)
	}
	if ref2.TTL != "1h30m0s" {
		t.Fatalf("nil ttl must keep TTL, got %q", ref2.TTL)
	}
	if ref2.TouchedAt == ref.TouchedAt {
		t.Fatal("Touch must advance the clock")
	}
	// zero ttl = clear.
	zero := time.Duration(0)
	ref3, err := w.Touch("app", "main", &zero, later)
	if err != nil {
		t.Fatal(err)
	}
	if ref3.TTL != "" {
		t.Fatalf("zero ttl must clear, got %q", ref3.TTL)
	}
}

func TestDurableWritesStampTheClock(t *testing.T) {
	requireSQLite(t) // this package's existing helper; if absent, use the exec.LookPath skip pattern from gc_chain_test.go
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// Checkpoint stamps.
	mustExecSQL(t, w.CheckoutPath("app", "main"), "CREATE TABLE t (v);")
	if _, err := w.Checkpoint("app", "main", "cp1"); err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.TouchedAt == "" {
		t.Fatal("Checkpoint must stamp TouchedAt")
	}
	// Fork stamps the CHILD and does not touch the parent (spec: creating a
	// child does not extend the parent).
	before := ref.TouchedAt
	if _, err := w.Fork("app", "main", "kid", "", 2*time.Hour); err != nil {
		t.Fatal(err)
	}
	kid, _, err := w.Store.GetRef("app", "kid")
	if err != nil {
		t.Fatal(err)
	}
	if kid.TTL != "2h0m0s" || kid.TouchedAt == "" {
		t.Fatalf("fork -ttl must set child TTL+clock: %+v", kid)
	}
	parent, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if parent.TouchedAt != before {
		t.Fatal("fork must not extend the parent's clock")
	}
}
```

If `internal/ops` has no `mustExecSQL` helper, add one to the new test file (exec.Command sqlite3 + CombinedOutput + t.Fatalf, matching gc_chain_test.go's pattern).

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/store -run TTLFields -v && go test ./internal/ops -run 'Touch|StampTheClock' -v`
Expected: FAIL — `r.TTL` undefined, `w.Touch` undefined, `Fork` argument count mismatch.

- [ ] **Step 3: Implement**

1. `internal/store/store.go`: add the three fields to `Ref` and `refWire` (and wherever refWire is written back — follow the existing field-by-field copy). Add:

```go
// Touch stamps the ref's activity clock. Reaping (ops.Reap) measures TTL
// from the later of this stamp and the lease expiry, so any durable write
// or lease activity defers expiry.
func (r *Ref) Touch(now time.Time) {
	r.TouchedAt = now.UTC().Format(time.RFC3339Nano)
}
```

2. `internal/store/lease.go`: in `ReleaseLease`, before the PutRef, call `ref.Touch(time.Now())` — a lease that was just live counts as activity, so a branch isn't instantly expired the moment its session closes.
3. `internal/ops/ops.go`: stamp `ref.Touch(time.Now())` immediately before the ref write in `Checkpoint`, `Rollback`, and `Promote` (each writes the branch's ref — find the PutRef sites). `Fork` gains the `ttl time.Duration` parameter: on the child ref set `TTL = ttl.String()` when ttl > 0 and `Touch(now)`; never write the parent ref. Update Fork's existing callers (`cmd/offshoot/main.go` fork command → new `-ttl` flag parsed with `time.ParseDuration`, default 0; `internal/mcp/tools.go` fork tool → pass 0 for now; any test callers → pass 0).
4. `internal/session/flush.go`: at the site that builds the new ref for PutRef, add `newRef.Touch(time.Now())` (both snapshot and segment paths go through the same ref write).
5. Create `internal/ops/touch.go`:

```go
package ops

import (
	"fmt"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

// Touch resets a branch's activity clock, and optionally sets (ttl > 0) or
// clears (ttl == 0) its TTL; a nil ttl keeps the current TTL. CAS-retried;
// refuses a branch a reaper has already claimed.
func (w *Workspace) Touch(db, branch string, ttl *time.Duration, now time.Time) (store.Ref, error) {
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
		if ref.Reaping {
			return store.Ref{}, fmt.Errorf("ops: %s@%s is being reaped; too late to touch", db, branch)
		}
		if ttl != nil {
			if *ttl > 0 {
				ref.TTL = ttl.String()
			} else {
				ref.TTL = ""
			}
		}
		ref.Touch(now)
		if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
			if isCAS(err) { // use this package's existing ErrCAS check helper; add errors.Is(err, store.ErrCAS) if none exists
				continue
			}
			return store.Ref{}, err
		}
		return ref, nil
	}
}
```

6. `Status()` in ops.go: populate the two new `BranchStatus` fields — `TTL` verbatim from the ref; `TTLRemaining` computed against `max(TouchedAt, LeaseExpiry)` + TTL (reuse the deadline helper Task 2 exports — for this task, compute inline and let Task 2 refactor into the shared helper), `"expired"` when past.
7. CLI: `touch <db>@<branch> [-ttl <duration>|-ttl none]` — parse the `@` form the way `fork` does; `-ttl none` → pointer to 0; `-ttl 2h` → pointer to parsed value; flag absent → nil. Print the resulting TTL and clock. `status` output: append `ttl=<TTL> remaining=<TTLRemaining>` for branches that have one.

- [ ] **Step 4: Run**

Run: `go test ./internal/store ./internal/ops ./internal/session -count=1 -race && go test ./... -count=1 -race && go vet ./...`
Expected: PASS, including every pre-existing test (the Fork signature change means callers were updated, not tests weakened — passing `0` preserves old behavior exactly).

- [ ] **Step 5: Commit**

```bash
git add internal/store internal/ops internal/session cmd/offshoot internal/mcp
git commit -m "feat: branches carry a TTL and an activity clock, offshoot touch resets it"
```

---

### Task 2: Reaping — expired branches are destroyed safely

**Files:**
- Create: `internal/ops/reap.go`, `internal/ops/reap_test.go`
- Modify: `internal/daemon/server.go` (janitor loop), `cmd/offshoot/main.go` (`serve -reap-every -gc-grace`, `gc` prints reaped)

**Interfaces:**
- Consumes: `store.ListRefs/GetRef/PutRef`, `ops.Destroy`, `ops.GC`, Task 1's fields
- Produces:

```go
// Reap destroys every branch whose TTL has expired. Safe against concurrent
// touches (CAS claim) and never reaps a protected branch or a live lease.
// Returns the "db@branch" names it reaped.
func (w *Workspace) Reap(now time.Time) ([]string, error)

// On *daemon.Server:
// StartJanitor runs Reap + GC(grace) every interval until Shutdown.
func (s *Server) StartJanitor(every, grace time.Duration)
```

- [ ] **Step 1: Write the failing tests**

`internal/ops/reap_test.go`:

```go
package ops

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

func setTTLAt(t *testing.T, w *Workspace, db, branch, ttl, touchedAt string) {
	t.Helper()
	ref, etag, err := w.Store.GetRef(db, branch)
	if err != nil {
		t.Fatal(err)
	}
	ref.TTL, ref.TouchedAt = ttl, touchedAt
	if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
		t.Fatal(err)
	}
}

func TestReapDestroysOnlyExpiredUnprotectedUnleased(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	old := now.Add(-3 * time.Hour).Format(time.RFC3339Nano)
	for _, br := range []string{"expired", "fresh", "shielded", "leased", "immortal"} {
		if _, err := w.Fork("app", "main", br, "", 0); err != nil {
			t.Fatal(err)
		}
	}
	setTTLAt(t, w, "app", "expired", "1h", old)
	setTTLAt(t, w, "app", "fresh", "1h", now.Format(time.RFC3339Nano))
	setTTLAt(t, w, "app", "shielded", "1h", old)
	if ref, etag, err := w.Store.GetRef("app", "shielded"); err != nil {
		t.Fatal(err)
	} else {
		ref.Protected = true
		if _, err := w.Store.PutRef("app", "shielded", ref, etag); err != nil {
			t.Fatal(err)
		}
	}
	setTTLAt(t, w, "app", "leased", "1h", old)
	if _, err := w.Store.AcquireLease("app", "leased", "holder-1", time.Hour, now); err != nil {
		t.Fatal(err)
	}
	// "immortal" has no TTL at all; "main" likewise.

	reaped, err := w.Reap(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped) != 1 || reaped[0] != "app@expired" {
		t.Fatalf("reaped = %v, want exactly [app@expired]", reaped)
	}
	if _, _, err := w.Store.GetRef("app", "expired"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expired ref must be gone, err=%v", err)
	}
	for _, br := range []string{"main", "fresh", "shielded", "leased", "immortal"} {
		if _, _, err := w.Store.GetRef("app", br); err != nil {
			t.Fatalf("%s must survive: %v", br, err)
		}
	}
}

func TestExpiredLeaseDefersToTTLNotBlocksForever(t *testing.T) {
	// Spec: a wedged holder loses the lease first, then TTL applies — an
	// EXPIRED lease does not shield a branch, but the clock includes the
	// lease expiry ("last durable write or lease renewal, whichever is later").
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "wedged", "", 0); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	setTTLAt(t, w, "app", "wedged", "1h", now.Add(-5*time.Hour).Format(time.RFC3339Nano))
	if _, err := w.Store.AcquireLease("app", "wedged", "holder-1", time.Minute, now.Add(-3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// Lease expired ~3h ago; TTL 1h past that is also gone → reapable.
	reaped, err := w.Reap(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped) != 1 || reaped[0] != "app@wedged" {
		t.Fatalf("reaped = %v, want [app@wedged]", reaped)
	}
	// But if the lease expired RECENTLY, TTL counts from that expiry.
	if _, err := w.Fork("app", "main", "recent", "", 0); err != nil {
		t.Fatal(err)
	}
	setTTLAt(t, w, "app", "recent", "1h", now.Add(-5*time.Hour).Format(time.RFC3339Nano))
	if _, err := w.Store.AcquireLease("app", "recent", "holder-2", time.Minute, now.Add(-30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	reaped, err = w.Reap(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped) != 0 {
		t.Fatalf("lease expired 29m ago + 1h TTL = not yet expired; reaped %v", reaped)
	}
}

func TestConcurrentTouchAndReapHaveExactlyOneWinner(t *testing.T) {
	for i := 0; i < 20; i++ {
		w := newWS(t)
		if err := w.Create("app"); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Fork("app", "main", "contested", "", 0); err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC()
		setTTLAt(t, w, "app", "contested", "1h", now.Add(-2*time.Hour).Format(time.RFC3339Nano))

		var wg sync.WaitGroup
		var touchErr error
		var reaped []string
		wg.Add(2)
		go func() { defer wg.Done(); _, touchErr = w.Touch("app", "contested", nil, now) }()
		go func() { defer wg.Done(); reaped, _ = w.Reap(now) }()
		wg.Wait()

		_, _, getErr := w.Store.GetRef("app", "contested")
		gone := errors.Is(getErr, store.ErrNotFound)
		switch {
		case len(reaped) == 1 && gone:
			// Reaper won; the touch must NOT have claimed success.
			if touchErr == nil {
				t.Fatalf("iter %d: both touch and reap claimed success", i)
			}
		case len(reaped) == 0 && !gone && touchErr == nil:
			// Toucher won; branch alive with a fresh clock.
		default:
			t.Fatalf("iter %d: incoherent outcome reaped=%v touchErr=%v getErr=%v", i, reaped, touchErr, getErr)
		}
	}
}

func TestReapedLineageIsCollectableByGC(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "doomed", "", 0); err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "doomed")
	if err != nil {
		t.Fatal(err)
	}
	lineage := ref.Lineage
	now := time.Now().UTC()
	setTTLAt(t, w, "app", "doomed", "1h", now.Add(-2*time.Hour).Format(time.RFC3339Nano))
	if _, err := w.Reap(now); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	keys, err := w.Store.B.List(store.LineagePrefix(lineage))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("reaped lineage must be swept by GC, %d objects remain", len(keys))
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/ops -run 'Reap|OneWinner|ExpiredLease' -v -race`
Expected: FAIL — `w.Reap` undefined.

- [ ] **Step 3: Implement `internal/ops/reap.go`**

```go
package ops

import (
	"fmt"
	"os"
	"sort"
	"time"
)

// reapDeadline reports when a ref's TTL expires: TTL counted from the later
// of the activity clock and the lease expiry. ok is false when the ref has
// no TTL or an unparseable one (unparseable = warn, never reap).
func reapDeadline(ttl, touchedAt, leaseExpiry string) (time.Time, bool) {
	d, err := time.ParseDuration(ttl)
	if err != nil || d <= 0 {
		return time.Time{}, false
	}
	clock, err := time.Parse(time.RFC3339Nano, touchedAt)
	if err != nil {
		// No/бroken clock: fall back to lease expiry alone; with neither,
		// refuse to reap (a TTL with no clock is a config bug, not consent
		// to delete).
		clock = time.Time{}
	}
	if exp, lerr := time.Parse(time.RFC3339Nano, leaseExpiry); lerr == nil && exp.After(clock) {
		clock = exp
	}
	if clock.IsZero() {
		return time.Time{}, false
	}
	return clock.Add(d), true
}

// Reap destroys every branch whose TTL has expired. Per the spec: a live
// lease always defers expiry; protected branches are never reaped; branches
// without a TTL live until destroyed. The claim is CAS'd so a concurrent
// touch and reap have exactly one winner.
func (w *Workspace) Reap(now time.Time) ([]string, error) {
	refs, err := w.Store.ListRefs()
	if err != nil {
		return nil, err
	}
	var reaped []string
	var firstErr error
	dbs := make([]string, 0, len(refs))
	for db := range refs {
		dbs = append(dbs, db)
	}
	sort.Strings(dbs)
	for _, db := range dbs {
		branches := append([]string(nil), refs[db]...)
		sort.Strings(branches)
		for _, branch := range branches {
			ok, err := w.reapOne(db, branch, now)
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("reap %s@%s: %w", db, branch, err)
			}
			if ok {
				reaped = append(reaped, db+"@"+branch)
			}
		}
	}
	return reaped, firstErr
}

func (w *Workspace) reapOne(db, branch string, now time.Time) (bool, error) {
	ref, etag, err := w.Store.GetRef(db, branch)
	if err != nil {
		return false, err
	}
	if ref.TTL == "" || ref.Protected {
		return false, nil
	}
	if ref.TTL != "" {
		if _, parses := reapDeadline(ref.TTL, ref.TouchedAt, ""); !parses {
			if _, derr := time.ParseDuration(ref.TTL); derr != nil {
				fmt.Fprintf(os.Stderr, "offshoot: warning: %s@%s has unparseable ttl %q; not reaping\n", db, branch, ref.TTL)
				return false, nil
			}
		}
	}
	// A live lease always defers expiry.
	if ref.LeaseHolder != "" {
		if exp, perr := time.Parse(time.RFC3339Nano, ref.LeaseExpiry); perr == nil && now.Before(exp) {
			return false, nil
		}
	}
	deadline, ok := reapDeadline(ref.TTL, ref.TouchedAt, ref.LeaseExpiry)
	if !ok || now.Before(deadline) {
		return false, nil
	}
	// CAS claim: mark the ref as reaping. A concurrent Touch either landed
	// first (our PutRef fails on ErrCAS -> re-evaluate next cycle) or will
	// fail loudly on seeing Reaping.
	ref.Reaping = true
	if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
		if isCAS(err) {
			return false, nil // lost to a concurrent writer; not an error
		}
		return false, err
	}
	// force=false: if a lease was acquired in the window after our claim,
	// Destroy refuses and we unwind the claim rather than kill a live writer.
	if err := w.Destroy(db, branch, false); err != nil {
		if ref2, etag2, gerr := w.Store.GetRef(db, branch); gerr == nil && ref2.Reaping {
			ref2.Reaping = false
			_, _ = w.Store.PutRef(db, branch, ref2, etag2) // best effort; next cycle retries
		}
		return false, err
	}
	return true, nil
}
```

`isCAS` — if the package already has an ErrCAS check helper, use it; otherwise `func isCAS(err error) bool { return errors.Is(err, store.ErrCAS) }` (check the exact sentinel name in `internal/store`).

Note: `Destroy` requires no changes — it already refuses live leases without force and handles tombstoning; a `Reaping` ref is just a ref to it. Also note the spec's protected rule wins over TTL even though `Destroy(force=true)` could bypass it — Reap deliberately never passes force.

Daemon janitor — in `internal/daemon/server.go`:

```go
// StartJanitor reaps expired branches and runs GC every interval until
// Shutdown. grace is passed to GC (tombstone age before deletion); the
// default in cmd is deliberately generous — the spec requires it to exceed
// the longest plausible in-flight fork.
func (s *Server) StartJanitor(every, grace time.Duration) {
	if every <= 0 {
		return
	}
	s.janitorWG.Add(1)
	go func() {
		defer s.janitorWG.Done()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-s.janitorStop: // close this in Shutdown before waiting on janitorWG
				return
			case <-t.C:
				if reaped, err := s.ws.Reap(time.Now()); err != nil {
					fmt.Fprintf(os.Stderr, "offshoot: janitor: reap: %v\n", err)
				} else if len(reaped) > 0 {
					fmt.Fprintf(os.Stderr, "offshoot: janitor: reaped %v\n", reaped)
				}
				if _, _, err := s.ws.GC(grace); err != nil {
					fmt.Fprintf(os.Stderr, "offshoot: janitor: gc: %v\n", err)
				}
			}
		}
	}()
}
```

Add `janitorStop chan struct{}` + `janitorWG sync.WaitGroup` to Server (initialize in NewServer; close+wait in Shutdown, mirroring how Shutdown already joins other goroutines — study the existing shutdown ordering first; the janitor must stop BEFORE sessions close so a reap never races the daemon's own teardown). Sessions hold live leases, so the janitor can never reap a branch this daemon is writing.

CLI: `serve` gains `-reap-every` (default `1m`) and `-gc-grace` (default `15m`), `0` disables the janitor; call `StartJanitor` after the server starts. `gc` command prints reap results too: call `w.Reap(time.Now())` before `w.GC(grace)` and print both.

Add a daemon-level test in `internal/daemon/lifecycle_test.go` (create the file; Task 3 extends it):

```go
func TestJanitorReapsExpiredBranchWhileDaemonRuns(t *testing.T) {
	// Start a server on a temp socket with StartJanitor(50*time.Millisecond, 0).
	// Fork a branch, backdate its TTL via the store directly (setTTLAt pattern),
	// then poll GetRef until ErrNotFound or 5s timeout.
	// Assert: branch is gone; "main" and the db survive.
}
```

Write it as real code following this package's existing server test setup (`server_test.go` has the socket + workspace scaffolding — reuse its helpers).

- [ ] **Step 4: Run**

Run: `go test ./internal/ops ./internal/daemon -count=1 -race -v -run 'Reap|Janitor|OneWinner|ExpiredLease' && go test ./... -count=1 -race && go vet ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/ops internal/daemon cmd/offshoot
git commit -m "feat: TTL reaping with CAS-safe claims and a daemon janitor loop"
```

---

### Task 3: Daemon lifecycle API — the protocol the SDKs will speak

**Files:**
- Modify: `internal/daemon/protocol.go`, `internal/daemon/server.go`
- Test: `internal/daemon/lifecycle_test.go` (extend)

**Interfaces:**
- Consumes: `ops.Workspace` methods, Task 1/2 additions, existing session map in Server
- Produces (the SDK contract — Tasks 4/5/6 depend on these exact wire shapes):

```go
// Request gains:
//   Name   string `json:"name,omitempty"`   // (exists) checkpoint name / new-branch name / rollback target / promote target
//   From   string `json:"from,omitempty"`   // fork: source checkpoint name ("" = head)
//   TTL    string `json:"ttl,omitempty"`    // fork/touch: Go duration; touch: "none" clears, "" keeps
//   Force  bool   `json:"force,omitempty"`  // destroy/promote
// Op set becomes: open | flush | status | close | shutdown |
//   create | checkout | fork | destroy | rollback | promote | touch | branches

// Response gains:
//   Branches []BranchInfo `json:"branches,omitempty"`

type BranchInfo struct {
	Branch       string   `json:"branch"`
	HeadTXID     uint64   `json:"head_txid"`
	Protected    bool     `json:"protected"`
	TTL          string   `json:"ttl,omitempty"`
	TTLRemaining string   `json:"ttl_remaining,omitempty"`
	LeaseHolder  string   `json:"lease_holder,omitempty"`
	Checkpoints  []string `json:"checkpoints,omitempty"` // sorted names
}
```

Semantics (write these into the op handlers' doc comments — they are the API contract):
- `create {db}` → `ops.Create`.
- `checkout {db, branch}` → `ops.Checkout`, returns `checkout` path. Refuses (`"close the session first"`) if this daemon has an open session on that branch — the session already owns that path.
- `fork {db, branch: source, name: new, from, ttl}` → if this daemon has an open session on the source, `Flush("")` it first, then `ops.Fork`; else at-rest `ops.Fork`. Returns `txid` (fork point).
- `destroy {db, branch, force}` / `rollback {db, branch, name: checkpoint}` / `promote {db, branch: source, name: target, force}` → refuse when this daemon has an open session on the affected branch (destroy/rollback: `branch`; promote: the **target**; promote with an open SOURCE session flushes it first, matching fork). Otherwise delegate to ops.
- `touch {db, branch, ttl}` → `ops.Touch` (ttl "" = nil, "none" = &0, else parsed). Returns nothing beyond ok.
- `branches {db}` → BranchInfo list from `Status()` filtered to db, or from ListRefs+GetRef; sorted by branch name.

- [ ] **Step 1: Write the failing tests**

Extend `internal/daemon/lifecycle_test.go` with real tests using the package's existing client/server test scaffolding, covering at minimum:

```go
// TestLifecycleOpsRoundTrip: create db2 via daemon; branches lists main;
// fork main->try with ttl "1h"; branches shows try with TTL set; open a
// session on try, write a row (sqlite3 CLI), flush with name "good";
// fork FROM THE OPEN SESSION (op fork, source try) -> must succeed and the
// new branch must contain the just-written row (checkout + count);
// destroy try while its session is open -> error mentioning "close";
// close the session; destroy try -> ok; branches no longer lists it.

// TestPromoteGuards: promote onto a branch with an open session -> error;
// promote whose SOURCE has an open session flushes first (write a row,
// don't flush manually, promote source->fresh-target, checkout target,
// row must be present).

// TestRollbackGuard: rollback of a branch with an open session -> error;
// after close -> ok and the checkout reflects the checkpoint.

// TestUnknownOpStillErrors: op "zap" -> ok=false with an error (guards the
// dispatch default as the op list grows).
```

Write these as complete tests (this package's `server_test.go` shows how to start a server on a temp socket and how existing tests exec sqlite3 against checkout paths — mirror those patterns; every sqlite3/exec error checked with CombinedOutput+Fatalf).

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/daemon -run 'Lifecycle|PromoteGuards|RollbackGuard|UnknownOp' -v`
Expected: FAIL — unknown ops return errors ("unknown op" from dispatch).

- [ ] **Step 3: Implement**

Extend `dispatch` with the new cases; one small handler per op (`opCreate`, `opCheckoutAtRest`, `opFork`, `opDestroy`, `opRollback`, `opPromote`, `opTouch`, `opBranches`). Session lookups go through the existing sessions map under the existing mutex; the flush-then-fork path reuses `Session.Flush`. Keep handlers thin — validation and real work stay in ops. TTL string parsing: `"" → nil`, `"none" → &zero`, else `time.ParseDuration` (error → ok=false). `opBranches` computes TTLRemaining with Task 2's `reapDeadline` helper (export it from ops as `ReapDeadline` if needed — do that rather than duplicating the clock logic).

- [ ] **Step 4: Run**

Run: `go test ./internal/daemon -count=1 -race -v && go test ./... -count=1 -race && go vet ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/daemon
git commit -m "feat: daemon speaks the full lifecycle API"
```

---

### Task 4: Python SDK

**Files:**
- Create: `sdk/python/pyproject.toml`, `sdk/python/offshoot/__init__.py`, `sdk/python/offshoot/client.py`, `sdk/python/tests/test_client.py`
- Modify: `Makefile` (`test-python-sdk` target)

**Interfaces:**
- Consumes: Task 3's wire protocol, the `offshoot` binary (built for tests)
- Produces (Task 6 depends on these exact names):

```python
offshoot.connect(socket_path) -> Client                # context manager
Client.create(db)
Client.open(db, branch="main") -> Session              # daemon session (lease + live capture)
Client.checkout(db, branch) -> str                     # at-rest checkout path
Client.fork(db, source, new, from_checkpoint=None, ttl=None) -> int
Client.destroy(db, branch, force=False)
Client.rollback(db, branch, to)
Client.promote(db, source, onto, force=False)
Client.touch(db, branch, ttl=...)                      # ttl: None keeps, timedelta/str sets, 0/"none" clears
Client.branches(db) -> list[Branch]                    # Branch: dataclass mirroring BranchInfo
Client.status() -> list[dict]
Client.close()
Session.path -> str
Session.flush(name="") -> int
Session.close()
class OffshootError(Exception)                          # .message from Response.error
```

- [ ] **Step 1: Write the package and failing tests**

`sdk/python/pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "offshoot-db"
version = "0.1.0"
description = "Python client for offshoot: branchable SQLite for agents"
requires-python = ">=3.10"
license = "Apache-2.0"

[tool.setuptools.packages.find]
include = ["offshoot*"]
```

`sdk/python/offshoot/client.py` — complete implementation contract:

```python
"""Thin client for the offshoot daemon's lifecycle API.

Wire protocol: newline-delimited JSON over a unix socket; one request, one
response, no pipelining (matches internal/daemon/protocol.go).
Stdlib only — no dependencies.
"""
from __future__ import annotations

import json
import socket
from dataclasses import dataclass, field
from datetime import timedelta


class OffshootError(Exception):
    """An error returned by the daemon."""


@dataclass
class Branch:
    branch: str
    head_txid: int
    protected: bool = False
    ttl: str = ""
    ttl_remaining: str = ""
    lease_holder: str = ""
    checkpoints: list[str] = field(default_factory=list)


def _ttl_str(ttl) -> str:
    if ttl is None:
        return ""
    if ttl == 0 or ttl == "none":
        return "none"
    if isinstance(ttl, timedelta):
        return f"{int(ttl.total_seconds())}s"
    return str(ttl)  # a Go duration string like "2h"


def connect(socket_path: str) -> "Client":
    return Client(socket_path)


class Client:
    def __init__(self, socket_path: str):
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.connect(socket_path)
        self._rfile = self._sock.makefile("rb")

    # -- context manager: __enter__ returns self, __exit__ calls close() --

    def _call(self, op: str, **fields) -> dict:
        req = {"op": op, **{k: v for k, v in fields.items() if v not in ("", None, False)}}
        self._sock.sendall(json.dumps(req).encode() + b"\n")
        line = self._rfile.readline()
        if not line:
            raise OffshootError("daemon closed the connection")
        resp = json.loads(line)
        if not resp.get("ok", False):
            raise OffshootError(resp.get("error", "unknown daemon error"))
        return resp

    # Lifecycle methods: each is a direct mapping onto one op, using the
    # exact field names from protocol.go (db, branch, name, from, ttl, force).
    # fork sends {"op":"fork","db":db,"branch":source,"name":new,
    #             "from":from_checkpoint or "", "ttl":_ttl_str(ttl)} and
    # returns resp["txid"]. touch's ttl uses _ttl_str. branches maps each
    # entry into Branch. open returns Session(self, resp["checkout"], db, branch).
    ...


class Session:
    def __init__(self, client: Client, path: str, db: str, branch: str):
        self._client, self.path, self._db, self._branch = client, path, db, branch

    def flush(self, name: str = "") -> int:
        resp = self._client._call("flush", db=self._db, branch=self._branch, name=name)
        return resp.get("txid", 0)

    def close(self) -> None:
        self._client._call("close", db=self._db, branch=self._branch)
```

The `...` marks the mechanical method bodies (create/open/checkout/fork/destroy/rollback/promote/touch/branches/status/close) — write every one; each is 2-5 lines following `_call`. `Client.close()` closes the socket. Note: `from` is a Python keyword, so the fork method's `_call` must pass it via a dict merge, e.g. `self._call("fork", db=db, branch=source, name=new, ttl=_ttl_str(ttl), **{"from": from_checkpoint or ""})`.

`sdk/python/tests/test_client.py` — full end-to-end against a real daemon:

```python
import os
import shutil
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import offshoot
from offshoot.client import OffshootError

REPO = Path(__file__).resolve().parents[3]


def build_binary(tmp: Path) -> Path:
    binpath = os.environ.get("OFFSHOOT_BIN")
    if binpath:
        return Path(binpath)
    out = tmp / "offshoot"
    subprocess.run(["go", "build", "-o", str(out), "./cmd/offshoot"],
                   cwd=REPO, check=True)
    return out


class DaemonFixture:
    """One daemon on a temp store+socket for the whole test class."""

    def __init__(self):
        self.dir = Path(tempfile.mkdtemp(prefix="offshoot-sdk-"))
        self.bin = build_binary(self.dir)
        self.sock = str(self.dir / "d.sock")
        self.proc = subprocess.Popen(
            [str(self.bin), "-store", str(self.dir / "store"), "serve",
             "-socket", self.sock],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        deadline = time.time() + 10
        while not os.path.exists(self.sock):
            if time.time() > deadline:
                raise RuntimeError("daemon did not start: " +
                                   self.proc.stderr.peek().decode(errors="replace"))
            if self.proc.poll() is not None:
                raise RuntimeError(self.proc.stderr.read().decode(errors="replace"))
            time.sleep(0.05)

    def stop(self):
        self.proc.terminate()
        self.proc.wait(timeout=10)
        shutil.rmtree(self.dir, ignore_errors=True)


class TestClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = DaemonFixture()

    @classmethod
    def tearDownClass(cls):
        cls.d.stop()

    def test_full_lifecycle(self):
        with offshoot.connect(self.d.sock) as c:
            c.create("app")
            s = c.open("app")
            db = sqlite3.connect(s.path)
            db.execute("CREATE TABLE t (v TEXT)")
            db.execute("INSERT INTO t VALUES ('one')")
            db.commit()
            txid = s.flush("v1")
            self.assertGreater(txid, 0)
            # Fork from the LIVE session — the row must be there.
            c.fork("app", "main", "try", ttl="1h")
            p = c.checkout("app", "try")
            rows = sqlite3.connect(p).execute("SELECT count(*) FROM t").fetchone()[0]
            self.assertEqual(rows, 1)
            # branches reflects TTL and checkpoints.
            info = {b.branch: b for b in c.branches("app")}
            self.assertIn("try", info)
            self.assertTrue(info["try"].ttl)
            self.assertIn("v1", info["main"].checkpoints)
            # rollback the fork? no session on it — write via at-rest is out of
            # scope; instead prove destroy guard + touch.
            with self.assertRaises(OffshootError):
                c.destroy("app", "main")  # protected by default
            c.touch("app", "try", ttl="none")
            info = {b.branch: b for b in c.branches("app")}
            self.assertEqual(info["try"].ttl, "")
            db.close()
            s.close()
            c.destroy("app", "try")
            self.assertNotIn("try", {b.branch for b in c.branches("app")})

    def test_errors_are_loud(self):
        with offshoot.connect(self.d.sock) as c:
            with self.assertRaises(OffshootError) as cm:
                c.checkout("nope", "main")
            self.assertTrue(str(cm.exception))
```

Makefile target:

```make
test-python-sdk:
	cd sdk/python && python3 -m unittest discover -s tests -v
```

- [ ] **Step 2: Run to verify failure shape**

Run: `make test-python-sdk`
Expected: FAIL until client.py's method bodies exist (import errors first, then assertion progress). Iterate.

- [ ] **Step 3: Implement the remaining method bodies and fix what the tests expose**

If a daemon-side gap surfaces (e.g. `create` op missing a field), fix the Go side too — Task 3's tests must be extended to cover whatever was missed, not just the Python side patched.

- [ ] **Step 4: Run**

Run: `make test-python-sdk && go test ./... -count=1 -race`
Expected: PASS both.

- [ ] **Step 5: Commit**

```bash
git add sdk/python Makefile
git commit -m "feat: Python SDK — stdlib-only client for the daemon lifecycle API"
```

---

### Task 5: TypeScript SDK

**Files:**
- Create: `sdk/typescript/package.json`, `sdk/typescript/tsconfig.json`, `sdk/typescript/src/client.ts`, `sdk/typescript/test/client.test.ts`
- Modify: `Makefile` (`test-ts-sdk` target)

**Interfaces:**
- Consumes: Task 3's wire protocol
- Produces: `connect(socketPath): Promise<Client>` with the same method surface as Python (`create/open/checkout/fork/destroy/rollback/promote/touch/branches/status/close`; `Session {path, flush(name?), close()}`; `OffshootError`). Method options use one options object: `fork(db, source, newBranch, {from, ttl})`, `destroy(db, branch, {force})`.

- [ ] **Step 1: Write the package and failing tests**

`sdk/typescript/package.json`:

```json
{
  "name": "@offshoot-db/client",
  "version": "0.1.0",
  "description": "TypeScript client for offshoot: branchable SQLite for agents",
  "license": "Apache-2.0",
  "type": "module",
  "main": "dist/client.js",
  "types": "dist/client.d.ts",
  "engines": { "node": ">=20" },
  "scripts": {
    "build": "tsc",
    "test": "tsc && node --test test-dist/test/"
  },
  "devDependencies": {
    "typescript": "^5.5.0",
    "@types/node": "^20.0.0"
  }
}
```

`tsconfig.json`: `strict: true`, `module`/`moduleResolution` `nodenext`, `target es2022`, `declaration: true`, `outDir test-dist`, `rootDir .`, include `src` and `test`; also emit `dist/` for publish via a second minimal config or `--outDir` override in `build` — keep it simple: `build` script becomes `tsc -p tsconfig.build.json` with a `tsconfig.build.json` that includes only `src` and outputs `dist`.

`src/client.ts` — complete implementation contract (write every method):

```typescript
// Thin client for the offshoot daemon's lifecycle API.
// Wire protocol: newline-delimited JSON over a unix socket; one request,
// one response, no pipelining. Zero runtime dependencies.
import { createConnection, type Socket } from "node:net";

export class OffshootError extends Error {}

export interface Branch {
  branch: string;
  head_txid: number;
  protected?: boolean;
  ttl?: string;
  ttl_remaining?: string;
  lease_holder?: string;
  checkpoints?: string[];
}

export async function connect(socketPath: string): Promise<Client> {
  const sock = createConnection(socketPath);
  await new Promise<void>((res, rej) => {
    sock.once("connect", () => res());
    sock.once("error", rej);
  });
  return new Client(sock);
}

export class Client {
  private buf = "";
  private queue: Array<{ res: (v: any) => void; rej: (e: Error) => void }> = [];

  constructor(private sock: Socket) {
    sock.setEncoding("utf8");
    sock.on("data", (chunk: string) => {
      this.buf += chunk;
      let i: number;
      while ((i = this.buf.indexOf("\n")) >= 0) {
        const line = this.buf.slice(0, i);
        this.buf = this.buf.slice(i + 1);
        const waiter = this.queue.shift();
        if (!waiter) continue;
        try {
          const resp = JSON.parse(line);
          if (!resp.ok) waiter.rej(new OffshootError(resp.error ?? "unknown daemon error"));
          else waiter.res(resp);
        } catch (e) {
          waiter.rej(e as Error);
        }
      }
    });
    sock.on("error", (e) => this.failAll(e));
    sock.on("close", () => this.failAll(new OffshootError("daemon closed the connection")));
  }

  private failAll(e: Error) {
    for (const w of this.queue.splice(0)) w.rej(e);
  }

  private call(op: string, fields: Record<string, unknown> = {}): Promise<any> {
    const req: Record<string, unknown> = { op };
    for (const [k, v] of Object.entries(fields)) {
      if (v !== undefined && v !== "" && v !== false) req[k] = v;
    }
    return new Promise((res, rej) => {
      this.queue.push({ res, rej });
      this.sock.write(JSON.stringify(req) + "\n");
    });
  }

  // create/open/checkout/fork/destroy/rollback/promote/touch/branches/status:
  // one method per op, exact wire field names (db, branch, name, from, ttl,
  // force). open returns new Session(this, resp.checkout, db, branch).
  // close() destroys the socket.
}

export class Session {
  constructor(
    private client: Client,
    public readonly path: string,
    private db: string,
    private branch: string,
  ) {}
  async flush(name = ""): Promise<number> {
    const r = await (this.client as any).call("flush", { db: this.db, branch: this.branch, name });
    return r.txid ?? 0;
  }
  async close(): Promise<void> {
    await (this.client as any).call("close", { db: this.db, branch: this.branch });
  }
}
```

(Resolve the `(this.client as any).call` wart properly: make `call` internal via a module-private symbol or just make it public-but-underscored `_call`. Pick one and be consistent.)

Important protocol note: the daemon answers strictly in order on one connection, so the queue-based correlation is sound — but do NOT allow interleaved `call`s to write while another request's response is pending if the daemon closes on error. The serialized-queue design above is acceptable because responses come back in request order; document this on the class.

`test/client.test.ts` — node:test end-to-end mirroring the Python suite: build the binary (`OFFSHOOT_BIN` env or `go build` via `child_process.execFileSync` from the repo root), start `serve` on a temp socket, then one big lifecycle test (create → open → write rows via the `sqlite3` CLI using `execFileSync` since the SDK has no DB driver → flush("v1") → fork with ttl → checkout → count rows via sqlite3 CLI → branches assertions → protected-destroy rejection → touch none → close → destroy) plus an errors-are-loud test. Skip cleanly (`t.skip`) if `go` or `sqlite3` are not on PATH.

Makefile:

```make
test-ts-sdk:
	cd sdk/typescript && npm install --no-audit --no-fund && npm test
```

- [ ] **Step 2: Run to verify failure, then implement the method bodies**

Run: `make test-ts-sdk` — iterate to green.

- [ ] **Step 3: Run everything**

Run: `make test-ts-sdk && make test-python-sdk && go test ./... -count=1 -race`
Expected: PASS. Add `sdk/typescript/node_modules/`, `sdk/typescript/dist/`, `sdk/typescript/test-dist/`, and Python `__pycache__/` to `.gitignore`.

- [ ] **Step 4: Commit**

```bash
git add sdk/typescript Makefile .gitignore
git commit -m "feat: TypeScript SDK — zero-dependency client for the daemon lifecycle API"
```

---

### Task 6: LangGraph companion — fork the world when the thread forks

**Files:**
- Create: `sdk/python/offshoot/langgraph.py`, `sdk/python/tests/test_langgraph.py`
- Create: `examples/langgraph-rewind/README.md`, `examples/langgraph-rewind/agent.py`

**Interfaces:**
- Consumes: Task 4's `Client`/`Session`
- Produces:

```python
class ThreadForks:
    """Maps agent thread ids to offshoot branches so rewinding a thread can
    rewind the database it wrote to."""
    def __init__(self, client, db, base_branch="main", ttl="24h", prefix="thread-"): ...
    def path(self, thread_id) -> str          # open (forking from base if new) and return the writable DB path
    def checkpoint(self, thread_id, checkpoint_id) -> int   # flush, named after the agent checkpoint
    def fork_thread(self, from_thread, at_checkpoint, new_thread) -> str  # fork the DB at that checkpoint; returns new path
    def close(self, thread_id=None)           # close one or all sessions
    def branch_for(self, thread_id) -> str    # deterministic sanitized branch name
```

Design rules (write these into the module docstring):
- One offshoot branch per thread (`prefix + sanitized(thread_id)`), forked from `base_branch` on first use, with a TTL so abandoned threads reap themselves — attempt branches are the workload TTLs exist for.
- `checkpoint(thread_id, checkpoint_id)` names the flush after the LangGraph checkpoint id, so `fork_thread(from_thread, at_checkpoint, new_thread)` can fork the database at exactly the state the conversation is rewound to.
- Sanitization: checkpoint/thread ids may contain characters `ValidateName` rejects; map to `[a-z0-9-]` with a short hash suffix on collision-prone truncation, deterministic. Names must stay within offshoot's validation rules — write a test for a UUID-shaped id and a nasty `../` id.
- This is a **companion**, not a `BaseCheckpointSaver` subclass: it does not persist LangGraph state itself. The README shows the 6-line integration (call `checkpoint()` after `graph.invoke/stream` steps; call `fork_thread()` when re-running from a historical checkpoint). Do not claim upstream integration.

- [ ] **Step 1: Write the failing tests**

`sdk/python/tests/test_langgraph.py` — uses the DaemonFixture from test_client.py (import it), no langgraph dependency:

```python
# TestThreadLifecycle: path("t1") forks from main and returns a writable
#   sqlite path; a row written there + checkpoint("t1", "ckpt-1"); a second
#   row + checkpoint("t1", "ckpt-2"); fork_thread("t1", "ckpt-1", "t2")
#   returns a path whose row count is 1 (the ckpt-1 world), while t1 still
#   has 2; the offshoot branch names are branch_for("t1")/branch_for("t2");
#   branches shows both carry the default TTL.
# TestNameSanitization: branch_for on a UUID id and on "../evil" both yield
#   names the daemon accepts (fork succeeds), are distinct, and are
#   deterministic (same input -> same name).
# TestCloseAll: close() closes every session; a subsequent destroy of the
#   thread branches succeeds (no open-session guard error).
```

Write these as complete unittest code in the same style as test_client.py.

- [ ] **Step 2: Run to verify they fail, implement, iterate to green**

Run: `make test-python-sdk`

- [ ] **Step 3: The example**

`examples/langgraph-rewind/agent.py`: a runnable script that needs ONLY the Python SDK and sqlite3 (no langgraph import): it simulates the agent loop explicitly — comments mark where LangGraph's `graph.stream(...)` calls would sit — writing orders into the DB across three "turns", checkpointing each, then rewinding to turn 1 and taking a different action on a forked thread, printing both world states. If `langgraph` IS importable, a `--real` flag runs the same flow through an actual `StateGraph` with a sqlite-writing tool node (guarded import; document `pip install langgraph` in the README). README leads with the positioning sentence: *LangGraph can rewind the conversation; it can't rewind the database the agent wrote to — this example rewinds both.* Every command in the README must be copy-paste runnable from the repo root (Plan 5's lesson: a reviewer will run it verbatim).

Add a smoke test at the end of `test_langgraph.py` that runs `agent.py` (without `--real`) via subprocess against the fixture daemon and asserts exit 0 and that its stdout shows the two diverged world states.

- [ ] **Step 4: Run everything**

Run: `make test-python-sdk && go test ./... -count=1 -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sdk/python examples/langgraph-rewind
git commit -m "feat: LangGraph companion — fork the database when the thread forks"
```

---

### Task 7: Documentation, wiring, and the adversarial pass

**Files:**
- Modify: `README.md`, `Makefile`
- Test: extend `internal/daemon/lifecycle_test.go`

**Interfaces:** none new.

- [ ] **Step 1: Adversarial tests for the seams this plan created**

Append to `internal/daemon/lifecycle_test.go` (complete code, following the file's existing patterns):

```go
// TestJanitorNeverReapsThisDaemonsOwnSession: open a session on a branch,
// set an already-expired TTL on it directly in the store, run the janitor
// fast (50ms) for ~20 cycles; the branch must survive (live lease) and the
// session must still flush successfully. Close the session; within 5s the
// janitor must reap it. This is the spec sentence "a branch with an active
// lease is never reaped — expiry defers until the lease is released" as a test.

// TestForkFromLiveSessionSeesLatestWrite: regression-style: write a row,
// do NOT flush, daemon-op fork the branch, checkout the child: row present
// (the flush-then-fork contract), and the SOURCE session still healthy
// (flush + status ok afterwards).
```

Run them, fix anything they expose (fix source, not tests), then run the full suite.

- [ ] **Step 2: README + Makefile wiring**

- README "Branches" / daemon section: TTL semantics in the spec's own words (measured from last durable write or lease renewal, whichever is later; `offshoot touch` resets; live lease defers; protected never reaped; no TTL = lives until destroyed), `fork -ttl 2h`, `touch`, and the janitor flags (`serve -reap-every 1m -gc-grace 15m`, `0` disables).
- README "Integration surface": Python and TypeScript quickstart blocks (each ≤ 15 lines, copy-paste runnable given a running daemon — show `offshoot serve &` first), and a pointer to `examples/langgraph-rewind/`.
- Makefile: `test-sdks: test-python-sdk test-ts-sdk` convenience target. Do NOT add SDK tests to the default `test` target (they need python3/node on PATH); document `make test-sdks` in the README's contributing/testing note instead.
- State honestly in the README what the SDKs are: thin clients that require a running daemon; CLI mode needs no daemon and no SDK.

- [ ] **Step 3: Full verification**

Run: `go test ./... -count=1 -race && go vet ./... && make test-sdks && make test-torture`
Expected: all green; torture numbers reported (capture engine untouched this plan, but flush.go changed in Task 1 — the run is the proof).

- [ ] **Step 4: Commit**

```bash
git add README.md Makefile internal/daemon
git commit -m "test: TTL/lease reap race under a live daemon; document TTLs and SDK quickstarts"
```

---

## Self-Review (performed at plan-writing time)

1. **Spec coverage:** TTL semantics implement the spec's § TTL semantics sentence-by-sentence (Task 2's tests quote it); `touch` is the spec's explicit reset; the janitor is the daemon's "TTL/GC loop"; SDKs are the spec's "thin clients over the lifecycle API" — the lifecycle API had to grow daemon ops first (Task 3), which the spec's daemon-mode description ("lifecycle API over unix socket") already promises; the LangGraph integration implements "a checkpointer companion: fork the database when the thread forks" as a companion, honestly not claiming upstream listing (that is a 90-day metric, not a deliverable). Deliberately out of scope, noted for the launch-prep decision: Prometheus metrics, single-token auth, HTTP binding (spec v1 items not in Plan 8's charter — flag to the user at plan review).
2. **Placeholder scan:** two deliberate elisions, both bounded with an explicit instruction and the complete pattern to follow: the mechanical Python/TS method bodies (each "2-5 lines following `_call`", with `fork`'s tricky keyword case spelled out) and three daemon/langgraph test skeletons whose full scaffolding pattern is named (server_test.go / test_client.py). No TBDs.
3. **Type consistency:** `Fork(db, src, new, at string, ttl time.Duration)` is used identically in Tasks 1, 2 (test callers), 3 (opFork); wire field names (`db, branch, name, from, ttl, force`) match between protocol.go (Task 3), client.py (Task 4), client.ts (Task 5); `reapDeadline` is defined in Task 2 and consumed (exported if needed) in Task 3's opBranches; `ThreadForks` consumes only Task 4's published surface.
