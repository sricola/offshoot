# offshoot Plan 5: Daemon Mode with Live Capture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `offshoot serve` — a long-running daemon that holds a branch under lease, captures every committed transaction from an agent writing to the checkout, and flushes durable snapshots to the store *without ever pausing the writer*.

**Architecture:** A session binds four things: a leased branch, a materialized checkout the agent writes to, a shadow replica kept in lockstep by Plan 1's capture engine, and a renewal loop. Because the replica is always at a transaction boundary, the daemon can encode *it* as an LTX snapshot and upload under the lease's epoch — the live checkout is never quiesced. Losing the lease is loud and terminal for the session: its epoch is dead, so it stops rather than writing into a fenced prefix.

**Tech Stack:** Go 1.24+, existing `internal/capture` (WAL capture engine, GO-verdict spike), `internal/replay` (page replayer), `internal/ltxio`, `internal/store`, `internal/ops`; stdlib `net` (unix socket) and `encoding/json`.

**Spec:** `docs/superpowers/specs/2026-07-29-offshoot-design.md` § Architecture (daemon mode), § WAL capture and the connection contract, § Security posture. **Plan sequence:** Plans 1-4 merged (capture spike GO; local lifecycle; S3 backends; leases + epoch fencing) → **this plan** → Plan 6 (incremental LTX segments, TTL reaping, MCP server, SDKs, launch demo).

## Global Constraints

- Module `github.com/sricola/offshoot`; Go 1.24+; cgo (mattn); Linux/macOS only
- **The connection contract from the spec is binding:** daemon and agent must share a kernel and a local POSIX filesystem; checkouts are WAL-mode SQLite files the daemon does not own connections to
- A session's writes go under its **lease epoch**; losing the lease ends the session loudly — never a silent retry, never a write under a dead epoch
- Flush must never quiesce or lock the agent's checkout; the agent writes throughout
- The daemon exposes a unix socket only, `0600`, under the user's runtime/cache dir; no TCP in this plan
- Every ref mutation stays CAS'd; a lost CAS is loud and retryable
- **Durability is explicit:** the API reports the txid a session is durable through; between flushes, writes are acknowledged by SQLite but not yet in the store, and nothing may claim otherwise
- Every Plan-2/3/4 test must keep passing unmodified; at-rest CLI behavior is unchanged when no daemon is running
- Tests must not depend on wall-clock sleeps for correctness — poll with deadlines
- Commit messages: conventional commits, ending with the repo's session trailers

## File Structure

```
internal/session/session.go      Session: lease + checkout + replica + capture engine
internal/session/session_test.go
internal/session/flush.go        Encode the replica as a snapshot, upload, CAS the ref forward
internal/session/flush_test.go
internal/session/renew.go        Lease renewal loop; fenced-shutdown handling
internal/session/renew_test.go
internal/daemon/protocol.go      Request/Response types shared by server and client
internal/daemon/server.go        Unix-socket server, session registry, graceful shutdown
internal/daemon/server_test.go
internal/daemon/client.go        Dial + call; socket discovery
internal/daemon/client_test.go
cmd/offshoot/main.go             (modify) `serve`, `session open|flush|status|close`; socket flag
README.md                        (modify) daemon quickstart and durability semantics
```

---

### Task 1: Session — lease, checkout, replica, capture

**Files:**
- Create: `internal/session/session.go`, `internal/session/session_test.go`

**Interfaces:**
- Consumes: `ops.Workspace` (Checkout, CheckoutPath, AcquireLease, ReleaseLease, Store), `store.Lease`, `capture.NewEngine(capture.Options{DBPath,StateDir,Sink,Poll})` with `Run(ctx) error`/`Rebased() int`, `capture.Sink` interface `{Rebase(snapshotPath string) error; Apply(pageSize uint32, frames []wal.Frame) error}`, `replay.New(path) *Replica` with `Rebase/Apply/Path`
- Produces:

```go
package session

// Session binds a leased branch to a live checkout, a shadow replica kept in
// lockstep by the capture engine, and the lease that authorizes its writes.
type Session struct { /* unexported */ }

type Options struct {
	WS         *ops.Workspace
	DB, Branch string
	Holder     string        // defaults to ops.LocalHolder()
	LeaseTTL   time.Duration // defaults to ops.DefaultLeaseTTL
	Dir        string        // scratch dir for the replica and capture state; defaults to a temp dir
}

// Open acquires the lease, materializes the checkout, seeds the replica from
// the branch head, and starts capturing. The returned Session runs until
// Close or until it loses its lease.
func Open(ctx context.Context, o Options) (*Session, error)

func (s *Session) CheckoutPath() string  // the file the agent writes to
func (s *Session) ReplicaPath() string   // the shadow the daemon owns
func (s *Session) DB() string
func (s *Session) Branch() string
func (s *Session) Lease() store.Lease

// Err returns the terminal error that ended the session (lease loss, capture
// failure), or nil while it is healthy.
func (s *Session) Err() error

// Close stops capture, releases the lease, and removes the scratch dir. It is
// safe to call twice.
func (s *Session) Close() error
```

The replica sink is `replay.Replica` behind a tiny adapter (the capture engine's `Sink` is satisfied by `Rebase`/`Apply`; Plan 1 used exactly this adapter in tests). Capture runs in a goroutine; a non-nil error from `Run` is recorded in `Err()` and ends the session.

- [ ] **Step 1: Write the failing test**

`internal/session/session_test.go`:

```go
package session

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
)

func newWS(t *testing.T) *ops.Workspace {
	t.Helper()
	w, err := ops.Init(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func requireSQLite(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
}

// waitFor polls until cond is true or the deadline passes.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestOpenHoldsLeaseAndCaptures(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if s.Lease().Holder == "" || s.Lease().Epoch < 2 {
		t.Fatalf("lease = %+v", s.Lease())
	}
	// The branch is leased in the store, not just in memory.
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != s.Lease().Holder {
		t.Fatalf("ref holder = %q, want %q", ref.LeaseHolder, s.Lease().Holder)
	}

	// An agent writes to the checkout with no coordination at all.
	if out, err := exec.Command("sqlite3", s.CheckoutPath(),
		"CREATE TABLE t (v); INSERT INTO t VALUES (1),(2);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	// The replica converges without the writer ever being paused.
	waitFor(t, 10*time.Second, "replica to converge", func() bool {
		out, err := exec.Command("sqlite3", s.ReplicaPath(), "SELECT count(*) FROM t;").Output()
		return err == nil && string(out) == "2\n"
	})
	if s.Err() != nil {
		t.Fatalf("session errored: %v", s.Err())
	}
}

func TestOpenRefusesLeasedBranch(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s1, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main", Holder: "one"})
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	if _, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main", Holder: "two"}); err == nil {
		t.Fatal("a second session on a leased branch must be refused")
	}
}

func TestCloseReleasesLeaseAndCleansUp(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	replica := s.ReplicaPath()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close must be idempotent: %v", err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != "" {
		t.Fatalf("Close must release the lease, holder = %q", ref.LeaseHolder)
	}
	if _, err := os.Stat(replica); !os.IsNotExist(err) {
		t.Fatalf("Close must remove the scratch replica, stat err = %v", err)
	}
	// The branch is immediately acquirable by someone else.
	if _, err := w.AcquireLease("app", "main", "next", ops.DefaultLeaseTTL); err != nil {
		t.Fatalf("branch not acquirable after Close: %v", err)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/session -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement**

`internal/session/session.go`:

```go
// Package session binds a leased branch to a live checkout, a shadow replica
// kept in lockstep by the WAL capture engine, and the lease that authorizes
// the daemon's writes. The agent writing to the checkout is never paused.
package session

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/sricola/offshoot/internal/capture"
	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/replay"
	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/wal"
)

type Options struct {
	WS         *ops.Workspace
	DB, Branch string
	Holder     string
	LeaseTTL   time.Duration
	Dir        string
}

// replicaSink adapts replay.Replica to the capture engine's Sink.
type replicaSink struct{ r *replay.Replica }

func (s replicaSink) Rebase(path string) error                  { return s.r.Rebase(path) }
func (s replicaSink) Apply(ps uint32, f []wal.Frame) error      { return s.r.Apply(ps, f) }

type Session struct {
	ws           *ops.Workspace
	db, branch   string
	dir          string
	ownsDir      bool
	checkoutPath string
	replica      *replay.Replica

	cancel   context.CancelFunc
	engDone  chan error
	captured *capture.Engine

	mu     sync.Mutex
	lease  store.Lease
	err    error
	closed bool
}

// Open acquires the lease, materializes the checkout, seeds the replica, and
// starts capturing.
func Open(ctx context.Context, o Options) (*Session, error) {
	if o.WS == nil {
		return nil, errors.New("session: workspace is required")
	}
	if o.Holder == "" {
		o.Holder = ops.LocalHolder()
	}
	if o.LeaseTTL == 0 {
		o.LeaseTTL = ops.DefaultLeaseTTL
	}
	dir, ownsDir := o.Dir, false
	if dir == "" {
		d, err := os.MkdirTemp("", "offshoot-session-*")
		if err != nil {
			return nil, err
		}
		dir, ownsDir = d, true
	}
	cleanup := func() {
		if ownsDir {
			os.RemoveAll(dir)
		}
	}

	lease, err := o.WS.AcquireLease(o.DB, o.Branch, o.Holder, o.LeaseTTL)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("session: acquire %s@%s: %w", o.DB, o.Branch, err)
	}

	checkoutPath, err := o.WS.Checkout(o.DB, o.Branch)
	if err != nil {
		o.WS.ReleaseLease(lease)
		cleanup()
		return nil, err
	}

	s := &Session{
		ws: o.WS, db: o.DB, branch: o.Branch, dir: dir, ownsDir: ownsDir,
		checkoutPath: checkoutPath,
		replica:      replay.New(filepath.Join(dir, "replica.db")),
		lease:        lease,
	}

	cctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.captured = capture.NewEngine(capture.Options{
		DBPath: checkoutPath, StateDir: dir, Sink: replicaSink{s.replica},
	})
	s.engDone = make(chan error, 1)
	go func() { s.engDone <- s.captured.Run(cctx) }()
	go s.watchEngine()
	return s, nil
}

// watchEngine records a terminal capture failure. A clean shutdown (Close)
// cancels the context and Run returns nil, which is not an error.
func (s *Session) watchEngine() {
	err := <-s.engDone
	if err == nil {
		return
	}
	s.fail(fmt.Errorf("session: capture stopped: %w", err))
}

// fail records the first terminal error and stops the session's work.
func (s *Session) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
	s.cancel()
}

func (s *Session) CheckoutPath() string { return s.checkoutPath }
func (s *Session) ReplicaPath() string  { return s.replica.Path() }
func (s *Session) DB() string           { return s.db }
func (s *Session) Branch() string       { return s.branch }

func (s *Session) Lease() store.Lease {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lease
}

func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close stops capture, releases the lease, and removes the scratch dir.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	lease := s.lease
	s.mu.Unlock()

	s.cancel()
	<-s.engDone

	var relErr error
	if err := s.ws.ReleaseLease(lease); err != nil && !errors.Is(err, store.ErrLeaseLost) {
		relErr = err
	}
	if s.ownsDir {
		os.RemoveAll(s.dir)
	}
	return relErr
}
```

(add `"path/filepath"` to the imports)

- [ ] **Step 4: Run**

Run: `go test ./internal/session -v -race -timeout 120s`
Expected: PASS ×3. If the replica never converges, check the capture engine's contract: it needs the checkout in WAL mode (Plan 2's `Checkout` materializes WAL-mode files) and its own `StateDir`.

- [ ] **Step 5: Full suite and commit**

Run: `go test ./... -count=1 && go vet ./...`

```bash
git add internal/session
git commit -m "feat: session binding a leased branch to a live checkout and shadow replica"
```

---

### Task 2: Durable flush without pausing the writer

**Files:**
- Create: `internal/session/flush.go`, `internal/session/flush_test.go`

**Interfaces:**
- Consumes: Task 1's `Session` internals, `ltxio.EncodeSnapshot(dbPath, txid, w)`, `store.SnapshotKey(lineage, epoch, txid)`, `store.Ref.SetCheckpoint`, `ops.Workspace.Store`
- Produces:

```go
package session

// ErrFenced reports that the session's lease was lost, so its epoch is dead
// and it must not write. The session is terminal once this is returned.
var ErrFenced = errors.New("session: fenced — lease lost")

// Flush uploads the replica's current state as a snapshot under the session's
// lease epoch and advances the branch head. name is optional: when non-empty
// the flushed state is also recorded as a named checkpoint. Returns the txid
// the branch is now durable through.
//
// Flush never touches the agent's checkout: it encodes the replica, which the
// capture engine keeps at a transaction boundary.
func (s *Session) Flush(name string) (uint64, error)

// DurableTXID is the txid the store is durable through for this session, or 0
// before the first flush.
func (s *Session) DurableTXID() uint64
```

Flush algorithm: refuse if `Err() != nil`; re-read the ref; if `ref.LeaseHolder != lease.Holder || ref.Epoch != lease.Epoch`, fail the session with `ErrFenced`; `txid = ref.HeadTXID + 1`; encode the replica (a plain file the daemon owns — no WAL, since `replay.Replica.Apply` writes pages directly and removes sidecars); create-only put at `SnapshotKey(ref.Lineage, lease.Epoch, txid)`, overwriting an orphan from a crashed prior attempt exactly as `ops.Checkpoint` does; set `ref.HeadTXID, ref.HeadEpoch = txid, lease.Epoch`, optionally `ref.SetCheckpoint(name, txid, lease.Epoch)`; CAS the ref; on `ErrCAS`, delete the uploaded object and return a loud retryable error.

- [ ] **Step 1: Write the failing test**

`internal/session/flush_test.go`:

```go
package session

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/store"
)

func TestFlushMakesWritesDurableWithoutPausingTheWriter(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if out, err := exec.Command("sqlite3", s.CheckoutPath(),
		"CREATE TABLE t (v); INSERT INTO t VALUES (1),(2),(3);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	waitFor(t, 10*time.Second, "capture", func() bool {
		out, err := exec.Command("sqlite3", s.ReplicaPath(), "SELECT count(*) FROM t;").Output()
		return err == nil && string(out) == "3\n"
	})

	// A writer holds the checkout open across the flush.
	writer := exec.Command("sqlite3", s.CheckoutPath(),
		"PRAGMA busy_timeout=5000; INSERT INTO t VALUES (4);")
	if out, err := writer.CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	txid, err := s.Flush("v1")
	if err != nil {
		t.Fatal(err)
	}
	if txid == 0 || s.DurableTXID() != txid {
		t.Fatalf("txid = %d, DurableTXID = %d", txid, s.DurableTXID())
	}

	// A fresh checkout of the branch from the store contains the flushed data.
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.HeadTXID != txid || ref.HeadEpoch != s.Lease().Epoch {
		t.Fatalf("ref head = (%d,%d), want (%d,%d)",
			ref.HeadTXID, ref.HeadEpoch, txid, s.Lease().Epoch)
	}
	if ref.Checkpoints["v1"].TXID != txid {
		t.Fatalf("checkpoint v1 = %+v", ref.Checkpoints["v1"])
	}

	// The agent's checkout is still usable immediately after the flush.
	if out, err := exec.Command("sqlite3", s.CheckoutPath(),
		"INSERT INTO t VALUES (5);").CombinedOutput(); err != nil {
		t.Fatalf("flush disturbed the writer: %v: %s", err, out)
	}
}

func TestFlushedStateIsRecoverableByAnotherWorkspace(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s.CheckoutPath(),
		"CREATE TABLE t (v); INSERT INTO t VALUES ('durable');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	waitFor(t, 10*time.Second, "capture", func() bool {
		out, err := exec.Command("sqlite3", s.ReplicaPath(), "SELECT count(*) FROM t;").Output()
		return err == nil && string(out) == "1\n"
	})
	if _, err := s.Flush(""); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Re-open the same store fresh and check the branch out: the data is there.
	w2, err := ops.Open(w.Spec)
	if err != nil {
		t.Fatal(err)
	}
	path, err := w2.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("sqlite3", path, "SELECT v FROM t;").Output()
	if err != nil || string(out) != "durable\n" {
		t.Fatalf("recovered content = %q err=%v", out, err)
	}
}

func TestFlushAfterFencingIsRefused(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main",
		Holder: "session-a", LeaseTTL: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Someone reclaims the expired lease, fencing this session.
	if _, err := w.AcquireLease("app", "main", "thief", ops.DefaultLeaseTTL); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Flush(""); !errors.Is(err, ErrFenced) {
		t.Fatalf("want ErrFenced, got %v", err)
	}
	if !errors.Is(s.Err(), ErrFenced) {
		t.Fatalf("fencing must be terminal for the session, Err = %v", s.Err())
	}
	// Nothing was written under the dead epoch that the branch references.
	ref, _, _ := w.Store.GetRef("app", "main")
	if ref.HeadEpoch == s.Lease().Epoch && ref.HeadTXID > 1 {
		t.Fatal("fenced session advanced the branch")
	}
	_ = store.Lease{}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/session -run TestFlush -v`
Expected: FAIL — `Flush` undefined.

- [ ] **Step 3: Implement**

`internal/session/flush.go`:

```go
package session

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/store"
)

// ErrFenced reports that the session's lease was lost: its epoch is dead, so
// it must not write. Fencing is terminal for a session.
var ErrFenced = errors.New("session: fenced — lease lost")

// Flush uploads the replica's current state as a snapshot under the session's
// lease epoch and advances the branch head. It never touches the agent's
// checkout — it encodes the replica, which capture keeps at a transaction
// boundary.
func (s *Session) Flush(name string) (uint64, error) {
	if err := s.Err(); err != nil {
		return 0, err
	}
	if name != "" {
		if err := store.ValidateName(name); err != nil {
			return 0, err
		}
	}
	lease := s.Lease()
	st := s.ws.Store

	ref, etag, err := st.GetRef(s.db, s.branch)
	if err != nil {
		return 0, err
	}
	if ref.LeaseHolder != lease.Holder || ref.Epoch != lease.Epoch {
		err := fmt.Errorf("%w: %s@%s now held by %q at epoch %d",
			ErrFenced, s.db, s.branch, ref.LeaseHolder, ref.Epoch)
		s.fail(err)
		return 0, err
	}
	if name != "" {
		if _, exists := ref.Checkpoints[name]; exists {
			return 0, fmt.Errorf("session: checkpoint %q already exists on %s@%s",
				name, s.db, s.branch)
		}
	}

	txid := ref.HeadTXID + 1
	var buf bytes.Buffer
	if err := ltxio.EncodeSnapshot(s.replica.Path(), txid, &buf); err != nil {
		return 0, fmt.Errorf("session: encode replica: %w", err)
	}

	snapKey := store.SnapshotKey(ref.Lineage, lease.Epoch, txid)
	if _, err := st.B.PutIf(snapKey, buf.Bytes(), ""); err != nil {
		if !errors.Is(err, store.ErrCAS) {
			return 0, fmt.Errorf("session: upload snapshot: %w", err)
		}
		// The only thing that can already occupy this key is an orphan from a
		// crashed prior attempt: HeadTXID only advances on a successful ref
		// write, so nothing references txid beyond head yet.
		if err := st.B.Put(snapKey, buf.Bytes()); err != nil {
			return 0, fmt.Errorf("session: overwrite orphaned snapshot: %w", err)
		}
	}

	ref.HeadTXID, ref.HeadEpoch = txid, lease.Epoch
	if name != "" {
		ref.SetCheckpoint(name, txid, lease.Epoch)
	}
	if _, err := st.PutRef(s.db, s.branch, ref, etag); err != nil {
		if delErr := st.B.Delete(snapKey); delErr != nil {
			return 0, fmt.Errorf("session: ref update failed (%v) and cleanup failed: %w", err, delErr)
		}
		if errors.Is(err, store.ErrCAS) {
			return 0, fmt.Errorf("session: flush lost a race (retry): %w", err)
		}
		return 0, fmt.Errorf("session: advance ref: %w", err)
	}

	s.mu.Lock()
	s.durable = txid
	s.mu.Unlock()
	return txid, nil
}

// DurableTXID is the txid the store is durable through for this session.
func (s *Session) DurableTXID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.durable
}
```

Add `durable uint64` to the `Session` struct in `session.go`.

- [ ] **Step 4: Run**

Run: `go test ./internal/session -v -race -timeout 180s && go test ./... -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/session
git commit -m "feat: flush the replica as a durable snapshot without pausing the writer"
```

---

### Task 3: Lease renewal and fenced shutdown

**Files:**
- Create: `internal/session/renew.go`, `internal/session/renew_test.go`
- Modify: `internal/session/session.go` (start the loop in `Open`, stop it in `Close`)

**Interfaces:**
- Consumes: Task 1-2 internals, `ops.Workspace.RenewLease`, `store.ErrLeaseLost`
- Produces: `Options.RenewEvery time.Duration` (defaults to `LeaseTTL/3`); an internal renewal goroutine that keeps `s.lease` fresh and calls `s.fail(ErrFenced)` on `store.ErrLeaseLost`

A session that cannot renew is fenced: its epoch is dead. The loop must record that terminally so `Flush` refuses, rather than letting the daemon keep serving a session whose writes would land in a superseded prefix.

- [ ] **Step 1: Write the failing test**

`internal/session/renew_test.go`:

```go
package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
)

func TestRenewKeepsLeaseAliveBeyondTTL(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main",
		LeaseTTL: 300 * time.Millisecond, RenewEvery: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Well past the original TTL, the lease is still live and ours.
	deadline := time.Now().Add(900 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if err := s.Err(); err != nil {
		t.Fatalf("session died despite renewal: %v", err)
	}
	infos, err := w.Leases()
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].Expired {
		t.Fatalf("lease not kept alive: %+v", infos)
	}
	if infos[0].Holder != s.Lease().Holder {
		t.Fatalf("holder = %q, want %q", infos[0].Holder, s.Lease().Holder)
	}
}

func TestRenewalDetectsFencingAndEndsSession(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", Holder: "session-a",
		LeaseTTL: 100 * time.Millisecond, RenewEvery: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Wait out the TTL, then steal the branch.
	time.Sleep(150 * time.Millisecond)
	if _, err := w.AcquireLease("app", "main", "thief", ops.DefaultLeaseTTL); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "session to notice it was fenced", func() bool {
		return errors.Is(s.Err(), ErrFenced)
	})
	if _, err := s.Flush(""); !errors.Is(err, ErrFenced) {
		t.Fatalf("a fenced session must refuse to flush, got %v", err)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/session -run TestRenew -v`
Expected: FAIL — `RenewEvery` is not a field of `Options`.

- [ ] **Step 3: Implement**

`internal/session/renew.go`:

```go
package session

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sricola/offshoot/internal/store"
)

// renewLoop keeps the session's lease fresh. Losing the lease is terminal:
// the session's epoch is dead, so anything it wrote afterwards would land in
// a fenced prefix — it must stop rather than keep serving.
func (s *Session) renewLoop(ctx context.Context, every, ttl time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		l, err := s.ws.RenewLease(s.Lease(), ttl)
		if err != nil {
			if errors.Is(err, store.ErrLeaseLost) {
				s.fail(fmt.Errorf("%w: %v", ErrFenced, err))
				return
			}
			// A transient store error is not fencing; try again next tick.
			continue
		}
		s.mu.Lock()
		s.lease = l
		s.mu.Unlock()
	}
}
```

In `session.go`, add `RenewEvery time.Duration` to `Options`, default it in `Open`:

```go
	if o.RenewEvery == 0 {
		o.RenewEvery = o.LeaseTTL / 3
		if o.RenewEvery <= 0 {
			o.RenewEvery = time.Second
		}
	}
```

and start the loop next to the capture goroutine:

```go
	go s.renewLoop(cctx, o.RenewEvery, o.LeaseTTL)
```

- [ ] **Step 4: Run**

Run: `go test ./internal/session -v -race -count=2 -timeout 180s && go test ./... -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/session
git commit -m "feat: lease renewal loop that ends a session loudly when fenced"
```

---

### Task 4: Daemon server over a unix socket

**Files:**
- Create: `internal/daemon/protocol.go`, `internal/daemon/server.go`, `internal/daemon/server_test.go`

**Interfaces:**
- Consumes: `session.Open/Options/Session`, `ops.Workspace`
- Produces:

```go
package daemon

// Request and Response are newline-delimited JSON on a unix socket.
type Request struct {
	Op     string `json:"op"`     // "open" | "flush" | "status" | "close" | "shutdown"
	DB     string `json:"db,omitempty"`
	Branch string `json:"branch,omitempty"`
	Name   string `json:"name,omitempty"` // checkpoint name for flush
}

type Response struct {
	OK       bool          `json:"ok"`
	Error    string        `json:"error,omitempty"`
	Checkout string        `json:"checkout,omitempty"`
	TXID     uint64        `json:"txid,omitempty"`
	Sessions []SessionInfo `json:"sessions,omitempty"`
}

type SessionInfo struct {
	DB, Branch  string `json:"db"`
	Checkout    string `json:"checkout"`
	Holder      string `json:"holder"`
	Epoch       uint64 `json:"epoch"`
	DurableTXID uint64 `json:"durable_txid"`
	Error       string `json:"error,omitempty"`
}

type Server struct { /* unexported */ }

// NewServer listens on socketPath (created 0600) and serves sessions backed
// by ws. Call Serve to run, Shutdown to stop.
func NewServer(ws *ops.Workspace, socketPath string) (*Server, error)
func (s *Server) Serve() error                  // blocks until Shutdown
func (s *Server) Shutdown(ctx context.Context) error // closes sessions, releases leases, removes the socket
func (s *Server) SocketPath() string
```

Semantics: `open` returns the checkout path for an agent to use, or an error if the branch is already open here or leased elsewhere; `flush` flushes one session; `status` lists all; `close` ends one session (releasing its lease); `shutdown` stops the daemon. Every response is a single JSON line. One connection may issue many requests. Shutdown closes every session so no lease is orphaned.

- [ ] **Step 1: Write the failing test**

`internal/daemon/server_test.go`:

```go
package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
)

func newServer(t *testing.T) (*Server, *ops.Workspace) {
	t.Helper()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	w, err := ops.Init(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(w, filepath.Join(dir, "sock"))
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})
	return srv, w
}

// call sends one request on a fresh connection and returns the response.
func call(t *testing.T, sock string, req Request) Response {
	t.Helper()
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := json.NewEncoder(c).Encode(req); err != nil {
		t.Fatal(err)
	}
	var resp Response
	if err := json.NewDecoder(bufio.NewReader(c)).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestServerOpenFlushStatusClose(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	fi, err := os.Stat(sock)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("socket perm = %o, want 600", perm)
	}

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK || open.Checkout == "" {
		t.Fatalf("open = %+v", open)
	}
	if out, err := exec.Command("sqlite3", open.Checkout,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	// Flush may race capture; retry briefly until the row is durable.
	var txid uint64
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main", Name: ""})
		if resp.OK {
			txid = resp.TXID
			ref, _, err := w.Store.GetRef("app", "main")
			if err != nil {
				t.Fatal(err)
			}
			if ref.HeadTXID == txid {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if txid == 0 {
		t.Fatal("flush never succeeded")
	}

	st := call(t, sock, Request{Op: "status"})
	if !st.OK || len(st.Sessions) != 1 {
		t.Fatalf("status = %+v", st)
	}
	if st.Sessions[0].DB != "app" || st.Sessions[0].DurableTXID != txid {
		t.Fatalf("session info = %+v", st.Sessions[0])
	}

	cl := call(t, sock, Request{Op: "close", DB: "app", Branch: "main"})
	if !cl.OK {
		t.Fatalf("close = %+v", cl)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != "" {
		t.Fatalf("close must release the lease, holder = %q", ref.LeaseHolder)
	}
}

func TestServerRefusesDoubleOpen(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("first open = %+v", r)
	}
	r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if r.OK {
		t.Fatal("second open of the same branch must be refused")
	}
	if r.Error == "" {
		t.Fatal("a refusal must carry an error message")
	}
}

func TestShutdownReleasesEveryLease(t *testing.T) {
	srv, w := newServer(t)
	if r := call(t, srv.SocketPath(), Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open = %+v", r)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != "" {
		t.Fatalf("shutdown must release leases, holder = %q", ref.LeaseHolder)
	}
	if _, err := os.Stat(srv.SocketPath()); !os.IsNotExist(err) {
		t.Fatalf("shutdown must remove the socket, stat err = %v", err)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/daemon -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement**

`internal/daemon/protocol.go` — exactly the types in the Produces block above, with a package doc comment describing the newline-delimited JSON protocol.

`internal/daemon/server.go`:

```go
// Package daemon serves offshoot sessions over a unix socket: a long-running
// process holds branch leases, captures WAL continuously, and flushes durable
// snapshots while agents write to their checkouts.
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"sync"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/session"
)

type Server struct {
	ws   *ops.Workspace
	ln   net.Listener
	sock string

	mu       sync.Mutex
	sessions map[string]*session.Session // "db@branch"
	closing  bool
}

func key(db, branch string) string { return db + "@" + branch }

func NewServer(ws *ops.Workspace, socketPath string) (*Server, error) {
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("daemon: listen %s: %w", socketPath, err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		ln.Close()
		return nil, err
	}
	return &Server{ws: ws, ln: ln, sock: socketPath,
		sessions: map[string]*session.Session{}}, nil
}

func (s *Server) SocketPath() string { return s.sock }

func (s *Server) Serve() error {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			s.mu.Lock()
			closing := s.closing
			s.mu.Unlock()
			if closing {
				return nil
			}
			return err
		}
		go s.handle(c)
	}
}

func (s *Server) handle(c net.Conn) {
	defer c.Close()
	dec := json.NewDecoder(c)
	enc := json.NewEncoder(c)
	for {
		var req Request
		if err := dec.Decode(&req); err != nil {
			return // client hung up or sent garbage
		}
		resp := s.dispatch(req)
		if err := enc.Encode(resp); err != nil {
			return
		}
	}
}

func (s *Server) dispatch(req Request) Response {
	switch req.Op {
	case "open":
		return s.opOpen(req)
	case "flush":
		return s.opFlush(req)
	case "status":
		return s.opStatus()
	case "close":
		return s.opClose(req)
	case "shutdown":
		go s.Shutdown(context.Background())
		return Response{OK: true}
	default:
		return errResp(fmt.Errorf("daemon: unknown op %q", req.Op))
	}
}

func errResp(err error) Response { return Response{Error: err.Error()} }

func (s *Server) opOpen(req Request) Response {
	branch := req.Branch
	if branch == "" {
		branch = "main"
	}
	s.mu.Lock()
	if _, exists := s.sessions[key(req.DB, branch)]; exists {
		s.mu.Unlock()
		return errResp(fmt.Errorf("daemon: %s is already open here", key(req.DB, branch)))
	}
	s.mu.Unlock()

	sess, err := session.Open(context.Background(), session.Options{
		WS: s.ws, DB: req.DB, Branch: branch,
	})
	if err != nil {
		return errResp(err)
	}
	s.mu.Lock()
	// Re-check under the lock: a concurrent open may have won the race.
	if _, exists := s.sessions[key(req.DB, branch)]; exists {
		s.mu.Unlock()
		sess.Close()
		return errResp(fmt.Errorf("daemon: %s is already open here", key(req.DB, branch)))
	}
	s.sessions[key(req.DB, branch)] = sess
	s.mu.Unlock()
	return Response{OK: true, Checkout: sess.CheckoutPath()}
}

func (s *Server) lookup(db, branch string) (*session.Session, error) {
	if branch == "" {
		branch = "main"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[key(db, branch)]
	if !ok {
		return nil, fmt.Errorf("daemon: %s is not open", key(db, branch))
	}
	return sess, nil
}

func (s *Server) opFlush(req Request) Response {
	sess, err := s.lookup(req.DB, req.Branch)
	if err != nil {
		return errResp(err)
	}
	txid, err := sess.Flush(req.Name)
	if err != nil {
		return errResp(err)
	}
	return Response{OK: true, TXID: txid}
}

func (s *Server) opStatus() Response {
	s.mu.Lock()
	keys := make([]string, 0, len(s.sessions))
	for k := range s.sessions {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	infos := make([]SessionInfo, 0, len(keys))
	for _, k := range keys {
		sess := s.sessions[k]
		info := SessionInfo{
			DB: sess.DB(), Branch: sess.Branch(), Checkout: sess.CheckoutPath(),
			Holder: sess.Lease().Holder, Epoch: sess.Lease().Epoch,
			DurableTXID: sess.DurableTXID(),
		}
		if err := sess.Err(); err != nil {
			info.Error = err.Error()
		}
		infos = append(infos, info)
	}
	s.mu.Unlock()
	return Response{OK: true, Sessions: infos}
}

func (s *Server) opClose(req Request) Response {
	branch := req.Branch
	if branch == "" {
		branch = "main"
	}
	s.mu.Lock()
	sess, ok := s.sessions[key(req.DB, branch)]
	if ok {
		delete(s.sessions, key(req.DB, branch))
	}
	s.mu.Unlock()
	if !ok {
		return errResp(fmt.Errorf("daemon: %s is not open", key(req.DB, branch)))
	}
	if err := sess.Close(); err != nil {
		return errResp(err)
	}
	return Response{OK: true}
}

// Shutdown stops accepting, closes every session so no lease is orphaned, and
// removes the socket. It is safe to call twice.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	sessions := make([]*session.Session, 0, len(s.sessions))
	for k, sess := range s.sessions {
		sessions = append(sessions, sess)
		delete(s.sessions, k)
	}
	s.mu.Unlock()

	s.ln.Close()
	var firstErr error
	for _, sess := range sessions {
		if err := sess.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := os.Remove(s.sock); err != nil && !os.IsNotExist(err) && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

var _ = errors.Is // keep errors imported for future use
```

Remove the trailing `var _ = errors.Is` line and the `errors` import if nothing else uses them.

- [ ] **Step 4: Run**

Run: `go test ./internal/daemon -v -race -timeout 180s && go test ./... -count=1`
Expected: PASS ×3.

- [ ] **Step 5: Commit**

```bash
git add internal/daemon
git commit -m "feat: daemon server with session registry over a unix socket"
```

---

### Task 5: Client, `offshoot serve`, and the session subcommands

**Files:**
- Create: `internal/daemon/client.go`, `internal/daemon/client_test.go`
- Modify: `cmd/offshoot/main.go`, `README.md`

**Interfaces:**
- Consumes: `Request`, `Response` (Task 4)
- Produces:

```go
package daemon

// DefaultSocketPath returns the conventional socket path for a store spec:
// <user cache dir>/offshoot/<sha256(spec)[:16]>.sock, or OFFSHOOT_SOCKET when set.
func DefaultSocketPath(spec string) (string, error)

// Call sends one request to the daemon at socketPath and returns its response.
// It reports a clear error when no daemon is listening.
func Call(socketPath string, req Request) (Response, error)

// Running reports whether a daemon is accepting connections at socketPath.
func Running(socketPath string) bool
```

CLI additions: `offshoot serve [-socket PATH]` runs the daemon until SIGINT/SIGTERM, then shuts down gracefully (releasing leases); `offshoot session open <db>[@branch]` prints the checkout path; `offshoot session flush <db>[@branch] [name]` prints the durable txid; `offshoot session status`; `offshoot session close <db>[@branch]`; `offshoot session shutdown`. All of the session subcommands talk to the daemon and fail with a clear "no daemon is running at <path>; start one with `offshoot serve`" when the socket is absent.

- [ ] **Step 1: Write the failing test**

`internal/daemon/client_test.go`:

```go
package daemon

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultSocketPathIsStableAndPerStore(t *testing.T) {
	a1, err := DefaultSocketPath("s3://bucket/p")
	if err != nil {
		t.Fatal(err)
	}
	a2, _ := DefaultSocketPath("s3://bucket/p")
	b, _ := DefaultSocketPath("s3://bucket/other")
	if a1 != a2 {
		t.Fatalf("unstable: %s vs %s", a1, a2)
	}
	if a1 == b {
		t.Fatal("different stores must get different sockets")
	}
	if !strings.HasSuffix(a1, ".sock") {
		t.Fatalf("socket path = %s", a1)
	}
}

func TestSocketPathHonorsEnv(t *testing.T) {
	t.Setenv("OFFSHOOT_SOCKET", "/tmp/custom.sock")
	p, err := DefaultSocketPath("anything")
	if err != nil || p != "/tmp/custom.sock" {
		t.Fatalf("p=%s err=%v", p, err)
	}
}

func TestCallWithoutDaemonIsClear(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "absent.sock")
	if Running(sock) {
		t.Fatal("Running must be false with no listener")
	}
	_, err := Call(sock, Request{Op: "status"})
	if err == nil {
		t.Fatal("Call must fail when no daemon is listening")
	}
	if !strings.Contains(err.Error(), "no daemon") {
		t.Fatalf("error should name the missing daemon: %v", err)
	}
}

func TestCallRoundTripsAgainstAServer(t *testing.T) {
	srv, _ := newServer(t) // from server_test.go, same package
	resp, err := Call(srv.SocketPath(), Request{Op: "status"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("resp = %+v", resp)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/daemon -run 'Socket|Call' -v`
Expected: FAIL — `DefaultSocketPath` undefined.

- [ ] **Step 3: Implement the client**

`internal/daemon/client.go`:

```go
package daemon

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"
)

// DefaultSocketPath returns the conventional socket path for a store spec.
func DefaultSocketPath(spec string) (string, error) {
	if p := os.Getenv("OFFSHOOT_SOCKET"); p != "" {
		return p, nil
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("daemon: no cache dir for the socket (set OFFSHOOT_SOCKET): %w", err)
	}
	sum := sha256.Sum256([]byte(spec))
	dir := filepath.Join(cache, "offshoot")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	return filepath.Join(dir, hex.EncodeToString(sum[:8])+".sock"), nil
}

// Running reports whether a daemon is accepting connections at socketPath.
func Running(socketPath string) bool {
	c, err := net.DialTimeout("unix", socketPath, 500*time.Millisecond)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

// Call sends one request to the daemon and returns its response.
func Call(socketPath string, req Request) (Response, error) {
	c, err := net.DialTimeout("unix", socketPath, 2*time.Second)
	if err != nil {
		return Response{}, fmt.Errorf(
			"daemon: no daemon is running at %s; start one with `offshoot serve`: %w",
			socketPath, err)
	}
	defer c.Close()
	if err := json.NewEncoder(c).Encode(req); err != nil {
		return Response{}, err
	}
	var resp Response
	if err := json.NewDecoder(c).Decode(&resp); err != nil {
		return Response{}, fmt.Errorf("daemon: reading response: %w", err)
	}
	if !resp.OK && resp.Error != "" {
		return resp, fmt.Errorf("daemon: %s", resp.Error)
	}
	return resp, nil
}
```

- [ ] **Step 4: Wire the CLI**

In `cmd/offshoot/main.go` add two cases. `serve`:

```go
	case "serve":
		sock := ""
		if len(rest) == 2 && rest[0] == "-socket" {
			sock = rest[1]
		} else if len(rest) != 0 {
			return fmt.Errorf("usage: offshoot serve [-socket PATH]")
		}
		if sock == "" {
			p, err := daemon.DefaultSocketPath(root)
			if err != nil {
				return err
			}
			sock = p
		}
		srv, err := daemon.NewServer(w, sock)
		if err != nil {
			return err
		}
		fmt.Println("offshoot serving on", sock)
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
		errc := make(chan error, 1)
		go func() { errc <- srv.Serve() }()
		select {
		case <-sigc:
			fmt.Println("offshoot: shutting down, releasing leases")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			return srv.Shutdown(ctx)
		case err := <-errc:
			return err
		}
```

and `session`:

```go
	case "session":
		if len(rest) == 0 {
			return fmt.Errorf("usage: offshoot session open|flush|status|close|shutdown ...")
		}
		sock, err := daemon.DefaultSocketPath(root)
		if err != nil {
			return err
		}
		sub, args := rest[0], rest[1:]
		target := func() (string, string, error) {
			if len(args) < 1 {
				return "", "", fmt.Errorf("usage: offshoot session %s <db>[@branch]", sub)
			}
			return ops.ParseTarget(args[0])
		}
		switch sub {
		case "open":
			db, branch, err := target()
			if err != nil {
				return err
			}
			resp, err := daemon.Call(sock, daemon.Request{Op: "open", DB: db, Branch: branch})
			if err != nil {
				return err
			}
			fmt.Println(resp.Checkout)
			return nil
		case "flush":
			db, branch, err := target()
			if err != nil {
				return err
			}
			name := ""
			if len(args) == 2 {
				name = args[1]
			}
			resp, err := daemon.Call(sock, daemon.Request{Op: "flush", DB: db, Branch: branch, Name: name})
			if err != nil {
				return err
			}
			fmt.Printf("durable through txid %d\n", resp.TXID)
			return nil
		case "status":
			resp, err := daemon.Call(sock, daemon.Request{Op: "status"})
			if err != nil {
				return err
			}
			for _, in := range resp.Sessions {
				line := fmt.Sprintf("%s@%s durable=%d epoch=%d holder=%s checkout=%s",
					in.DB, in.Branch, in.DurableTXID, in.Epoch, in.Holder, in.Checkout)
				if in.Error != "" {
					line += " ERROR=" + in.Error
				}
				fmt.Println(line)
			}
			return nil
		case "close":
			db, branch, err := target()
			if err != nil {
				return err
			}
			_, err = daemon.Call(sock, daemon.Request{Op: "close", DB: db, Branch: branch})
			return err
		case "shutdown":
			_, err := daemon.Call(sock, daemon.Request{Op: "shutdown"})
			return err
		default:
			return fmt.Errorf("unknown session subcommand %q", sub)
		}
```

(add imports `context`, `os/signal`, `syscall`, and the `internal/daemon` package; add usage lines for `serve` and the five `session` forms; note `root` here is the store spec variable the file already computes)

- [ ] **Step 5: Document**

Add to `README.md` after the leases section:

```markdown
## Daemon mode

At rest, every offshoot command opens the store, does its work, and exits — so
a checkpoint has to quiesce the database. The daemon removes that constraint:
it holds the branch under lease and captures every committed transaction while
your agent keeps writing.

    offshoot serve &                      # holds leases, captures continuously
    offshoot session open app             # prints the checkout path to write to
    sqlite3 "$(offshoot session open app)" "INSERT INTO t VALUES ('agent wrote this');"
    offshoot session flush app v1          # durable in the store, writer never paused
    offshoot session status                # durable txid per session
    offshoot session close app             # releases the lease

**Durability is explicit.** Between flushes, writes are committed to SQLite but
not yet in the store; `session status` reports the txid each session is durable
through. A session that loses its lease is fenced and stops — it will not write
under a dead epoch — and `session status` shows the error.

The daemon serves a unix socket (mode 0600) under your cache directory, one per
store; override with `OFFSHOOT_SOCKET`. Daemon and agent must share a kernel
and a local filesystem: the checkout is a real SQLite file both processes open.
```

- [ ] **Step 6: Run and hand-drive**

Run: `go test ./... -count=1 -race && go vet ./... && go build ./cmd/offshoot`

Then drive it by hand and paste the transcript into your report:

```bash
export OFFSHOOT_STORE=$(mktemp -d)/store OFFSHOOT_SOCKET=$(mktemp -d)/sock
./offshoot init && ./offshoot create app
./offshoot serve & sleep 1
P=$(./offshoot session open app)
sqlite3 "$P" "CREATE TABLE t (v); INSERT INTO t VALUES ('hello');"
sleep 1
./offshoot session flush app v1
./offshoot session status
./offshoot session close app
./offshoot session shutdown
```

- [ ] **Step 7: Commit**

```bash
git add internal/daemon cmd/offshoot README.md
git commit -m "feat: daemon client, offshoot serve, and session subcommands"
```

---

### Task 6: Crash recovery — an abandoned session's branch is reclaimable

**Files:**
- Create: `internal/daemon/recovery_test.go`
- Modify: source only if the tests expose a gap

**Interfaces:** none new. This task proves that a daemon killed without shutdown leaves the store in a state a new daemon can take over, with no data loss beyond the documented flush window.

- [ ] **Step 1: Write the tests**

`internal/daemon/recovery_test.go`:

```go
package daemon

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/session"
)

// TestAbandonedLeaseIsReclaimableAfterExpiry simulates a daemon killed without
// shutdown: its lease is never released, but it expires and a new daemon takes
// the branch over, fencing the dead one.
func TestAbandonedLeaseIsReclaimableAfterExpiry(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	w, err := ops.Init(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}

	// Session A takes a very short lease and is then abandoned (no Close).
	a, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", Holder: "daemon-a",
		LeaseTTL: 50 * time.Millisecond, RenewEvery: time.Hour, // never renews
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	// A new daemon reclaims the expired lease.
	b, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", Holder: "daemon-b",
	})
	if err != nil {
		t.Fatalf("expired lease must be reclaimable: %v", err)
	}
	defer b.Close()
	if b.Lease().Epoch <= a.Lease().Epoch {
		t.Fatalf("reclaim must bump the epoch: %d -> %d", a.Lease().Epoch, b.Lease().Epoch)
	}
	// The abandoned session cannot write.
	if _, err := a.Flush(""); err == nil {
		t.Fatal("an abandoned, fenced session must not be able to flush")
	}
	a.Close()
}

// TestFlushedDataSurvivesDaemonRestart proves the durability contract: data
// flushed before a hard stop is present after a new daemon opens the branch.
func TestFlushedDataSurvivesDaemonRestart(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	if out, err := exec.Command("sqlite3", open.Checkout,
		"CREATE TABLE t (v); INSERT INTO t VALUES ('flushed');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	deadline := time.Now().Add(10 * time.Second)
	var flushed bool
	for time.Now().Before(deadline) {
		if r := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main"}); r.OK {
			ref, _, err := w.Store.GetRef("app", "main")
			if err != nil {
				t.Fatal(err)
			}
			if ref.HeadTXID == r.TXID {
				flushed = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !flushed {
		t.Fatal("flush never succeeded")
	}

	// Hard stop: shut the daemon down and stand a new one up on the same store.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	srv2, err := NewServer(w, sock)
	if err != nil {
		t.Fatal(err)
	}
	go srv2.Serve()
	defer func() {
		c2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel2()
		srv2.Shutdown(c2)
	}()

	open2 := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open2.OK {
		t.Fatalf("reopen after restart = %+v", open2)
	}
	out, err := exec.Command("sqlite3", open2.Checkout, "SELECT v FROM t;").Output()
	if err != nil || string(out) != "flushed\n" {
		t.Fatalf("flushed data lost across restart: %q err=%v", out, err)
	}
}
```

- [ ] **Step 2: Run and investigate**

Run: `go test ./internal/daemon -run 'Abandon|Restart' -v -race -count=2 -timeout 300s`
Expected: PASS. A failure is a real finding — likely candidates: `session.Open` not reclaiming an expired lease, or `Checkout` refusing to re-materialize over the previous daemon's checkout file. Fix the source and document what you found.

- [ ] **Step 3: Full suite and commit**

Run: `go test ./... -count=1 -race && go vet ./...`

```bash
git add internal/daemon internal/session
git commit -m "test: abandoned-session reclaim and flushed-data survival across restart"
```

---

### Task 7: Adversarial pass — the agent never stops writing

**Files:**
- Create: `internal/session/stress_test.go`
- Modify: source only if the tests expose a gap

**Interfaces:** none new. This is the end-to-end claim the product rests on: an agent writes continuously while the daemon flushes repeatedly, and every flushed state is a real, consistent database.

- [ ] **Step 1: Write the stress test**

`internal/session/stress_test.go`:

```go
package session

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/store"
)

// TestFlushesUnderContinuousWritesAreConsistent runs an agent writing in a
// loop while the daemon flushes repeatedly. Every flushed snapshot must be a
// valid SQLite database whose row count is monotonically non-decreasing — a
// torn or half-applied flush would show up as a decode failure or a count that
// goes backwards.
func TestFlushesUnderContinuousWritesAreConsistent(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if out, err := exec.Command("sqlite3", s.CheckoutPath(),
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			exec.Command("sqlite3", s.CheckoutPath(),
				"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").Run()
		}
	}()

	dir := t.TempDir()
	var lastCount int
	flushes := 0
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && flushes < 8 {
		txid, err := s.Flush(fmt.Sprintf("f%d", flushes))
		if err != nil {
			close(stop)
			<-done
			t.Fatalf("flush %d failed: %v", flushes, err)
		}
		flushes++

		// Materialize exactly what the store now holds and check it.
		ref, _, err := w.Store.GetRef("app", "main")
		if err != nil {
			t.Fatal(err)
		}
		data, _, err := w.Store.B.Get(store.SnapshotKey(ref.Lineage, ref.HeadEpoch, txid))
		if err != nil {
			t.Fatalf("flushed snapshot %d missing: %v", txid, err)
		}
		out := fmt.Sprintf("%s/check-%d.db", dir, txid)
		if _, err := ltxio.Materialize(bytesReader(data), out); err != nil {
			t.Fatalf("flushed snapshot %d does not decode: %v", txid, err)
		}
		res, err := exec.Command("sqlite3", out, "PRAGMA integrity_check; SELECT count(*) FROM t;").Output()
		if err != nil {
			t.Fatalf("flushed snapshot %d is not a usable database: %v", txid, err)
		}
		var ok string
		var count int
		if _, err := fmt.Sscanf(string(res), "%s\n%d", &ok, &count); err != nil {
			t.Fatalf("unexpected sqlite output %q: %v", res, err)
		}
		if ok != "ok" {
			t.Fatalf("integrity_check on flushed snapshot %d = %q", txid, ok)
		}
		if count < lastCount {
			t.Fatalf("row count went backwards across flushes: %d then %d", lastCount, count)
		}
		lastCount = count
		time.Sleep(100 * time.Millisecond)
	}
	close(stop)
	<-done

	if flushes < 3 {
		t.Fatalf("only %d flushes completed; the test did not exercise contention", flushes)
	}
	if lastCount == 0 {
		t.Fatal("no rows were ever captured — the writer and capture never overlapped")
	}
	if s.Err() != nil {
		t.Fatalf("session errored under load: %v", s.Err())
	}
}
```

Add a small helper in the same file:

```go
func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
```

(and import `"bytes"`)

- [ ] **Step 2: Run and investigate**

Run: `go test ./internal/session -run TestFlushesUnderContinuousWrites -v -race -count=2 -timeout 600s`
Expected: PASS. Failures to take seriously: a snapshot that fails `integrity_check` means the replica was encoded mid-apply (the flush needs to serialize against the capture engine's Apply — fix by holding a mutex around encode that Apply also takes, or by having capture expose a consistent-point signal); a row count going backwards means a flush captured an older state than a previous one. Fix the source; document what you found.

- [ ] **Step 3: Full suite and commit**

Run: `go test ./... -count=1 -race && go vet ./...`

```bash
git add internal/session
git commit -m "test: flushes stay consistent under continuous agent writes"
```

---

## Self-Review (performed at plan-writing time)

1. **Spec coverage:** implements spec § Architecture's daemon mode (long-running process, lifecycle API over a unix socket, continuous WAL capture, per-branch lease), § WAL capture and the connection contract (shared kernel and local filesystem; the daemon never owns the agent's connections), § Security posture (unix socket 0600), and the durability-reporting requirement ("the API exposes durable through TXID X"). Deferred and stated in the header: incremental LTX segments, TTL reaping, MCP server, SDKs, launch demo (Plan 6). Fork/rollback/promote through the daemon are deliberately out of scope — they remain at-rest CLI operations in this plan, because they repoint a branch and would need to fence any open session first.
2. **Placeholder scan:** none; Tasks 6 and 7 name the specific failure modes to expect and prescribe fixing source rather than expectations, and Task 4's implementation notes call out removing the vestigial `errors` import if unused.
3. **Type consistency:** `session.Options{WS,DB,Branch,Holder,LeaseTTL,Dir,RenewEvery}` is used identically in Tasks 1, 3, 6; `Session.Flush(name) (uint64, error)`, `DurableTXID()`, `Err()`, `Lease()`, `CheckoutPath()`, `ReplicaPath()`, `DB()`, `Branch()`, `Close()` match every call site in Tasks 2, 4, 6, 7; `daemon.Request{Op,DB,Branch,Name}` / `Response{OK,Error,Checkout,TXID,Sessions}` / `SessionInfo` are consistent across Tasks 4, 5, 6; `DefaultSocketPath`/`Call`/`Running` in Task 5 match their CLI uses; `store.SnapshotKey(lineage, epoch, txid)` and `Ref.SetCheckpoint(name, txid, epoch)` match Plan 4's signatures.
