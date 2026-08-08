package session

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/capture"
	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

func TestFlushMakesWritesDurableWithoutPausingTheWriter(t *testing.T) {
	testutil.RequireSQLite3(t)
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

	txid, err := s.Flush("v1", nil)
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
	testutil.RequireSQLite3(t)
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
	if _, err := s.Flush("", nil); err != nil {
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
	testutil.RequireSQLite3(t)
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
	if _, err := s.Flush("", nil); !errors.Is(err, ErrFenced) {
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

// failRefPutIf wraps a store.Backend and turns any PutIf on a "refs/" key
// into a non-CAS failure, simulating an ambiguous transient error (e.g. an
// S3 timeout) on the ref write specifically — the snapshot upload (a
// "data/" key) is left untouched, so this reproduces a flush that
// successfully uploaded its snapshot but then hit an inconclusive error
// trying to advance the ref.
type failRefPutIf struct {
	store.Backend
}

func (f failRefPutIf) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if strings.HasPrefix(key, "refs/") {
		return "", fmt.Errorf("test: injected non-CAS ref failure on %s", key)
	}
	return f.Backend.PutIf(key, data, ifMatch)
}

// TestFlushDoesNotDeleteSnapshotOnNonCASRefFailure guards the critical fix:
// a non-CAS PutRef failure is ambiguous (the write may have landed
// server-side even though the client saw an error), so Flush must not
// delete the snapshot it just uploaded. Deleting would leave a possibly-live
// ref pointing at a missing object — silent, unrecoverable data loss.
func TestFlushDoesNotDeleteSnapshotOnNonCASRefFailure(t *testing.T) {
	testutil.RequireSQLite3(t)
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
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	waitFor(t, 10*time.Second, "capture", func() bool {
		out, err := exec.Command("sqlite3", s.ReplicaPath(), "SELECT count(*) FROM t;").Output()
		return err == nil && string(out) == "1\n"
	})

	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	wantTxid := ref.HeadTXID + 1
	wantKey := store.SnapshotKey(ref.Lineage, s.Lease().Epoch, wantTxid)

	// Inject a non-CAS failure on the ref write. w.Store and s.ws.Store are
	// the same *store.Store, so mutating w.Store.B is visible to Flush.
	orig := w.Store.B
	w.Store.B = failRefPutIf{orig}

	_, err = s.Flush("", nil)
	if err == nil {
		t.Fatal("Flush must fail when the ref write fails")
	}
	if errors.Is(err, store.ErrCAS) {
		t.Fatalf("injected failure must NOT be reported as ErrCAS, got: %v", err)
	}

	// Restore the real backend to inspect what's actually in the store.
	w.Store.B = orig
	if data, _, err := w.Store.B.Get(wantKey); err != nil || len(data) == 0 {
		t.Fatalf("snapshot at %s must still exist after an ambiguous non-CAS ref failure: data=%d err=%v",
			wantKey, len(data), err)
	}
}

func TestFlushWritesASegmentThenSnapshotsOnCadence(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	if out, err := exec.Command("sqlite3", s.CheckoutPath(),
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	var members []store.ChainMember
	for i := 0; i < 5; i++ {
		if out, err := exec.Command("sqlite3", s.CheckoutPath(),
			"INSERT INTO t (v) VALUES ('row');").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		if _, err := s.Flush("", nil); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := w.Store.B.List(store.LineagePrefix(ref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if m, ok := store.ParseMemberKey(k); ok {
			members = append(members, m)
		}
	}
	var snaps, segs int
	for _, m := range members {
		if m.Snapshot {
			snaps++
		} else {
			segs++
		}
	}
	if segs == 0 {
		t.Fatal("with SnapshotEvery=3 some flushes must write segments")
	}
	if snaps < 2 {
		t.Fatalf("the cadence must produce periodic snapshots, got %d", snaps)
	}
	// Close the session before checking the branch out again: the capture
	// engine holds a persistent read lock on the checkout for as long as the
	// session is open (blocking any foreign checkpoint, by design — see
	// internal/capture/engine.go), which is incidental to what this
	// assertion cares about but pre-existing and unrelated to segments —
	// w.Checkout's own quiesce step would otherwise fail with "database is
	// busy" (tracked for final review, not a segment/snapshot bug).
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// The branch still reads correctly across the mixed chain.
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("sqlite3", path, "SELECT count(*) FROM t;").Output()
	if err != nil || string(out) != "5\n" {
		t.Fatalf("rows after mixed chain = %q err=%v", out, err)
	}
}

func TestSnapshotEveryOneKeepsOldBehavior(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v);").Run()
	for i := 0; i < 3; i++ {
		exec.Command("sqlite3", s.CheckoutPath(), "INSERT INTO t VALUES ('x');").Run()
		if _, err := s.Flush("", nil); err != nil {
			t.Fatal(err)
		}
	}
	ref, _, _ := w.Store.GetRef("app", "main")
	keys, _ := w.Store.B.List(store.LineagePrefix(ref.Lineage))
	for _, k := range keys {
		if m, ok := store.ParseMemberKey(k); ok && !m.Snapshot {
			t.Fatalf("SnapshotEvery=1 must never write a segment, found %s", k)
		}
	}
}

// TestFlushSegmentAcrossAShrinkingCommit covers recordApply's shrink branch
// (newCommit < prevCommit), which none of the other tests exercise — they
// only ever INSERT, so the database only ever grows. A bulk insert followed
// by a DELETE + VACUUM, flushed with SnapshotEvery high enough to stay off
// the snapshot cadence, forces exactly that branch: VACUUM's commit frame
// reports a smaller page count than the replica already has on disk, so
// recordApply must fold OUT the dropped trailing pages' old checksum
// contribution (the loop guarded by "newCommit < prevCommit") and drop any
// of those same page numbers pageSet is still holding from the earlier
// insert's transaction (pageSet.dropAbove — see its doc comment for why
// record() alone can't do this: a shrinking transaction's own frames never
// mention the pages it drops). If either loop's math (or the lock-page
// skip) were wrong, the segment's declared postApplyChecksum would not
// match what ltxio.MaterializeChain computes by actually replaying the
// pages — a mismatch MaterializeChain treats as a hard failure, not a
// silent misread — so a passing materialize-and-read-back below is genuine
// evidence the shrink path is correct, not just that it ran. (PRAGMA
// incremental_vacuum was tried first and rejected: empirically, against
// this package's sqlite3 CLI in WAL mode, it left freed pages on the
// freelist without ever truncating the file — freelist_count moved but
// page_count did not — so it never reaches recordApply's shrink branch at
// all. Plain VACUUM does, reliably.)
//
// This does not additionally cover the lock-page skip itself: reaching the
// lock page's own pgno (ltx.LockPgno of a 4096-byte page is 262145,
// PENDING_BYTE/pageSize+1) requires a database north of ~1GB, which isn't a
// practical size for a unit test to build and shrink across on every test
// run. recordApply's three loops were reviewed by hand against
// ltxio.MaterializeChain's identical skip (see the code and its comments)
// instead.
func TestFlushSegmentAcrossAShrinkingCommit(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// SnapshotEvery high enough that neither flush below reaches the cadence
	// trigger; the database stays well under minPagesForFractionCheck too,
	// so the only thing that can force the second flush to snapshot instead
	// of segment is a bug in this test's own arithmetic, not the cadence or
	// fraction heuristics.
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 100,
	})
	if err != nil {
		t.Fatal(err)
	}

	if out, err := exec.Command("sqlite3", s.CheckoutPath(),
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	// Bulk insert enough rows to span several pages, then flush: the
	// session's first flush is always forced to a snapshot (txid 1 cannot
	// start a segment chain), establishing the baseline this test's segment
	// flush below continues from.
	insertBulk := "INSERT INTO t (v) SELECT randomblob(300) FROM " +
		"(WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<60) SELECT x FROM c);"
	if out, err := exec.Command("sqlite3", s.CheckoutPath(), insertBulk).CombinedOutput(); err != nil {
		t.Fatalf("bulk insert: %v: %s", err, out)
	}
	if _, err := s.Flush("", nil); err != nil {
		t.Fatalf("baseline flush: %v", err)
	}

	// Shrink: drop all but 3 rows, then VACUUM reclaims the freed pages and
	// truncates the file. This is a separate transaction from the delete
	// (VACUUM commits on its own), so DrainNow below catches both, and
	// recordApply runs once per transaction — VACUUM's is what exercises
	// the shrink branch.
	if out, err := exec.Command("sqlite3", s.CheckoutPath(),
		"DELETE FROM t WHERE id > 3;").CombinedOutput(); err != nil {
		t.Fatalf("delete: %v: %s", err, out)
	}
	if out, err := exec.Command("sqlite3", s.CheckoutPath(),
		"VACUUM;").CombinedOutput(); err != nil {
		t.Fatalf("vacuum: %v: %s", err, out)
	}
	shrinkTxid, err := s.Flush("", nil)
	if err != nil {
		t.Fatalf("shrink flush: %v", err)
	}

	// Confirm this flush actually took the segment path — otherwise the
	// test would pass without ever exercising recordApply's shrink loop.
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := w.Store.B.List(store.LineagePrefix(ref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, k := range keys {
		m, ok := store.ParseMemberKey(k)
		if !ok || m.MaxTXID != shrinkTxid {
			continue
		}
		found = true
		if m.Snapshot {
			t.Fatalf("shrink flush (txid %d) was a snapshot, not a segment — this test needs the segment "+
				"path to exercise recordApply's shrink branch; adjust SnapshotEvery or bulk size", shrinkTxid)
		}
	}
	if !found {
		t.Fatalf("no chain member found for shrink flush txid %d", shrinkTxid)
	}

	// Close before checking out again: w.Checkout re-materializes the fixed
	// checkout path, which requires quiescing it — impossible while this
	// session's capture engine still holds its persistent read lock on that
	// same file (pre-existing, unrelated to segments; see
	// TestFlushWritesASegmentThenSnapshotsOnCadence's identical comment).
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// A fresh materialization resolves snapshot+segment chain, verifying
	// the shrink segment's checksum along the way (ltxio.MaterializeChain
	// fails loudly on a mismatch, rather than silently misreading).
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatalf("checkout after shrink: %v", err)
	}
	out, err := exec.Command("sqlite3", path, "SELECT count(*) FROM t;").Output()
	if err != nil || string(out) != "3\n" {
		t.Fatalf("rows after shrinking segment = %q err=%v", out, err)
	}
}

// TestConcurrentFlushIsSerialized guards the flushMu fix: without
// serialization, concurrent Flush calls on one Session compute the same
// txid/key and race. With flushMu, every call's GetRef-through-PutRef is
// atomic with respect to the others on this Session, so every call should
// succeed with its own distinct txid (a retryable error is tolerated too,
// in case of external contention, but must never be ErrFenced or a
// duplicated txid).
func TestConcurrentFlushIsSerialized(t *testing.T) {
	testutil.RequireSQLite3(t)
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
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	waitFor(t, 10*time.Second, "capture", func() bool {
		out, err := exec.Command("sqlite3", s.ReplicaPath(), "SELECT count(*) FROM t;").Output()
		return err == nil && string(out) == "1\n"
	})

	const n = 4
	var wg sync.WaitGroup
	txids := make([]uint64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			txids[i], errs[i] = s.Flush("", nil)
		}(i)
	}
	wg.Wait()

	seen := map[uint64]int{}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			if errors.Is(errs[i], ErrFenced) {
				t.Fatalf("call %d: fencing must not happen from self-contention: %v", i, errs[i])
			}
			if !strings.Contains(errs[i].Error(), "retry") {
				t.Fatalf("call %d: failure must be retryable, got: %v", i, errs[i])
			}
			continue
		}
		if txids[i] == 0 {
			t.Fatalf("call %d: succeeded but returned txid 0", i)
		}
		seen[txids[i]]++
	}
	for txid, count := range seen {
		if count > 1 {
			t.Fatalf("txid %d was returned by %d successful calls (must be unique per success)", txid, count)
		}
	}
	if len(seen) == 0 {
		t.Fatal("no call succeeded")
	}

	// The ref's head afterwards must reference an object that actually
	// exists in the backend — a snapshot or, since consecutive successful
	// calls here advance TXID one at a time without SnapshotEvery forcing
	// every one of them to snapshot, possibly an incremental segment.
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := w.Store.B.List(store.LineagePrefix(ref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	var headKey string
	for _, k := range keys {
		if m, ok := store.ParseMemberKey(k); ok && m.Epoch == ref.HeadEpoch && m.MaxTXID == ref.HeadTXID {
			headKey = k
			break
		}
	}
	if headKey == "" {
		t.Fatalf("no snapshot or segment found for ref head (epoch %d, txid %d)", ref.HeadEpoch, ref.HeadTXID)
	}
	if data, _, err := w.Store.B.Get(headKey); err != nil || len(data) == 0 {
		t.Fatalf("ref head %s must reference an existing object: data=%d err=%v",
			headKey, len(data), err)
	}
}

// mustExec runs sql against the SQLite file at path via the sqlite3 CLI,
// failing the test with the command's combined output on error.
func mustExec(t *testing.T, path, sql string) {
	t.Helper()
	if out, err := exec.Command("sqlite3", path, sql).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
}

// countingBackend wraps a store.Backend and counts every mutating call
// (Put, PutIf, Delete) it observes, regardless of outcome. Used by
// TestIdleAutoFlushWritesNothing to prove the idle short-circuit performs
// literally zero backend writes across several ticks with nothing pending —
// the PM-review blocking amendment this task exists to satisfy.
type countingBackend struct {
	store.Backend
	mu          sync.Mutex
	writes      int
	gets, lists int
}

func (c *countingBackend) Put(key string, data []byte) error {
	c.mu.Lock()
	c.writes++
	c.mu.Unlock()
	return c.Backend.Put(key, data)
}

func (c *countingBackend) PutIf(key string, data []byte, ifMatch string) (string, error) {
	c.mu.Lock()
	c.writes++
	c.mu.Unlock()
	return c.Backend.PutIf(key, data, ifMatch)
}

func (c *countingBackend) Delete(key string) error {
	c.mu.Lock()
	c.writes++
	c.mu.Unlock()
	return c.Backend.Delete(key)
}

// Get and List are counted SEPARATELY from writes (gets/lists, not folded
// into c.writes) — TestReadOnlySessionWithCleanCheckoutMakesNoStoreWrites
// and TestCleanReopenReadsChecksumFromSidecarNotTheStore need to assert
// zero of EACH independently: a settling-flush suppression that avoided
// every write but still fetched the head object to read its checksum would
// pass the writes-only assertion while still doing exactly the full-object
// download I2 (review round 3) exists to prevent.
func (c *countingBackend) Get(key string) ([]byte, string, error) {
	c.mu.Lock()
	c.gets++
	c.mu.Unlock()
	return c.Backend.Get(key)
}

func (c *countingBackend) List(prefix string) ([]string, error) {
	c.mu.Lock()
	c.lists++
	c.mu.Unlock()
	return c.Backend.List(prefix)
}

func (c *countingBackend) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}

// GetCount reports Get calls observed so far — see Get's doc comment.
func (c *countingBackend) GetCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets
}

// ListCount reports List calls observed so far — see Get's doc comment
// (Chain resolution, ops.Workspace.HeadPostApplyChecksum's now-deleted
// caller, went through List before Get; counting both makes the
// zero-store-read assertion unambiguous about which RPC, if any, crept
// back in).
func (c *countingBackend) ListCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lists
}

// TestAutoFlushShipsWritesWithoutManualFlush pins Options.FlushEvery: a
// session opened with a short auto-flush cadence must make a committed write
// durable on its own, with the caller never calling Flush.
//
// Every session performs one unconditional settling flush shortly after its
// startup rebase lands, even with nothing written (see
// appliedGen/flushedRebaseGen's doc comment on Session) — so a bare
// "HeadTXID > 1" is satisfied by that settling flush alone and would pass
// even if this test's own write were never captured at all. This waits for
// the session to settle FIRST, captures the settled ref, then requires the
// ref to advance BEYOND that settled txid after the write — and, since a
// txid bump alone still isn't proof of WHAT shipped, reads the row back from
// a fresh materialization as the actual content proof.
func TestAutoFlushShipsWritesWithoutManualFlush(t *testing.T) {
	testutil.RequireSQLite3(t)
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

	waitFor(t, 10*time.Second, "the session to settle after startup", func() bool {
		_, _, ok := s.LastFlush()
		return ok && !s.autoFlushPending()
	})
	settled, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	mustExec(t, s.CheckoutPath(), "CREATE TABLE t (v); INSERT INTO t VALUES ('x');")
	// No manual flush. Within a few ticks the write must be durable, past
	// whatever the settling flush alone already produced.
	deadline := time.Now().Add(10 * time.Second)
	for {
		ref, _, err := w.Store.GetRef("app", "main")
		if err != nil {
			t.Fatal(err)
		}
		if ref.HeadTXID > settled.HeadTXID {
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

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("sqlite3", path, "SELECT v FROM t;").Output()
	if err != nil || string(out) != "x\n" {
		t.Fatalf("auto-flushed content missing from a fresh materialization: %q err=%v", out, err)
	}
}

// failNRefPutIf wraps a store.Backend and turns each of the next N PutIf
// calls on a "refs/" key into a non-CAS failure — the same class of
// ambiguous transient error failRefPutIf injects, but healing after N
// occurrences instead of forever — simulating a transient outage (e.g. a
// flaky network) that later recovers. N is an atomic.Int64 rather than a
// plain field guarded by installing/swapping the whole wrapper: a session
// with FlushEvery set reads s.ws.Store.B from its own flushLoop goroutine as
// soon as Open returns, so a test must install this wrapper as w.Store.B
// BEFORE calling Open (an unarmed instance always lets everything through,
// n starting at 0) and arm the actual failure count afterward via arm() —
// swapping w.Store.B itself after Open would race that goroutine's read
// under -race.
type failNRefPutIf struct {
	store.Backend
	n atomic.Int64
}

// arm sets the number of upcoming refs/ PutIf calls to fail from now on.
// Safe to call concurrently with PutIf.
func (f *failNRefPutIf) arm(n int64) { f.n.Store(n) }

func (f *failNRefPutIf) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if strings.HasPrefix(key, "refs/") {
		for {
			cur := f.n.Load()
			if cur <= 0 {
				break
			}
			if f.n.CompareAndSwap(cur, cur-1) {
				return "", fmt.Errorf("test: injected transient ref failure on %s", key)
			}
		}
	}
	return f.Backend.PutIf(key, data, ifMatch)
}

// TestAutoFlushFailureSurfacesAndRecovers pins the auto-flush failure
// contract: a failing tick records LastFlushErr but must not kill the
// session (Err() stays nil, the checkout stays usable), and once the backend
// heals a later tick clears LastFlushErr again.
func TestAutoFlushFailureSurfacesAndRecovers(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// Install BEFORE Open — see failNRefPutIf's doc comment. Unarmed (n=0),
	// so the startup settling flush and lease acquisition go through
	// untouched.
	fp := &failNRefPutIf{Backend: w.Store.B}
	w.Store.B = fp

	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", FlushEvery: 60 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	mustExec(t, s.CheckoutPath(), "CREATE TABLE t (v); INSERT INTO t VALUES ('x');")

	// Let an initial successful flush (the unconditional startup settling
	// flush, this write, or both coalesced) land before injecting failures,
	// so the failure this test asserts on is unambiguously its own attempt's,
	// not a race with that first flush.
	waitFor(t, 5*time.Second, "an initial successful flush", func() bool {
		_, _, ok := s.LastFlush()
		return ok
	})

	fp.arm(2)
	mustExec(t, s.CheckoutPath(), "INSERT INTO t VALUES ('y');")

	waitFor(t, 5*time.Second, "LastFlushErr to be set by a failing tick", func() bool {
		return s.LastFlushErr() != nil
	})
	if err := s.Err(); err != nil {
		t.Fatalf("a transient auto-flush failure must not kill the session: %v", err)
	}
	// The checkout is still usable after the failure.
	mustExec(t, s.CheckoutPath(), "INSERT INTO t VALUES ('z');")

	waitFor(t, 5*time.Second, "LastFlushErr to clear once the backend heals", func() bool {
		return s.LastFlushErr() == nil
	})
	if _, txid, ok := s.LastFlush(); !ok || txid == 0 {
		t.Fatalf("LastFlush must report the successful recovery, got txid=%d ok=%v", txid, ok)
	}
}

// TestAutoFlushTransitionLogsRecordKindAutoAndFailure extends
// TestAutoFlushFailureSurfacesAndRecovers's fault-injection scenario with
// task 7's structured transition-log contract: a failing auto-flush tick
// logs "flush-failed kind=auto error=...", and the later tick that recovers
// once the backend heals logs "flushed kind=auto txid=...".
func TestAutoFlushTransitionLogsRecordKindAutoAndFailure(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	fp := &failNRefPutIf{Backend: w.Store.B}
	w.Store.B = fp

	var s *Session
	out := captureStderr(t, func() {
		var err error
		s, err = Open(context.Background(), Options{
			WS: w, DB: "app", Branch: "main", FlushEvery: 60 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		mustExec(t, s.CheckoutPath(), "CREATE TABLE t (v); INSERT INTO t VALUES ('x');")
		waitFor(t, 5*time.Second, "an initial successful flush", func() bool {
			_, _, ok := s.LastFlush()
			return ok
		})

		fp.arm(2)
		mustExec(t, s.CheckoutPath(), "INSERT INTO t VALUES ('y');")
		waitFor(t, 5*time.Second, "LastFlushErr to be set by a failing tick", func() bool {
			return s.LastFlushErr() != nil
		})
		waitFor(t, 5*time.Second, "LastFlushErr to clear once the backend heals", func() bool {
			return s.LastFlushErr() == nil
		})
	})

	if !strings.Contains(out, `offshoot: session: app@main: flush-failed kind="auto" error=`) {
		t.Fatalf("missing flush-failed(auto) transition log in:\n%s", out)
	}
	if !strings.Contains(out, `offshoot: session: app@main: flushed kind="auto" txid=`) {
		t.Fatalf("missing flushed(auto) transition log in:\n%s", out)
	}
}

// TestIdleAutoFlushWritesNothing is the PM-review blocking amendment: a tick
// with nothing pending (no committed frames, and no rebase, since the last
// successful flush) must write NOTHING — no object PUT, no ref PUT, no
// flushesSinceSnapshot advance. Without this, an idle session under a short
// cadence would upload a full snapshot every SnapshotEvery ticks forever,
// since Flush always advances txid and writes something once actually
// called, regardless of whether anything changed.
//
// This does one real write and waits for the session to fully settle (via
// Session.autoFlushPending, not just "a flush happened") before measuring: every
// session performs one unconditional settling flush shortly after its
// startup rebase (rebaseGen 0->1) regardless of whether the agent ever
// writes anything — see appliedGen/flushedRebaseGen's doc comment on
// Session — so counting from Open itself would either race that settling
// flush into the "idle" window (flaky) or, if deleted entirely, this test
// would still pass with a write that was never captured, which caught
// neither the original PM-amendment bug nor the rebase-folding one. Counting
// only after the session is provably caught up isolates exactly the
// idle-tick behavior this test exists to pin.
func TestIdleAutoFlushWritesNothing(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	cb := &countingBackend{Backend: w.Store.B}
	w.Store.B = cb

	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main",
		FlushEvery: 40 * time.Millisecond,
		// Long enough that lease renewal (which also writes the ref) cannot
		// fire during this test's short observation window, isolating the
		// backend write count to whatever the flush loop itself does.
		LeaseTTL: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	mustExec(t, s.CheckoutPath(), "CREATE TABLE t (v); INSERT INTO t VALUES ('x');")
	waitFor(t, 10*time.Second, "the write to ship via auto-flush", func() bool {
		ref, _, err := w.Store.GetRef("app", "main")
		return err == nil && ref.HeadTXID > 1
	})
	waitFor(t, 10*time.Second, "the session to fully settle (nothing pending)", func() bool {
		return s.LastFlushErr() == nil && !s.autoFlushPending()
	})

	baseline := cb.Count()
	time.Sleep(500 * time.Millisecond) // ~12 idle ticks at 40ms
	if got := cb.Count(); got != baseline {
		t.Fatalf("idle auto-flush ticks performed %d backend write(s) after settling (baseline %d, now %d), want 0",
			got-baseline, baseline, got)
	}
}

// TestFlushLoopFlushesRebaseFoldedContentWhenOtherwiseIdle is CRITICAL fix
// (i): a rebase (capture.Engine.rebase's checkpoint(TRUNCATE) — see
// appliedGen/flushedRebaseGen's doc comment on Session) can fold a real
// commit into the replica's baseline without that commit ever passing
// through Apply, so appliedGen alone never notices it. This drives
// replicaSink.Rebase directly with a modified snapshot — exactly what the
// engine's own rebase() hands to Sink.Rebase — completely independent of the
// live checkout, so no Apply call for this content ever happens, and asserts
// the ref still advances within a few ticks: the rebaseGen half of
// flushLoop's pending check must catch what the appliedGen half cannot.
func TestFlushLoopFlushesRebaseFoldedContentWhenOtherwiseIdle(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", FlushEvery: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Let the startup settling flush land and the session fully settle
	// first, so the rebase below is unambiguously what triggers the next
	// flush, not a race with startup.
	waitFor(t, 5*time.Second, "the session to settle after startup", func() bool {
		_, _, ok := s.LastFlush()
		return ok && !s.autoFlushPending()
	})
	before, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	// A snapshot with content the live checkout (and therefore this
	// session's WAL reader) never saw — built independently, the way the
	// capture engine builds its own rebase snapshot from the checkpointed
	// main file, so no Apply call for this content is possible.
	snapPath := filepath.Join(t.TempDir(), "rebase-snapshot.db")
	mustExec(t, snapPath, "CREATE TABLE rebased (v); INSERT INTO rebased VALUES ('rebase-only');")
	if err := (replicaSink{s}).Rebase(snapPath); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, "flushLoop to ship the rebase-folded content", func() bool {
		ref, _, err := w.Store.GetRef("app", "main")
		return err == nil && ref.HeadTXID > before.HeadTXID
	})

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("sqlite3", path, "SELECT v FROM rebased;").Output()
	if err != nil || string(out) != "rebase-only\n" {
		t.Fatalf("rebase-folded content missing after flush: %q err=%v", out, err)
	}
}

// TestFlushLoopRetriesAfterRebaseDuringUpload is CRITICAL fix (ii): a rebase
// racing a flush's upload window (replicaMu released after encode, before
// PutRef confirms) must not let that flush's success bookkeeping mark the
// raced rebase's content durable — flushedRebaseGen's gate (mirroring
// forceSnapshot's own) must leave it pending so the NEXT tick retries and
// actually ships it. Uses FlushUploadHook, FlushEncodeHook's sibling, to
// pause a real auto-flush exactly inside that window.
func TestFlushLoopRetriesAfterRebaseDuringUpload(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}

	// Installed BEFORE Open: flushLoop's goroutine (started inside Open)
	// reads the package-level FlushUploadHook variable on every tick, so
	// assigning it afterward — as an earlier version of this test did —
	// races that read under -race exactly the way swapping w.Store.B after
	// Open does (see failNRefPutIf's doc comment for the identical hazard).
	// The write here happens-before flushLoop's goroutine via Open's own
	// `go` statement, so this assignment itself is race-free; armed (an
	// atomic.Bool, safe for concurrent use by construction) is what
	// actually turns the pause on and off afterward, never the hook
	// variable itself.
	var armed atomic.Bool
	entered := make(chan struct{})
	proceed := make(chan struct{})
	var enteredOnce, proceedOnce sync.Once
	// release closes proceed exactly once — used both by the deliberate
	// mid-test release below and by the safety-net cleanup defer, so a
	// t.Fatal after the deliberate release cannot double-close (which would
	// panic) and a t.Fatal before it still gets proceed closed exactly once.
	release := func() { proceedOnce.Do(func() { close(proceed) }) }
	FlushUploadHook = func() {
		if !armed.Load() {
			return
		}
		enteredOnce.Do(func() { close(entered) })
		<-proceed
	}
	// Registered before Open, so on ANY exit path (including an early
	// t.Fatal) it runs LAST — after the deferred s.Close() below (registered
	// after Open, so LIFO runs it first) has fully joined flushLoop. Nilling
	// this any earlier could race flushLoop's own read of it — see this
	// hook's installation comment above.
	defer func() { FlushUploadHook = nil }()

	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", FlushEvery: 60 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Registered before the unblock defer just below, so LIFO runs it
	// second (after the unblock, before the FlushUploadHook=nil defer
	// above).
	defer s.Close()
	// Registered last, so LIFO runs it FIRST, before s.Close() above: a
	// t.Fatal on the "auto-flush never reached the upload hook" path below
	// would otherwise leave armed true and proceed unclosed. If flushLoop's
	// own goroutine then reaches the hook a moment later — a real, if
	// narrow, race against exactly that timeout firing — it blocks at
	// <-proceed forever, holding flushMu, and the deferred s.Close() above
	// would hang right behind it waiting for flushLoop to notice ctx.Done(),
	// which it cannot do while stuck inside Flush. Disarming and
	// unconditionally releasing here first closes that window regardless of
	// which line triggered the exit, so a failure here can never leave a
	// goroutine wedged for the rest of the package's test run.
	defer func() {
		armed.Store(false)
		release()
	}()

	waitFor(t, 5*time.Second, "the session to settle after startup", func() bool {
		_, _, ok := s.LastFlush()
		return ok && !s.autoFlushPending()
	})
	ref0, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	armed.Store(true)
	// Give the loop something to flush so it actually reaches the hook
	// instead of idling.
	mustExec(t, s.CheckoutPath(), "CREATE TABLE t (v); INSERT INTO t VALUES (1);")

	select {
	case <-entered:
		// The auto-flush is now paused immediately after encode, replicaMu
		// released, upload not yet attempted.
	case <-time.After(10 * time.Second):
		t.Fatal("auto-flush never reached the upload hook")
	}

	// Race a rebase in while that flush is paused mid-upload — exactly the
	// window flushedRebaseGen's gating exists to protect.
	snapPath := filepath.Join(t.TempDir(), "raced-rebase.db")
	mustExec(t, snapPath, "CREATE TABLE raced (v); INSERT INTO raced VALUES ('raced');")
	if err := (replicaSink{s}).Rebase(snapPath); err != nil {
		t.Fatal(err)
	}

	// Disarm before releasing: the next (retry) flush must sail through the
	// hook rather than pausing again.
	armed.Store(false)
	release()

	// The paused flush completes and succeeds — the race doesn't fail or
	// corrupt an upload already begun — advancing the ref once.
	var afterFirst store.Ref
	deadline := time.Now().Add(10 * time.Second)
	for {
		afterFirst, _, err = w.Store.GetRef("app", "main")
		if err != nil {
			t.Fatal(err)
		}
		if afterFirst.HeadTXID > ref0.HeadTXID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the paused flush never completed")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Because the rebase raced this flush's upload window, flushedRebaseGen
	// must have been left at its pre-race value: the session must not
	// believe the raced rebase's content is durable, so the NEXT tick must
	// retry and ship it.
	deadline = time.Now().Add(10 * time.Second)
	for {
		ref, _, err := w.Store.GetRef("app", "main")
		if err != nil {
			t.Fatal(err)
		}
		if ref.HeadTXID > afterFirst.HeadTXID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("flushLoop never retried after a rebase raced the upload window")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Safe only now: Close joins flushLoop's goroutine (<-s.flushDone)
	// before returning, so it can never call the hook again — see
	// FlushUploadHook's own installation above for why this must not
	// happen any earlier.
	FlushUploadHook = nil

	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("sqlite3", path, "SELECT v FROM raced;").Output()
	if err != nil || string(out) != "raced\n" {
		t.Fatalf("raced rebase content missing after retry: %q err=%v", out, err)
	}
}

// TestReadOnlySessionWithCleanCheckoutMakesNoStoreWrites pins the
// settling-flush suppression (M2 follow-up, ledgered — see rebaseline's doc
// comment): a session opened against a checkout that is ALREADY proven
// clean-and-current (materialized here via a bare w.Checkout call before the
// session ever exists — this test deliberately does not depend on Commit
// B's Close-time sidecar refresh, so it stays meaningful even if that half
// is deferred separately) must perform literally zero backend writes, ever
// — not even the one-PUT-per-lifetime settling flush every prior session
// performed. Extends TestIdleAutoFlushWritesNothing's countingBackend
// pattern; the difference from that test is entirely in the setup (a
// pre-existing clean checkout here vs. a brand-new one there) and in
// asserting zero writes from the moment Open returns, not just after the
// session has already settled.
func TestReadOnlySessionWithCleanCheckoutMakesNoStoreWrites(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// Materialize + sidecar-stamp the checkout against the current head
	// BEFORE wrapping the backend in a counter: this write must not be
	// counted, and it is what makes ops.CheckoutProven report Clean=true
	// once Open calls it below.
	if _, err := w.Checkout("app", "main"); err != nil {
		t.Fatal(err)
	}

	cb := &countingBackend{Backend: w.Store.B}
	w.Store.B = cb

	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main",
		FlushEvery: 40 * time.Millisecond,
		// Long enough that lease renewal (which also writes the ref) cannot
		// fire during this test's short observation window — see
		// TestIdleAutoFlushWritesNothing's identical choice.
		LeaseTTL: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if !s.cleanAtOpen {
		t.Fatal("expected cleanAtOpen: the checkout was materialized fresh against the current head just before Open")
	}
	if !s.headPostApplyValid {
		t.Fatal("expected headPostApplyValid: the checkout's fresh materialize just stamped a checksum-bearing sidecar")
	}
	// Open itself must have made exactly two backend Gets, both tiny ref
	// metadata reads (AcquireLease's own GetRef, then CheckoutProven's) —
	// never a Get on a "data/" object key, which is what a head-checksum
	// fetch would have needed. This is the direct, positive proof: review
	// round 2's version of this suppression fetched the head chain's LAST
	// object on this exact path, a full download whenever the head was a
	// snapshot; review round 3 replaced it with reading the checksum out of
	// the LOCAL sidecar CheckoutProven's own GetRef-driven checkoutState
	// call already parsed, so this count must stay at exactly the two ref
	// reads Open always needed anyway, regardless of what the head object
	// is or how large it is.
	if got := cb.GetCount(); got != 2 {
		t.Fatalf("Open performed %d backend Get(s), want exactly 2 (AcquireLease's + CheckoutProven's own GetRef) — anything more means a head object was fetched", got)
	}
	if got := cb.ListCount(); got != 0 {
		t.Fatalf("Open performed %d backend List(s), want 0 (no chain resolution needed for a clean-at-open checkout)", got)
	}

	// baseline excludes AcquireLease's own ref write and CheckoutProven's
	// own GetRef (both legitimate, unavoidable parts of any Open call,
	// unrelated to the settling-flush suppression under test) — same
	// reasoning as TestIdleAutoFlushWritesNothing's baseline, just captured
	// right after Open returns instead of after the session settles, since
	// here nothing past this point should ever touch the backend at all —
	// GetCount/ListCount included: review round 3's whole point is that
	// reading headPostApplyChecksum out of the LOCAL sidecar (see
	// ops.CheckoutResult.PostApplyChecksum) must add no Get (the previous,
	// since-reverted design fetched the head chain's last object — a full
	// DOWNLOAD whenever the head is a snapshot, exactly the case a
	// permanently-idle read-only session like this one always has) and no
	// List (Store.Chain resolution) beyond whatever Open already needed.
	baseline, baselineGets, baselineLists := cb.Count(), cb.GetCount(), cb.ListCount()
	// Spans the startup rebase, its (suppressed) settle decision, and
	// several idle flushLoop ticks — ~12 at 40ms.
	time.Sleep(500 * time.Millisecond)
	if got := cb.Count(); got != baseline {
		t.Fatalf("a read-only session with a clean-at-open checkout performed %d backend write(s) after Open returned (baseline %d, now %d), want 0 (no settling flush, no idle-tick writes)",
			got-baseline, baseline, got)
	}
	if got := cb.GetCount(); got != baselineGets {
		t.Fatalf("a read-only session with a clean-at-open checkout performed %d backend Get(s) after Open returned (baseline %d, now %d), want 0 (the settling-flush suppression must read its checksum from the local .sum sidecar, never fetch the head object)",
			got-baselineGets, baselineGets, got)
	}
	if got := cb.ListCount(); got != baselineLists {
		t.Fatalf("a read-only session with a clean-at-open checkout performed %d backend List(s) after Open returned (baseline %d, now %d), want 0",
			got-baselineLists, baselineLists, got)
	}
	if s.autoFlushPending() {
		t.Fatal("expected the suppressed startup rebase to leave nothing pending")
	}
}

// TestSettleStillHappensWhenCheckoutWasStaleAtOpen guards the suppression's
// safety boundary from the other side: a checkout whose on-disk content
// differs from the branch head when Open runs — simulated here by dirtying
// a materialized checkout directly, bypassing ops entirely, exactly the
// kind of un-checkpointed local edit a stray sqlite3 client or an unclean
// prior shutdown can leave behind — must NOT be treated as clean-at-open,
// and the settling flush must still happen, same as before this change.
func TestSettleStillHappensWhenCheckoutWasStaleAtOpen(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	// Dirties the checkout without touching its .sum sidecar: checkoutState
	// will read this as "modified" (sidecar identity still matches ref, but
	// the file's content no longer matches the recorded hash).
	mustExec(t, path, "CREATE TABLE stray (v); INSERT INTO stray VALUES ('predates-session');")

	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", FlushEvery: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if s.cleanAtOpen {
		t.Fatal("expected a stray-edited checkout to NOT be recognized clean at open")
	}

	waitFor(t, 5*time.Second, "the settling flush", func() bool {
		ref, _, err := w.Store.GetRef("app", "main")
		return err == nil && ref.HeadTXID > 1
	})
}

// TestSettleStillHappensWithOldFormatSidecarLackingChecksum pins review
// round 3's fail-toward-settling contract: a sidecar written before
// PostApplyChecksum existed (or, equivalently, one from a store backend
// that never records it) still decodes — sumRecord.PostApplyChecksum's
// `omitempty` JSON tag means the field just isn't there, and Go decodes a
// missing field to its zero value, 0, exactly the "absent" sentinel this
// whole mechanism already uses everywhere else (see
// CheckoutResult.PostApplyChecksum's doc comment). The checkout is
// genuinely, fully clean by every OTHER measure (hash and identity both
// match) — cleanAtOpen must still be true — but with no checksum to trust,
// headPostApplyValid must be false and the settle must proceed exactly as
// it always did before this suppression existed.
func TestSettleStillHappensWithOldFormatSidecarLackingChecksum(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	// Rewrite the sidecar to the pre-review-round-3 format: same
	// hash/lineage/epoch/txid Checkout just stamped — still fully "clean"
	// by checkoutState's own identity+hash check — but with no
	// "post_apply_checksum" field at all. The hash is read back out of the
	// sidecar Checkout just wrote (not recomputed here) so this test
	// doesn't duplicate fileSum's own hashing logic.
	raw, err := os.ReadFile(path + ".sum")
	if err != nil {
		t.Fatal(err)
	}
	var rec struct {
		Hash string `json:"hash"`
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	oldFormat := fmt.Sprintf(`{"hash":%q,"lineage":%q,"epoch":%d,"txid":%d}`,
		rec.Hash, ref.Lineage, ref.HeadEpoch, ref.HeadTXID)
	if err := os.WriteFile(path+".sum", []byte(oldFormat), 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", FlushEvery: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if !s.cleanAtOpen {
		t.Fatal("expected cleanAtOpen: the old-format sidecar's hash and identity still fully match — the checkout genuinely IS clean, it just predates checksum-recording")
	}
	if s.headPostApplyValid {
		t.Fatal("expected headPostApplyValid == false: an old-format sidecar has no post_apply_checksum to trust (decodes to 0, treated as absent)")
	}

	waitFor(t, 5*time.Second, "the settling flush despite cleanAtOpen (no checksum available to suppress on)", func() bool {
		ref, _, err := w.Store.GetRef("app", "main")
		return err == nil && ref.HeadTXID > 1
	})
}

// TestSettlingSuppressionCatchesRaceWindowFold pins CRITICAL fix 1 from code
// review: cleanAtOpen alone proves the checkout's content at T0 (the moment
// Open's synchronous CheckoutProven call ran), NOT at T1 (the moment the
// engine's actual startup rebase — the one that seeds the replica —
// physically runs, asynchronously, possibly after Open has already
// returned to its caller: see the WaitReady/Resumed comment in Open and
// cleanAtOpen's own doc comment). A write landing in the T0..T1 window
// gets folded into the checkout's main file by the real rebase's
// checkpoint(TRUNCATE) without ever passing through Apply, so a
// suppression gated on cleanAtOpen alone would silently believe there was
// nothing to settle and that write would never become durable.
//
// This is exercised via white-box rewind rather than by racing real
// wall-clock timing (inherently flaky for a window this narrow): let
// Open's REAL startup rebase land first and settle normally (proving
// cleanAtOpen/headPostApplyValid are set correctly for this checkout), then
// manually rewind rebaseGen/flushedRebaseGen back to 0 and re-drive
// rebaseline via replicaSink.Rebase — the SAME technique
// TestFlushLoopFlushesRebaseFoldedContentWhenOtherwiseIdle uses for a
// mid-session rebase — but targeting the 0->1 transition specifically, with
// content headPostApplyChecksum (fetched once, in Open, before any of this)
// knows nothing about. This directly models "the fold happened between the
// checksum fetch and the real rebase" without needing to actually win that
// race.
//
// Fail-verified: against the pre-fix code (suppression gated on
// `first && s.cleanAtOpen` alone, no checksum comparison), this test times
// out — the rewound "first" rebase is suppressed again regardless of its
// content, and the settle this test waits for never happens.
func TestSettlingSuppressionCatchesRaceWindowFold(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Checkout("app", "main"); err != nil {
		t.Fatal(err)
	}

	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", FlushEvery: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if !s.cleanAtOpen || !s.headPostApplyValid {
		t.Fatal("expected cleanAtOpen and headPostApplyValid for a checkout materialized fresh against head")
	}
	waitFor(t, 5*time.Second, "the (suppressed) startup rebase to land with nothing pending", func() bool {
		s.replicaMu.Lock()
		gen := s.rebaseGen
		s.replicaMu.Unlock()
		return gen == 1 && !s.autoFlushPending()
	})
	before, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	// White-box rewind: pretend this is still the FIRST rebase, and drive it
	// with content headPostApplyChecksum (fetched once, at Open, describing
	// the ORIGINAL "init" content) knows nothing about — modeling a write
	// that raced into the checkpoint before the real rebase ran.
	s.replicaMu.Lock()
	s.rebaseGen = 0
	s.flushedRebaseGen = 0
	s.replicaMu.Unlock()

	snapPath := filepath.Join(t.TempDir(), "race-window-fold.db")
	mustExec(t, snapPath, "CREATE TABLE raced (v); INSERT INTO raced VALUES ('race-window-fold');")
	if err := (replicaSink{s}).Rebase(snapPath); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, "the settling flush despite cleanAtOpen (checksum mismatch must block suppression)", func() bool {
		ref, _, err := w.Store.GetRef("app", "main")
		return err == nil && ref.HeadTXID > before.HeadTXID
	})

	// Verify what actually got durably uploaded by materializing the
	// object the branch's ref now points to DIRECTLY — not via
	// s.Close()+w.Checkout(). This white-box test's fake rebase above
	// touched only s.replica, never the real checkout file, so Close's own
	// (separately and independently tested — see
	// TestCloseRefreshesSidecarSoReopenCleanSkips et al.) sidecar-refresh
	// logic would otherwise stamp the checkout's OWN stale, untouched bytes
	// as "clean" for this new txid and mask exactly the thing this test
	// needs to prove; going straight at the store's own object sidesteps
	// that entirely and is the more direct proof anyway.
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	snapKey := store.SnapshotKey(ref.Lineage, ref.HeadEpoch, ref.HeadTXID)
	data, _, err := w.Store.B.Get(snapKey)
	if err != nil {
		t.Fatalf("expected the settling flush to have written a snapshot at %s: %v", snapKey, err)
	}
	dst := filepath.Join(t.TempDir(), "materialized.db")
	if _, err := ltxio.Materialize(bytes.NewReader(data), dst); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("sqlite3", dst, "SELECT v FROM raced;").Output()
	if err != nil || string(out) != "race-window-fold\n" {
		t.Fatalf("race-window-folded content missing from the settled snapshot: %q err=%v", out, err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestCleanAtOpenSessionsFirstRealFlushIsASegment pins a consequence of the
// settling-flush suppression that wasn't previously reachable: with the
// settle skipped, a clean-at-open session's very FIRST real flush (the one
// its own write triggers) continues DIRECTLY from the branch's pre-session
// head as a SEGMENT — not a snapshot — since there is no intervening
// settle-snapshot to reset flushesSinceSnapshot/forceSnapshot first (see
// flush.go's updated comment on the snapshot-vs-segment decision). The
// chain must still resolve correctly end to end.
func TestCleanAtOpenSessionsFirstRealFlushIsASegment(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Checkout("app", "main"); err != nil {
		t.Fatal(err)
	}

	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if !s.cleanAtOpen {
		t.Fatal("expected cleanAtOpen for a checkout materialized fresh against head")
	}
	waitFor(t, 5*time.Second, "the (suppressed) startup rebase to settle with nothing pending", func() bool {
		return !s.autoFlushPending()
	})

	mustExec(t, s.CheckoutPath(), "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	txid, err := s.Flush("", nil)
	if err != nil {
		t.Fatal(err)
	}
	if txid != 2 {
		t.Fatalf("expected the first real flush to be txid 2 (continuing directly from Create's txid 1, no intervening settle snapshot), got %d", txid)
	}

	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := w.Store.B.List(store.LineagePrefix(ref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	foundSegment := false
	for _, k := range keys {
		if m, ok := store.ParseMemberKey(k); ok && !m.Snapshot && m.MaxTXID == txid {
			foundSegment = true
		}
	}
	if !foundSegment {
		t.Fatal("expected the first real flush to be written as a SEGMENT, not a snapshot")
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("sqlite3", path, "SELECT v FROM t;").Output()
	if err != nil || string(out) != "1\n" {
		t.Fatalf("chain did not resolve correctly after a clean-at-open session's first real flush: %q err=%v", out, err)
	}
}

// TestCloseRefreshesSidecarSoReopenCleanSkips pins Commit B (sidecar refresh
// on clean Close, M2 follow-up, ledgered): open, write, flush, close — a
// textbook clean shutdown — then reopen the SAME db@branch and confirm the
// checkout is clean-skipped rather than re-materialized. Extends
// internal/ops's TestCheckoutSkipsRematerializeWhenCleanAndCurrent same-inode
// pattern into the session layer, where the "clean" stamp now has to survive
// a real session's Close rather than only ops.Checkout/Checkpoint's own
// in-line refresh.
func TestCloseRefreshesSidecarSoReopenCleanSkips(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}

	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	checkoutPath := s.CheckoutPath()
	mustExec(t, checkoutPath, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	if _, err := s.Flush("", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	st1, err := os.Stat(checkoutPath)
	if err != nil {
		t.Fatal(err)
	}

	s2, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	if !s2.cleanAtOpen {
		t.Fatal("expected the reopen to see a checkout the prior session's clean Close already stamped current")
	}
	st2, err := os.Stat(s2.CheckoutPath())
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(st1, st2) {
		t.Fatal("reopen after a clean close should clean-skip re-materializing the checkout (same inode expected)")
	}
}

// TestCleanCheckoutServedWithoutChainValidationAcrossClose pins, as an
// intentional and now-ledgered tradeoff (controller sign-off — see
// docs/status.md's "Clean-and-current checkout served without chain
// validation" row and TestMissingSegmentIsLoud's own modification), the
// consequence of Commit B's sidecar refresh: once a session's clean Close
// has stamped the checkout sidecar, a LATER Checkout call for that SAME
// clean-and-current checkout is served straight from disk without ever
// touching the chain — including when the chain itself has since been
// corrupted (e.g. a segment deleted out from under it). This is not new in
// kind (ops.Checkout's clean-skip fast path already had this property for
// Checkout/Checkpoint/Rollback/Promote-produced sidecars — see Milestone 2
// Task 1), only in reach: it now also persists across an ordinary session's
// clean Close, not just those four ops entry points.
func TestCleanCheckoutServedWithoutChainValidationAcrossClose(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, s.CheckoutPath(), "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	if _, err := s.Flush("", nil); err != nil {
		t.Fatal(err)
	}
	// A second flush guarantees a SEGMENT gets written (the first flush on
	// a fresh branch is always a snapshot — txid==1 forces it), giving this
	// test a chain member deleting is meaningful to break.
	mustExec(t, s.CheckoutPath(), "INSERT INTO t VALUES (2);")
	if _, err := s.Flush("", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := w.Store.B.List(store.LineagePrefix(ref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	var victim string
	for _, k := range keys {
		if m, ok := store.ParseMemberKey(k); ok && !m.Snapshot {
			victim = k
		}
	}
	if victim == "" {
		t.Skip("no segment was written; nothing to corrupt for this test")
	}
	if err := w.Store.B.Delete(victim); err != nil {
		t.Fatal(err)
	}

	// The checkout on disk is untouched and still clean-and-current — this
	// must succeed even though the chain behind it is now broken, because
	// Checkout correctly never consults it.
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatalf("a clean-and-current checkout must be served without touching the (now-broken) chain: %v", err)
	}
	out, err := exec.Command("sqlite3", path, "SELECT v FROM t ORDER BY v;").Output()
	if err != nil || string(out) != "1\n2\n" {
		t.Fatalf("clean-skip must still serve correct, already-on-disk content: %q err=%v", out, err)
	}
}

// TestCloseAfterFailedFlushDoesNotStampSidecar guards Commit B's safety
// guard from the other side: a session that writes, then hits a flush
// failure that leaves that write unflushed (an injected non-CAS ref-write
// error, same fault as TestFlushDoesNotDeleteSnapshotOnNonCASRefFailure —
// NOT a fencing failure, so s.Err() stays nil and the pending check below is
// what actually has to catch this), must NOT stamp the sidecar on Close: a
// later Checkout must still see this checkout as needing re-materialization,
// not incorrectly treat the unflushed write as durable.
func TestCloseAfterFailedFlushDoesNotStampSidecar(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	checkoutPath := s.CheckoutPath()
	stBefore, err := os.Stat(checkoutPath)
	if err != nil {
		t.Fatal(err)
	}

	mustExec(t, checkoutPath, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")

	// Inject a non-CAS failure on the ref write, same fault
	// TestFlushDoesNotDeleteSnapshotOnNonCASRefFailure uses — installed only
	// AFTER Open (whose own AcquireLease also writes a "refs/" key) and
	// restored right after the failed Flush, so Close's later ReleaseLease
	// call isn't hit by it too.
	orig := w.Store.B
	w.Store.B = failRefPutIf{orig}
	_, err = s.Flush("", nil)
	w.Store.B = orig
	if err == nil {
		t.Fatal("expected the injected ref-write failure to fail Flush")
	}
	if s.Err() != nil {
		t.Fatalf("a non-CAS ref failure must not fence the session, got %v", s.Err())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	p2, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	stAfter, err := os.Stat(p2)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(stBefore, stAfter) {
		t.Fatal("a checkout left with unflushed content after a failed flush must not be treated as clean on the next Checkout (expected a fresh inode from re-materializing)")
	}
}

// TestCloseDoesNotStampSidecarAfterUnverifiedShutdown pins CRITICAL fix 2
// from code review: commitSidecarRefresh must gate its stamp on the capture
// engine's OWN shutdown having verified the checkout clean
// (capture.State.Clean — see engine.go's shutdown and commitSidecarRefresh's
// doc comment), not on independently re-quiescing and re-hashing the
// checkout itself. An earlier version of this function did the latter,
// which could stamp content folded in by that independent quiesce even
// when shutdown's own verification (drain, checkpoint(RESTART) matching
// what was consumed, no WAL race, checkpoint(TRUNCATE)) had explicitly
// refused to vouch for it.
//
// Forces one of shutdown's four early-return paths — the
// `int64(logN) != consumed` check right after checkpoint(RESTART) — for
// real: capture.ShutdownRaceHook lands a foreign write in the exact window
// shutdown() leaves open between releasing its final read lock and issuing
// RESTART, deterministically, the same mechanism
// TestEngineShutdownLeavesUnverifiedStateAfterRacedRestart (internal/capture)
// uses to pin that the engine itself leaves Clean=false there. This session
// must then NOT stamp the sidecar, even though everything the session
// itself was ever asked to flush WAS successfully flushed.
func TestCloseDoesNotStampSidecarAfterUnverifiedShutdown(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	checkoutPath := s.CheckoutPath()
	stBefore, err := os.Stat(checkoutPath)
	if err != nil {
		t.Fatal(err)
	}

	mustExec(t, checkoutPath, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	if _, err := s.Flush("", nil); err != nil {
		t.Fatal(err)
	}

	capture.ShutdownRaceHook = func() {
		if out, err := exec.Command("sqlite3", checkoutPath,
			"PRAGMA busy_timeout=5000; INSERT INTO t VALUES (2);").CombinedOutput(); err != nil {
			t.Errorf("hook write: %v: %s", err, out)
		}
	}
	defer func() { capture.ShutdownRaceHook = nil }()

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	p2, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	stAfter, err := os.Stat(p2)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(stBefore, stAfter) {
		t.Fatal("a close whose shutdown never verified the checkout's final state clean must not stamp the sidecar (expected a fresh inode from re-materializing)")
	}
}

// TestSidecarNotStampedAfterMidSessionRebase pins singleStartupRebase's
// gate directly: a session that took a mid-session rebase — driven here the
// same way TestFlushLoopFlushesRebaseFoldedContentWhenOtherwiseIdle does,
// with content unrelated to the actual checkout — must not stamp its
// checkout's sidecar on Close, even though everything IS otherwise flushed
// and settled (autoFlushPending() is false). Without this gate, the
// checkout — which the fake rebase never touched — would be stamped as
// matching durable content it doesn't actually contain, and a later
// Checkout would silently clean-skip instead of re-materializing the real
// (rebase-folded) content.
func TestSidecarNotStampedAfterMidSessionRebase(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", FlushEvery: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkoutPath := s.CheckoutPath()
	stBefore, err := os.Stat(checkoutPath)
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, "the session to settle after startup", func() bool {
		_, _, ok := s.LastFlush()
		return ok && !s.autoFlushPending()
	})

	snapPath := filepath.Join(t.TempDir(), "rebase-snapshot.db")
	mustExec(t, snapPath, "CREATE TABLE rebased (v); INSERT INTO rebased VALUES ('rebase-only');")
	if err := (replicaSink{s}).Rebase(snapPath); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "flushLoop to ship the rebase-folded content", func() bool {
		return !s.autoFlushPending() && s.LastFlushErr() == nil
	})

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	p2, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	stAfter, err := os.Stat(p2)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(stBefore, stAfter) {
		t.Fatal("a checkout that never physically received a mid-session rebase's content must not be treated as clean on the next Checkout (expected a fresh inode from re-materializing)")
	}
	out, err := exec.Command("sqlite3", p2, "SELECT v FROM rebased;").Output()
	if err != nil || string(out) != "rebase-only\n" {
		t.Fatalf("rebase-folded content missing after re-materializing: %q err=%v", out, err)
	}
}

// TestNamedFlushStampsCheckpointMetaAndCreatedAt is the live-session
// equivalent of ops.TestCheckpointStampsCreatedAtAndMeta: a daemon session's
// named Flush is how a checkpoint gets created against an open session (see
// server.opFlush's doc comment for why there is no separate daemon
// "checkpoint" op), so it needs the identical meta/CreatedAt behavior.
func TestNamedFlushStampsCheckpointMetaAndCreatedAt(t *testing.T) {
	testutil.RequireSQLite3(t)
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
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	txid, err := s.Flush("v1", map[string]string{"eval_run": "42"})
	if err != nil {
		t.Fatal(err)
	}

	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	cp, ok := ref.Checkpoints["v1"]
	if !ok || cp.TXID != txid {
		t.Fatalf("checkpoint v1 = %+v ok=%v, want txid %d", cp, ok, txid)
	}
	if cp.CreatedAt == "" {
		t.Fatal("named flush must stamp the checkpoint's CreatedAt")
	}
	if cp.Meta["eval_run"] != "42" {
		t.Fatalf("named flush's meta did not round-trip onto the checkpoint: %+v", cp.Meta)
	}
}

// TestFlushRejectsMetaWithoutAName guards the "meta needs a checkpoint to
// attach to" rule: an unnamed (auto or manual) flush creates no checkpoint,
// so metadata passed alongside one would otherwise be silently dropped.
func TestFlushRejectsMetaWithoutAName(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.Flush("", map[string]string{"eval_run": "42"}); err == nil {
		t.Fatal("Flush must reject non-empty meta with an empty checkpoint name")
	}
}

// TestFlushRejectsMetaOverCap mirrors ops.TestCheckpointRejectsMetaOverCap
// for the live-session path.
func TestFlushRejectsMetaOverCap(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	tooMany := map[string]string{}
	for i := 0; i < ops.MaxMetaKeys+1; i++ {
		tooMany[fmt.Sprintf("k%d", i)] = "v"
	}
	if _, err := s.Flush("v1", tooMany); err == nil {
		t.Fatal("Flush must reject metadata over the cap")
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := ref.Checkpoints["v1"]; exists {
		t.Fatal("a rejected named flush must not have created the checkpoint")
	}
}

// TestSnapshotFlushEncodesOutsideReplicaMu proves the audit-performance M2
// restructuring: a snapshot flush clones the replica to a scratch file under
// replicaMu, then RELEASES replicaMu before the O(database size)
// EncodeSnapshot — so the capture engine's Apply/Rebase are never stalled
// behind the encode. flushSnapshotEncodeHook fires exactly between the
// release and the encode; inside it the test must be able to acquire
// replicaMu (the lock is genuinely free during the encode) and must see the
// scratch clone on disk. It then proves the scratch path changes nothing
// about WHAT is written: the uploaded snapshot object is byte-identical to
// encoding the live replica directly (the pre-M2 behavior), and the scratch
// file is gone once Flush returns.
func TestSnapshotFlushEncodesOutsideReplicaMu(t *testing.T) {
	testutil.RequireSQLite3(t)
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
		"CREATE TABLE t (v); INSERT INTO t VALUES ('x'),('y');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	var hookFired, lockWasFree, scratchExisted atomic.Bool
	flushSnapshotEncodeHook = func() {
		hookFired.Store(true)
		if s.replicaMu.TryLock() {
			lockWasFree.Store(true)
			s.replicaMu.Unlock()
		}
		if m, _ := filepath.Glob(filepath.Join(s.dir, "flush-scratch-*.db")); len(m) == 1 {
			scratchExisted.Store(true)
		}
	}
	defer func() { flushSnapshotEncodeHook = nil }()

	// A session's first flush is forced to the snapshot path (startup rebase
	// sets forceSnapshot), so the hook must fire.
	txid, err := s.Flush("", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !hookFired.Load() {
		t.Fatal("first flush must take the snapshot path and hit flushSnapshotEncodeHook")
	}
	if !lockWasFree.Load() {
		t.Fatal("replicaMu must be released before the snapshot encode runs")
	}
	if !scratchExisted.Load() {
		t.Fatal("the scratch clone must exist on disk while the encode runs")
	}
	if m, _ := filepath.Glob(filepath.Join(s.dir, "flush-scratch-*.db")); len(m) != 0 {
		t.Fatalf("Flush must remove its scratch clone on return, found %v", m)
	}

	// Content equivalence with the pre-M2 behavior: encoding the live replica
	// directly (under replicaMu, as the old code did) must produce a snapshot
	// that materializes to exactly the same database bytes, with the same
	// verified trailer checksum, as what the flush uploaded from the scratch.
	// (The raw LTX streams cannot be compared byte-for-byte: EncodeSnapshot
	// stamps time.Now() into the header, so even two back-to-back direct
	// encodes differ there — a variance the old code had too. Materialize
	// verifies each stream's trailer checksum against its actual content, so
	// equal materialized files IS the full "same durable bytes, same
	// checksum" claim.) Nothing has written to the checkout since the flush
	// drained it, so the replica is still frozen at txid.
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	uploaded, _, err := w.Store.B.Get(store.SnapshotKey(ref.Lineage, ref.HeadEpoch, txid))
	if err != nil {
		t.Fatalf("uploaded snapshot object: %v", err)
	}
	var direct bytes.Buffer
	s.replicaMu.Lock()
	_, err = ltxio.EncodeSnapshot(s.replica.Path(), txid, &direct)
	s.replicaMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	fromUpload := filepath.Join(t.TempDir(), "from-upload.db")
	fromDirect := filepath.Join(t.TempDir(), "from-direct.db")
	if _, err := ltxio.Materialize(bytes.NewReader(uploaded), fromUpload); err != nil {
		t.Fatalf("materialize uploaded snapshot: %v", err)
	}
	if _, err := ltxio.Materialize(bytes.NewReader(direct.Bytes()), fromDirect); err != nil {
		t.Fatalf("materialize direct encode: %v", err)
	}
	a, err := os.ReadFile(fromUpload)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(fromDirect)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("scratch-clone encode diverged from direct encode: materialized %d vs %d bytes",
			len(a), len(b))
	}
}
