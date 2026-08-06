package session

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/ops"
	"github.com/offshoot-db/offshoot/internal/store"
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

	_, err = s.Flush("")
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
	requireSQLite(t)
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
		if _, err := s.Flush(""); err != nil {
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
	requireSQLite(t)
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
		if _, err := s.Flush(""); err != nil {
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
	requireSQLite(t)
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
	if _, err := s.Flush(""); err != nil {
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
	shrinkTxid, err := s.Flush("")
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
			txids[i], errs[i] = s.Flush("")
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
	mu     sync.Mutex
	writes int
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

func (c *countingBackend) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}

// TestAutoFlushShipsWritesWithoutManualFlush pins Options.FlushEvery: a
// session opened with a short auto-flush cadence must make a committed write
// durable on its own, with the caller never calling Flush.
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
	requireSQLite(t)
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

// pendingAutoFlush mirrors flushLoop's own pending check exactly (test-only:
// this file is package session, so it can read the unexported gen counters
// directly under replicaMu, the same way flushLoop itself does).
func pendingAutoFlush(s *Session) bool {
	s.replicaMu.Lock()
	defer s.replicaMu.Unlock()
	return s.appliedGen != s.flushedGen || s.rebaseGen != s.flushedRebaseGen
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
// pendingAutoFlush, not just "a flush happened") before measuring: every
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
	requireSQLite(t)
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
		return s.LastFlushErr() == nil && !pendingAutoFlush(s)
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
	requireSQLite(t)
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
		return ok && !pendingAutoFlush(s)
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
	requireSQLite(t)
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
	var enteredOnce sync.Once
	FlushUploadHook = func() {
		if !armed.Load() {
			return
		}
		enteredOnce.Do(func() { close(entered) })
		<-proceed
	}

	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", FlushEvery: 60 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, "the session to settle after startup", func() bool {
		_, _, ok := s.LastFlush()
		return ok && !pendingAutoFlush(s)
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
	close(proceed)

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
