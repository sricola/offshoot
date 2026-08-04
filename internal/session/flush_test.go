package session

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
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
	defer s.Close()

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
