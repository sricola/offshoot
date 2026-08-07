package ops

import (
	"errors"
	"os/exec"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

// TestStaleWriterCannotCorruptLineage simulates the pause-and-resume hazard:
// holder A is fenced by holder B, then wakes up and writes. A's object must
// land under a dead epoch and must not be reachable from any ref.
func TestStaleWriterCannotCorruptLineage(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES ('good');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "good", nil); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	a, err := w.Store.AcquireLease("app", "main", "writer-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	// A is fenced by B reclaiming after expiry.
	b, err := w.Store.AcquireLease("app", "main", "writer-b", time.Minute, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if b.Epoch <= a.Epoch {
		t.Fatalf("reclaim must bump the epoch: %d -> %d", a.Epoch, b.Epoch)
	}

	// A wakes up and writes an object under ITS epoch, as a resumed writer
	// mid-flush would.
	staleKey := store.SnapshotKey(mustRef(t, w).Lineage, a.Epoch, 99)
	if err := w.Store.B.Put(staleKey, []byte("garbage-from-a-fenced-writer")); err != nil {
		t.Fatal(err)
	}

	// The branch is untouched: content still reads, and nothing references
	// the stale object.
	p2, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatalf("fenced writer damaged the branch: %v", err)
	}
	got, _ := exec.Command("sqlite3", p2, "SELECT v FROM t;").Output()
	if string(got) != "good\n" {
		t.Fatalf("content = %q, want good", got)
	}
	ref := mustRef(t, w)
	for name, cp := range ref.Checkpoints {
		if cp.Epoch == a.Epoch {
			t.Fatalf("checkpoint %q references the fenced epoch %d", name, a.Epoch)
		}
	}
	if ref.HeadEpoch == a.Epoch && ref.HeadTXID == 99 {
		t.Fatal("head references the fenced writer's object")
	}

	// A's attempt to advance the ref is refused outright.
	if _, err := w.Store.RenewLease(a, time.Minute, now.Add(3*time.Minute)); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("fenced renew: want ErrLeaseLost, got %v", err)
	}
}

// TestFencedEpochObjectsAreCollectable proves the fenced writer's garbage is
// not immortal: it lives in the branch's lineage, so destroying the branch
// and running GC removes it along with everything else in that lineage.
func TestFencedEpochObjectsAreCollectable(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	ref := mustRef(t, w)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	a, err := w.Store.AcquireLease("app", "main", "writer-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Store.AcquireLease("app", "main", "writer-b", time.Minute, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	staleKey := store.SnapshotKey(ref.Lineage, a.Epoch, 42)
	if err := w.Store.B.Put(staleKey, []byte("garbage")); err != nil {
		t.Fatal(err)
	}

	if err := w.Destroy("app", "main", true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.B.Get(staleKey); err == nil {
		t.Fatal("fenced-epoch object survived GC of its lineage")
	}
}

func mustRef(t *testing.T, w *Workspace) store.Ref {
	t.Helper()
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	return ref
}
