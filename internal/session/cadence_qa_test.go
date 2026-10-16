package session

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// trailingSegments returns the length of the trailing run of segment
// members in db@branch's resolved chain at head — the branch's DURABLE
// divergence from its last snapshot, exactly what flush.go's seeding probe
// reads.
func trailingSegments(t *testing.T, w *ops.Workspace, db, branch string) int {
	t.Helper()
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		t.Fatal(err)
	}
	chain, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for i := len(chain) - 1; i >= 0 && !chain[i].Snapshot; i-- {
		n++
	}
	return n
}

// ownMembers lists the snapshot and segment members stored under db@branch's
// OWN lineage prefix (a shared fork's base pointer is not a member and is
// skipped by ParseMemberKey), returned as (snapshots, segments).
func ownMembers(t *testing.T, w *ops.Workspace, db, branch string) (snaps, segs []store.ChainMember) {
	t.Helper()
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := w.Store.B.List(store.LineagePrefix(ref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		m, ok := store.ParseMemberKey(k)
		if !ok {
			continue
		}
		if m.Snapshot {
			snaps = append(snaps, m)
		} else {
			segs = append(segs, m)
		}
	}
	return snaps, segs
}

// TestSharedForkSessionSeedsSnapshotCadenceFromDurableDivergence pins
// docs/reference.md#session-flush's snapshot cadence as a property of the
// BRANCH, not the process (status.md's bounded-replay row; Session's
// divergenceSeeded doc comment): a session opened on a copy-on-write
// SHARED fork seeds its counter from the durable trailing-segment run of
// the resolved chain, so with SnapshotEvery N and a parent already N−2
// segments past its last snapshot, the child's FIRST snapshot lands on its
// 2nd flush — (N − divergence) — not its Nth. Mutation verified against:
// deleting `s.flushesSinceSnapshot = trailing` in flush.go's seeding
// probe (the child then needs N flushes to snapshot).
func TestSharedForkSessionSeedsSnapshotCadenceFromDurableDivergence(t *testing.T) {
	testutil.RequireSQLite3(t)
	const every = 4
	w := newWS(t)
	w.SnapshotEvery = every // keeps the fork floor and the divergence floor in agreement, as the daemon does
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}

	// Parent: a clean-at-open session (checkout materialized fresh against
	// head, as TestCleanAtOpenSessionsFirstRealFlushIsASegment does) so its
	// flushes append segments directly onto Create's txid-1 snapshot.
	if _, err := w.Checkout("app", "main"); err != nil {
		t.Fatal(err)
	}
	parent, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main", SnapshotEvery: every})
	if err != nil {
		t.Fatal(err)
	}
	if !parent.cleanAtOpen {
		t.Fatal("precondition: parent must be clean at open (settling flush suppressed)")
	}
	waitFor(t, 5*time.Second, "the parent's suppressed startup rebase to settle", func() bool {
		return !parent.autoFlushPending()
	})
	mustExec(t, parent.CheckoutPath(), "CREATE TABLE t (v);")
	rows := 0
	for i := 0; trailingSegments(t, w, "app", "main") != every-2; i++ {
		if i >= 2*every {
			t.Fatalf("parent never reached a trailing run of %d segments within %d flushes", every-2, i)
		}
		mustExec(t, parent.CheckoutPath(), fmt.Sprintf("INSERT INTO t VALUES ('parent-%d');", i))
		rows++
		if _, err := parent.Flush("", nil); err != nil {
			t.Fatalf("parent flush %d: %v", i, err)
		}
	}
	if err := parent.Close(); err != nil {
		t.Fatal(err)
	}
	divergence := trailingSegments(t, w, "app", "main")
	if divergence != every-2 {
		t.Fatalf("precondition: parent divergence = %d, want %d", divergence, every-2)
	}

	// Child: a SHARED fork (base pointer, no objects of its own).
	if _, err := w.Fork("app", "main", "kid", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	kidRef, _, err := w.Store.GetRef("app", "kid")
	if err != nil {
		t.Fatal(err)
	}
	if kidRef.Base == nil {
		t.Fatalf("precondition: the fork must be shared (base pointer), got ref %+v", kidRef)
	}
	if snaps, segs := ownMembers(t, w, "app", "kid"); len(snaps)+len(segs) != 0 {
		t.Fatalf("a shared fork must start with no members of its own, got %d snapshots %d segments", len(snaps), len(segs))
	}
	if got := trailingSegments(t, w, "app", "kid"); got != divergence {
		t.Fatalf("the child's resolved chain must carry the parent's divergence %d, got %d", divergence, got)
	}

	if _, err := w.Checkout("app", "kid"); err != nil {
		t.Fatal(err)
	}
	kid, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "kid", SnapshotEvery: every})
	if err != nil {
		t.Fatal(err)
	}
	if !kid.cleanAtOpen {
		t.Fatal("precondition: child must be clean at open (settling flush suppressed)")
	}
	waitFor(t, 5*time.Second, "the child's suppressed startup rebase to settle", func() bool {
		return !kid.autoFlushPending()
	})

	// Flush 1 (seeded counter = divergence = N−2): a segment.
	mustExec(t, kid.CheckoutPath(), "INSERT INTO t VALUES ('kid-1');")
	tx1, err := kid.Flush("", nil)
	if err != nil {
		t.Fatal(err)
	}
	snaps, segs := ownMembers(t, w, "app", "kid")
	if len(snaps) != 0 || len(segs) != 1 || segs[0].MaxTXID != tx1 {
		t.Fatalf("after the child's 1st flush (txid %d): %d snapshots %d segments %+v, want exactly one segment at that txid", tx1, len(snaps), len(segs), segs)
	}

	// Flush 2 = (N − divergence): the cadence is due — the child's first
	// snapshot lands here, not on its Nth flush.
	mustExec(t, kid.CheckoutPath(), "INSERT INTO t VALUES ('kid-2');")
	tx2, err := kid.Flush("", nil)
	if err != nil {
		t.Fatal(err)
	}
	snaps, segs = ownMembers(t, w, "app", "kid")
	if len(snaps) != 1 || snaps[0].MaxTXID != tx2 {
		t.Fatalf("after the child's 2nd flush (txid %d) = flush (N − divergence) = %d: snapshots %+v, want exactly one at txid %d (the counter must be seeded from the parent's %d durable segments)",
			tx2, every-divergence, snaps, tx2, divergence)
	}
	if len(segs) != 1 {
		t.Fatalf("after the child's 2nd flush: segments %+v, want only the 1st flush's", segs)
	}
	if got := trailingSegments(t, w, "app", "kid"); got != 0 {
		t.Fatalf("the child's resolved chain must end on its new snapshot, trailing segments = %d", got)
	}
	if err := kid.Close(); err != nil {
		t.Fatal(err)
	}

	// The seeded chain still resolves end to end: the parent's rows plus
	// the child's two.
	path, err := w.Checkout("app", "kid")
	if err != nil {
		t.Fatal(err)
	}
	out, err := sqlite3CLI(path, "SELECT count(*) FROM t;").Output()
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("%d\n", rows+2); string(out) != want {
		t.Fatalf("child rows after the seeded cadence = %q, want %q", out, want)
	}
}
