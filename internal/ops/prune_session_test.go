package ops_test

import (
	"context"
	"testing"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/session"
	"github.com/sricola/offshoot/internal/testutil"
)

// TestPruneBesideALiveSessionStaysPruned pins PruneCheckpoints' safety
// claim: a prune landing while a session holds the branch neither fails
// the session's next flush nor is undone by it, since the flush rebuilds
// its head write from a fresh read of the ref. (External package: it needs
// internal/session.)
func TestPruneBesideALiveSessionStaysPruned(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := session.Open(context.Background(), session.Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	leaseSQL(t, s.CheckoutPath(), "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	if _, err := s.Flush("one", nil); err != nil {
		t.Fatal(err)
	}

	// The prune lands inside the next flush, after it read the ref and
	// before its head write, so that write loses its compare-and-swap and
	// must rebuild from the pruned ref. main is protected by default; the
	// forced prune is the live-session case under test, not the gate.
	pruned := false
	session.FlushEncodeHook = func() {
		if _, err := w.PruneCheckpoints("app", "main", ops.PruneOptions{Names: []string{"one"}, Force: true}); err != nil {
			t.Errorf("prune beside a live session: %v", err)
			return
		}
		pruned = true
	}
	defer func() { session.FlushEncodeHook = nil }()

	leaseSQL(t, s.CheckoutPath(), "INSERT INTO t VALUES (2);")
	if _, err := s.Flush("two", nil); err != nil {
		t.Fatalf("flush racing a prune: %v", err)
	}
	session.FlushEncodeHook = nil
	if !pruned {
		t.Fatal("the prune never ran inside the flush")
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ref.Checkpoints["one"]; ok {
		t.Fatal("the session's flush put the pruned checkpoint back")
	}
	if _, ok := ref.Checkpoints["two"]; !ok {
		t.Fatal("the session's own checkpoint is missing")
	}
}
