package session

import (
	"context"
	"fmt"
	"os/exec"
	"testing"

	"github.com/offshoot-db/offshoot/internal/store"
)

// TestEveryFlushIsExactlyMaterializable is the format change's core contract:
// whatever a flush reported durable must materialize to exactly the database
// the agent had at that moment, whether it was stored as a snapshot or a
// segment.
func TestEveryFlushIsExactlyMaterializable(t *testing.T) {
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

	for i := 0; i < 10; i++ {
		if out, err := exec.Command("sqlite3", s.CheckoutPath(),
			fmt.Sprintf("INSERT INTO t (v) VALUES ('row-%d');", i)).CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		name := fmt.Sprintf("cp%d", i)
		if _, err := s.Flush(name); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
		// Every checkpoint so far must still materialize to the right row count.
		for j := 0; j <= i; j++ {
			br := fmt.Sprintf("check-%d-%d", i, j)
			if _, err := w.Fork("app", "main", br, fmt.Sprintf("cp%d", j)); err != nil {
				t.Fatalf("fork at cp%d: %v", j, err)
			}
			p, err := w.Checkout("app", br)
			if err != nil {
				t.Fatalf("checkout %s: %v", br, err)
			}
			out, err := exec.Command("sqlite3", p, "SELECT count(*) FROM t;").Output()
			if err != nil {
				t.Fatalf("%s: %v", br, err)
			}
			if want := fmt.Sprintf("%d\n", j+1); string(out) != want {
				t.Fatalf("cp%d materialized %s rows, want %s", j, out, want)
			}
		}
	}
}

// TestChainSurvivesSessionRestart proves a chain written by one session is
// readable and extendable by the next.
func TestChainSurvivesSessionRestart(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s1, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s1.CheckoutPath(), "CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	for i := 0; i < 3; i++ {
		if out, err := exec.Command("sqlite3", s1.CheckoutPath(), "INSERT INTO t VALUES ('a');").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		if _, err := s1.Flush(""); err != nil {
			t.Fatal(err)
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	out, err := exec.Command("sqlite3", s2.CheckoutPath(), "SELECT count(*) FROM t;").Output()
	if err != nil || string(out) != "3\n" {
		t.Fatalf("second session sees %q err=%v", out, err)
	}
	if out, err := exec.Command("sqlite3", s2.CheckoutPath(), "INSERT INTO t VALUES ('b');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := s2.Flush(""); err != nil {
		t.Fatalf("flush after restart: %v", err)
	}
	// w.Checkout materializes to the same fixed checkout path s2 already has
	// open; the capture engine holds a continuous read lock on that path (by
	// design — see engine.beginRead's doc comment) until the session closes,
	// so Checkout must wait for that release first, exactly like every other
	// checkout-after-live-session test in this package (e.g.
	// TestFlushedStateIsRecoverableByAnotherWorkspace in flush_test.go).
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	out, err = exec.Command("sqlite3", p, "SELECT count(*) FROM t;").Output()
	if err != nil {
		t.Fatalf("checkout query: %v", err)
	}
	if string(out) != "4\n" {
		t.Fatalf("rows after restart+flush = %q", out)
	}
}

// TestMissingSegmentIsLoud proves a chain with a deleted member fails closed
// rather than silently materializing an older state.
func TestMissingSegmentIsLoud(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	for i := 0; i < 3; i++ {
		if out, err := exec.Command("sqlite3", s.CheckoutPath(), "INSERT INTO t VALUES ('x');").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		if _, err := s.Flush(""); err != nil {
			t.Fatal(err)
		}
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
		t.Skip("no segment was written; nothing to remove")
	}
	if err := w.Store.B.Delete(victim); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Checkout("app", "main"); err == nil {
		t.Fatal("a missing chain member must fail loudly, not silently read an older state")
	}
}
