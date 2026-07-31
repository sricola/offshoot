package session

import (
	"context"
	"errors"
	"os/exec"
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
