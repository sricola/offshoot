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
	if _, err := a.Flush("", nil); err == nil {
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
