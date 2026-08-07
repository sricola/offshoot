package daemon

import (
	"os/exec"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/store"
)

// TestServeSnapshotEveryWiringDrivesCadence proves -snapshot-every's daemon
// wiring (Server.SetSnapshotEvery, plumbed through opOpen into
// session.Options.SnapshotEvery, Milestone 4 Task 6a) actually reaches the
// session's flush cadence: with SetSnapshotEvery(4) and more than four
// flushes over the socket, the resulting materialization chain both stays
// bounded by the cadence (mirroring ops_test's
// TestReplayStaysBoundedAcrossManyFlushes) and shows more than one snapshot
// across the lineage's full object listing — proof the 4-flush cadence
// actually recurred, not just the mandatory first-ever snapshot.
func TestServeSnapshotEveryWiringDrivesCadence(t *testing.T) {
	srv, w := newServer(t)
	srv.SetSnapshotEvery(4)
	sock := srv.SocketPath()

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK || open.Checkout == "" {
		t.Fatalf("open = %+v", open)
	}
	if out, err := exec.Command("sqlite3", open.Checkout, "CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("CREATE TABLE failed: %v: %s", err, out)
	}

	for i := 0; i < 9; i++ {
		if out, err := exec.Command("sqlite3", open.Checkout,
			"INSERT INTO t VALUES ('x');").CombinedOutput(); err != nil {
			t.Fatalf("INSERT %d failed: %v: %s", i, err, out)
		}
		// Flush may race capture; retry briefly until it lands, mirroring
		// TestServerOpenFlushStatusClose's own retry loop.
		deadline := time.Now().Add(10 * time.Second)
		var ok bool
		for time.Now().Before(deadline) {
			resp := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main"})
			if resp.OK {
				ref, _, err := w.Store.GetRef("app", "main")
				if err != nil {
					t.Fatal(err)
				}
				if ref.HeadTXID == resp.TXID {
					ok = true
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !ok {
			t.Fatalf("flush %d never landed", i)
		}
	}

	cl := call(t, sock, Request{Op: "close", DB: "app", Branch: "main"})
	if !cl.OK {
		t.Fatalf("close = %+v", cl)
	}

	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	// Bounded replay: a chain to head must stay within the 4-flush cadence,
	// exactly like ops_test.TestReplayStaysBoundedAcrossManyFlushes asserts
	// for the embeddable session library directly — this is the same
	// guarantee, now proven to hold when SnapshotEvery arrives via the
	// daemon flag instead of session.Options set directly.
	chain, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) > 4 {
		t.Fatalf("replay must stay bounded by the -snapshot-every 4 cadence, chain is %d members", len(chain))
	}
	if !chain[0].Snapshot {
		t.Fatal("a chain must start at a snapshot")
	}

	// Cadence recurrence: with 9 flushes at a 4-flush cadence, more than the
	// single mandatory first-ever snapshot must have been written — proof
	// the daemon-plumbed cadence actually repeated, not just fired once at
	// session start regardless of -snapshot-every's value.
	keys, err := w.Store.B.List(store.LineagePrefix(ref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	var snapshots int
	for _, k := range keys {
		if m, ok := store.ParseMemberKey(k); ok && m.Snapshot {
			snapshots++
		}
	}
	if snapshots < 2 {
		t.Fatalf("expected the 4-flush cadence to recur across 9 flushes (>1 snapshot), got %d snapshot(s)", snapshots)
	}
}
