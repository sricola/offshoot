package session

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/ltxio"
	"github.com/offshoot-db/offshoot/internal/store"
)

// TestFlushesUnderContinuousWritesAreConsistent runs an agent writing in a
// loop while the daemon flushes repeatedly. Every flushed snapshot must be a
// valid SQLite database whose row count is monotonically non-decreasing — a
// torn or half-applied flush would show up as a decode failure or a count that
// goes backwards.
func TestFlushesUnderContinuousWritesAreConsistent(t *testing.T) {
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
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			exec.Command("sqlite3", s.CheckoutPath(),
				"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").Run()
		}
	}()

	dir := t.TempDir()
	var lastCount int
	flushes := 0
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && flushes < 8 {
		txid, err := s.Flush(fmt.Sprintf("f%d", flushes))
		if err != nil {
			close(stop)
			<-done
			t.Fatalf("flush %d failed: %v", flushes, err)
		}
		flushes++

		// Materialize exactly what the store now holds and check it.
		ref, _, err := w.Store.GetRef("app", "main")
		if err != nil {
			t.Fatal(err)
		}
		data, _, err := w.Store.B.Get(store.SnapshotKey(ref.Lineage, ref.HeadEpoch, txid))
		if err != nil {
			t.Fatalf("flushed snapshot %d missing: %v", txid, err)
		}
		out := fmt.Sprintf("%s/check-%d.db", dir, txid)
		if _, err := ltxio.Materialize(bytesReader(data), out); err != nil {
			t.Fatalf("flushed snapshot %d does not decode: %v", txid, err)
		}
		res, err := exec.Command("sqlite3", out, "PRAGMA integrity_check; SELECT count(*) FROM t;").Output()
		if err != nil {
			t.Fatalf("flushed snapshot %d is not a usable database: %v", txid, err)
		}
		var ok string
		var count int
		if _, err := fmt.Sscanf(string(res), "%s\n%d", &ok, &count); err != nil {
			t.Fatalf("unexpected sqlite output %q: %v", res, err)
		}
		if ok != "ok" {
			t.Fatalf("integrity_check on flushed snapshot %d = %q", txid, ok)
		}
		if count < lastCount {
			t.Fatalf("row count went backwards across flushes: %d then %d", lastCount, count)
		}
		lastCount = count
		time.Sleep(100 * time.Millisecond)
	}
	close(stop)
	<-done

	if flushes < 3 {
		t.Fatalf("only %d flushes completed; the test did not exercise contention", flushes)
	}
	if lastCount == 0 {
		t.Fatal("no rows were ever captured — the writer and capture never overlapped")
	}
	if s.Err() != nil {
		t.Fatalf("session errored under load: %v", s.Err())
	}
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
