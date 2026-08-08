package session

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// TestFlushesUnderContinuousWritesAreConsistent runs an agent writing in a
// loop while the daemon flushes repeatedly. Every flushed snapshot must be a
// valid SQLite database whose row count is monotonically non-decreasing — a
// torn or half-applied flush would show up as a decode failure or a count that
// goes backwards.
//
// SnapshotEvery: 1 here is deliberate, not incidental: this test verifies
// consistency under contention by fetching each flush's OWN object directly
// and decoding it standalone via ltxio.Materialize, which only understands a
// full snapshot — resolving a snapshot+segment chain is a separate concern
// tasks 1-3 already cover elsewhere. Forcing every flush to snapshot keeps
// this test focused on what it actually exists to catch (a torn or dropped
// write under load), independent of the cadence feature.
func TestFlushesUnderContinuousWritesAreConsistent(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main", SnapshotEvery: 1})
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
		txid, err := s.Flush(fmt.Sprintf("f%d", flushes), nil)
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

// TestFlushIncludesEverythingCommittedBeforeIt is the direct contract test
// for DrainNow/Flush: a row committed synchronously immediately before a
// Flush call must be present in that exact flushed snapshot. This is
// DrainNow's documented contract ("every transaction already committed to
// the checkout's WAL as of the moment this call is issued is captured
// before it returns") verified directly, not inferred.
//
// TestFlushesUnderContinuousWritesAreConsistent above exercises similar
// contention but only asserts row counts never go backwards across
// flushes — an assertion a short (but still monotonically-growing) flush
// satisfies just fine, so it cannot catch the hazard this test targets: a
// flush whose snapshot silently omits a specific write that was already
// durably committed to the checkout before Flush was even called. Nor does
// a plain continuous-writer harness reliably catch it on its own: the
// capture engine's checkpoint-takeover path (see internal/capture/engine.go's
// takeover()) periodically performs its own full resync via a raw file
// copy whenever it detects a fold it hadn't consumed — which, empirically,
// papers over a too-short drain almost every time a background writer is
// simply left running unthrottled, masking the exact hazard this test needs
// to expose. To isolate drain's own behavior from that safety net, this
// test pins a second connection's read transaction open for the duration of
// each round: WAL readers never block writers, so the row-writing goroutine
// below is unaffected, but a pinned old snapshot means checkpoint can never
// fully succeed, so takeover's fold-detected rebase can never fire — only
// Flush's own DrainNow-driven catch-up determines what gets captured.
//
// Verified to fail against the pre-fix engine.go, where drain() was bounded
// only by a 2s wall-clock drainBudget with no way to distinguish "caught up
// to everything" from "ran out of time" (both returned (n, nil) alike): with
// the checkpoint-blocking connection pinned open, most rounds' marker row
// was completely absent from the flushed snapshot despite Flush reporting
// success — see the task-7 hardening report for the captured failure output
// (and the row counts proving the omission wasn't just "a bit behind": the
// flushed snapshot was missing the marker AND most of that round's backlog
// entirely, not merely trailing it).
//
// SnapshotEvery: 1 is deliberate here too, for the same reason as
// TestFlushesUnderContinuousWritesAreConsistent above: this test decodes
// each flush's OWN object directly with ltxio.Materialize (a full-snapshot-
// only decoder) to isolate DrainNow's contract from chain-resolution
// concerns, which are exercised elsewhere.
func TestFlushIncludesEverythingCommittedBeforeIt(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main", SnapshotEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if out, err := exec.Command("sqlite3", s.CheckoutPath(),
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	wdb, err := sql.Open("sqlite3", s.CheckoutPath()+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	defer wdb.Close()
	wdb.SetMaxOpenConns(1)

	dir := t.TempDir()
	const rounds = 4
	const backlogPerRound = 300
	for i := 0; i < rounds; i++ {
		// Pin an old snapshot open on a second connection: this blocks
		// checkpoint (and therefore takeover's fold-detected rebase) for the
		// duration of the round without blocking the writer above — see the
		// doc comment for why that isolation is what makes this test
		// deterministic instead of load-dependent.
		blocker, err := sql.Open("sqlite3", s.CheckoutPath()+"?_busy_timeout=5000")
		if err != nil {
			t.Fatalf("round %d: open blocker: %v", i, err)
		}
		blocker.SetMaxOpenConns(1)
		if _, err := blocker.Exec("BEGIN; SELECT count(*) FROM sqlite_master;"); err != nil {
			t.Fatalf("round %d: pin blocker snapshot: %v", i, err)
		}

		// A bounded backlog, large enough that a single drainBudget-limited
		// drain() call cannot fully consume it — see drainBudget's doc
		// comment for the ~100-250-transactions-per-2s throughput this
		// exceeds.
		for j := 0; j < backlogPerRound; j++ {
			if _, err := wdb.Exec("INSERT INTO t (v) VALUES (randomblob(64));"); err != nil {
				t.Fatalf("round %d: backlog insert %d: %v", i, j, err)
			}
		}
		// The marker: the last row committed before Flush is called, and so
		// the exact write DrainNow's contract says Flush must not omit.
		marker := fmt.Sprintf("marker-%d", i)
		if _, err := wdb.Exec("INSERT INTO t (v) VALUES (?);", marker); err != nil {
			t.Fatalf("round %d: insert marker: %v", i, err)
		}

		txid, err := s.Flush(fmt.Sprintf("marker-flush-%d", i), nil)
		flushErr := err

		// Release the blocker regardless of outcome, so the engine can
		// checkpoint/rebase and settle before the next round.
		blocker.Exec("ROLLBACK")
		blocker.Close()

		if flushErr != nil {
			t.Fatalf("round %d: flush failed: %v", i, flushErr)
		}

		// Materialize exactly the snapshot this flush wrote, the same way a
		// fresh checkout of the branch at this txid would.
		ref, _, err := w.Store.GetRef("app", "main")
		if err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		data, _, err := w.Store.B.Get(store.SnapshotKey(ref.Lineage, ref.HeadEpoch, txid))
		if err != nil {
			t.Fatalf("round %d: flushed snapshot %d missing: %v", i, txid, err)
		}
		out := fmt.Sprintf("%s/marker-check-%d.db", dir, txid)
		if _, err := ltxio.Materialize(bytesReader(data), out); err != nil {
			t.Fatalf("round %d: flushed snapshot %d does not decode: %v", i, txid, err)
		}

		res, err := exec.Command("sqlite3", out,
			fmt.Sprintf("SELECT count(*) FROM t WHERE v = '%s';", marker)).CombinedOutput()
		if err != nil {
			t.Fatalf("round %d: query flushed snapshot: %v: %s", i, err, res)
		}
		if count := strings.TrimSpace(string(res)); count != "1" {
			t.Fatalf("round %d: marker row %q committed to the checkout before Flush(%q) is missing from "+
				"the flushed snapshot (txid %d, count=%s) — Flush advanced the branch head to a snapshot "+
				"that omits a write already committed before Flush was called",
				i, marker, fmt.Sprintf("marker-flush-%d", i), txid, count)
		}

		// Let the ticker path checkpoint/rebase cleanly before the next
		// round pins a fresh blocker.
		time.Sleep(300 * time.Millisecond)
	}

	if s.Err() != nil {
		t.Fatalf("session errored under load: %v", s.Err())
	}
}
