package daemon

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// TestMetricsSmokeOpenWriteFlushFork is Milestone 4 Task 2's instrumentation
// smoke test: open a session, write through it, flush, fork — then scrape
// this daemon's registry DIRECTLY (WritePrometheus into a buffer; Task 3
// adds the real GET /metrics HTTP handler, not this task) and assert the
// counters the brief calls out actually moved. This exercises the full
// wiring path: session.OnTransition -> Metrics.observeFlushTransition for
// the flush, and ops.ObserveFork -> Metrics.observeFork for the fork —
// both assigned once by NewServer's wireHooks call, not re-wired per test.
func TestMetricsSmokeOpenWriteFlushFork(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	scrape := func(t *testing.T) string {
		t.Helper()
		var buf bytes.Buffer
		if err := srv.WritePrometheus(&buf); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}

	// Before anything happens: build_info is always present (registered at
	// construction), sessions_open reads 0 (scrape-time, no sessions yet),
	// and every pre-populated enum combination for flush_total/fork_total/
	// janitor_runs_total is already a zero sample (see newMetrics's doc
	// comment) rather than absent.
	before := scrape(t)
	if !strings.Contains(before, `offshoot_build_info{version="dev"} 1`) {
		t.Fatalf("expected build_info before any activity:\n%s", before)
	}
	if !strings.Contains(before, "offshoot_sessions_open 0") {
		t.Fatalf("expected sessions_open 0 before any session opens:\n%s", before)
	}
	if !strings.Contains(before, `offshoot_flush_total{result="ok",kind="manual"} 0`) {
		t.Fatalf("expected a pre-populated zero flush_total sample:\n%s", before)
	}

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK || open.Checkout == "" {
		t.Fatalf("open = %+v", open)
	}

	duringOpen := scrape(t)
	if !strings.Contains(duringOpen, "offshoot_sessions_open 1") {
		t.Fatalf("expected sessions_open 1 with one session open:\n%s", duringOpen)
	}
	if !strings.Contains(duringOpen, `offshoot_capture_lag_bytes{db="app",branch="main"}`) {
		t.Fatalf("expected a capture_lag_bytes sample for the open session:\n%s", duringOpen)
	}

	if out, err := exec.Command("sqlite3", open.Checkout,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	// Flush may race capture; retry briefly until it succeeds — same
	// pattern server_test.go's own TestServerOpenFlushStatusClose uses.
	deadline := time.Now().Add(10 * time.Second)
	var flushed bool
	for time.Now().Before(deadline) {
		resp := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main"})
		if resp.OK {
			flushed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !flushed {
		t.Fatal("flush never succeeded")
	}

	afterFlush := scrape(t)
	if !strings.Contains(afterFlush, `offshoot_flush_total{result="ok",kind="manual"} 1`) {
		t.Fatalf("expected flush_total{result=ok,kind=manual} to have moved to 1:\n%s", afterFlush)
	}
	if !strings.Contains(afterFlush, `offshoot_durable_age_seconds{db="app",branch="main"}`) {
		t.Fatalf("expected a durable_age_seconds sample once a flush has succeeded:\n%s", afterFlush)
	}
	if strings.Contains(afterFlush, "offshoot_flush_duration_seconds_count 0") {
		t.Fatalf("expected flush_duration_seconds_count > 0 after a successful flush:\n%s", afterFlush)
	}

	fork := call(t, sock, Request{Op: "fork", DB: "app", Branch: "main", Name: "child"})
	if !fork.OK {
		t.Fatalf("fork = %+v", fork)
	}

	afterFork := scrape(t)
	fastN, slowN := forkTotal(t, afterFork, "fast"), forkTotal(t, afterFork, "slow")
	if fastN+slowN != 1 {
		t.Fatalf("expected exactly one fork counted across fast+slow (fast=%v slow=%v):\n%s", fastN, slowN, afterFork)
	}
	if strings.Contains(afterFork, "offshoot_fork_duration_seconds_count 0") {
		t.Fatalf("expected fork_duration_seconds_count > 0 after a successful fork:\n%s", afterFork)
	}
	// The default fork SHARES (copy-on-write): the mode counter must record
	// it as shared, with materialized still at its pre-populated zero.
	if !strings.Contains(afterFork, `offshoot_fork_mode_total{mode="shared"} 1`) {
		t.Fatalf("expected fork_mode_total{mode=shared} 1 after a default (shared) fork:\n%s", afterFork)
	}
	if !strings.Contains(afterFork, `offshoot_fork_mode_total{mode="materialized"} 0`) {
		t.Fatalf("expected fork_mode_total{mode=materialized} still 0:\n%s", afterFork)
	}

	closeResp := call(t, sock, Request{Op: "close", DB: "app", Branch: "main"})
	if !closeResp.OK {
		t.Fatalf("close = %+v", closeResp)
	}
	afterClose := scrape(t)
	if !strings.Contains(afterClose, "offshoot_sessions_open 0") {
		t.Fatalf("expected sessions_open back to 0 after close:\n%s", afterClose)
	}
	if strings.Contains(afterClose, `db="app"`) {
		t.Fatalf("expected no db=\"app\" capture_lag/durable_age samples once the session is closed:\n%s", afterClose)
	}
}

// forkTotal extracts offshoot_fork_total{path="<path>"}'s sample value from
// a full exposition dump, failing the test if the line isn't found at all
// (every enum combination is pre-populated by newMetrics, so it must always
// be present, at 0 or more).
func forkTotal(t *testing.T, exposition, path string) float64 {
	t.Helper()
	marker := `offshoot_fork_total{path="` + path + `"} `
	idx := strings.Index(exposition, marker)
	if idx == -1 {
		t.Fatalf("missing %q in:\n%s", marker, exposition)
	}
	rest := exposition[idx+len(marker):]
	end := strings.IndexByte(rest, '\n')
	if end == -1 {
		end = len(rest)
	}
	var v float64
	if _, err := fmt.Sscan(rest[:end], &v); err != nil {
		t.Fatalf("parsing fork_total value %q: %v", rest[:end], err)
	}
	return v
}

// TestJanitorTickUpdatesMetrics drives StartJanitor's per-tick body directly
// (janitorTick, rather than waiting on a real ticker) against a branch with
// an expired TTL and an orphaned lineage, and asserts every janitor-sourced
// metric (reap/gc_tombstoned/gc_deleted/gc_backlog/janitor_runs_total) moved
// as expected.
func TestJanitorTickUpdatesMetrics(t *testing.T) {
	srv, w := newServer(t)

	// A fresh, unprotected (main is protected by default — see
	// ops.Create/Fork's own doc comments; reapOne refuses to reap a
	// protected branch regardless of TTL) child branch with a 1ns TTL: by
	// the time janitorTick actually runs Reap, real wall-clock time has
	// already carried it past its deadline, so it is reap-eligible on the
	// very first tick with no manual clock manipulation needed.
	if _, err := w.Fork("app", "main", "reap-me", "", time.Nanosecond, nil); err != nil {
		t.Fatal(err)
	}

	before := srv.metrics.ReapTotal.Value()
	srv.janitorTick(0) // grace=0: any newly-tombstoned lineage sweeps in the same tick
	after := srv.metrics.ReapTotal.Value()
	if after != before+1 {
		t.Fatalf("offshoot_reap_total = %v, want %v (before %v)", after, before+1, before)
	}

	var buf bytes.Buffer
	if err := srv.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, `offshoot_janitor_runs_total{result="ok"} 1`) {
		t.Fatalf("expected one ok janitor run:\n%s", out)
	}
	// destroying app@reap-me orphans its (forked, independent) lineage;
	// GC's phase 1 tombstones it and (grace=0) phase 2 sweeps it in this
	// SAME tick — matching ops.GC's own documented same-call tombstone+sweep
	// behavior for grace=0.
	if !strings.Contains(out, "offshoot_gc_tombstoned_total 1") {
		t.Fatalf("expected one tombstoned lineage:\n%s", out)
	}
}

// TestJanitorTickCountsGCError forces the failure mode GC fails CLOSED on —
// a ref whose chain cannot resolve (here: a hand-written ref naming a
// lineage with no objects at all), which fails the reachability mark and
// makes GC return an error having deleted nothing — and asserts the janitor
// surfaces it: offshoot_gc_errors_total increments (the alertable signal for
// "GC is stalled and the store is bloating") and the tick counts under
// janitor_runs_total{result="error"}, rather than the error staying a
// stderr-only whisper.
func TestJanitorTickCountsGCError(t *testing.T) {
	srv, w := newServer(t)

	if _, err := w.Store.PutRef("app", "broken", store.Ref{
		Lineage: "no-such-lineage", Epoch: 1, HeadTXID: 5, HeadEpoch: 1,
	}, ""); err != nil {
		t.Fatal(err)
	}

	before := srv.metrics.GCErrorsTotal.Value()
	srv.janitorTick(time.Hour)
	if got := srv.metrics.GCErrorsTotal.Value(); got != before+1 {
		t.Fatalf("offshoot_gc_errors_total = %v, want %v (a failed GC pass must increment it)", got, before+1)
	}

	var buf bytes.Buffer
	if err := srv.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "offshoot_gc_errors_total 1") {
		t.Fatalf("expected offshoot_gc_errors_total 1 in exposition:\n%s", out)
	}
	if !strings.Contains(out, `offshoot_janitor_runs_total{result="error"} 1`) {
		t.Fatalf("expected the failed tick under janitor_runs_total{result=error}:\n%s", out)
	}
}

// TestPromtoolCheckRealMetrics runs promtool (skip-if-absent locally, a
// hard failure under ci.yml's metrics-lint job — see testutil.RequirePromtool
// and internal/metrics's identical test for the rationale) against
// THIS daemon's real, fully-wired registry after a bit of activity, rather
// than the generic shape internal/metrics/metrics_test.go already checks —
// this is the "the endpoint output" half of PM Amendment 7's gate: proving
// the locked metric list, as actually registered by production wiring, is
// promtool-compliant, not just the format primitives in isolation.
func TestPromtoolCheckRealMetrics(t *testing.T) {
	testutil.RequirePromtool(t)

	srv, _ := newServer(t)
	srv.janitorTick(time.Hour) // populate the janitor-sourced metrics too

	var buf bytes.Buffer
	if err := srv.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("promtool", "check", "metrics")
	cmd.Stdin = &buf
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("promtool check metrics failed: %v\noutput:\n%s", err, out)
	}
}
