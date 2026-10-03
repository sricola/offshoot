package daemon

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/session"
)

// TestOpBranchesReportsPendingForInFlightOpen exercises the daemon-only
// half of the state split ops.BranchStateAt's doc comment describes: while
// an opOpen has reserved a session slot but is still inside its (slow,
// unlocked) session.Open call, "branches" must report "pending" for that
// branch — even though ops.BranchStateAt itself, given only the ref and
// checkout at this point, would say something else (idle here, since no
// lease has been acquired yet and there's no checkout). Uses the same
// openDelay test hook TestShutdownDuringInFlightOpenLeavesNoLease (server_
// test.go) already established for holding an open deterministically
// in-flight instead of racing real timing.
func TestOpBranchesReportsPendingForInFlightOpen(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	entered := make(chan struct{})
	proceed := make(chan struct{})
	openDelay = func() {
		close(entered)
		<-proceed
	}
	defer func() { openDelay = nil }()

	openDone := make(chan struct{})
	go func() {
		defer close(openDone)
		rawCall(sock, Request{Op: "open", DB: "app", Branch: "main"})
	}()
	<-entered // opOpen has reserved app@main and is blocked before session.Open

	resp := call(t, sock, Request{Op: "branches", DB: "app"})
	if !resp.OK {
		t.Fatalf("branches = %+v", resp)
	}
	br := branchInfo(t, resp, "main")
	if br.State != "pending" {
		t.Fatalf("state = %q, want pending", br.State)
	}

	openDelay = nil
	close(proceed) // let the blocked open finish so the test can clean up
	<-openDone
}

// TestOpBranchesReportsErrorForFencedSession exercises the daemon-only
// "error" state: a session that IS open here, but whose Err() has gone
// non-nil, must report "error" — outranking even what ops.BranchStateAt
// would independently compute from the ref alone. That precedence is the
// interesting case this test pins: after the rival AcquireLease below, the
// ref itself shows a live lease held by "thief", so ops.BranchStateAt on
// the ref ALONE would say "active" — this daemon still correctly reports
// "error" because it knows, from its own session map, that ITS session is
// the one that's dead, not merely that someone holds a lease.
//
// Fencing is manufactured exactly as session package's own
// TestRenewalDetectsFencingAndEndsSession does (a session opened directly,
// not through the daemon's opOpen, so the test controls its LeaseTTL/
// RenewEvery — opOpen itself has no knob for either, and the real defaults
// are far too slow for a test to wait out): RenewEvery is set well beyond
// LeaseTTL so the lease actually lapses before the session's renewal loop
// gets a chance to renew it, then a rival AcquireLease steals it and the
// test polls Err() until the renewal loop notices. The resulting session
// is inserted directly into srv.sessions (white-box — this test file is in
// package daemon) to put the daemon in the state opOpen would have left it
// in had this been a real daemon-driven open.
func TestOpBranchesReportsErrorForFencedSession(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	sess, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", Holder: "session-a",
		LeaseTTL: 100 * time.Millisecond, RenewEvery: 400 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sess.Close() })

	// Wait out the TTL before stealing — see session package's own
	// TestRenewalDetectsFencingAndEndsSession for exactly why this can't be
	// skipped: stealing before the lease actually lapses just loses the CAS
	// race (ErrLeaseHeld), it doesn't fence anything.
	time.Sleep(150 * time.Millisecond)
	if _, err := w.AcquireLease("app", "main", "thief", ops.DefaultLeaseTTL); err != nil {
		t.Fatalf("rival acquire must succeed once session-a's lease lapses: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for sess.Err() == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if sess.Err() == nil {
		t.Fatal("session did not detect fencing in time")
	}

	srv.mu.Lock()
	srv.sessions[key("app", "main")] = sess
	srv.mu.Unlock()

	resp := call(t, sock, Request{Op: "branches", DB: "app"})
	if !resp.OK {
		t.Fatalf("branches = %+v", resp)
	}
	br := branchInfo(t, resp, "main")
	if br.State != "error" {
		t.Fatalf("state = %q, want error (must outrank the ref's own live lease)", br.State)
	}
}

// TestOpBranchesReportsActiveForHealthyOpenSession pins the flip side: an
// open session with no error needs NO daemon-side casing at all — its own
// live lease is exactly what already makes ops.BranchStateAt itself report
// "active", so this is really a test that branchState doesn't accidentally
// override a healthy session's state with something else.
func TestOpBranchesReportsActiveForHealthyOpenSession(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}

	resp := call(t, sock, Request{Op: "branches", DB: "app"})
	if !resp.OK {
		t.Fatalf("branches = %+v", resp)
	}
	br := branchInfo(t, resp, "main")
	if br.State != "active" {
		t.Fatalf("state = %q, want active", br.State)
	}
}

// TestBranchesReportsAnAtRestCheckpointAsActive is a wiring pin: a lease
// under a checkpoint: holder (acquired by hand here; a real running
// checkpoint's is asserted through ops.BranchState, Status and Leases in
// internal/ops's TestConcurrentAtRestCheckpointsAreSerialized) reaches the
// daemon's branches op as state active with that holder, which is what
// both SDKs' Branch.state and Branch.lease_holder carry.
func TestBranchesReportsAnAtRestCheckpointAsActive(t *testing.T) {
	srv, w := newServer(t)
	holder := "checkpoint:" + ops.LocalHolder() + "/0123abcd"
	if _, err := w.AcquireLease("app", "main", holder, time.Minute); err != nil {
		t.Fatal(err)
	}
	resp := call(t, srv.SocketPath(), Request{Op: "branches", DB: "app"})
	if !resp.OK {
		t.Fatalf("branches = %+v", resp)
	}
	if br := branchInfo(t, resp, "main"); br.State != "active" || br.LeaseHolder != holder {
		t.Fatalf("branch during a checkpoint: state %q holder %q, want active and %q", br.State, br.LeaseHolder, holder)
	}
}

// TestOpenRefusedWhileACheckpointHoldsTheBranch is a wiring pin: the
// daemon's open op (what an SDK session uses) on a branch held under a
// checkpoint: holder (acquired by hand here; the real checkpoint is
// internal/ops's TestSessionOpenDuringAtRestCheckpointIsRefused) fails
// with the lease-held error naming the holder, and leaves the lease alone.
func TestOpenRefusedWhileACheckpointHoldsTheBranch(t *testing.T) {
	srv, w := newServer(t)
	holder := "checkpoint:" + ops.LocalHolder() + "/0123abcd"
	l, err := w.AcquireLease("app", "main", holder, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := rawCall(srv.SocketPath(), Request{Op: "open", DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.OK || !strings.Contains(resp.Error, "lease is held") || !strings.Contains(resp.Error, holder) {
		t.Fatalf("open during a checkpoint = %+v, want a lease-held refusal naming %s", resp, holder)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != holder || ref.Epoch != l.Epoch {
		t.Fatalf("the refused open changed the checkpoint's lease: %q@%d", ref.LeaseHolder, ref.Epoch)
	}
}
