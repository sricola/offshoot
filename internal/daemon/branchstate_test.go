package daemon

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/session"
	"github.com/sricola/offshoot/internal/store"
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
	srv.sessions[key("app", "main")] = &slot{sess: sess}
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

// TestOpenRefusedByCheckpointHolderSaysInProgress is a wiring pin: the
// daemon's open op (what an SDK session uses) on a branch held under a
// checkpoint: holder (acquired by hand here; the real checkpoint is
// internal/ops's TestSessionOpenDuringAtRestCheckpointIsRefused) says a
// checkpoint is in progress and to retry, since that lease clears within
// seconds, and leaves the lease alone.
func TestOpenRefusedByCheckpointHolderSaysInProgress(t *testing.T) {
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
	if want := "daemon: a checkpoint is in progress on app@main; retry in a few seconds"; resp.OK || resp.Error != want {
		t.Fatalf("open during a checkpoint = %+v, want the error %q", resp, want)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != holder || ref.Epoch != l.Epoch {
		t.Fatalf("the refused open changed the checkpoint's lease: %q@%d", ref.LeaseHolder, ref.Epoch)
	}
}

// orphanWant is the refusal of an open under ref's lease when ref's holder
// is the one this daemon recorded as orphaned on app@main.
func orphanWant(ref store.Ref) string {
	expiry, _ := time.Parse(time.RFC3339Nano, ref.LeaseExpiry)
	return "daemon: app@main is held by an earlier session of this daemon whose lease release failed (holder " + ref.LeaseHolder +
		"); it lapses at " + ops.LapseTime(expiry) +
		"; free it now with 'offshoot lease release app@main --holder " + ref.LeaseHolder + "'"
}

// orphanAppMain opens app@main on srv and closes it with every release
// attempt failing (fr armed), so the daemon records that session's holder
// as orphaned, and returns the ref the close left behind.
func orphanAppMain(t *testing.T, sock string, w *ops.Workspace, fr *failReleases) store.Ref {
	t.Helper()
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open = %+v", r)
	}
	holder := getStatus(t, sock, "app", "main").Holder
	fr.arm(3) // every attempt Close's release makes
	if r := call(t, sock, Request{Op: "close", DB: "app", Branch: "main"}); r.OK || !strings.Contains(r.Error, "injected release failure") {
		t.Fatalf("close = %+v, want the injected release failure", r)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != holder || !store.LeaseLive(ref, time.Now()) {
		t.Fatalf("after the failed close the ref holds %q (live %v), want the closed session's %q", ref.LeaseHolder, store.LeaseLive(ref, time.Now()), holder)
	}
	return ref
}

// TestOpenRefusedByOwnOrphanedLeaseNamesTheHolder: a session of this
// daemon whose close could not release its lease leaves it live under the
// session's own holder, which the daemon records. An open of the branch is
// refused with the wording for exactly that lease: an earlier session of
// this daemon, its exact holder, when it lapses (rounded up), and the
// `lease release --holder` that frees only it. Once it is freed, the next
// open succeeds under a new holder.
func TestOpenRefusedByOwnOrphanedLeaseNamesTheHolder(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	fr := &failReleases{Backend: w.Store.B}
	w.Store.B = fr // before any session exists
	orphan := orphanAppMain(t, sock, w, fr)

	resp, err := rawCall(sock, Request{Op: "open", DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if want := orphanWant(orphan); resp.OK || resp.Error != want {
		t.Fatalf("open under this daemon's orphaned lease = %+v, want the error %q", resp, want)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != orphan.LeaseHolder || ref.Epoch != orphan.Epoch {
		t.Fatalf("the refused open changed the orphaned lease: %q@%d", ref.LeaseHolder, ref.Epoch)
	}

	if err := w.ReleaseLeaseByHolder("app", "main", orphan.LeaseHolder); err != nil {
		t.Fatal(err)
	}
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open after the orphan was freed = %+v", r)
	}
	if h := getStatus(t, sock, "app", "main").Holder; h == orphan.LeaseHolder {
		t.Fatalf("the reopen holds as the orphan's holder %s", h)
	}
}

// TestTwinDaemonPrefixGetsGenericAdvice: a live session: holder that this
// daemon never minted is not its orphan, even when it carries this
// daemon's own session:<host>/<pid>/ prefix, as the holders of a second
// daemon in a look-alike container (same hostname, offshoot at PID 1) do.
// That session may be live, so the refusal is ops.LeaseHeldAdvice's: close
// it, or, if its daemon has exited, free it by its exact holder; never
// "an earlier session of this daemon ... free it now". A bare holder (a
// `lease acquire`) keeps the plain lease-held error.
func TestTwinDaemonPrefixGetsGenericAdvice(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	twin := "session:" + ops.LocalHolder() + "/deadbeef"
	l, err := w.AcquireLease("app", "main", twin, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := rawCall(sock, Request{Op: "open", DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	want := "daemon: " + ops.LeaseHeldAdvice("app", "main", ref)
	if resp.OK || resp.Error != want || strings.Contains(resp.Error, "earlier session of this daemon") {
		t.Fatalf("open under a twin daemon's lease = %+v, want the error %q", resp, want)
	}
	for _, part := range []string{
		`held by "` + twin + `"`, "(an open daemon session)", "close the session",
		"if the daemon that held it has exited, free it with 'offshoot lease release app@main --holder " + twin + "'",
	} {
		if !strings.Contains(resp.Error, part) {
			t.Fatalf("twin refusal %q lacks %q", resp.Error, part)
		}
	}
	if ref.LeaseHolder != twin || ref.Epoch != l.Epoch {
		t.Fatalf("the refused open changed the twin's lease: %q@%d", ref.LeaseHolder, ref.Epoch)
	}

	if err := w.ReleaseLease(l); err != nil {
		t.Fatal(err)
	}
	bare := ops.LocalHolder()
	if _, err := w.AcquireLease("app", "main", bare, time.Minute); err != nil {
		t.Fatal(err)
	}
	resp, err = rawCall(sock, Request{Op: "open", DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.OK || strings.Contains(resp.Error, "earlier session of this daemon") || !strings.Contains(resp.Error, "lease is held") || !strings.Contains(resp.Error, bare) {
		t.Fatalf("open under a bare holder's lease = %+v, want the plain lease-held refusal naming %s", resp, bare)
	}
}

// TestRollbackAndCompactRefusalsOfferNoForce: the rollback and compact ops
// take no force (nor do the SDKs' rollback and compact), so their refusal
// of a branch an at-rest checkpoint holds says to retry when it finishes,
// without ops' --force advice for the CLI.
func TestRollbackAndCompactRefusalsOfferNoForce(t *testing.T) {
	srv, w := newServer(t)
	holder := "checkpoint:" + ops.LocalHolder() + "/0123abcd"
	if _, err := w.AcquireLease("app", "main", holder, time.Minute); err != nil {
		t.Fatal(err)
	}
	for _, req := range []Request{
		{Op: "rollback", DB: "app", Branch: "main", Name: "seed", NoBackup: true},
		{Op: "compact", DB: "app", Branch: "main"},
	} {
		resp, err := rawCall(srv.SocketPath(), req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.OK || !strings.Contains(resp.Error, holder) || !strings.Contains(resp.Error, "retry when it finishes") || strings.Contains(resp.Error, "--force") {
			t.Fatalf("%s during a checkpoint = %+v, want a refusal that says to retry and offers no --force", req.Op, resp)
		}
	}
}

// TestOrphanedHolderRefusalNeverNamesALiveSession: the orphaned-holder
// refusal reads the ref while the failed open's reservation still stands.
// Read after the reservation is released, it could see a session another
// open of this daemon took in between, and its `lease release --holder`
// advice would fence that live session. openRefusedReleased runs such an
// open in exactly that window.
func TestOrphanedHolderRefusalNeverNamesALiveSession(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	fr := &failReleases{Backend: w.Store.B}
	w.Store.B = fr // before any session exists
	orphanRef := orphanAppMain(t, sock, w, fr)
	orphan := orphanRef.LeaseHolder
	var once sync.Once
	var other Response
	var otherErr error
	openRefusedReleased = func() {
		once.Do(func() {
			if otherErr = w.ReleaseLeaseByHolder("app", "main", orphan); otherErr == nil {
				other, otherErr = rawCall(sock, Request{Op: "open", DB: "app", Branch: "main"})
			}
		})
	}
	t.Cleanup(func() { openRefusedReleased = nil })

	resp, err := rawCall(sock, Request{Op: "open", DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if otherErr != nil || !other.OK {
		t.Fatalf("the open in the window = %+v, %v; want it to succeed", other, otherErr)
	}
	live := getStatus(t, sock, "app", "main").Holder
	if strings.Contains(resp.Error, live) {
		t.Fatalf("the refusal names the live session's holder %s: %q", live, resp.Error)
	}
	if want := orphanWant(orphanRef); resp.OK || resp.Error != want {
		t.Fatalf("open refused by the orphaned lease = %+v, want the error %q", resp, want)
	}
}

// TestStatusHolderIsTheSessionsOwn: status reports an open session's
// holder as session:<host>/<pid>/<8 hex>, the holder its session.Open
// generated.
func TestStatusHolderIsTheSessionsOwn(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open = %+v", r)
	}
	st := getStatus(t, sock, "app", "main")
	shape := regexp.MustCompile(`^session:` + regexp.QuoteMeta(ops.LocalHolder()) + `/[0-9a-f]{8}$`)
	if !shape.MatchString(st.Holder) {
		t.Fatalf("status holder %q, want it to match %s", st.Holder, shape)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != st.Holder {
		t.Fatalf("status holder %q, ref holder %q", st.Holder, ref.LeaseHolder)
	}
}
