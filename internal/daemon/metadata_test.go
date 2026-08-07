package daemon

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/ops"
)

// TestOpDbsListsSortedDatabases exercises the new "dbs" op end to end: every
// database this store has at least one ref for, sorted, as
// store.Store.ListRefs's keys — see opDbs's doc comment.
func TestOpDbsListsSortedDatabases(t *testing.T) {
	srv, w := newServer(t) // newServer already creates "app"
	sock := srv.SocketPath()

	if err := w.Create("zeta"); err != nil {
		t.Fatal(err)
	}
	if err := w.Create("beta"); err != nil {
		t.Fatal(err)
	}

	r := call(t, sock, Request{Op: "dbs"})
	if !r.OK {
		t.Fatalf("dbs op failed: %+v", r)
	}
	want := []string{"app", "beta", "zeta"}
	if len(r.Databases) != len(want) {
		t.Fatalf("databases = %v, want %v", r.Databases, want)
	}
	for i, db := range want {
		if r.Databases[i] != db {
			t.Fatalf("databases = %v, want %v (sorted)", r.Databases, want)
		}
	}
}

// TestOpDbsOnEmptyStoreIsEmptyNotError pins that a store with nothing
// created yet returns ok=true with an empty (or nil) list, not an error —
// unlike "branches", which errors on an unknown db (there's no single db to
// name here; the whole point of "dbs" is discovering what exists).
func TestOpDbsOnEmptyStoreIsEmptyNotError(t *testing.T) {
	dir := t.TempDir()
	w, err := ops.Init(dir + "/store")
	if err != nil {
		t.Fatal(err)
	}
	// Short-named temp dir for the socket, independent of t.TempDir() (which
	// nests under the test's full name): unix socket paths are capped at
	// ~104-108 bytes on several platforms, notably macOS — see newServer's
	// identical comment in server_test.go.
	sockDir, err := os.MkdirTemp("", "offshoot-daemon-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	srv, err := NewServer(w, sockDir+"/sock")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})

	r := call(t, srv.SocketPath(), Request{Op: "dbs"})
	if !r.OK {
		t.Fatalf("dbs on an empty store must not error: %+v", r)
	}
	if len(r.Databases) != 0 {
		t.Fatalf("dbs on an empty store = %v, want empty", r.Databases)
	}
}

// TestForkMetaLandsOnBranchesResponse exercises the daemon "fork" op's new
// Meta field end to end, and that "branches" surfaces the enriched
// touched_at/checkpoints_v2 fields alongside the untouched, wire-compat
// "checkpoints" names array.
func TestForkMetaLandsOnBranchesResponse(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	r := call(t, sock, Request{
		Op: "fork", DB: "app", Branch: "main", Name: "attempt-1",
		Meta: map[string]string{"eval_run": "42"},
	})
	if !r.OK {
		t.Fatalf("fork with meta failed: %+v", r)
	}

	br := call(t, sock, Request{Op: "branches", DB: "app"})
	if !br.OK {
		t.Fatalf("branches failed: %+v", br)
	}
	info := branchInfo(t, br, "attempt-1")
	if info.TouchedAt == "" {
		t.Fatal("BranchInfo.TouchedAt must be populated for a freshly forked branch")
	}
	found := false
	for _, name := range info.Checkpoints {
		if name == "fork" {
			found = true
		}
	}
	if !found {
		t.Fatalf("Checkpoints (the old field) must still list 'fork': %+v", info.Checkpoints)
	}
	var cpV2 *CheckpointInfo
	for i := range info.CheckpointsV2 {
		if info.CheckpointsV2[i].Name == "fork" {
			cpV2 = &info.CheckpointsV2[i]
		}
	}
	if cpV2 == nil {
		t.Fatalf("CheckpointsV2 must list 'fork': %+v", info.CheckpointsV2)
	}
	if cpV2.CreatedAt == "" {
		t.Fatalf("CheckpointsV2 entry for 'fork' must have a CreatedAt: %+v", cpV2)
	}
	if cpV2.TXID == 0 {
		t.Fatalf("CheckpointsV2 entry for 'fork' must have a nonzero txid: %+v", cpV2)
	}

	// Meta itself doesn't ride the wire on BranchInfo (checkpoints_v2 is
	// {name, txid, created_at} only) — verify it landed in the store
	// instead, which is where a client would look it up via a future op if
	// one is ever added; this task only requires it be stored.
	ref, _, err := srv.ws.Store.GetRef("app", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	if ref.Meta["eval_run"] != "42" {
		t.Fatalf("fork's meta did not land on the child ref: %+v", ref.Meta)
	}
}

// TestForkRejectsMetaOverCapOverDaemon confirms the daemon surfaces
// ops.ValidateMeta's cap refusal as an ordinary error response, not a panic
// or a silently-truncated map.
func TestForkRejectsMetaOverCapOverDaemon(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	tooMany := map[string]string{}
	for i := 0; i < ops.MaxMetaKeys+1; i++ {
		tooMany[string(rune('a'+i%26))+string(rune('0'+i/26))] = "v"
	}
	r := call(t, sock, Request{Op: "fork", DB: "app", Branch: "main", Name: "attempt-1", Meta: tooMany})
	if r.OK {
		t.Fatal("fork over the meta cap must be refused")
	}
}

// TestOpForkValidatesMetaBeforeFlushingAnOpenSession pins opFork's
// validate-then-flush ordering: ops.ValidateMeta(req.Meta) must run BEFORE
// flushIfOpen, so an over-cap fork against a branch with a live, unflushed
// session is refused without ever flushing that session — the fork is going
// to fail regardless of what flushIfOpen would have done, and ValidateMeta
// is cheap, in-memory, and needs no store I/O to run first. Before the fix,
// flushIfOpen ran first, so an over-cap fork against an open session still
// paid for (and produced the durable side effect of) a flush before opFork
// ever looked at Meta.
func TestOpForkValidatesMetaBeforeFlushingAnOpenSession(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	t.Cleanup(func() { call(t, sock, Request{Op: "close", DB: "app", Branch: "main"}) })
	// A write the caller never flushes: if flushIfOpen ran, this is exactly
	// what it would carry into a durable flush.
	sqliteExec(t, open.Checkout, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")

	before, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	tooMany := map[string]string{}
	for i := 0; i < ops.MaxMetaKeys+1; i++ {
		tooMany[string(rune('a'+i%26))+string(rune('0'+i/26))] = "v"
	}
	r := call(t, sock, Request{Op: "fork", DB: "app", Branch: "main", Name: "over-cap-child", Meta: tooMany})
	if r.OK {
		t.Fatal("fork with over-cap meta against an open session must be refused")
	}

	after, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if after.HeadTXID != before.HeadTXID {
		t.Fatalf("an over-cap fork must not flush the open session: main's head_txid went from %d to %d",
			before.HeadTXID, after.HeadTXID)
	}

	if _, _, err := w.Store.GetRef("app", "over-cap-child"); err == nil {
		t.Fatal("a refused fork must not create the child branch")
	}
}

// TestFlushMetaCreatesCheckpointWithMeta exercises the daemon "flush" op's
// new Meta field: a live session's named flush IS how a checkpoint is
// created against it (see server.opFlush's doc comment), so meta rides
// there rather than a separate "checkpoint" op.
func TestFlushMetaCreatesCheckpointWithMeta(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open failed: %+v", open)
	}
	t.Cleanup(func() { call(t, sock, Request{Op: "close", DB: "app", Branch: "main"}) })

	r := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main", Name: "v1", Meta: map[string]string{"agent": "claude"}})
	if !r.OK {
		t.Fatalf("named flush with meta failed: %+v", r)
	}

	ref, _, err := srv.ws.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	cp, ok := ref.Checkpoints["v1"]
	if !ok {
		t.Fatal("named flush must create the checkpoint")
	}
	if cp.Meta["agent"] != "claude" {
		t.Fatalf("flush's meta did not land on the checkpoint: %+v", cp.Meta)
	}
	if cp.CreatedAt == "" {
		t.Fatal("named flush must stamp the checkpoint's CreatedAt")
	}
}

// --- Wire compatibility: an old-shaped client (no "meta" field, or no
// field at all in the raw JSON) must keep working against the new daemon
// for every op this task touched. Requests are written as raw JSON text
// here, not via the Request struct, so this actually proves the ABSENCE of
// the new field decodes fine — not just that a Go zero value round-trips.

// rawJSONCall writes a literal JSON request line (bypassing the Request struct
// entirely, to prove decoding tolerates bytes an old client would actually
// send) and returns the decoded Response.
func rawJSONCall(t *testing.T, sock, rawJSON string) Response {
	t.Helper()
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte(rawJSON + "\n")); err != nil {
		t.Fatal(err)
	}
	var resp Response
	if err := json.NewDecoder(c).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestWireCompatOldShapedForkRequestWorks(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	// Exactly the fields an old client (pre-Milestone-3) would have sent:
	// no "meta" key at all.
	resp := rawJSONCall(t, sock, `{"op":"fork","db":"app","branch":"main","name":"attempt-1"}`)
	if !resp.OK {
		t.Fatalf("old-shaped fork request must still work: %+v", resp)
	}
}

func TestWireCompatOldShapedFlushRequestWorks(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	open := rawJSONCall(t, sock, `{"op":"open","db":"app","branch":"main"}`)
	if !open.OK {
		t.Fatalf("open failed: %+v", open)
	}
	t.Cleanup(func() { call(t, sock, Request{Op: "close", DB: "app", Branch: "main"}) })

	resp := rawJSONCall(t, sock, `{"op":"flush","db":"app","branch":"main","name":"v1"}`)
	if !resp.OK {
		t.Fatalf("old-shaped flush request must still work: %+v", resp)
	}
}

func TestWireCompatOldShapedBranchesRequestWorks(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	resp := rawJSONCall(t, sock, `{"op":"branches","db":"app"}`)
	if !resp.OK {
		t.Fatalf("old-shaped branches request must still work: %+v", resp)
	}
	if len(resp.Branches) == 0 {
		t.Fatal("expected at least the main branch")
	}
	// An old client only ever reads Checkpoints; it must still be populated
	// exactly as before, regardless of the new CheckpointsV2/TouchedAt
	// fields riding alongside it.
	main := branchInfo(t, resp, "main")
	found := false
	for _, name := range main.Checkpoints {
		if name == "init" {
			found = true
		}
	}
	if !found {
		t.Fatalf("Checkpoints must still list 'init': %+v", main.Checkpoints)
	}
}
