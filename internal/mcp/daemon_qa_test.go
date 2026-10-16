package mcp

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/store"
)

// QA regression tests for the MCP tools against a real daemon
// (newDaemonTools, daemon_test.go). Each test's doc comment names the spec
// sentence it pins and the production mutation it was verified to catch.
// Helpers mustRef/sameHead/oversizedMeta live in contract_qa_test.go.

// TestPromoteRefusesClosingTargetSessionWithRetry pins docs/reference.md's
// closing-session rule for offshoot_promote (daemon_test.go covers rollback
// only): a target whose daemon session is closing is refused with the
// "retry" wording — never "close it before", since its close is already
// running — and the target's ref and safety-fork slot are untouched.
//
// Mutation verified: tools.go refuseIfSessionOpen, the
// `if info.State == daemon.SessionStateClosing { ... }` branch deleted —
// the refusal falls through to the generic "close it before promoting onto
// it" wording.
func TestPromoteRefusesClosingTargetSessionWithRetry(t *testing.T) {
	ts, w, sock := newDaemonTools(t)
	for _, b := range []string{"attempt-1", "attempt-2"} {
		if _, err := w.Fork("app", "main", b, "", 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	closed, release := closingDaemonSession(t, sock, "app", "attempt-2") // the promote TARGET
	before := mustRef(t, w, "app", "attempt-2")
	r := call(t, ts, "offshoot_promote", map[string]any{
		"database": "app", "source": "attempt-1", "target": "attempt-2"})
	after := mustRef(t, w, "app", "attempt-2")
	release()
	<-closed

	got := text(r)
	if !r.IsError || !strings.Contains(got, "is closing; retry promoting onto it in a few seconds") ||
		strings.Contains(got, "close it before") {
		t.Fatalf("promote onto a closing target = %s, want the closing refusal", got)
	}
	if !sameHead(before, after) {
		t.Fatalf("attempt-2 must be untouched by the refused promote: before=%+v after=%+v", before, after)
	}
	if _, _, err := w.Store.GetRef("app", "attempt-2"+ops.PromoteBackupSuffix); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a refused promote must mint no safety fork, GetRef err = %v", err)
	}
}

// TestDestroyRefusesClosingSessionWithRetry is
// TestPromoteRefusesClosingTargetSessionWithRetry's destroy sibling: a
// branch whose daemon session is closing is refused with the "retry"
// wording and still exists, unchanged, afterwards.
//
// Mutation verified: the same deleted closing branch in
// refuseIfSessionOpen — the refusal reads "close it before destroying it".
func TestDestroyRefusesClosingSessionWithRetry(t *testing.T) {
	ts, w, sock := newDaemonTools(t)
	if _, err := w.Fork("app", "main", "attempt-1", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	closed, release := closingDaemonSession(t, sock, "app", "attempt-1")
	before := mustRef(t, w, "app", "attempt-1")
	r := call(t, ts, "offshoot_destroy", map[string]any{"database": "app", "branch": "attempt-1"})
	after, _, err := w.Store.GetRef("app", "attempt-1")
	release()
	<-closed

	got := text(r)
	if !r.IsError || !strings.Contains(got, "is closing; retry destroying it in a few seconds") ||
		strings.Contains(got, "close it before") {
		t.Fatalf("destroy during a close = %s, want the closing refusal", got)
	}
	if err != nil {
		t.Fatalf("attempt-1 must survive the refused destroy: %v", err)
	}
	if !sameHead(before, after) {
		t.Fatalf("attempt-1 must be untouched: before=%+v after=%+v", before, after)
	}
}

// TestRepointsRefuseOpenSessionEvenWithForce pins docs/reference.md and
// docs/agents.md ("If a daemon session is open on this branch, the call is
// refused instead of proceeding — `force` does not override this") for all
// three repointing tools, with force:true on the call and the server both
// without and WITH -allow-force (the case where ops would otherwise honor
// force and bypass the session's lease). daemon_test.go's open-session
// refusals never set force. The refusal is the "close it before <op>"
// wording (not ops' own lease error, which would mean the guard was
// skipped) and the branch's ref is unchanged.
//
// Mutations verified: tools.go destroy, `if r, refused :=
// t.refuseIfSessionOpen(...); refused {` changed to `refused && !a.Force`
// — with -allow-force the forced destroy deletes the branch under its
// session, without it the refusal becomes ops' lease error; the same edit
// in promote; and the refuseIfSessionOpen call deleted from rollback.
func TestRepointsRefuseOpenSessionEvenWithForce(t *testing.T) {
	for _, allow := range []bool{false, true} {
		t.Run(fmt.Sprintf("allowForce=%v", allow), func(t *testing.T) {
			ts, w, sock := newDaemonTools(t)
			ts.SetAllowForce(allow)
			for _, b := range []string{"attempt-rb", "attempt-src", "attempt-tgt", "attempt-dst"} {
				if _, err := w.Fork("app", "main", b, "", 0, nil); err != nil {
					t.Fatal(err)
				}
			}
			// The rollback branch gets two live checkpoints so its head is
			// past the one rolled back to; a refused rollback leaves it there.
			rbPath := openSession(t, sock, "app", "attempt-rb")
			sqliteExec(t, rbPath, "CREATE TABLE t (v); INSERT INTO t VALUES ('v1');")
			if r := call(t, ts, "offshoot_checkpoint", map[string]any{
				"database": "app", "branch": "attempt-rb", "name": "v1"}); r.IsError {
				t.Fatalf("checkpoint v1: %s", text(r))
			}
			sqliteExec(t, rbPath, "INSERT INTO t VALUES ('v2');")
			if r := call(t, ts, "offshoot_checkpoint", map[string]any{
				"database": "app", "branch": "attempt-rb", "name": "v2"}); r.IsError {
				t.Fatalf("checkpoint v2: %s", text(r))
			}
			openSession(t, sock, "app", "attempt-tgt") // the promote TARGET
			openSession(t, sock, "app", "attempt-dst")

			cases := []struct {
				tool, branch, wording string
				args                  map[string]any
			}{
				{"offshoot_rollback", "attempt-rb", "is open on app@attempt-rb; close it before rolling it back",
					map[string]any{"database": "app", "branch": "attempt-rb", "to": "v1", "force": true}},
				{"offshoot_promote", "attempt-tgt", "is open on app@attempt-tgt; close it before promoting onto it",
					map[string]any{"database": "app", "source": "attempt-src", "target": "attempt-tgt", "force": true}},
				{"offshoot_destroy", "attempt-dst", "is open on app@attempt-dst; close it before destroying it",
					map[string]any{"database": "app", "branch": "attempt-dst", "force": true}},
			}
			for _, c := range cases {
				before := mustRef(t, w, "app", c.branch)
				r := call(t, ts, c.tool, c.args)
				if !r.IsError || !strings.Contains(text(r), c.wording) {
					t.Fatalf("%s with force:true against an open session = %s, want a refusal containing %q",
						c.tool, text(r), c.wording)
				}
				after, _, err := w.Store.GetRef("app", c.branch)
				if err != nil {
					t.Fatalf("%s: app@%s must survive the refused call: %v", c.tool, c.branch, err)
				}
				if !sameHead(before, after) {
					t.Fatalf("%s: app@%s must be untouched: before=%+v after=%+v", c.tool, c.branch, before, after)
				}
			}
			if rb := mustRef(t, w, "app", "attempt-rb"); rb.HeadTXID != rb.Checkpoints["v2"].TXID {
				t.Fatalf("attempt-rb head = %d, want v2's %d (the refused rollback must not move it)",
					rb.HeadTXID, rb.Checkpoints["v2"].TXID)
			}
		})
	}
}

// TestLiveCheckpointMetaStoredAndCapped pins offshoot_checkpoint's `meta`
// ("string->string, at most 32 keys") on the LIVE path (an open daemon
// session, so the checkpoint is the daemon's flush op): the map is carried
// over the socket and reads back from the named checkpoint on the ref, and
// a map one key over the cap is refused naming the limit, creating no
// checkpoint. tools_test.go's meta test covers the at-rest path only.
//
// Mutation verified: tools.go checkpoint, the daemon flush request's
// `Meta: a.Meta` deleted — the read-back is empty and the oversized map is
// accepted.
func TestLiveCheckpointMetaStoredAndCapped(t *testing.T) {
	ts, w, sock := newDaemonTools(t)
	path := openSession(t, sock, "app", "main")
	sqliteExec(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES ('x');")

	r := call(t, ts, "offshoot_checkpoint", map[string]any{
		"database": "app", "name": "v1", "meta": map[string]any{"run_id": "eval-42", "agent": "claude"}})
	if r.IsError {
		t.Fatalf("live checkpoint with meta: %s", text(r))
	}
	if !strings.Contains(text(r), "captured live") {
		t.Fatalf("checkpoint against an open session must be the live path: %s", text(r))
	}
	want := map[string]string{"run_id": "eval-42", "agent": "claude"}
	if got := mustRef(t, w, "app", "main").Checkpoints["v1"].Meta; !reflect.DeepEqual(got, want) {
		t.Fatalf("live checkpoint v1 meta read back = %v, want %v", got, want)
	}

	r = call(t, ts, "offshoot_checkpoint", map[string]any{"database": "app", "name": "v2", "meta": oversizedMeta()})
	if !r.IsError || !strings.Contains(text(r), "exceeds the 32-key limit") {
		t.Fatalf("live checkpoint with %d meta keys must be refused naming the cap, got: %s", ops.MaxMetaKeys+1, text(r))
	}
	if _, ok := mustRef(t, w, "app", "main").Checkpoints["v2"]; ok {
		t.Fatal("an over-cap meta must not create the checkpoint")
	}
}
