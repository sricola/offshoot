package mcp

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/store"
)

// QA regression tests for the MCP tool contract at rest (newTools: nothing
// listens on the socket, so every call takes the at-rest path). Each test's
// doc comment names the spec sentence it pins and the production mutation
// it was verified to catch.

// errNthGetBackend wraps a store.Backend and fails the n-th Get of one key
// with an injected error, passing every other call through. Unlike
// tools_test.go's errOnceGetBackend (which fails the FIRST Get), this picks
// WHICH read of a key fails, so a test can reach a specific GetRef inside a
// handler that reads the same key more than once — promote reads its
// target's ref in guardProtectedSafetyFork first and in
// refuseForceOnProtected second.
type errNthGetBackend struct {
	store.Backend
	key   string
	n     int
	count int
}

func (b *errNthGetBackend) Get(key string) ([]byte, string, error) {
	if key == b.key {
		b.count++
		if b.count == b.n {
			return nil, "", fmt.Errorf("injected transient backend error")
		}
	}
	return b.Backend.Get(key)
}

// mustRef reads db@branch's ref, failing the test on any error.
func mustRef(t *testing.T, w *ops.Workspace, db, branch string) store.Ref {
	t.Helper()
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		t.Fatalf("GetRef %s@%s: %v", db, branch, err)
	}
	return ref
}

// sameHead reports whether two reads of a ref agree on the two fields a
// refused repoint must leave untouched: its head txid and its lineage.
func sameHead(a, b store.Ref) bool {
	return a.HeadTXID == b.HeadTXID && a.Lineage == b.Lineage
}

// checkoutAndWrite materializes db@main through offshoot_checkout and runs
// sql against the returned path, returning that path.
func checkoutAndWrite(t *testing.T, ts *OffshootTools, sql string) string {
	t.Helper()
	co := call(t, ts, "offshoot_checkout", map[string]any{"database": "app"})
	if co.IsError {
		t.Fatalf("checkout: %s", text(co))
	}
	path := strings.TrimSpace(lastPath(text(co)))
	sqliteExec(t, path, sql)
	return path
}

// oversizedMeta is one key past ops.MaxMetaKeys, the documented cap
// ("at most 32 keys" in every tool description that takes `meta`).
func oversizedMeta() map[string]any {
	big := map[string]any{}
	for i := 0; i < ops.MaxMetaKeys+1; i++ {
		big[fmt.Sprintf("k%d", i)] = "v"
	}
	return big
}

// TestForkAtForksTheNamedCheckpointNotHead pins docs/agents.md's
// offshoot_fork row and tools.go's fork description ("Forks from the
// branch's current head by default, or from a named checkpoint via `at`"):
// a fork taken `at` checkpoint a carries exactly a's rows, not the rows
// checkpointed after it, and reports a's txid as the fork point — while a
// plain fork of the same branch (the control) carries the head's rows.
//
// Mutation verified: tools.go fork, at-rest branch, `t.ws.Fork(a.Database,
// branch, a.NewBranch, a.At, ...)` with a.At replaced by "" — the `at` fork
// then has both rows (row count = 2, want 1).
func TestForkAtForksTheNamedCheckpointNotHead(t *testing.T) {
	ts, w := newTools(t)
	path := checkoutAndWrite(t, ts, "CREATE TABLE t (v); INSERT INTO t VALUES ('before');")
	if r := call(t, ts, "offshoot_checkpoint", map[string]any{"database": "app", "name": "a"}); r.IsError {
		t.Fatalf("checkpoint a: %s", text(r))
	}
	sqliteExec(t, path, "INSERT INTO t VALUES ('after');")
	if r := call(t, ts, "offshoot_checkpoint", map[string]any{"database": "app", "name": "b"}); r.IsError {
		t.Fatalf("checkpoint b: %s", text(r))
	}

	r := call(t, ts, "offshoot_fork", map[string]any{"database": "app", "new_branch": "from-a", "at": "a"})
	if r.IsError {
		t.Fatalf("fork at=a: %s", text(r))
	}
	main := mustRef(t, w, "app", "main")
	aTX := main.Checkpoints["a"].TXID
	sc, ok := r.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("fork structuredContent = %T, want a map", r.StructuredContent)
	}
	if fmt.Sprint(sc["txid"]) != fmt.Sprint(aTX) {
		t.Fatalf("fork at=a reported txid %v, want checkpoint a's txid %d", sc["txid"], aTX)
	}
	if fork := mustRef(t, w, "app", "from-a"); fork.HeadTXID != aTX {
		t.Fatalf("from-a head txid = %d, want checkpoint a's %d", fork.HeadTXID, aTX)
	}
	fpath, err := w.Checkout("app", "from-a")
	if err != nil {
		t.Fatal(err)
	}
	if n := sqliteCount(t, fpath, "SELECT COUNT(*) FROM t;"); n != 1 {
		t.Fatalf("fork at=a row count = %d, want 1 (only the row checkpointed as a)", n)
	}

	// Control: the same fork without `at` is from the head and sees both
	// rows, so the 1 above is `at` doing its job, not a thin checkout.
	if r := call(t, ts, "offshoot_fork", map[string]any{"database": "app", "new_branch": "from-head"}); r.IsError {
		t.Fatalf("fork at head: %s", text(r))
	}
	hpath, err := w.Checkout("app", "from-head")
	if err != nil {
		t.Fatal(err)
	}
	if n := sqliteCount(t, hpath, "SELECT COUNT(*) FROM t;"); n != 2 {
		t.Fatalf("fork at head row count = %d, want 2", n)
	}
}

// TestPromoteForceOnUnprotectedTargetProceedsWithoutAllowForce mirrors
// tools_test.go's destroy case (TestMCPRefusesForceOnProtectedByDefault's
// attempt-2 leg) for offshoot_promote: docs/reference.md "-allow-force" —
// force against an UNPROTECTED branch "never needed this flag": it is
// downgraded, never refused, and the promote runs exactly as an unforced
// call would (target repointed at the source head, safety fork kept).
//
// Mutation verified: refuseForceOnProtected's `if ref.Protected {` replaced
// by `if true {` (refusing force on every branch) — the promote is refused
// naming -allow-force.
func TestPromoteForceOnUnprotectedTargetProceedsWithoutAllowForce(t *testing.T) {
	ts, w := newTools(t)
	for _, b := range []string{"attempt-1", "attempt-2"} {
		if _, err := w.Fork("app", "main", b, "", 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	// Advance attempt-1 past attempt-2 so the promote visibly moves the
	// target's head.
	co := call(t, ts, "offshoot_checkout", map[string]any{"database": "app", "branch": "attempt-1"})
	if co.IsError {
		t.Fatalf("checkout attempt-1: %s", text(co))
	}
	sqliteExec(t, strings.TrimSpace(lastPath(text(co))), "CREATE TABLE t (v); INSERT INTO t VALUES ('x');")
	if r := call(t, ts, "offshoot_checkpoint", map[string]any{"database": "app", "branch": "attempt-1", "name": "v1"}); r.IsError {
		t.Fatalf("checkpoint: %s", text(r))
	}
	src := mustRef(t, w, "app", "attempt-1")
	if tgt := mustRef(t, w, "app", "attempt-2"); tgt.HeadTXID == src.HeadTXID {
		t.Fatalf("precondition: source and target heads must differ, both %d", src.HeadTXID)
	}

	r := call(t, ts, "offshoot_promote", map[string]any{
		"database": "app", "source": "attempt-1", "target": "attempt-2", "force": true})
	if r.IsError {
		t.Fatalf("forced promote onto an unprotected target needs no -allow-force: %s", text(r))
	}
	if tgt := mustRef(t, w, "app", "attempt-2"); tgt.HeadTXID != src.HeadTXID {
		t.Fatalf("attempt-2 head = %d after promote, want the source head %d", tgt.HeadTXID, src.HeadTXID)
	}
	if _, _, err := w.Store.GetRef("app", "attempt-2"+ops.PromoteBackupSuffix); err != nil {
		t.Fatalf("the downgraded (unforced) promote must still keep the safety fork: %v", err)
	}
}

// TestPromoteForceUnderLiveLeaseNeedsAllowForce mirrors
// TestMCPDestroyNeedsAllowForceForLiveLeaseToo for offshoot_promote:
// docs/reference.md "-allow-force" — without the flag MCP never honors
// force at all, so an agent's force:true cannot take over a live lease on
// an (unprotected) target either; ops.PromoteWith's own lease check refuses
// the downgraded call, the tool error names -allow-force, and the target is
// untouched. With -allow-force the same call proceeds.
//
// Mutation verified: tools.go promote, `a.Force = eff` deleted (the raw
// force reaches PromoteWith) — the forced promote bypasses the lease and
// succeeds where it must be refused.
func TestPromoteForceUnderLiveLeaseNeedsAllowForce(t *testing.T) {
	ts, w := newTools(t)
	for _, b := range []string{"attempt-1", "attempt-2"} {
		if _, err := w.Fork("app", "main", b, "", 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.AcquireLease("app", "attempt-2", "holder", time.Hour); err != nil {
		t.Fatal(err)
	}
	before := mustRef(t, w, "app", "attempt-2")

	r := call(t, ts, "offshoot_promote", map[string]any{
		"database": "app", "source": "attempt-1", "target": "attempt-2", "force": true})
	if !r.IsError {
		t.Fatalf("forced promote onto a leased target must be refused without -allow-force, got: %s", text(r))
	}
	if !strings.Contains(text(r), "-allow-force") {
		t.Fatalf("refusal must name -allow-force: %s", text(r))
	}
	if after := mustRef(t, w, "app", "attempt-2"); !sameHead(before, after) {
		t.Fatalf("attempt-2 must be untouched by the refused promote: before=%+v after=%+v", before, after)
	}
	if _, _, err := w.Store.GetRef("app", "attempt-2"+ops.PromoteBackupSuffix); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a refused promote must mint no safety fork, GetRef err = %v", err)
	}

	ts.SetAllowForce(true)
	if r := call(t, ts, "offshoot_promote", map[string]any{
		"database": "app", "source": "attempt-1", "target": "attempt-2", "force": true}); r.IsError {
		t.Fatalf("promote with -allow-force under a live lease: %s", text(r))
	}
}

// TestPromoteForceFailsClosedOnTransientGetRefError is
// TestMCPForceFailsClosedOnTransientGetRefError's promote sibling, aimed at
// refuseForceOnProtected's OWN GetRef (docs/agents.md: the gate "fails
// closed" on a read error). promote reads the target's ref twice before
// ops — guardProtectedSafetyFork first, refuseForceOnProtected second — so
// the injected error hits the SECOND read: the first guard passes, the
// force gate cannot confirm main is safe to force and downgrades force to
// false, and ops' own unforced protected check refuses. The tool error
// names -allow-force, is NOT guardProtectedSafetyFork's "cannot read"
// refusal, and main is untouched.
//
// Mutation verified: tools.go refuseForceOnProtected, the GetRef-error
// branch `return ToolResult{}, false, false` changed to
// `return ToolResult{}, false, force` — the raw force reaches PromoteWith,
// whose own (successful) GetRef sees main protected and honors it: main is
// promoted onto.
func TestPromoteForceFailsClosedOnTransientGetRefError(t *testing.T) {
	ts, w := newTools(t)
	if _, err := w.Fork("app", "main", "attempt-1", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	before := mustRef(t, w, "app", "main")
	w.Store.B = &errNthGetBackend{Backend: w.Store.B, key: store.RefKey("app", "main"), n: 2}

	r := call(t, ts, "offshoot_promote", map[string]any{
		"database": "app", "source": "attempt-1", "target": "main", "force": true})
	if !r.IsError {
		t.Fatalf("forced promote onto protected main must be refused when the force gate's GetRef errors, got: %s", text(r))
	}
	if !strings.Contains(text(r), "-allow-force") {
		t.Fatalf("refusal must name -allow-force: %s", text(r))
	}
	if strings.Contains(text(r), "cannot read") {
		t.Fatalf("the injected error reached guardProtectedSafetyFork, not refuseForceOnProtected: %s", text(r))
	}
	if after := mustRef(t, w, "app", "main"); !sameHead(before, after) {
		t.Fatalf("main must be untouched: before=%+v after=%+v", before, after)
	}
}

// TestToolSurfaceIsExactlyNineWithPinnedRequiredAndDefaults pins
// docs/reference.md's "offshoot mcp" tool list (nine tools, no protect or
// export tool) and, per tool, the schema's `required` list and the
// documented `branch` default ("`branch` defaults to \"main\" if omitted"
// on checkout, checkpoint, fork, rollback and touch; destroy's `branch` is
// required with no default; promote and diff have no `branch`). Read back
// through JSON, the shape an MCP client receives from tools/list.
//
// Mutation verified: tools.go rollback schema `reqStr("to")` changed to
// `optStr("to")` — the pinned required list no longer matches.
func TestToolSurfaceIsExactlyNineWithPinnedRequiredAndDefaults(t *testing.T) {
	ts, _ := newTools(t)
	want := map[string]struct {
		required      []string
		branchDefault string // "" means: no `branch` default advertised
	}{
		"offshoot_list":       {nil, ""},
		"offshoot_checkout":   {[]string{"database"}, "main"},
		"offshoot_checkpoint": {[]string{"database", "name"}, "main"},
		"offshoot_fork":       {[]string{"database", "new_branch"}, "main"},
		"offshoot_rollback":   {[]string{"database", "to"}, "main"},
		"offshoot_promote":    {[]string{"database", "source", "target"}, ""},
		"offshoot_destroy":    {[]string{"database", "branch"}, ""},
		"offshoot_touch":      {[]string{"database"}, "main"},
		"offshoot_diff":       {[]string{"database", "left", "right"}, ""},
	}
	tools := ts.Tools()
	if len(tools) != 9 {
		names := make([]string, 0, len(tools))
		for _, tl := range tools {
			names = append(names, tl.Name)
		}
		t.Fatalf("Tools() = %d tools %v, want exactly 9", len(tools), names)
	}
	type wireTool struct {
		Name        string `json:"name"`
		InputSchema struct {
			Required   []string                  `json:"required"`
			Properties map[string]map[string]any `json:"properties"`
		} `json:"inputSchema"`
	}
	seen := map[string]bool{}
	for _, tl := range tools {
		raw, err := json.Marshal(tl)
		if err != nil {
			t.Fatal(err)
		}
		var wt wireTool
		if err := json.Unmarshal(raw, &wt); err != nil {
			t.Fatalf("%s: inputSchema does not decode: %v", tl.Name, err)
		}
		w, ok := want[wt.Name]
		if !ok {
			t.Errorf("unexpected tool %q", wt.Name)
			continue
		}
		seen[wt.Name] = true
		got := append([]string(nil), wt.InputSchema.Required...)
		sort.Strings(got)
		exp := append([]string(nil), w.required...)
		sort.Strings(exp)
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("%s: required = %v, want %v", wt.Name, got, exp)
		}
		branch, hasBranch := wt.InputSchema.Properties["branch"]
		switch {
		case w.branchDefault != "":
			if !hasBranch {
				t.Errorf("%s: no `branch` property, want one defaulting to %q", wt.Name, w.branchDefault)
			} else if branch["default"] != w.branchDefault {
				t.Errorf("%s: branch.default = %v, want %q", wt.Name, branch["default"], w.branchDefault)
			}
		case hasBranch:
			if _, has := branch["default"]; has {
				t.Errorf("%s: branch must advertise no default, got %v", wt.Name, branch["default"])
			}
		}
	}
	for name := range want {
		if !seen[name] {
			t.Errorf("missing tool %q", name)
		}
	}
}

// TestCheckpointMetaStoredAndCappedAtRest pins offshoot_checkpoint's
// description ("Optional `meta` (string->string, at most 32 keys) tags the
// result") on the at-rest path: the meta map is stored on the named
// checkpoint and reads back from the ref byte for byte, and a map one key
// over the cap is a tool error naming the limit that creates no checkpoint.
//
// Mutation verified: tools.go checkpoint, at-rest branch,
// `t.ws.CheckpointWith(..., a.Meta, ...)` with a.Meta replaced by nil —
// the read-back is empty and the oversized map is accepted.
func TestCheckpointMetaStoredAndCappedAtRest(t *testing.T) {
	ts, w := newTools(t)
	checkoutAndWrite(t, ts, "CREATE TABLE t (v); INSERT INTO t VALUES ('x');")

	r := call(t, ts, "offshoot_checkpoint", map[string]any{
		"database": "app", "name": "v1", "meta": map[string]any{"run_id": "eval-42", "agent": "claude"}})
	if r.IsError {
		t.Fatalf("checkpoint with meta: %s", text(r))
	}
	want := map[string]string{"run_id": "eval-42", "agent": "claude"}
	if got := mustRef(t, w, "app", "main").Checkpoints["v1"].Meta; !reflect.DeepEqual(got, want) {
		t.Fatalf("checkpoint v1 meta read back = %v, want %v", got, want)
	}

	r = call(t, ts, "offshoot_checkpoint", map[string]any{"database": "app", "name": "v2", "meta": oversizedMeta()})
	if !r.IsError || !strings.Contains(text(r), "exceeds the 32-key limit") {
		t.Fatalf("checkpoint with %d meta keys must be refused naming the cap, got: %s", ops.MaxMetaKeys+1, text(r))
	}
	if _, ok := mustRef(t, w, "app", "main").Checkpoints["v2"]; ok {
		t.Fatal("an over-cap meta must not create the checkpoint")
	}
}
