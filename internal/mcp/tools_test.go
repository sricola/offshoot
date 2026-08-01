package mcp

import (
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/offshoot-db/offshoot/internal/ops"
)

func newTools(t *testing.T) (*OffshootTools, *ops.Workspace) {
	t.Helper()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	spec := filepath.Join(t.TempDir(), "store")
	w, err := ops.Init(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	return NewOffshootTools(w, spec), w
}

func call(t *testing.T, ts *OffshootTools, name string, args map[string]any) ToolResult {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	res, err := ts.Call(context.Background(), name, raw)
	if err != nil {
		t.Fatalf("%s returned an RPC error: %v", name, err)
	}
	return res
}

func text(r ToolResult) string {
	var b strings.Builder
	for _, c := range r.Content {
		b.WriteString(c.Text)
	}
	return b.String()
}

func TestToolsAdvertiseSchemas(t *testing.T) {
	ts, _ := newTools(t)
	tools := ts.Tools()
	if len(tools) < 7 {
		t.Fatalf("want the full lifecycle surface, got %d tools", len(tools))
	}
	seen := map[string]bool{}
	for _, tl := range tools {
		if tl.Name == "" || tl.Description == "" || tl.InputSchema == nil {
			t.Errorf("incomplete tool: %+v", tl)
		}
		if seen[tl.Name] {
			t.Errorf("duplicate tool %q", tl.Name)
		}
		seen[tl.Name] = true
	}
	for _, want := range []string{"offshoot_list", "offshoot_checkout", "offshoot_checkpoint",
		"offshoot_fork", "offshoot_rollback", "offshoot_promote", "offshoot_destroy"} {
		if !seen[want] {
			t.Errorf("missing tool %q", want)
		}
	}
}

func TestForkCheckpointRollbackThroughTools(t *testing.T) {
	ts, _ := newTools(t)

	co := call(t, ts, "offshoot_checkout", map[string]any{"database": "app"})
	if co.IsError {
		t.Fatalf("checkout: %s", text(co))
	}
	path := strings.TrimSpace(lastPath(text(co)))
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES ('original');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if r := call(t, ts, "offshoot_checkpoint", map[string]any{
		"database": "app", "name": "v1"}); r.IsError {
		t.Fatalf("checkpoint: %s", text(r))
	}
	if r := call(t, ts, "offshoot_fork", map[string]any{
		"database": "app", "new_branch": "attempt-1"}); r.IsError {
		t.Fatalf("fork: %s", text(r))
	}

	// The agent wrecks its attempt, then rolls back.
	aco := call(t, ts, "offshoot_checkout", map[string]any{
		"database": "app", "branch": "attempt-1"})
	apath := strings.TrimSpace(lastPath(text(aco)))
	if out, err := exec.Command("sqlite3", apath, "DROP TABLE t;").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if r := call(t, ts, "offshoot_checkpoint", map[string]any{
		"database": "app", "branch": "attempt-1", "name": "broken"}); r.IsError {
		t.Fatalf("checkpoint attempt: %s", text(r))
	}
	rb := call(t, ts, "offshoot_rollback", map[string]any{
		"database": "app", "branch": "attempt-1", "to": "fork"})
	if rb.IsError {
		t.Fatalf("rollback: %s", text(rb))
	}
	rpath := strings.TrimSpace(lastPath(text(rb)))
	got, err := exec.Command("sqlite3", rpath, "SELECT v FROM t;").Output()
	if err != nil || string(got) != "original\n" {
		t.Fatalf("rollback did not restore: %q err=%v", got, err)
	}
}

func TestProtectedBranchRefusalReachesTheAgent(t *testing.T) {
	ts, _ := newTools(t)
	if r := call(t, ts, "offshoot_fork", map[string]any{
		"database": "app", "new_branch": "attempt-1"}); r.IsError {
		t.Fatalf("fork: %s", text(r))
	}
	// main is protected: promote must refuse, and the message must say how.
	r := call(t, ts, "offshoot_promote", map[string]any{
		"database": "app", "source": "attempt-1", "target": "main"})
	if !r.IsError {
		t.Fatal("promoting onto protected main without force must be refused")
	}
	if !strings.Contains(strings.ToLower(text(r)), "protected") ||
		!strings.Contains(strings.ToLower(text(r)), "force") {
		t.Fatalf("refusal must tell the agent what to do: %s", text(r))
	}
	// With force it succeeds.
	if r := call(t, ts, "offshoot_promote", map[string]any{
		"database": "app", "source": "attempt-1", "target": "main", "force": true}); r.IsError {
		t.Fatalf("forced promote: %s", text(r))
	}
}

func TestUserErrorsAreToolErrorsNotRPCErrors(t *testing.T) {
	ts, _ := newTools(t)
	r := call(t, ts, "offshoot_rollback", map[string]any{
		"database": "app", "to": "no-such-checkpoint"})
	if !r.IsError {
		t.Fatal("rolling back to a missing checkpoint must be a tool error")
	}
	if !strings.Contains(text(r), "no-such-checkpoint") {
		t.Fatalf("message should name the bad checkpoint: %s", text(r))
	}
}

func TestUnknownToolIsAnRPCError(t *testing.T) {
	ts, _ := newTools(t)
	if _, err := ts.Call(context.Background(), "offshoot_nope", json.RawMessage(`{}`)); err == nil {
		t.Fatal("an unknown tool name must be an RPC-level error")
	}
}

// lastPath returns the last whitespace-separated token that looks like a path.
func lastPath(s string) string {
	fields := strings.Fields(s)
	for i := len(fields) - 1; i >= 0; i-- {
		if strings.Contains(fields[i], "/") {
			return fields[i]
		}
	}
	return ""
}
