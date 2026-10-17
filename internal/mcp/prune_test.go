package mcp

import (
	"reflect"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/testutil"
)

// TestPruneCheckpointsTool covers offshoot_prune_checkpoints: by name and
// by keep, the structured result, the all-or-nothing refusal of an unknown
// name, and the protected-branch gate — refused without -allow-force, with
// the human's CLI command named and never a "retry with force" hint, and
// honoured with it.
func TestPruneCheckpointsTool(t *testing.T) {
	ts, w := newTools(t)
	if r := call(t, ts, "offshoot_fork", map[string]any{"database": "app", "new_branch": "work"}); r.IsError {
		t.Fatalf("fork: %s", text(r))
	}
	path, err := w.Checkout("app", "work")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		if out, err := testutil.SQLite3CLI(path, "CREATE TABLE IF NOT EXISTS t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		if _, err := w.Checkpoint("app", "work", name, nil); err != nil {
			t.Fatal(err)
		}
	}

	r := call(t, ts, "offshoot_prune_checkpoints", map[string]any{"database": "app", "branch": "work", "names": []string{"a"}})
	if r.IsError {
		t.Fatalf("prune a: %s", text(r))
	}
	sc, _ := r.StructuredContent.(map[string]any)
	if got := sc["pruned"]; !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("structured pruned = %#v, want [a]", got)
	}
	if got := sc["kept"]; got != 2 {
		t.Fatalf("structured kept = %#v, want 2", got)
	}
	if r := call(t, ts, "offshoot_prune_checkpoints", map[string]any{"database": "app", "branch": "work", "names": []string{"b", "nope"}}); !r.IsError || !strings.Contains(text(r), `"nope"`) {
		t.Fatalf("unknown name: want a refusal naming it, got %s", text(r))
	}
	if r := call(t, ts, "offshoot_prune_checkpoints", map[string]any{"database": "app", "branch": "work", "keep": 0}); r.IsError {
		t.Fatalf("keep 0: %s", text(r))
	}
	ref, _, _ := w.Store.GetRef("app", "work")
	if len(ref.Checkpoints) != 0 {
		t.Fatalf("checkpoints after keep 0 = %v", ref.Checkpoints)
	}
	if r := call(t, ts, "offshoot_prune_checkpoints", map[string]any{"database": "app", "branch": "work"}); !r.IsError {
		t.Fatal("neither names nor keep must be refused")
	}

	// main is protected by default.
	for _, force := range []bool{false, true} {
		r := call(t, ts, "offshoot_prune_checkpoints", map[string]any{"database": "app", "names": []string{"init"}, "force": force})
		if !r.IsError || !strings.Contains(text(r), "offshoot prune app@main") || strings.Contains(text(r), "use --force") {
			t.Fatalf("protected prune (force=%v) without -allow-force: want a refusal naming the CLI command, got %s", force, text(r))
		}
	}
	if ref, _, _ := w.Store.GetRef("app", "main"); len(ref.Checkpoints) == 0 {
		t.Fatal("a refused prune deleted main's checkpoints")
	}
	ts.SetAllowForce(true)
	if r := call(t, ts, "offshoot_prune_checkpoints", map[string]any{"database": "app", "names": []string{"init"}, "force": true}); r.IsError {
		t.Fatalf("forced prune with -allow-force: %s", text(r))
	}
}
