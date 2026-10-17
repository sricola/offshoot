package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/testutil"
)

func TestPruneCLI(t *testing.T) {
	testutil.RequireSQLite3(t)
	store := filepath.Join(t.TempDir(), "s")
	call(t, store, "init")
	call(t, store, "create", "app")
	call(t, store, "fork", "app", "work")
	path := strings.TrimSpace(call(t, store, "checkout", "app@work"))
	for _, name := range []string{"a", "b", "c"} {
		if out, err := testutil.SQLite3CLI(path, "CREATE TABLE IF NOT EXISTS t (v); INSERT INTO t VALUES ('"+name+"');").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		call(t, store, "checkpoint", "app@work", name)
	}

	if got, want := call(t, store, "prune", "app@work", "b", "a"), "pruned 2 checkpoints from app@work: a, b (2 kept)\n"; got != want {
		t.Fatalf("prune by name printed %q, want %q", got, want)
	}
	if got, want := call(t, store, "prune", "app@work", "--keep", "1"), "pruned 1 checkpoint from app@work: fork (1 kept)\n"; got != want {
		t.Fatalf("prune --keep printed %q, want %q", got, want)
	}
	if got, want := call(t, store, "prune", "app@work", "--keep", "5"), "nothing to prune on app@work (1 kept)\n"; got != want {
		t.Fatalf("prune --keep above the count printed %q, want %q", got, want)
	}
	if line := statusLineFor(t, call(t, store, "status"), "app@work"); !strings.Contains(line, " checkpoints=[c]") {
		t.Fatalf("status after prune: %q, want checkpoints=[c]", line)
	}

	// main is protected by default.
	if err := run([]string{"-store", store, "prune", "app", "init"}); err == nil || !strings.Contains(err.Error(), "protected") {
		t.Fatalf("unforced prune of protected main: want a protected refusal, got %v", err)
	}
	call(t, store, "prune", "app", "init", "--force")

	for _, args := range [][]string{
		{"prune", "app@work"},                     // neither names nor --keep
		{"prune", "app@work", "c", "--keep", "1"}, // both
		{"prune", "app@work", "--keep", "-1"},     // negative
		{"prune", "app@work", "--keep", "two"},    // not a number
		{"prune", "app@work", "--keep"},           // missing value
		{"prune"},                                 // no target
		{"prune", "app@work", "nope"},             // unknown checkpoint
	} {
		if err := run(append([]string{"-store", store}, args...)); err == nil {
			t.Errorf("offshoot %v: want an error", args)
		}
	}
}
