package main

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sricola/offshoot/internal/dbfile"
)

// TestEveryWorkflowRunsAtQuickScale runs all five topologies at -quick
// scale against a throwaway store: every step of every workflow must
// complete, every workflow must have produced depth-1 eval samples, and the
// report must render the table. Every in-process open of a checkout takes
// a dbfile.Hold (internal/dbfile's sites_test.go is the static half of that
// rule), and every hold is released by the time the run returns.
func TestEveryWorkflowRunsAtQuickScale(t *testing.T) {
	dir := t.TempDir()
	var holds atomic.Int64
	dbfile.HoldHookForTest = func(string) { holds.Add(1) }
	t.Cleanup(func() { dbfile.HoldHookForTest = nil })
	rep, err := run(context.Background(), config{store: dir, workflows: allWorkflowNames(), quick: true, concurrency: 2, warehouses: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range rep.workflows {
		if w.stepsDone != w.stepsTotal {
			t.Fatalf("%s: %d/%d steps", w.name, w.stepsDone, w.stepsTotal)
		}
		if w.evalP50[1] <= 0 {
			t.Fatalf("%s: no depth-1 eval sample", w.name)
		}
	}
	if !strings.Contains(rep.markdown(), "| mcts |") {
		t.Fatal("report lacks the mcts row")
	}
	if holds.Load() == 0 {
		t.Fatal("no dbfile.Hold was taken during a full run")
	}
	if st := dbfile.ReadStats(); st.Pins != 0 {
		t.Fatalf("%d pin(s) still outstanding after the run: a site released early-return paths incompletely", st.Pins)
	}
}
