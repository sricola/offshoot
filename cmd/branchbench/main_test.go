package main

import (
	"context"
	"strings"
	"testing"
)

// TestEveryWorkflowRunsAtQuickScale runs all five topologies at -quick
// scale against a throwaway store: every step of every workflow must
// complete, every workflow must have produced depth-1 eval samples, and the
// report must render the table.
func TestEveryWorkflowRunsAtQuickScale(t *testing.T) {
	dir := t.TempDir()
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
}
