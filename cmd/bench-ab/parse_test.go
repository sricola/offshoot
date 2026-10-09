package main

import (
	"math"
	"testing"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestParseRowDepthOne(t *testing.T) {
	line := "| failure_repro | 10/10 | 1.7 s | 50% | 14.4/14.7 → (d=1 is max) | 8.3/60.1 → (d=1 is max) | 56.4/57.6 → (d=1 is max) | 5.0/5.1 → (d=1 is max) | 1 | 82 MiB |"
	s, err := parseRow(line)
	if err != nil {
		t.Fatal(err)
	}
	if s.Workflow != "failure_repro" || !near(s.WallS, 1.7) || !near(s.ForkP50, 14.4) || !near(s.CheckoutP50, 8.3) || !near(s.CheckpointP50, 56.4) || !near(s.EvalP50, 5.0) {
		t.Fatalf("parsed %+v", s)
	}
}

func TestParseRowDeeperTreeReadsTheDepthOneP50(t *testing.T) {
	line := "| data_cleaning | 200/200 | 10.8 s | 42% | 31.4/54.7 → 27.2/51.5 | 51.4/418.8 → 29.1/139.6 | 98.7/136.8 → 92.7/146.1 | 79.5/128.1 → 12.6/178.4 | 200 | 8.3 GiB |"
	s, err := parseRow(line)
	if err != nil {
		t.Fatal(err)
	}
	if !near(s.ForkP50, 31.4) || !near(s.CheckoutP50, 51.4) || !near(s.CheckpointP50, 98.7) || !near(s.EvalP50, 79.5) || !near(s.WallS, 10.8) {
		t.Fatalf("parsed %+v, want the d=1 p50s", s)
	}
}

func TestParseRowWallInMinutes(t *testing.T) {
	line := "| mcts | 1000/1000 | 1 min 2 s | 67% | 51.7/62.8 → 31.4/66.6 | 399.7/419.6 → 75.4/317.7 | 111.2/145.0 → 90.9/130.3 | 76.2/119.5 → 109.5/245.5 | 890 | 30.7 GiB |"
	s, err := parseRow(line)
	if err != nil {
		t.Fatal(err)
	}
	if !near(s.WallS, 62) {
		t.Fatalf("wall %v, want 62", s.WallS)
	}
}

func TestParseRowRejectsGarbage(t *testing.T) {
	for _, line := range []string{
		"| Workflow | Steps | Wall | Branch overhead | Fork p50/p99 (d=1 → d=max) | Checkout | Checkpoint | Eval | Peak live | Store peak |",
		"|---|---|---|---|---|---|---|---|---|---|",
		"branchbench: simulation done: 10/10 steps in 1.8s",
		"| x | 1/1 | soon | 1% | a/b | c/d | e/f | g/h | 1 | 1 MiB |",
	} {
		if _, err := parseRow(line); err == nil {
			t.Fatalf("parsed %q without error", line)
		}
	}
}

func TestFindRowPicksTheWorkflow(t *testing.T) {
	out := "branchbench: failure_repro: store at /tmp/x\n| failure_repro | 10/10 | 1.7 s | 50% | 14.4/14.7 → (d=1 is max) | 8.3/60.1 → (d=1 is max) | 56.4/57.6 → (d=1 is max) | 5.0/5.1 → (d=1 is max) | 1 | 82 MiB |\n- `failure_repro` (flat, 1 worker; ...)\n"
	s, err := findRow(out, "failure_repro")
	if err != nil || !near(s.CheckpointP50, 56.4) {
		t.Fatalf("findRow: %+v %v", s, err)
	}
	if _, err := findRow(out, "simulation"); err == nil {
		t.Fatal("findRow found a workflow that is not in the output")
	}
}

func TestParseRowWallInMilliseconds(t *testing.T) {
	line := "| failure_repro | 10/10 | 950 ms | 50% | 14.4/14.7 → (d=1 is max) | 8.3/60.1 → (d=1 is max) | 56.4/57.6 → (d=1 is max) | 5.0/5.1 → (d=1 is max) | 1 | 82 MiB |"
	s, err := parseRow(line)
	if err != nil {
		t.Fatal(err)
	}
	if !near(s.WallS, 0.95) {
		t.Fatalf("wall %v, want 0.95", s.WallS)
	}
}

func TestParseRowRejectsATimedOutRun(t *testing.T) {
	line := "| mcts | 400/1000 (timed out) | 30.0 s | 67% | 51.7/62.8 → 31.4/66.6 | 399.7/419.6 → 75.4/317.7 | 111.2/145.0 → 90.9/130.3 | 76.2/119.5 → 109.5/245.5 | 890 | 30.7 GiB |"
	if _, err := parseRow(line); err == nil {
		t.Fatal("a timed-out run parsed as a sample")
	}
}
