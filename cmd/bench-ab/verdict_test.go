package main

import "testing"

func TestMedian(t *testing.T) {
	if m := median([]float64{3, 1, 2}); m != 2 {
		t.Fatalf("median odd = %v", m)
	}
	if m := median([]float64{4, 1, 3, 2}); m != 2.5 {
		t.Fatalf("median even = %v", m)
	}
}

func TestVerdictRequiresEveryAfterAboveEveryBefore(t *testing.T) {
	// After is slower on two rounds and faster on one: no verdict.
	if r, _, _ := verdict([]float64{45, 46, 45}, []float64{57, 56, 44}, 0.15); r {
		t.Fatal("regressed although one after run was below the before runs")
	}
	// Every after above every before, but by 10% with a 15% threshold.
	if r, _, _ := verdict([]float64{45, 46, 45}, []float64{50, 49, 50}, 0.15); r {
		t.Fatal("regressed on a 10% shift with a 15% threshold")
	}
	// Every after above every before by about 25%: regressed.
	if r, b, a := verdict([]float64{45.5, 45.5, 45.7}, []float64{56.2, 55.5, 56.7}, 0.15); !r || b != 45.5 || a != 56.2 {
		t.Fatalf("v0.2.15 vs v0.2.16 shape: regressed=%v before=%v after=%v", r, b, a)
	}
	// A wide before spread swallows the shift: no verdict.
	if r, _, _ := verdict([]float64{40, 60, 45}, []float64{57, 58, 59}, 0.15); r {
		t.Fatal("regressed although the largest before run exceeds the smallest after run")
	}
}

func TestVerdictRejectsTooFewRounds(t *testing.T) {
	if _, err := compareRounds([]float64{1}, []float64{2}, 0.15); err == nil {
		t.Fatal("one round accepted")
	}
}

func TestImprovedMirrorsTheRegressionRule(t *testing.T) {
	if !improved([]float64{56.2, 55.5, 56.7}, []float64{45.5, 45.5, 45.7}, 0.15) {
		t.Fatal("a 19% drop with no overlap was not labelled improved")
	}
	if improved([]float64{50, 49, 50}, []float64{45, 46, 45}, 0.15) {
		t.Fatal("a 10% drop was labelled improved with a 15% threshold")
	}
	if improved([]float64{56, 57, 44}, []float64{45, 46, 45}, 0.15) {
		t.Fatal("labelled improved although one before run was below the after runs")
	}
}
