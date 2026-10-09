package main

import (
	"fmt"
	"sort"
)

// median returns the middle value of xs, or the mean of the two middle
// values when len(xs) is even. It does not reorder xs.
func median(xs []float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

// verdict applies the A/B rule: a metric regressed when the after median
// exceeds the before median by more than threshold AND the smallest after
// sample exceeds the largest before sample, so a shift that any single pair
// of runs contradicts never counts. Alternating the two builds on one machine
// is what makes the second condition meaningful: drift during the series
// moves both sides together.
func verdict(before, after []float64, threshold float64) (regressed bool, beforeMedian, afterMedian float64) {
	beforeMedian, afterMedian = median(before), median(after)
	minAfter, maxBefore := after[0], before[0]
	for _, v := range after {
		if v < minAfter {
			minAfter = v
		}
	}
	for _, v := range before {
		if v > maxBefore {
			maxBefore = v
		}
	}
	return afterMedian > beforeMedian*(1+threshold) && minAfter > maxBefore, beforeMedian, afterMedian
}

// improved is the mirror of verdict: the after median sits below the before
// median by more than threshold AND the largest after sample is below the
// smallest before sample. It only labels the report; it never fails the run.
func improved(before, after []float64, threshold float64) bool {
	maxAfter, minBefore := after[0], before[0]
	for _, v := range after {
		if v > maxAfter {
			maxAfter = v
		}
	}
	for _, v := range before {
		if v < minBefore {
			minBefore = v
		}
	}
	return median(after) < median(before)*(1-threshold) && maxAfter < minBefore
}

// compareRounds is verdict with the sample-count check main relies on.
func compareRounds(before, after []float64, threshold float64) (result, error) {
	if len(before) < 2 || len(after) < 2 {
		return result{}, fmt.Errorf("bench-ab: need at least 2 rounds per side, have %d before and %d after", len(before), len(after))
	}
	r, b, a := verdict(before, after, threshold)
	return result{
		Regressed: r, Improved: improved(before, after, threshold),
		BeforeMedian: b, AfterMedian: a, Before: before, After: after,
	}, nil
}

// result is one metric's comparison for one workflow.
type result struct {
	Regressed, Improved       bool
	BeforeMedian, AfterMedian float64
	Before, After             []float64
}
