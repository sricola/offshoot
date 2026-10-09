package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// sample is one branchbench run's table row for one workflow, reduced to the
// figures the A/B compares: wall seconds and the depth-1 p50 of each
// latency column. Only p50 at depth 1 is read, since that is the figure the
// hand comparisons in docs/benchmarks.md used and the one with the most
// samples behind it.
type sample struct {
	Workflow                                            string
	WallS, ForkP50, CheckoutP50, CheckpointP50, EvalP50 float64
}

// rowRE matches a data row of branchbench's table. The latency cells read
// "<p50>/<p99> → ..." and only the leading p50 is captured.
var rowRE = regexp.MustCompile(`^\|\s*([a-z_]+)\s*\|\s*\d+/\d+\s*\|\s*([^|]+?)\s*\|\s*\d+%\s*\|\s*([0-9.]+)/[0-9.]+[^|]*\|\s*([0-9.]+)/[0-9.]+[^|]*\|\s*([0-9.]+)/[0-9.]+[^|]*\|\s*([0-9.]+)/[0-9.]+[^|]*\|`)

// wallRE accepts "1.7 s" and "1 min 2 s"; anything else is an error.
var wallRE = regexp.MustCompile(`^(?:(\d+) min )?([0-9.]+) s$`)

// wallMsRE accepts "950 ms", which branchbench's fmtWall prints for a run
// shorter than one second.
var wallMsRE = regexp.MustCompile(`^(\d+) ms$`)

// parseWall converts the table's wall cell to seconds.
func parseWall(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if m := wallMsRE.FindStringSubmatch(s); m != nil {
		ms, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			return 0, err
		}
		return ms / 1000, nil
	}
	m := wallRE.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("bench-ab: wall %q is not \"<n> ms\", \"<n> s\" or \"<m> min <n> s\"", s)
	}
	secs, err := strconv.ParseFloat(m[2], 64)
	if err != nil {
		return 0, err
	}
	if m[1] != "" {
		mins, _ := strconv.Atoi(m[1])
		secs += float64(mins) * 60
	}
	return secs, nil
}

// parseRow parses one table data row; header, rule and progress lines are
// errors, not skips, so a format change in branchbench fails the A/B
// instead of producing an empty comparison.
func parseRow(line string) (sample, error) {
	m := rowRE.FindStringSubmatch(line)
	if m == nil {
		return sample{}, fmt.Errorf("bench-ab: not a branchbench table row: %q", line)
	}
	wall, err := parseWall(m[2])
	if err != nil {
		return sample{}, err
	}
	f := func(s string) float64 { v, _ := strconv.ParseFloat(s, 64); return v }
	return sample{Workflow: m[1], WallS: wall, ForkP50: f(m[3]), CheckoutP50: f(m[4]), CheckpointP50: f(m[5]), EvalP50: f(m[6])}, nil
}

// findRow returns the table row for workflow in one branchbench run's
// stdout, or an error when the run printed none.
func findRow(out, workflow string) (sample, error) {
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "| "+workflow+" ") || strings.HasPrefix(line, "| "+workflow+"\t") || strings.HasPrefix(line, "|"+workflow+"|") {
			return parseRow(line)
		}
	}
	return sample{}, fmt.Errorf("bench-ab: no table row for workflow %q in the run's output", workflow)
}
