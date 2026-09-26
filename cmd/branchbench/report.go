package main

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

// workflowReport is one row of the output table plus the per-workflow line
// under it.
type workflowReport struct {
	wf                    workflow
	name                  string
	stepsDone, stepsTotal int
	wall, branchTime      time.Duration
	m                     *metrics
	maxDepth, peakLive    int
	storeDelta            int64
	crossDur              time.Duration
	crossBranches         int
	crossQueries          int
	casRetries            int
	timedOut              bool
	err                   error
	concurrency           int

	// p50/p99 per operation, keyed by depth; filled by finalize for depth 1
	// and for the deepest depth the workflow reached.
	forkP50, forkP99             map[int]time.Duration
	checkoutP50, checkoutP99     map[int]time.Duration
	checkpointP50, checkpointP99 map[int]time.Duration
	evalP50, evalP99             map[int]time.Duration
	samples                      map[string]map[int]int
}

func (w *workflowReport) finalize() {
	w.name = w.wf.name
	w.forkP50, w.forkP99 = map[int]time.Duration{}, map[int]time.Duration{}
	w.checkoutP50, w.checkoutP99 = map[int]time.Duration{}, map[int]time.Duration{}
	w.checkpointP50, w.checkpointP99 = map[int]time.Duration{}, map[int]time.Duration{}
	w.evalP50, w.evalP99 = map[int]time.Duration{}, map[int]time.Duration{}
	w.samples = map[string]map[int]int{}
	dst := map[string][2]map[int]time.Duration{
		"fork":       {w.forkP50, w.forkP99},
		"checkout":   {w.checkoutP50, w.checkoutP99},
		"checkpoint": {w.checkpointP50, w.checkpointP99},
		"eval":       {w.evalP50, w.evalP99},
	}
	depths := []int{1}
	if w.maxDepth > 1 {
		depths = append(depths, w.maxDepth)
	}
	for op, maps := range dst {
		w.samples[op] = map[int]int{}
		for _, d := range depths {
			p50, p99, n := w.m.quantiles(op, d)
			maps[0][d], maps[1][d] = p50, p99
			w.samples[op][d] = n
		}
	}
}

// report is the whole run.
type report struct {
	workflows   []*workflowReport
	seedBytes   int64
	concurrency int
	warehouses  int
	quick       bool
}

const tableHeader = "| Workflow | Steps | Wall | Branch overhead | Fork p50/p99 (d=1 → d=max) | " +
	"Checkout p50/p99 (d=1 → d=max) | Checkpoint p50/p99 (d=1 → d=max) | Eval p50/p99 (d=1 → d=max) | Peak live | Store Δ |"

func (r *report) markdown() string {
	var b strings.Builder
	b.WriteString(r.header() + "\n\n")
	b.WriteString(tableHeader + "\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|---|\n")
	for _, w := range r.workflows {
		fmt.Fprintf(&b, "| %s | %d/%d%s | %s | %s | %s | %s | %s | %s | %d | %s |\n",
			w.name, w.stepsDone, w.stepsTotal, w.stepsNote(), fmtWall(w.wall), w.overhead(),
			w.cell(w.forkP50, w.forkP99), w.cell(w.checkoutP50, w.checkoutP99),
			w.cell(w.checkpointP50, w.checkpointP99), w.cell(w.evalP50, w.evalP99),
			w.peakLive, fmtBytes(w.storeDelta))
	}
	b.WriteString("\n")
	for _, w := range r.workflows {
		b.WriteString(w.line() + "\n")
	}
	return b.String()
}

func (r *report) header() string {
	return fmt.Sprintf("%s/%s, %s, %d cores, Go %s, offshoot %s, measured %s, seed %.0f MiB, concurrency %d",
		runtime.GOOS, runtime.GOARCH, cpuModel(), runtime.NumCPU(), goVersion(), gitDescribe(),
		time.Now().Format("2006-01-02"), float64(r.seedBytes)/(1<<20), r.concurrency)
}

// line is the per-workflow sentence under the table: the parameter tuple
// that produced the row, the deepest branch it reached, and its
// cross-branch query cost.
func (w *workflowReport) line() string {
	var b strings.Builder
	fmt.Fprintf(&b, "- `%s` (%s; T=%d, S=%d, F_r=%d, F_i=%d, D=%d, C=%d, γ=%.1f, M_s=%d, M_d=%d, Q_v=%d): max depth reached %d",
		w.name, w.wf.shape, w.wf.workers, w.wf.steps, w.wf.rootFanout, w.wf.innerFanout, w.wf.maxDepth,
		w.wf.crossBranch, w.wf.prune, w.wf.schemaOps, w.wf.mutations, w.wf.evals, w.maxDepth)
	if w.crossQueries == 0 {
		b.WriteString("; no cross-branch queries (C=0)")
	} else {
		fmt.Fprintf(&b, "; %d cross-branch %s over %d live %s (the root included) in %s",
			w.crossQueries, plural(w.crossQueries, "query", "queries"), w.crossBranches,
			plural(w.crossBranches, "branch", "branches"), fmtWall(w.crossDur))
	}
	if w.maxDepth > 1 {
		fmt.Fprintf(&b, "; %d steps landed at d=1 and %d at d=%d (the sample counts behind those two p50/p99 pairs)",
			w.samples["fork"][1], w.samples["fork"][w.maxDepth], w.maxDepth)
	} else {
		fmt.Fprintf(&b, "; all %d steps landed at d=1", w.samples["fork"][1])
	}
	fmt.Fprintf(&b, "; branch-management time %s summed over workers (%.1fx wall at concurrency %d); %d CAS retries",
		fmtWall(w.branchTime), float64(w.branchTime)/float64(w.wall), w.concurrency, w.casRetries)
	if w.err != nil {
		fmt.Fprintf(&b, "; ABORTED: %v", w.err)
	}
	if w.timedOut {
		b.WriteString("; TIMED OUT")
	}
	return b.String()
}

func (w *workflowReport) stepsNote() string {
	switch {
	case w.timedOut:
		return " (timed out)"
	case w.err != nil:
		return " (aborted)"
	default:
		return ""
	}
}

// overhead is the branching overhead ratio: time spent in fork, checkout,
// checkpoint and destroy (summed across workers, so it can exceed wall
// under concurrency) over wall-clock time x concurrency, i.e. the fraction
// of the run's available worker time spent on branch management.
func (w *workflowReport) overhead() string {
	if w.wall <= 0 {
		return "n/a"
	}
	den := float64(w.wall) * float64(w.concurrency)
	return fmt.Sprintf("%.0f%%", 100*float64(w.branchTime)/den)
}

// cell renders "p50/p99 → p50/p99" in ms for depth 1 and the max depth.
func (w *workflowReport) cell(p50, p99 map[int]time.Duration) string {
	first := fmt.Sprintf("%s/%s", fmtMS(p50[1]), fmtMS(p99[1]))
	if w.maxDepth <= 1 {
		return first + " → (d=1 is max)"
	}
	return fmt.Sprintf("%s → %s/%s", first, fmtMS(p50[w.maxDepth]), fmtMS(p99[w.maxDepth]))
}

func fmtMS(d time.Duration) string { return fmt.Sprintf("%.1f", float64(d)/float64(time.Millisecond)) }

func fmtWall(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%d ms", d.Milliseconds())
	}
	return fmt.Sprintf("%.1f s", d.Seconds())
}

func fmtBytes(n int64) string {
	switch {
	case n < 0:
		return fmt.Sprintf("-%s", fmtBytes(-n))
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.0f MiB", float64(n)/(1<<20))
	default:
		return fmt.Sprintf("%.0f KiB", float64(n)/(1<<10))
	}
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// cpuModel reads the CPU brand from the OS, falling back to the core count
// alone when it cannot.
func cpuModel() string {
	switch runtime.GOOS {
	case "darwin":
		if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			return strings.TrimSpace(string(out))
		}
	case "linux":
		if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			for _, ln := range strings.Split(string(data), "\n") {
				if name, val, ok := strings.Cut(ln, ":"); ok && strings.TrimSpace(name) == "model name" {
					return strings.TrimSpace(val)
				}
			}
		}
	}
	return "unknown CPU"
}

// goVersion is runtime.Version() without its "go" prefix, so the header
// reads "Go 1.27.1" rather than "Go go1.27.1".
func goVersion() string { return strings.TrimPrefix(runtime.Version(), "go") }

func gitDescribe() string {
	out, err := exec.Command("git", "describe", "--tags", "--always", "--dirty").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}
