// Command branchbench re-runs BranchBench's five agentic-workflow
// topologies (Ang, Kim, Weldon, Durand, Kaffes, Wu — Columbia DAPLab,
// arXiv:2604.17180) against a local offshoot store and prints one markdown
// table, which docs/benchmarks.md pastes verbatim.
//
// Only BranchBench's PARAMETERS (workers, steps, fanouts, depth, prune
// probability, per-step op mix, cross-branch query count) are reused — its
// harness carries no license, so no file, schema or SQL of it is copied.
// The seed here is our own CH-benCHmark-shaped generator (see seed.go), the
// workload is our own SQL (workload.go), and nothing in this program talks
// to a network or a daemon: each step is fork -> checkout -> database/sql ->
// checkpoint against internal/ops, the same in-process API the CLI uses.
package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sricola/offshoot/internal/ops"
)

type config struct {
	store       string
	workflows   []string
	concurrency int
	quick       bool
	warehouses  int
	timeout     time.Duration
	keep        bool
}

func main() {
	var (
		store       = flag.String("store", "", "store directory (default: a fresh temp dir, removed at exit unless -keep)")
		list        = flag.String("workflows", strings.Join(allWorkflowNames(), ","), "comma-separated workflows to run")
		concurrency = flag.Int("concurrency", 8, "worker goroutines in flight (1 = sequential)")
		quick       = flag.Bool("quick", false, "scale every workflow down (seconds, not minutes)")
		warehouses  = flag.Int("seed-warehouses", 10, "CH-benCHmark warehouses in the seed database")
		timeout     = flag.Duration("timeout", 2*time.Hour, "per-workflow wall-clock cap (BranchBench's own cap is 2h)")
		keep        = flag.Bool("keep", false, "keep the store directory after the run")
	)
	flag.Parse()
	cfg := config{
		store: *store, workflows: strings.Split(*list, ","), concurrency: *concurrency,
		quick: *quick, warehouses: *warehouses, timeout: *timeout, keep: *keep,
	}
	rep, err := run(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "branchbench: %v\n", err)
		os.Exit(1)
	}
	fmt.Print(rep.markdown())
	for _, w := range rep.workflows {
		if w.err != nil {
			os.Exit(1)
		}
	}
}

// run builds the seed store and executes every requested workflow in order.
// It fails only on setup errors; a workflow that aborts or times out is
// reported in the table with the step count it reached.
func run(cfg config) (*report, error) {
	if cfg.concurrency < 1 {
		return nil, fmt.Errorf("-concurrency must be >= 1")
	}
	if cfg.warehouses < 1 {
		return nil, fmt.Errorf("-seed-warehouses must be >= 1")
	}
	if cfg.timeout <= 0 {
		cfg.timeout = 2 * time.Hour
	}
	if cfg.store == "" {
		dir, err := os.MkdirTemp("", "branchbench-*")
		if err != nil {
			return nil, err
		}
		cfg.store = dir
		if cfg.keep {
			fmt.Fprintf(os.Stderr, "branchbench: store kept at %s\n", dir)
		} else {
			defer os.RemoveAll(dir)
		}
	}
	ws, err := ops.Init(cfg.store)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(os.Stderr, "branchbench: seeding %d-warehouse CH-benCHmark database...\n", cfg.warehouses)
	seedBytes, err := buildSeed(ws, cfg.warehouses)
	if err != nil {
		return nil, fmt.Errorf("seed: %w", err)
	}
	fmt.Fprintf(os.Stderr, "branchbench: seed is %.0f MiB\n", float64(seedBytes)/(1<<20))
	rep := &report{seedBytes: seedBytes, concurrency: cfg.concurrency, quick: cfg.quick, warehouses: cfg.warehouses}
	for _, name := range cfg.workflows {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		wf, ok := workflowByName(name)
		if !ok {
			return nil, fmt.Errorf("unknown workflow %q (have %s)", name, strings.Join(allWorkflowNames(), ", "))
		}
		if cfg.quick {
			wf = wf.quick()
		}
		fmt.Fprintf(os.Stderr, "branchbench: %s (%s): %d workers x %d steps...\n", wf.name, wf.shape, wf.workers, wf.steps)
		r := &runner{ws: ws, wf: wf, cfg: cfg, tree: newTree(wf), m: newMetrics(), sem: make(chan struct{}, cfg.concurrency)}
		ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
		wr := r.runWorkflow(ctx)
		cancel()
		wr.finalize()
		if wr.err != nil {
			fmt.Fprintf(os.Stderr, "branchbench: %s aborted after %d/%d steps: %v\n", wf.name, wr.stepsDone, wr.stepsTotal, wr.err)
		} else {
			fmt.Fprintf(os.Stderr, "branchbench: %s done: %d/%d steps in %s\n", wf.name, wr.stepsDone, wr.stepsTotal, wr.wall.Round(time.Millisecond))
		}
		rep.workflows = append(rep.workflows, wr)
	}
	return rep, nil
}

// dirBytes sums the apparent sizes of every file under dir (the store, plus
// the materialized checkouts that live inside it for a local store).
func dirBytes(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // a file destroyed mid-walk is not an error here
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total, err
}
