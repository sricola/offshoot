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
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
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
	stop := watchForInterrupt()
	defer stop()
	rep, err := run(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "branchbench: %v\n", err)
		os.Exit(1)
	}
	fmt.Print(rep.markdown())
	for _, w := range rep.workflows {
		if w.err != nil || w.timedOut {
			os.Exit(1)
		}
	}
}

// activeStores tracks the store directories this process owns, so an
// interrupted run does not silently leave tens of GB in $TMPDIR.
var activeStores struct {
	mu   sync.Mutex
	dirs map[string]bool
	keep bool
}

func trackStore(dir string, keep bool) {
	activeStores.mu.Lock()
	defer activeStores.mu.Unlock()
	if activeStores.dirs == nil {
		activeStores.dirs = map[string]bool{}
	}
	activeStores.dirs[dir] = true
	activeStores.keep = keep
}

func untrackStore(dir string) {
	activeStores.mu.Lock()
	defer activeStores.mu.Unlock()
	delete(activeStores.dirs, dir)
}

// watchForInterrupt removes (or, under -keep, reports) the live store
// directories on SIGINT/SIGTERM. A full run holds tens of GB at its peak;
// leaving that behind on Ctrl-C would be a nasty surprise.
func watchForInterrupt() func() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig, ok := <-sigs
		if !ok {
			return
		}
		activeStores.mu.Lock()
		dirs := make([]string, 0, len(activeStores.dirs))
		for d := range activeStores.dirs {
			dirs = append(dirs, d)
		}
		keep := activeStores.keep
		activeStores.mu.Unlock()
		for _, d := range dirs {
			if keep {
				fmt.Fprintf(os.Stderr, "branchbench: %s: store left at %s (-keep)\n", sig, d)
				continue
			}
			fmt.Fprintf(os.Stderr, "branchbench: %s: removing store %s\n", sig, d)
			if err := os.RemoveAll(d); err != nil {
				fmt.Fprintf(os.Stderr, "branchbench: could not remove %s: %v\n", d, err)
			}
		}
		os.Exit(130)
	}()
	return func() { signal.Stop(sigs); close(sigs) }
}

// run executes every requested workflow in order, each one against its OWN
// fresh store: a new directory, a new ops.Init, a new seed (the generator is
// deterministic, so every workflow starts from a byte-identical database),
// removed again before the next workflow starts. That is what keeps the rows
// independent — no workflow measures latency or bytes against the branches a
// previous one left behind — and what bounds peak disk to the largest single
// workflow rather than the sum of all five.
//
// run fails only on setup errors; a workflow that aborts or times out is
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
	rep := &report{concurrency: cfg.concurrency, quick: cfg.quick, warehouses: cfg.warehouses}
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
		wr, err := runOne(cfg, wf, rep)
		if err != nil {
			return nil, err
		}
		rep.workflows = append(rep.workflows, wr)
	}
	return rep, nil
}

// runOne gives one workflow its own store, seeds it, runs it, and removes the
// store again (unless -keep).
func runOne(cfg config, wf workflow, rep *report) (*workflowReport, error) {
	dir, err := workflowStore(cfg, wf.name)
	if err != nil {
		return nil, err
	}
	trackStore(dir, cfg.keep)
	fmt.Fprintf(os.Stderr, "branchbench: %s: store at %s\n", wf.name, dir)
	if cfg.keep {
		defer untrackStore(dir)
	} else {
		defer func() {
			if err := os.RemoveAll(dir); err != nil {
				fmt.Fprintf(os.Stderr, "branchbench: could not remove %s: %v\n", dir, err)
			}
			untrackStore(dir)
		}()
	}
	ws, err := ops.Init(dir)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(os.Stderr, "branchbench: %s: seeding %d-warehouse CH-benCHmark database...\n", wf.name, cfg.warehouses)
	seedBytes, err := buildSeed(ws, cfg.warehouses)
	if err != nil {
		return nil, fmt.Errorf("%s: seed: %w", wf.name, err)
	}
	// The seed is deterministic, so every workflow's is the same size; the
	// header reports it once. A mismatch would mean the generator is not
	// deterministic after all, which is worth failing on.
	if rep.seedBytes == 0 {
		rep.seedBytes = seedBytes
	} else if rep.seedBytes != seedBytes {
		return nil, fmt.Errorf("%s: seed is %d bytes, but an earlier workflow's seed was %d — the seed generator is not deterministic",
			wf.name, seedBytes, rep.seedBytes)
	}
	fmt.Fprintf(os.Stderr, "branchbench: %s (%s): %d workers x %d steps...\n", wf.name, wf.shape, wf.workers, wf.steps)
	r := &runner{ws: ws, wf: wf, cfg: cfg, store: dir, seedBytes: seedBytes,
		tree: newTree(wf), m: newMetrics(), sem: make(chan struct{}, cfg.concurrency)}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	wr := r.runWorkflow(ctx)
	cancel()
	wr.finalize()
	if wr.err != nil {
		fmt.Fprintf(os.Stderr, "branchbench: %s aborted after %d/%d steps: %v\n", wf.name, wr.stepsDone, wr.stepsTotal, wr.err)
	} else {
		fmt.Fprintf(os.Stderr, "branchbench: %s done: %d/%d steps in %s, store peaked at %s\n",
			wf.name, wr.stepsDone, wr.stepsTotal, wr.wall.Round(time.Millisecond), fmtBytes(wr.storePeak))
	}
	return wr, nil
}

// workflowStore returns a fresh, empty directory for one workflow's store:
// under -store when one was given, else a temp dir.
func workflowStore(cfg config, name string) (string, error) {
	if cfg.store == "" {
		return os.MkdirTemp("", "branchbench-"+name+"-*")
	}
	dir := filepath.Join(cfg.store, name)
	if err := os.RemoveAll(dir); err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return dir, nil
}

// dirBytes sums the apparent sizes of every file under dir (the store, plus
// the materialized checkouts that live inside it for a local store). skipped
// counts entries it could not stat — usually a file a concurrent destroy
// removed mid-walk, but the count is reported rather than swallowed, because
// a large one would mean the store figures are understated.
func dirBytes(dir string) (total int64, skipped int, err error) {
	err = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			skipped++
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			skipped++
			return nil
		}
		total += info.Size()
		return nil
	})
	return total, skipped, err
}
