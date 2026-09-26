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
	"sync/atomic"
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
		store       = flag.String("store", "", "store directory (default: a fresh temp dir, removed at exit unless -keep); each workflow uses DIR/<workflow>, which is removed before the run and again after unless -keep")
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
	ctx, cancel := context.WithCancel(context.Background())
	interrupted, stop := watchForInterrupt(cancel)
	defer stop()
	rep, err := run(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "branchbench: %v\n", err)
		os.Exit(1)
	}
	if interrupted() {
		// The context cancellation above already unwound the in-flight
		// workflow: its workers stopped at the next step boundary,
		// runWorkflow returned, and runOne's own deferred cleanup removed
		// the store (or left it, under -keep) before run() returned here.
		os.Exit(130)
	}
	fmt.Print(rep.markdown())
	for _, w := range rep.workflows {
		if w.err != nil || w.timedOut {
			os.Exit(1)
		}
	}
}

// watchForInterrupt cancels ctx on SIGINT/SIGTERM instead of reaching into
// the store directly: canceling lets runOne's own deferred cleanup remove
// the store after the in-flight workflow's workers actually stop, rather
// than a signal handler's os.RemoveAll racing them while they still write
// to it. The returned interrupted func reports whether a signal was seen.
func watchForInterrupt(cancel context.CancelFunc) (interrupted func() bool, stop func()) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	var got atomic.Bool
	go func() {
		sig, ok := <-sigs
		if !ok {
			return
		}
		got.Store(true)
		fmt.Fprintf(os.Stderr, "branchbench: %s: stopping (finishing the in-flight step, then removing the store unless -keep)\n", sig)
		cancel()
	}()
	return got.Load, func() { signal.Stop(sigs); close(sigs) }
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
// reported in the table with the step count it reached. If ctx is canceled
// (SIGINT/SIGTERM — see watchForInterrupt), run finishes unwinding whichever
// workflow is in flight and then stops rather than starting the next one.
func run(ctx context.Context, cfg config) (*report, error) {
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
		wr, err := runOne(ctx, cfg, wf, rep)
		if err != nil {
			return nil, err
		}
		rep.workflows = append(rep.workflows, wr)
		if ctx.Err() != nil {
			break
		}
	}
	return rep, nil
}

// runOne gives one workflow its own store, seeds it, runs it, and removes the
// store again (unless -keep). ctx is the run's top-level context: canceling
// it (SIGINT/SIGTERM) propagates into runWorkflow's own child context, so
// the workers stop at the next step boundary and this function's deferred
// cleanup below still runs before returning.
func runOne(ctx context.Context, cfg config, wf workflow, rep *report) (*workflowReport, error) {
	dir, err := workflowStore(cfg, wf.name)
	if err != nil {
		return nil, err
	}
	if !cfg.keep {
		defer func() {
			if err := os.RemoveAll(dir); err != nil {
				fmt.Fprintf(os.Stderr, "branchbench: could not remove %s: %v\n", dir, err)
			}
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
	wctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	wr := r.runWorkflow(wctx)
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
// under -store when one was given (as DIR/<workflow>, removed before the
// run and again after unless -keep), else a temp dir. The path is printed
// before anything already there is removed, so it's on the record even if
// the removal itself fails or is interrupted.
func workflowStore(cfg config, name string) (string, error) {
	if cfg.store == "" {
		dir, err := os.MkdirTemp("", "branchbench-"+name+"-*")
		if err != nil {
			return "", err
		}
		fmt.Fprintf(os.Stderr, "branchbench: %s: store at %s\n", name, dir)
		return dir, nil
	}
	dir := filepath.Join(cfg.store, name)
	fmt.Fprintf(os.Stderr, "branchbench: %s: store at %s\n", name, dir)
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
