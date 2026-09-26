package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/store"
)

// runner executes one workflow against the store.
type runner struct {
	ws         *ops.Workspace
	wf         workflow
	cfg        config
	tree       *tree
	m          *metrics
	sem        chan struct{}
	schemaSeq  atomic.Int64
	orderSeq   atomic.Int64
	stepsDone  atomic.Int64
	maxDepth   atomic.Int64
	firstErr   error
	firstErrMu sync.Mutex
	cancel     context.CancelFunc
}

// maxCASAttempts bounds the retry of an ops call that lost a compare-and-
// swap race. Concurrent forks of different child names off the same parent
// do not contend on the parent's ref, but the store's manifest bump and the
// destroy claim are CAS writes, so a loss is possible under concurrency;
// retrying (rather than dropping to sequential) is what BranchBench's own
// multi-worker model expects.
const maxCASAttempts = 3

func isCASConflict(err error) bool {
	return errors.Is(err, store.ErrCAS) || strings.Contains(err.Error(), "compare-and-swap")
}

// timed runs one branch-management or eval operation, retrying a CAS loss,
// and records its latency at depth.
func (r *runner) timed(op string, depth int, fn func() error) error {
	start := time.Now()
	var err error
	for attempt := 1; ; attempt++ {
		err = fn()
		if err == nil || !isCASConflict(err) || attempt == maxCASAttempts {
			break
		}
		r.m.casRetry()
	}
	r.m.add(op, depth, time.Since(start))
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

// fail records the first error and stops the remaining workers: an ops
// error aborts the workflow at the step count reached so far, the way
// BranchBench reports a run that did not finish.
func (r *runner) fail(err error) {
	r.firstErrMu.Lock()
	if r.firstErr == nil {
		r.firstErr = err
	}
	r.firstErrMu.Unlock()
	if r.cancel != nil {
		r.cancel()
	}
}

func (r *runner) err() error {
	r.firstErrMu.Lock()
	defer r.firstErrMu.Unlock()
	return r.firstErr
}

// step runs one worker step: fork a child, materialize it, do the step's
// schema/mutation/eval work, checkpoint it so its own children can fork
// from published state, and prune it with probability gamma.
func (r *runner) step(wIdx, sIdx int, cur *node, rng *rand.Rand) (*node, error) {
	child := r.tree.reserve(cur, r.branchName(wIdx, sIdx))
	depth := child.depth
	for {
		cur := r.maxDepth.Load()
		if int64(depth) <= cur || r.maxDepth.CompareAndSwap(cur, int64(depth)) {
			break
		}
	}
	meta := map[string]string{
		"workflow": r.wf.name,
		"worker":   fmt.Sprint(wIdx),
		"step":     fmt.Sprint(sIdx),
		"depth":    fmt.Sprint(depth),
	}
	if err := r.timed("fork", depth, func() error {
		_, err := r.ws.Fork(seedDB, child.parent.name, child.name, "", 0, meta)
		return err
	}); err != nil {
		return nil, err
	}
	var path string
	if err := r.timed("checkout", depth, func() error {
		p, err := r.ws.Checkout(seedDB, child.name)
		path = p
		return err
	}); err != nil {
		return nil, err
	}
	if err := r.stepSQL(path, depth, rng); err != nil {
		return nil, err
	}
	if err := r.timed("checkpoint", depth, func() error {
		_, err := r.ws.Checkpoint(seedDB, child.name, "s", nil)
		return err
	}); err != nil {
		return nil, err
	}
	if rng.Float64() < r.wf.prune {
		if err := r.timed("destroy", depth, func() error {
			return r.ws.Destroy(seedDB, child.name, false)
		}); err != nil {
			return nil, err
		}
		r.tree.prune(child)
		return child.parent, nil
	}
	r.tree.publish(child)
	return child, nil
}

// stepSQL is the productive work of a step: M_s schema changes, M_d
// mutations, Q_v eval queries, against the branch's own checkout file with
// database/sql. The handle is closed before the caller checkpoints (a
// checkpoint must be able to quiesce the file).
func (r *runner) stepSQL(path string, depth int, rng *rand.Rand) error {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA synchronous=OFF`); err != nil {
		db.Close()
		return err
	}
	for i := 0; i < r.wf.schemaOps; i++ {
		if err := applySchemaChange(db, int(r.schemaSeq.Add(1))); err != nil {
			db.Close()
			return fmt.Errorf("schema change: %w", err)
		}
	}
	for i := 0; i < r.wf.mutations; i++ {
		id := mutationOrderBase + int(r.orderSeq.Add(1))
		if err := applyMutation(db, rng, id, r.cfg.warehouses); err != nil {
			db.Close()
			return fmt.Errorf("mutation: %w", err)
		}
	}
	for i := 0; i < r.wf.evals; i++ {
		q := evalQuery(r.wf, i)
		start := time.Now()
		if err := runQuery(db, q); err != nil {
			db.Close()
			return fmt.Errorf("eval query: %w", err)
		}
		r.m.add("eval", depth, time.Since(start))
	}
	return db.Close()
}

func (r *runner) branchName(wIdx, sIdx int) string {
	return fmt.Sprintf("%s-w%04d-s%03d", r.wf.branchPrefix(), wIdx, sIdx)
}

// crossBranch runs the workflow's C cross-branch queries. Each one walks
// every live branch, materializes it, sums ol_amount read-only, and
// aggregates in the driver — which is what BranchBench's harness does too
// (and what DoltHub's critique of it points out).
func (r *runner) crossBranch() (time.Duration, int, error) {
	if r.wf.crossBranch == 0 {
		return 0, 0, nil
	}
	branches := r.tree.liveBranches()
	start := time.Now()
	for q := 0; q < r.wf.crossBranch; q++ {
		var total float64
		for _, b := range branches {
			path, err := r.ws.Checkout(seedDB, b)
			if err != nil {
				return time.Since(start), len(branches), fmt.Errorf("cross-branch checkout %s: %w", b, err)
			}
			db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
			if err != nil {
				return time.Since(start), len(branches), err
			}
			db.SetMaxOpenConns(1)
			var sum sql.NullFloat64
			if err := db.QueryRow(`SELECT SUM(ol_amount) FROM order_line`).Scan(&sum); err != nil {
				db.Close()
				return time.Since(start), len(branches), fmt.Errorf("cross-branch query %s: %w", b, err)
			}
			total += sum.Float64
			if err := db.Close(); err != nil {
				return time.Since(start), len(branches), err
			}
		}
		_ = total // aggregated in the driver, as in BranchBench
	}
	return time.Since(start), len(branches), nil
}

// runWorkflow executes one workflow end to end and reports it.
func (r *runner) runWorkflow(ctx context.Context) *workflowReport {
	before, _ := dirBytes(r.cfg.store)
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.cancel = cancel
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < r.wf.workers; w++ {
		wg.Add(1)
		go func(wIdx int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(1000 + wIdx)))
			cur := r.tree.root
			for s := 0; s < r.wf.steps; s++ {
				if wctx.Err() != nil {
					return
				}
				r.sem <- struct{}{}
				next, err := r.step(wIdx, s, cur, rng)
				<-r.sem
				if err != nil {
					r.fail(fmt.Errorf("%s worker %d step %d: %w", r.wf.name, wIdx, s, err))
					return
				}
				cur = next
				r.stepsDone.Add(1)
			}
		}(w)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-wctx.Done():
		<-done
	}
	var (
		cbDur      time.Duration
		cbBranches int
	)
	if r.err() == nil && ctx.Err() == nil {
		d, n, err := r.crossBranch()
		cbDur, cbBranches = d, n
		if err != nil {
			r.fail(err)
		}
	}
	wall := time.Since(start)
	after, _ := dirBytes(r.cfg.store)
	branchTime, retries := r.m.totals()
	return &workflowReport{
		wf: r.wf, stepsDone: int(r.stepsDone.Load()), stepsTotal: r.wf.workers * r.wf.steps,
		wall: wall, branchTime: branchTime, m: r.m,
		maxDepth: int(r.maxDepth.Load()), peakLive: r.tree.peak, storeDelta: after - before,
		crossDur: cbDur, crossBranches: cbBranches, crossQueries: r.wf.crossBranch,
		casRetries: retries, timedOut: ctx.Err() != nil, err: r.err(),
		concurrency: cap(r.sem),
	}
}
