package daemon

import (
	"fmt"
	"os"
	"time"
)

// StartJanitor reaps expired branches and runs GC every interval until
// Shutdown. grace is passed to GC (tombstone age before deletion); the
// default in cmd is deliberately generous — the spec requires it to exceed
// the longest plausible in-flight fork. every <= 0 disables the janitor
// entirely (no goroutine is started).
//
// Sessions opened by this daemon hold live leases, so the janitor can never
// reap a branch this daemon is actively writing to (see ops.ReapDeadline: a live
// lease only ever pushes a branch's deadline into the future).
//
// Refuses to start once Shutdown has begun (s.closing): Shutdown's own
// close(s.janitorStop)+janitorWG.Wait() sequence assumes every Add it needs
// to wait for has already happened. A janitorWG.Add racing after Shutdown's
// Wait could observe the counter at zero and return early, or — per
// sync.WaitGroup's contract that a positive-delta Add must happen before any
// Wait call it's meant to be counted by — panic outright. Checking and
// adding under s.mu, the same lock Shutdown takes to set closing, makes the
// two calls linearize: either this call observes closing and backs off, or
// it wins the race and its Add is guaranteed visible to Shutdown's Wait.
func (s *Server) StartJanitor(every, grace time.Duration) {
	if every <= 0 {
		return
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	s.janitorWG.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.janitorWG.Done()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-s.janitorStop: // closed by Shutdown before it waits on janitorWG
				return
			case <-t.C:
				s.janitorTick(grace)
			}
		}
	}()
}

// janitorTick runs one reap+GC+ro-cache+stale-delete-claim pass and updates every
// janitor-sourced metric (offshoot_reap_total, offshoot_gc_tombstoned_total,
// offshoot_gc_deleted_total, offshoot_gc_errors_total, offshoot_gc_backlog,
// offshoot_ro_cache_bytes, offshoot_ro_cache_evictions_total,
// offshoot_janitor_runs_total{result})
// from its results — split out of StartJanitor's ticker loop so a test can
// drive exactly one tick deterministically instead of waiting on a real
// ticker. Reap/GC results are counted even when they also return an error:
// both ops.Workspace.Reap and ops.Workspace.GC keep processing everything
// they can and report a partial result alongside the first error they hit
// (see their own doc comments), so len(reaped)/tombstoned/deleted are real
// work actually done, not discarded just because something else in the same
// pass failed. ops.Workspace.EvictROCache follows the identical convention
// (see its own doc comment) for the same reason.
// offshoot_janitor_runs_total{result} is "error" if ANY step failed, "ok"
// only if all fully succeeded — a single counter per tick, not one per
// step, matching the locked metric's one label (result), not two.
func (s *Server) janitorTick(grace time.Duration) {
	failed := false

	reaped, reapErr := s.ws.Reap(time.Now())
	if reapErr != nil {
		fmt.Fprintf(os.Stderr, "offshoot: janitor: reap: %v\n", reapErr)
		failed = true
	} else if len(reaped) > 0 {
		fmt.Fprintf(os.Stderr, "offshoot: janitor: reaped %v\n", reaped)
	}
	if len(reaped) > 0 {
		s.metrics.ReapTotal.Add(float64(len(reaped)))
		// Milestone 4 Task 4a: one "reaped" event per branch the janitor
		// just destroyed, fed straight to the bus — publish is non-
		// blocking (see eventBus.publish's doc comment), so this can never
		// stall the janitor loop regardless of how many (or how slow)
		// subscribers exist.
		for _, k := range reaped {
			db, branch := splitKey(k)
			s.events.publish(newEvent("reaped", db, branch, nil))
		}
	}

	// Milestone 4 Task 6b: self-heal any Destroy claim (Ref.Deleting)
	// stranded by a crashed Destroy call, same cadence as reap/GC — see
	// ops.Workspace.ClearStaleDeleteClaims's doc comment for why this needs
	// its own age-based pass rather than piggybacking on Reap's own
	// deadline-driven self-heal.
	healed, healErr := s.ws.ClearStaleDeleteClaims(time.Now())
	if healErr != nil {
		fmt.Fprintf(os.Stderr, "offshoot: janitor: clear stale delete claims: %v\n", healErr)
		failed = true
	} else if len(healed) > 0 {
		fmt.Fprintf(os.Stderr, "offshoot: janitor: cleared stale delete claim(s) %v\n", healed)
	}

	tombstoned, deleted, gcErr := s.ws.GC(grace)
	if gcErr != nil {
		// LOUD by design: GC fails closed (a mark that cannot resolve every
		// ref deletes nothing — see ops.Workspace.GC/reachableObjects), so a
		// recurring error here means the store is bloating with nothing
		// reclaimed. The counter is the alertable signal; this stderr line
		// carries the cause.
		fmt.Fprintf(os.Stderr, "offshoot: janitor: gc: %v\n", gcErr)
		s.metrics.GCErrorsTotal.Inc()
		failed = true
	}
	if tombstoned > 0 {
		s.metrics.GCTombstonedTotal.Add(float64(tombstoned))
	}
	if deleted > 0 {
		s.metrics.GCDeletedTotal.Add(float64(deleted))
	}
	if backlog, err := s.ws.TombstoneBacklog(); err != nil {
		fmt.Fprintf(os.Stderr, "offshoot: janitor: gc backlog: %v\n", err)
	} else {
		s.metrics.GCBacklog.Set(float64(backlog))
	}

	// Milestone 4 Task 5: ro-cache budget/eviction. s.roCacheBudget is read
	// under s.mu (set once, before Serve starts accepting connections, via
	// SetROCacheBudget — same single-writer-before-Serve contract as
	// flushEvery), mirroring opOpen's own read of flushEvery.
	s.mu.Lock()
	budget := s.roCacheBudget
	s.mu.Unlock()
	evicted, usage, roErr := s.ws.EvictROCache(budget)
	if roErr != nil {
		fmt.Fprintf(os.Stderr, "offshoot: janitor: ro-cache: %v\n", roErr)
		failed = true
	}
	// Always set, even when budget <= 0 (unlimited) or usage is already
	// under budget: offshoot_ro_cache_bytes reports CURRENT usage every
	// pass regardless of whether this pass evicted anything, matching
	// GCBacklog's own always-set-even-at-zero treatment above. This is a
	// janitor-pass-cadence gauge, not a scrape-time one (unlike
	// SessionsOpen/CaptureLagBytes/DurableAgeSeconds in metrics.go's
	// collectSessionGauges) — see EvictROCache's doc comment and
	// docs/reference.md's `-ro-cache-budget` section for the staleness this
	// implies between passes.
	s.metrics.ROCacheBytes.Set(float64(usage))
	if len(evicted) > 0 {
		s.metrics.ROCacheEvictionsTotal.Add(float64(len(evicted)))
	}
	for _, e := range evicted {
		// Eviction is LOUD by design (Global Constraints): a log line per
		// eviction, on top of the counter/gauge above and the "evicted"
		// event below — an operator watching stderr sees exactly what left
		// disk and why, the same family as this function's other
		// "offshoot: janitor: ..." lines.
		fmt.Fprintf(os.Stderr, "offshoot: janitor: ro-cache: evicted %s@%s@%s (%d bytes)\n",
			e.DB, e.Branch, e.Checkpoint, e.Bytes)
		// Milestone 4 Task 4a reserved this event type with no emitter;
		// this is that emitter. publish is non-blocking (see
		// eventBus.publish's doc comment) — never stalls the janitor loop
		// regardless of subscriber count/speed, exactly like the "reaped"
		// publish above.
		s.events.publish(newEvent("evicted", e.DB, e.Branch, map[string]any{
			"checkpoint": e.Checkpoint,
			"bytes":      e.Bytes,
		}))
	}

	result := "ok"
	if failed {
		result = "error"
	}
	s.metrics.JanitorRunsTotal.WithLabelValues(result).Inc()
}
