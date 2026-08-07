package daemon

import (
	"io"
	"sort"
	"time"

	"github.com/offshoot-db/offshoot/internal/metrics"
	"github.com/offshoot-db/offshoot/internal/ops"
	"github.com/offshoot-db/offshoot/internal/session"
)

// Metrics is this daemon's metrics registry plus typed handles to every
// metric on Milestone 4 Task 2's LOCKED list — locked meaning the names
// below are API: renaming any of them after this daemon's first public
// release is a breaking change (see the plan's PM Amendment 2/7 and
// docs/status.md). Field names here are internal-only and free to change;
// the string literals passed to Registry.New* are what actually matters
// and must not drift from this list.
//
// Every metric is registered up front, in NewMetrics, even the two whose
// real data doesn't exist yet (offshoot_ro_cache_bytes,
// offshoot_ro_cache_evictions_total — Milestone 4 Task 5 wires the ro-cache
// janitor pass that actually moves these; until then they read 0, which is
// itself informative — "not evicting anything" is a true statement about a
// budget that doesn't exist yet) — registering the full locked set NOW,
// rather than growing the exposition output task-by-task, is what makes the
// name list actually locked from day one instead of accreting surprises
// later.
type Metrics struct {
	Registry *metrics.Registry

	BuildInfo *metrics.GaugeVec // {version}
	// buildVersion is set by SetVersion; guarded by no lock of its own
	// because it is written at most once, before Serve starts accepting
	// connections (see Server.SetVersion's doc comment, mirroring
	// SetFlushEvery's own single-writer-before-Serve contract).
	buildVersion string

	SessionsOpen      *metrics.Gauge
	CaptureLagBytes   *metrics.GaugeVec // {db,branch} — open sessions only
	DurableAgeSeconds *metrics.GaugeVec // {db,branch} — open sessions only

	FlushTotal    *metrics.CounterVec // {result,kind}
	FlushDuration *metrics.Histogram

	ForkTotal    *metrics.CounterVec // {path}
	ForkDuration *metrics.Histogram

	CheckpointDuration *metrics.Histogram

	ReapTotal         *metrics.Counter
	GCTombstonedTotal *metrics.Counter
	GCDeletedTotal    *metrics.Counter
	GCBacklog         *metrics.Gauge

	// ROCacheBytes/ROCacheEvictionsTotal: registered now at zero: Task 5
	// wires the ro-cache budget/eviction janitor pass that gives these real
	// values. See this type's doc comment.
	ROCacheBytes          *metrics.Gauge
	ROCacheEvictionsTotal *metrics.Counter

	JanitorRunsTotal *metrics.CounterVec // {result}
}

// newMetrics builds a fresh Metrics with every locked-list family
// registered (see Metrics's doc comment), and pre-populates every metric
// whose label set is a small, fixed, enumerable set of values (flush_total's
// result x kind, fork_total's path, janitor_runs_total's result) so those
// combinations expose a real "0" sample from the very first scrape rather
// than being entirely absent until their first occurrence — a rate() query
// over a combination that has genuinely never happened reads as 0, not "no
// data", exactly matching every combination that HAS happened at least
// once. capture_lag_bytes/durable_age_seconds/sessions_open are NOT
// pre-populated here: their label sets (and, for sessions_open, existence at
// all) are computed fresh at every scrape by the Collect callback
// registerSessionGaugeCollector adds — see its doc comment for why that is
// scrape-time, not incremental, by design.
func newMetrics() *Metrics {
	r := metrics.NewRegistry()
	m := &Metrics{
		Registry:  r,
		BuildInfo: r.NewGaugeVec("offshoot_build_info", "Always 1; the \"version\" label identifies the running build.", "version"),

		SessionsOpen: r.NewGauge("offshoot_sessions_open",
			"Number of sessions currently open in this daemon."),
		CaptureLagBytes: r.NewGaugeVec("offshoot_capture_lag_bytes",
			"WAL bytes committed by writers but not yet applied to the replica. Open sessions only.",
			"db", "branch"),
		DurableAgeSeconds: r.NewGaugeVec("offshoot_durable_age_seconds",
			"Seconds since the last successful flush. Open sessions only.",
			"db", "branch"),

		FlushTotal: r.NewCounterVec("offshoot_flush_total",
			"Session flushes, by result and kind.", "result", "kind"),
		FlushDuration: r.NewHistogram("offshoot_flush_duration_seconds",
			"Session flush latency in seconds.", metrics.DefaultDurationBuckets),

		ForkTotal: r.NewCounterVec("offshoot_fork_total",
			"Successful forks, by path (fast = single-snapshot object copy, slow = materialize + re-encode).",
			"path"),
		ForkDuration: r.NewHistogram("offshoot_fork_duration_seconds",
			"Successful fork latency in seconds.", metrics.DefaultDurationBuckets),

		CheckpointDuration: r.NewHistogram("offshoot_checkpoint_duration_seconds",
			"Successful at-rest checkpoint latency in seconds. Only populated by a process that calls "+
				"ops.Workspace.Checkpoint directly (CLI/MCP) — a live session's checkpoint is a named "+
				"flush instead, counted under offshoot_flush_duration_seconds; see internal/ops's "+
				"ObserveCheckpoint doc comment.", metrics.DefaultDurationBuckets),

		ReapTotal:         r.NewCounter("offshoot_reap_total", "Branches reaped by the janitor."),
		GCTombstonedTotal: r.NewCounter("offshoot_gc_tombstoned_total", "Lineages newly tombstoned by GC."),
		GCDeletedTotal:    r.NewCounter("offshoot_gc_deleted_total", "Lineages deleted by GC after their grace period."),
		GCBacklog: r.NewGauge("offshoot_gc_backlog",
			"Tombstoned lineages currently awaiting GC's grace period before deletion."),

		ROCacheBytes: r.NewGauge("offshoot_ro_cache_bytes",
			"Bytes used by the read-only checkout cache. Always 0 until Milestone 4 Task 5 wires the ro-cache budget/eviction pass."),
		ROCacheEvictionsTotal: r.NewCounter("offshoot_ro_cache_evictions_total",
			"Read-only checkout cache entries evicted. Always 0 until Milestone 4 Task 5."),

		JanitorRunsTotal: r.NewCounterVec("offshoot_janitor_runs_total",
			"Janitor loop ticks, by result.", "result"),
	}
	m.buildVersion = "dev"
	m.BuildInfo.WithLabelValues(m.buildVersion).Set(1)

	for _, result := range []string{"ok", "error"} {
		for _, kind := range []string{"auto", "manual"} {
			m.FlushTotal.WithLabelValues(result, kind)
		}
		m.JanitorRunsTotal.WithLabelValues(result)
	}
	for _, path := range []string{"fast", "slow"} {
		m.ForkTotal.WithLabelValues(path)
	}
	return m
}

// setVersion updates offshoot_build_info to report v instead of the "dev"
// default, clearing the prior label combination first (BuildInfo is a
// GaugeVec purely so its value carries a "version" label — there is always
// exactly one live version per process, never more than one combination at
// once).
func (m *Metrics) setVersion(v string) {
	m.BuildInfo.Reset()
	m.buildVersion = v
	m.BuildInfo.WithLabelValues(v).Set(1)
}

// observeFlushTransition is session.OnTransition's implementation for this
// daemon: it reads the "flushed"/"flush-failed" transitions' kv (see
// session.OnTransition's doc comment for the shape session's logTransition
// produces) and drives FlushTotal/FlushDuration. Every other transition kind
// (opened, fenced, closed, sidecar-refresh-skipped) is a no-op here — T2's
// locked metric list has no per-transition metric for them (sessions_open is
// computed at scrape time instead, see registerSessionGaugeCollector).
func (m *Metrics) observeFlushTransition(db, branch, event string, kv []any) {
	switch event {
	case "flushed", "flush-failed":
	default:
		return
	}
	result := "ok"
	if event == "flush-failed" {
		result = "error"
	}
	kind, _ := kvString(kv, "kind")
	if kind != "auto" && kind != "manual" {
		// Defensive: every real call site always sets "kind" to one of
		// these two (see session/flush.go's flushKind) — this only trips if
		// that ever changes without updating this switch, and falling back
		// to "manual" (the more common, less surprising default for an
		// unrecognized value) beats silently dropping the observation.
		kind = "manual"
	}
	m.FlushTotal.WithLabelValues(result, kind).Inc()
	if d, ok := kvFloat(kv, "duration_seconds"); ok {
		m.FlushDuration.Observe(d)
	}
}

// kvString/kvFloat read session.OnTransition's flat kv slice
// (["key1", val1, "key2", val2, ...] — see logTransition's doc comment) for
// a specific key, returning ok=false if the key is absent or its value
// isn't the expected type. Small, local helpers rather than a general kv
// package: this flat shape is specific to session's transition-log kv and
// not meant to be reused more broadly (Milestone 4 Task 4a's real event
// schema replaces it).
func kvString(kv []any, key string) (string, bool) {
	for i := 0; i+1 < len(kv); i += 2 {
		if k, ok := kv[i].(string); ok && k == key {
			v, ok := kv[i+1].(string)
			return v, ok
		}
	}
	return "", false
}

func kvFloat(kv []any, key string) (float64, bool) {
	for i := 0; i+1 < len(kv); i += 2 {
		if k, ok := kv[i].(string); ok && k == key {
			v, ok := kv[i+1].(float64)
			return v, ok
		}
	}
	return 0, false
}

// observeFork/observeCheckpoint are ops.ObserveFork/ops.ObserveCheckpoint's
// implementations for this daemon — see those package-level hooks' doc
// comments in internal/ops for the injection shape and why ops itself
// cannot import internal/metrics.
func (m *Metrics) observeFork(dur time.Duration, fast bool) {
	path := "slow"
	if fast {
		path = "fast"
	}
	m.ForkTotal.WithLabelValues(path).Inc()
	m.ForkDuration.Observe(dur.Seconds())
}

func (m *Metrics) observeCheckpoint(dur time.Duration) {
	m.CheckpointDuration.Observe(dur.Seconds())
}

// wireHooks assigns this daemon's process-wide instrumentation hooks
// (ops.ObserveFork, ops.ObserveCheckpoint, session.OnTransition) to close
// over m, and registers m's scrape-time session-gauge collector against srv.
// Called once, from NewServer, before Serve starts accepting connections —
// see OnTransition/ObserveFork's own doc comments for why a single,
// process-wide assignment (rather than something scoped per-Server) is the
// right shape for a binary that only ever runs one daemon Server per
// process.
func (m *Metrics) wireHooks(srv *Server) {
	ops.ObserveFork = m.observeFork
	ops.ObserveCheckpoint = m.observeCheckpoint
	session.OnTransition = m.observeFlushTransition
	m.Registry.Collect(srv.collectSessionGauges)
}

// collectSessionGauges is the Collect callback backing
// offshoot_sessions_open/offshoot_capture_lag_bytes/
// offshoot_durable_age_seconds — computed AT SCRAPE TIME from s.sessions,
// per this task's brief, rather than maintained incrementally: those three
// numbers are cheap point-in-time reads off already-live Session objects
// (CaptureLag stats a file; LastFlush reads an in-memory field — see
// Session.CaptureLag/LastFlush's own doc comments), so recomputing them
// fresh on every scrape is both simpler (no separate "a session closed,
// stop reporting it" bookkeeping to keep in sync — see GaugeVec.Reset's doc
// comment) and more honest (a value that is always exactly as current as
// the scrape that reads it, never stale between events).
//
// Mirrors opStatus's own snapshot-under-lock-then-build-outside-it shape
// (see opStatus's doc comment) for the identical reason: sess.CaptureLag()
// does real file I/O, and s.mu is the one lock every other RPC in this
// daemon also needs — building gauge values while holding it would
// serialize every concurrent open/flush/status/close behind however long a
// scrape's per-session I/O takes.
func (s *Server) collectSessionGauges() {
	s.mu.Lock()
	keys := make([]string, 0, len(s.sessions))
	for k, sess := range s.sessions {
		if sess != nil {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	sessList := make([]*session.Session, 0, len(keys))
	for _, k := range keys {
		sessList = append(sessList, s.sessions[k])
	}
	s.mu.Unlock()

	s.metrics.SessionsOpen.Set(float64(len(sessList)))
	s.metrics.CaptureLagBytes.Reset()
	s.metrics.DurableAgeSeconds.Reset()
	now := time.Now()
	for _, sess := range sessList {
		s.metrics.CaptureLagBytes.WithLabelValues(sess.DB(), sess.Branch()).Set(float64(sess.CaptureLag()))
		if t, _, ok := sess.LastFlush(); ok {
			s.metrics.DurableAgeSeconds.WithLabelValues(sess.DB(), sess.Branch()).Set(now.Sub(t).Seconds())
		}
	}
}

// WritePrometheus writes this daemon's full metrics exposition to w — the
// registry-scrape primitive Task 3's GET /metrics handler will call
// directly once it exists; exercised today by this daemon's own tests (see
// metrics_test.go) and available to any embedder that wants the numbers
// without an HTTP round trip.
func (s *Server) WritePrometheus(w io.Writer) error {
	return s.metrics.Registry.WritePrometheus(w)
}
