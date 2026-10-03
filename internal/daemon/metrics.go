package daemon

import (
	"io"
	"sort"
	"time"

	"github.com/sricola/offshoot/internal/dbfile"
	"github.com/sricola/offshoot/internal/metrics"
	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/session"
)

// Metrics is this daemon's metrics registry plus typed handles to every
// metric on Milestone 4 Task 2's LOCKED list — locked meaning the names
// below are API: renaming any of them after this daemon's first public
// release is a breaking change (see the plan's PM Amendment 2/7 and
// docs/status.md). Field names here are internal-only and free to change;
// the string literals passed to Registry.New* are what actually matters
// and must not drift from this list.
//
// Every metric is registered up front, in NewMetrics — registering the full
// locked set from the start, rather than growing the exposition output
// task-by-task, is what makes the name list actually locked from day one
// instead of accreting surprises later. offshoot_ro_cache_bytes/
// offshoot_ro_cache_evictions_total (Milestone 4 Task 5) are driven by the
// janitor's ro-cache pass (Server.janitorTick, internal/daemon/server.go) —
// both read 0 until the first janitor tick runs (or forever, on a daemon
// started with -reap-every 0, which disables the janitor entirely), which
// is itself informative: "no usage observed yet" for a gauge that updates
// once per pass, not continuously (see janitorTick's doc comment for the
// resulting between-passes staleness). The four offshoot_dbfile_* families
// (internal/dbfile's descriptor registry, the FD budget) are the opposite:
// read at scrape time by collectDBFile, never set by the janitor.
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
	// ForkModeTotal (offshoot_fork_mode_total{mode}) post-dates the
	// Milestone 4 locked list — added in v0.2.1 to complete the
	// copy-on-write observability story: shared vs materialized IS the
	// feature's storage cost, and status/branches only report it per-branch
	// at read time, not as a fork-time rate. Once shipped it is as locked as
	// the rest: renaming it is a breaking change.
	ForkModeTotal *metrics.CounterVec // {mode} — shared|materialized
	// RollbackTotal/PromoteTotal (offshoot_rollback_total{mode},
	// offshoot_promote_total{mode}) count repoints by storage mode, the
	// same split as ForkModeTotal: shared = a base pointer to the kept
	// history, materialized = a self-contained copy (the snapshot floor or
	// materialize). Locked once shipped, like the rest.
	RollbackTotal *metrics.CounterVec // {mode} — shared|materialized
	PromoteTotal  *metrics.CounterVec // {mode} — shared|materialized

	CheckpointDuration *metrics.Histogram
	// CheckpointOverwriteTotal (offshoot_checkpoint_overwrite_detected_total)
	// counts at-rest checkpoints whose post-CAS check found the store's
	// head may not be their own content: their object replaced by a racing
	// same-kind checkpoint's different content, a racer's snapshot anchoring
	// the head beside their segment, or an object they could not verify
	// (see ops.ObserveCheckpointOverwrite). Like CheckpointDuration it only
	// moves in a process that runs ops.Workspace.CheckpointWith itself.
	// Locked once shipped, like the rest.
	CheckpointOverwriteTotal *metrics.Counter

	ReapTotal         *metrics.Counter
	GCTombstonedTotal *metrics.Counter
	GCDeletedTotal    *metrics.Counter
	GCBacklog         *metrics.Gauge
	// GCErrorsTotal (offshoot_gc_errors_total) post-dates the Milestone 4
	// locked list — added in v0.2.1 because GC fails CLOSED (a mark that
	// cannot resolve every ref deletes nothing, by design), so a persistent
	// GC error means the store bloats silently unless it is surfaced. Once
	// shipped it is as locked as the rest: renaming it is a breaking change.
	GCErrorsTotal *metrics.Counter

	// ROCacheBytes/ROCacheEvictionsTotal: driven by the janitor's ro-cache
	// pass (Milestone 4 Task 5) — see this type's doc comment and
	// Server.janitorTick.
	ROCacheBytes          *metrics.Gauge
	ROCacheEvictionsTotal *metrics.Counter

	// DBFile*: internal/dbfile's descriptor registry, read at scrape time
	// (collectDBFile) because dbfile is process-wide and ops closes stranded
	// descriptors outside the janitor too. The counter mirrors dbfile's
	// cumulative totals, so every Server in a process reports the same
	// process-wide numbers; production runs one.
	DBFileDescriptors    *metrics.Gauge
	DBFilePins           *metrics.Gauge
	DBFileStrandedPinned *metrics.Gauge
	DBFileEvictedTotal   *metrics.CounterVec // {reason} stranded|budget

	JanitorRunsTotal *metrics.CounterVec // {result}
}

// newMetrics builds a fresh Metrics with every locked-list family
// registered (see Metrics's doc comment), and pre-populates every metric
// whose label set is a small, fixed, enumerable set of values (flush_total's
// result x kind, fork_total's path, fork_mode_total's mode,
// janitor_runs_total's result, dbfile_evicted_total's reason) so those
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
		ForkModeTotal: r.NewCounterVec("offshoot_fork_mode_total",
			"Successful forks, by storage mode (shared = a base pointer into the parent's chain, "+
				"zero data objects copied; materialized = a full snapshot copy in the child's own "+
				"lineage — the fork-time snapshot floor).",
			"mode"),
		RollbackTotal: r.NewCounterVec("offshoot_rollback_total",
			"Rollbacks whose repoint landed, by storage mode (shared = a base pointer to the kept "+
				"checkpoint, zero data objects copied; materialized = a full snapshot copy).",
			"mode"),
		PromoteTotal: r.NewCounterVec("offshoot_promote_total",
			"Promotes whose repoint landed, by storage mode (shared = a base pointer to the source "+
				"head, zero data objects copied; materialized = a full snapshot copy).",
			"mode"),

		CheckpointDuration: r.NewHistogram("offshoot_checkpoint_duration_seconds",
			"Successful at-rest checkpoint latency in seconds. Only populated by a process that calls "+
				"ops.Workspace.Checkpoint directly (CLI/MCP) — a live session's checkpoint is a named "+
				"flush instead, counted under offshoot_flush_duration_seconds; see internal/ops's "+
				"ObserveCheckpoint doc comment.", metrics.DefaultDurationBuckets),
		CheckpointOverwriteTotal: r.NewCounter("offshoot_checkpoint_overwrite_detected_total",
			"At-rest checkpoints that committed but found the store's head may not be their own content "+
				"(object replaced by a racing same-kind checkpoint, a racer's snapshot beside their segment, "+
				"or an object that could not be verified); unless the checkout equals the head, its checksum "+
				"is then not trusted and the next checkpoint writes a snapshot. Only populated by a process "+
				"that runs checkpoints itself (CLI/MCP)."),

		ReapTotal:         r.NewCounter("offshoot_reap_total", "Branches reaped by the janitor."),
		GCTombstonedTotal: r.NewCounter("offshoot_gc_tombstoned_total", "Objects newly tombstoned by GC."),
		GCDeletedTotal:    r.NewCounter("offshoot_gc_deleted_total", "Objects deleted by GC after their grace period."),
		GCBacklog: r.NewGauge("offshoot_gc_backlog",
			"Tombstoned objects currently awaiting GC's grace period before deletion."),
		GCErrorsTotal: r.NewCounter("offshoot_gc_errors_total",
			"Janitor GC passes that returned an error. GC fails closed (an incomplete reachability "+
				"mark deletes nothing), so a persistently increasing value means garbage is "+
				"accumulating unreclaimed — see the paired offshoot: janitor: gc: stderr line for the cause."),

		ROCacheBytes: r.NewGauge("offshoot_ro_cache_bytes",
			"Bytes used by the read-only checkout cache (checkouts-ro). Updated once per janitor pass, not continuously; see docs/reference.md."),
		ROCacheEvictionsTotal: r.NewCounter("offshoot_ro_cache_evictions_total",
			"Read-only checkout cache entries evicted by the janitor's LRU pass under -ro-cache-budget."),

		DBFileDescriptors: r.NewGauge("offshoot_dbfile_descriptors",
			"Checkout descriptors internal/dbfile holds open: cached plus stranded (path renamed over or removed). Read at scrape time."),
		DBFilePins: r.NewGauge("offshoot_dbfile_pins",
			"Outstanding pins on checkout inodes: one per open capture engine or in-process SQLite open, one per in-flight raw read. Read at scrape time."),
		DBFileStrandedPinned: r.NewGauge("offshoot_dbfile_stranded_pinned",
			"Stranded descriptors whose inode is still pinned. Non-zero briefly while a session or read outlives its file; non-zero across janitor passes is a pin leak."),
		DBFileEvictedTotal: r.NewCounterVec("offshoot_dbfile_evicted_total",
			"Descriptors internal/dbfile closed, by reason: stranded (its checkout was renamed over or removed) or budget (least recently used past -fd-budget).",
			"reason"),

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
	for _, mode := range []string{"shared", "materialized"} {
		m.ForkModeTotal.WithLabelValues(mode)
		m.RollbackTotal.WithLabelValues(mode)
		m.PromoteTotal.WithLabelValues(mode)
	}
	for _, reason := range []string{"stranded", "budget"} {
		m.DBFileEvictedTotal.WithLabelValues(reason)
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
func (m *Metrics) observeFork(dur time.Duration, fast, shared bool) {
	path := "slow"
	if fast {
		path = "fast"
	}
	m.ForkTotal.WithLabelValues(path).Inc()
	m.ForkModeTotal.WithLabelValues(storageMode(shared)).Inc()
	m.ForkDuration.Observe(dur.Seconds())
}

// storageMode is the {mode} label for a shared-or-copied lineage.
func storageMode(shared bool) string {
	if shared {
		return "shared"
	}
	return "materialized"
}

func (m *Metrics) observeRollback(shared bool) {
	m.RollbackTotal.WithLabelValues(storageMode(shared)).Inc()
}

func (m *Metrics) observePromote(shared bool) {
	m.PromoteTotal.WithLabelValues(storageMode(shared)).Inc()
}

func (m *Metrics) observeCheckpoint(dur time.Duration) {
	m.CheckpointDuration.Observe(dur.Seconds())
}

func (m *Metrics) observeCheckpointOverwrite() {
	m.CheckpointOverwriteTotal.Inc()
}

// wireHooks assigns this daemon's process-wide instrumentation hooks
// (ops.ObserveFork, ops.ObserveCheckpoint, ops.ObserveCheckpointOverwrite,
// ops.ObserveRollback, ops.ObservePromote, session.OnTransition) to close
// over m, and registers m's scrape-time collectors: the session gauges
// against srv, and the dbfile registry's (collectDBFile).
// Called once, from NewServer, before Serve starts accepting connections —
// see OnTransition/ObserveFork's own doc comments for why a single,
// process-wide assignment (rather than something scoped per-Server) is the
// right shape for a binary that only ever runs one daemon Server per
// process.
func (m *Metrics) wireHooks(srv *Server) {
	ops.ObserveFork = m.observeFork
	ops.ObserveCheckpoint = m.observeCheckpoint
	ops.ObserveCheckpointOverwrite = m.observeCheckpointOverwrite
	ops.ObserveRollback = m.observeRollback
	ops.ObservePromote = m.observePromote
	session.OnTransition = m.observeFlushTransition
	m.Registry.Collect(srv.collectSessionGauges)
	m.Registry.Collect(m.collectDBFile)
}

// collectDBFile is the Collect callback behind the offshoot_dbfile_*
// families: a point-in-time read of internal/dbfile's registry, the same
// scrape-time shape as collectSessionGauges.
func (m *Metrics) collectDBFile() {
	st := dbfile.ReadStats()
	m.DBFileDescriptors.Set(float64(st.Descriptors()))
	m.DBFilePins.Set(float64(st.Pins))
	m.DBFileStrandedPinned.Set(float64(st.StrandedPinned))
	syncCounter(m.DBFileEvictedTotal.WithLabelValues("stranded"), st.EvictedStranded)
	syncCounter(m.DBFileEvictedTotal.WithLabelValues("budget"), st.EvictedBudget)
}

// syncCounter raises c to total, a cumulative count kept elsewhere.
// Collectors run serialized per registry (Registry.WritePrometheus's
// scrapeMu), so no two calls race on c.
func syncCounter(c *metrics.Counter, total uint64) {
	if d := float64(total) - c.Value(); d > 0 {
		c.Add(d)
	}
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
