// Package metrics is offshoot's hand-rolled, zero-dependency Prometheus
// text-exposition-format metrics registry. The exposition format itself is a
// handful of stable, trivially-reproducible lines (# HELP/# TYPE comments
// plus `name{labels} value` samples, cumulative histogram buckets, a
// trailing +Inf bucket, _sum/_count) — pulling in
// github.com/prometheus/client_golang for that would trade well under two
// hundred lines of code for a real dependency graph, which is not a good
// trade for a project whose design spec is explicit about preferring the
// stdlib (Milestone 4 PM Amendment 7). This package is kept `internal`
// specifically so a later swap to client_golang, if that dependency ever
// becomes worth it, is a call-site-only change inside this module, never a
// public API break for anything that consumes offshoot.
//
// Registry is the only exported entry point: it holds Counter, Gauge, and
// Histogram (fixed-bucket) metrics, plus labeled CounterVec/GaugeVec
// variants for metrics whose label sets are bounded by construction (see
// each locked metric's own doc comment in internal/daemon for its
// cardinality argument — this package itself does not enforce boundedness,
// callers do, by only ever calling WithLabelValues for values they know are
// finite in number). Every exported type here is safe for concurrent use.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// DefaultDurationBuckets are the fixed histogram buckets (seconds) offshoot
// uses for its duration histograms (flush/fork/checkpoint) — wide enough to
// span a sub-millisecond local flush and a multi-minute large-database fork
// or checkpoint, without per-metric tuning. Buckets are upper bounds (a
// Prometheus histogram's own convention): an observation of exactly a bound
// falls into that bucket (le = "<=").
var DefaultDurationBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
	1, 2.5, 5, 10, 30, 60, 120, 300,
}

// Registry holds every metric family registered against it, in registration
// order (preserved on output — WritePrometheus is deterministic run to run
// for a given sequence of New* calls, which golden-file tests rely on) and
// every scrape-time Collect callback. The zero value is not usable; use
// NewRegistry.
type Registry struct {
	mu         sync.Mutex
	families   []*family
	names      map[string]bool
	collectors []func()
}

// family is one registered metric (Counter/Gauge/Histogram/*Vec), holding
// enough to emit its HELP/TYPE header and delegate sample rendering to the
// metric's own render closure — captured at registration time so it closes
// over the concrete metric object without this package needing a shared
// "metric" interface every type must implement.
type family struct {
	name, help, typ string
	render          func(w *strings.Builder)
}

// NewRegistry returns an empty Registry ready for New*/Collect calls.
func NewRegistry() *Registry {
	return &Registry{names: map[string]bool{}}
}

// register records a new family under name, panicking if name was already
// registered on this Registry — a duplicate metric name is a programming
// error (two call sites racing to define the same metric), not a runtime
// condition any caller should need to handle, exactly like
// github.com/prometheus/client_golang's own MustRegister panics on
// AlreadyRegisteredError misuse.
func (r *Registry) register(name, help, typ string, render func(w *strings.Builder)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.names[name] {
		panic(fmt.Sprintf("metrics: %q already registered", name))
	}
	r.names[name] = true
	r.families = append(r.families, &family{name: name, help: help, typ: typ, render: render})
}

// Collect registers f to run once, synchronously, at the START of every
// WritePrometheus call, before any family is rendered — the mechanism for
// metrics whose value must reflect state AT SCRAPE TIME rather than being
// maintained incrementally (offshoot_sessions_open,
// offshoot_capture_lag_bytes, offshoot_durable_age_seconds: see
// internal/daemon's registration site for why those three specifically need
// this rather than being updated as events happen). f typically calls
// Set/Reset on Gauge/GaugeVec objects this same caller already holds a
// reference to. Collectors run with no Registry lock held (see
// WritePrometheus), so f is free to do real work — including, as the daemon's
// collector does, taking ITS OWN locks to read live state — without risking
// a deadlock against this Registry.
func (r *Registry) Collect(f func()) {
	r.mu.Lock()
	r.collectors = append(r.collectors, f)
	r.mu.Unlock()
}

// WritePrometheus runs every registered collector (in registration order),
// then writes every registered family, in registration order, as Prometheus
// text-exposition format: a "# HELP name help text" line, a "# TYPE name
// type" line, then one or more "name{labels} value" sample lines (cumulative
// bucket lines plus _sum/_count for a Histogram).
//
// Families are snapshotted under r.mu and collectors are run and families
// rendered AFTER releasing it: a collector (see Collect) is free to call
// WithLabelValues on any Vec this Registry holds, and Vec's own locking is
// entirely separate from Registry.mu (see CounterVec/GaugeVec's doc
// comments) — holding r.mu across collector execution would serve no
// purpose here (the family list itself never changes after startup
// registration) while adding a lock-ordering hazard for no benefit.
func (r *Registry) WritePrometheus(w io.Writer) error {
	r.mu.Lock()
	collectors := append([]func(){}, r.collectors...)
	families := append([]*family{}, r.families...)
	r.mu.Unlock()

	for _, c := range collectors {
		c()
	}

	var buf strings.Builder
	for _, f := range families {
		fmt.Fprintf(&buf, "# HELP %s %s\n", f.name, escapeHelp(f.help))
		fmt.Fprintf(&buf, "# TYPE %s %s\n", f.name, f.typ)
		f.render(&buf)
	}
	_, err := io.WriteString(w, buf.String())
	return err
}

// ---- Counter ----

// Counter is a monotonically-increasing value. The zero value is a valid
// Counter at 0, but callers get one via Registry.NewCounter /
// CounterVec.WithLabelValues rather than constructing one directly, so it is
// always attached to a registered family.
type Counter struct {
	bits atomic.Uint64 // math.Float64bits of the current value
}

// Add increases the counter by delta, which must be >= 0 — a negative delta
// on a Counter is a programming error (that is what Gauge is for), and
// panicking immediately at the call site is far more useful than silently
// producing a metric that occasionally goes backwards, which no Prometheus
// counter is ever supposed to do (rate() assumes monotonicity). Safe under
// concurrent callers via a compare-and-swap retry loop on the float64 bit
// pattern — cheaper than a mutex for the common case (no writer contention)
// and just as correct under it.
func (c *Counter) Add(delta float64) {
	if delta < 0 {
		panic(fmt.Sprintf("metrics: Counter.Add called with negative delta %v", delta))
	}
	for {
		old := c.bits.Load()
		next := math.Float64bits(math.Float64frombits(old) + delta)
		if c.bits.CompareAndSwap(old, next) {
			return
		}
	}
}

// Inc is Add(1).
func (c *Counter) Inc() { c.Add(1) }

// Value returns the counter's current value.
func (c *Counter) Value() float64 { return math.Float64frombits(c.bits.Load()) }

// NewCounter registers and returns a new, unlabeled Counter.
func (r *Registry) NewCounter(name, help string) *Counter {
	c := &Counter{}
	r.register(name, help, "counter", func(w *strings.Builder) {
		fmt.Fprintf(w, "%s %s\n", name, formatFloat(c.Value()))
	})
	return c
}

// ---- Gauge ----

// Gauge is a value that can move in either direction. Safe for concurrent
// use, same CAS-retry-loop approach as Counter.
type Gauge struct {
	bits atomic.Uint64
}

// Set stores v as the gauge's current value, discarding whatever was there.
func (g *Gauge) Set(v float64) { g.bits.Store(math.Float64bits(v)) }

// Add adds delta (which may be negative) to the gauge's current value.
func (g *Gauge) Add(delta float64) {
	for {
		old := g.bits.Load()
		next := math.Float64bits(math.Float64frombits(old) + delta)
		if g.bits.CompareAndSwap(old, next) {
			return
		}
	}
}

// Value returns the gauge's current value.
func (g *Gauge) Value() float64 { return math.Float64frombits(g.bits.Load()) }

// NewGauge registers and returns a new, unlabeled Gauge.
func (r *Registry) NewGauge(name, help string) *Gauge {
	g := &Gauge{}
	r.register(name, help, "gauge", func(w *strings.Builder) {
		fmt.Fprintf(w, "%s %s\n", name, formatFloat(g.Value()))
	})
	return g
}

// ---- Histogram ----

// Histogram observes float64 values into a fixed set of buckets (upper
// bounds, "le" = less-than-or-equal, plus an implicit trailing +Inf bucket)
// and tracks a running sum and count, per the Prometheus histogram
// exposition contract: each _bucket line is CUMULATIVE (includes every
// observation at or below its own bound, not just its own slice), so a
// consumer computing a quantile from the raw text needs no extra
// bucket-to-bucket arithmetic. None of offshoot's locked histograms carry
// labels (flush/fork/checkpoint duration are each singular, process-wide
// measures), so there is no HistogramVec — only Histogram.
type Histogram struct {
	bounds  []float64       // ascending; bounds[i] is bucket i's upper bound
	counts  []atomic.Uint64 // counts[i]: observations in (bounds[i-1], bounds[i]], own slice only — NOT cumulative; cumulative is computed at render time
	inf     atomic.Uint64   // observations > the largest bound
	sumBits atomic.Uint64   // math.Float64bits of the running sum
	total   atomic.Uint64   // total observation count
}

// Observe records v into the bucket for the smallest configured bound >= v,
// or the +Inf bucket if v exceeds every bound, and folds v into the running
// sum/count.
func (h *Histogram) Observe(v float64) {
	idx := sort.SearchFloat64s(h.bounds, v)
	if idx == len(h.bounds) {
		h.inf.Add(1)
	} else {
		h.counts[idx].Add(1)
	}
	h.total.Add(1)
	for {
		old := h.sumBits.Load()
		next := math.Float64bits(math.Float64frombits(old) + v)
		if h.sumBits.CompareAndSwap(old, next) {
			return
		}
	}
}

// NewHistogram registers and returns a new Histogram with the given bucket
// upper bounds (need not be pre-sorted; a defensive copy is sorted at
// construction so a caller's slice is never mutated or aliased).
func (r *Registry) NewHistogram(name, help string, buckets []float64) *Histogram {
	bounds := append([]float64(nil), buckets...)
	sort.Float64s(bounds)
	h := &Histogram{bounds: bounds, counts: make([]atomic.Uint64, len(bounds))}
	r.register(name, help, "histogram", func(w *strings.Builder) {
		var cumulative uint64
		for i, b := range h.bounds {
			cumulative += h.counts[i].Load()
			fmt.Fprintf(w, "%s_bucket{le=%q} %d\n", name, formatFloat(b), cumulative)
		}
		cumulative += h.inf.Load()
		fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, cumulative)
		fmt.Fprintf(w, "%s_sum %s\n", name, formatFloat(math.Float64frombits(h.sumBits.Load())))
		fmt.Fprintf(w, "%s_count %d\n", name, h.total.Load())
	})
	return h
}

// ---- CounterVec / GaugeVec ----

// CounterVec is a family of Counters distinguished by a fixed, ordered list
// of label names — e.g. offshoot_flush_total{result,kind}. Every child
// Counter is created lazily on first WithLabelValues call for that
// combination and lives for the process's lifetime (Counters, unlike
// Gauges, never need to "disappear" — a count of 0 for a combination that
// hasn't happened yet is exactly as meaningful as a count of 0 that once
// happened and stopped). Callers are responsible for the label set staying
// bounded (see the package doc comment); this type itself places no cap on
// distinct label combinations.
type CounterVec struct {
	labelNames []string

	mu     sync.Mutex
	values map[string][]string // key -> the actual label values, insertion order preserved via keys of children
	counts map[string]*Counter
}

// WithLabelValues returns the Counter for this exact ordered combination of
// label values, creating it (at 0) if this is the first time this
// combination has been seen. Panics if len(values) != the label names this
// vec was registered with — a caller passing the wrong arity is a
// programming error, not a runtime condition to handle gracefully.
func (v *CounterVec) WithLabelValues(values ...string) *Counter {
	if len(values) != len(v.labelNames) {
		panic(fmt.Sprintf("metrics: WithLabelValues got %d values, want %d (%v)",
			len(values), len(v.labelNames), v.labelNames))
	}
	key := vecKey(values)
	v.mu.Lock()
	defer v.mu.Unlock()
	c, ok := v.counts[key]
	if !ok {
		c = &Counter{}
		v.counts[key] = c
		v.values[key] = append([]string(nil), values...)
	}
	return c
}

// NewCounterVec registers and returns a new CounterVec with the given
// (ordered) label names.
func (r *Registry) NewCounterVec(name, help string, labelNames ...string) *CounterVec {
	v := &CounterVec{
		labelNames: append([]string(nil), labelNames...),
		values:     map[string][]string{},
		counts:     map[string]*Counter{},
	}
	r.register(name, help, "counter", func(w *strings.Builder) {
		v.mu.Lock()
		keys := make([]string, 0, len(v.counts))
		for k := range v.counts {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		type row struct {
			labels []string
			val    float64
		}
		rows := make([]row, 0, len(keys))
		for _, k := range keys {
			rows = append(rows, row{v.values[k], v.counts[k].Value()})
		}
		v.mu.Unlock()
		for _, rw := range rows {
			fmt.Fprintf(w, "%s{%s} %s\n", name, labelPairs(v.labelNames, rw.labels), formatFloat(rw.val))
		}
	})
	return v
}

// GaugeVec is CounterVec's Gauge counterpart, with one addition: Reset,
// needed because a gauge's label set legitimately shrinks (a session
// closes, its {db,branch} pair should stop being reported at all, not
// linger at a stale last-known value forever — see
// offshoot_capture_lag_bytes/offshoot_durable_age_seconds's doc comments in
// internal/daemon for the collector that relies on this).
type GaugeVec struct {
	labelNames []string

	mu     sync.Mutex
	values map[string][]string
	gauges map[string]*Gauge
}

// WithLabelValues returns the Gauge for this exact ordered combination of
// label values, creating it (at 0) if this is the first time this
// combination has been seen since construction or the last Reset. Same
// arity panic as CounterVec.WithLabelValues.
func (v *GaugeVec) WithLabelValues(values ...string) *Gauge {
	if len(values) != len(v.labelNames) {
		panic(fmt.Sprintf("metrics: WithLabelValues got %d values, want %d (%v)",
			len(values), len(v.labelNames), v.labelNames))
	}
	key := vecKey(values)
	v.mu.Lock()
	defer v.mu.Unlock()
	g, ok := v.gauges[key]
	if !ok {
		g = &Gauge{}
		v.gauges[key] = g
		v.values[key] = append([]string(nil), values...)
	}
	return g
}

// Reset discards every child Gauge this vec has ever created, so the next
// WithLabelValues call for any combination starts fresh at 0 and a scrape
// that runs before any WithLabelValues call sees no sample lines for this
// family at all. Intended to be called by a scrape-time Collect callback
// immediately before repopulating exactly the currently-live label
// combinations — see this type's doc comment.
func (v *GaugeVec) Reset() {
	v.mu.Lock()
	v.values = map[string][]string{}
	v.gauges = map[string]*Gauge{}
	v.mu.Unlock()
}

// NewGaugeVec registers and returns a new GaugeVec with the given (ordered)
// label names.
func (r *Registry) NewGaugeVec(name, help string, labelNames ...string) *GaugeVec {
	v := &GaugeVec{
		labelNames: append([]string(nil), labelNames...),
		values:     map[string][]string{},
		gauges:     map[string]*Gauge{},
	}
	r.register(name, help, "gauge", func(w *strings.Builder) {
		v.mu.Lock()
		keys := make([]string, 0, len(v.gauges))
		for k := range v.gauges {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		type row struct {
			labels []string
			val    float64
		}
		rows := make([]row, 0, len(keys))
		for _, k := range keys {
			rows = append(rows, row{v.values[k], v.gauges[k].Value()})
		}
		v.mu.Unlock()
		for _, rw := range rows {
			fmt.Fprintf(w, "%s{%s} %s\n", name, labelPairs(v.labelNames, rw.labels), formatFloat(rw.val))
		}
	})
	return v
}

// vecKey joins label values into a map key. "\x00" is not a valid byte
// inside any label value this codebase's Vec metrics are ever called with
// (store.ValidateName-constrained db/branch names, or fixed small enum
// strings like "ok"/"error"/"auto"/"manual"/"fast"/"slow" — see each
// locked-metric registration site in internal/daemon), so it cannot
// collide two distinct label combinations onto the same key.
func vecKey(values []string) string { return strings.Join(values, "\x00") }

// ---- exposition-format formatting helpers ----

// labelPairs renders name=value pairs (label values escaped and quoted) in
// the given, fixed name order — the same order every WithLabelValues call
// for this Vec is required to use, so labels always come out in a
// consistent, caller-controlled order rather than whatever order a map
// might have iterated in.
func labelPairs(names, values []string) string {
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = fmt.Sprintf("%s=\"%s\"", n, escapeLabelValue(values[i]))
	}
	return strings.Join(parts, ",")
}

// escapeLabelValue escapes a label value per the Prometheus text exposition
// format: backslash -> \\, double-quote -> \", newline -> \n. Order matters
// — backslash must be escaped FIRST, or a value containing a literal
// backslash followed by 'n' would be indistinguishable from an escaped
// newline once the newline pass ran second.
func escapeLabelValue(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// escapeHelp escapes a HELP line's text per the exposition format: backslash
// and newline only (HELP text is not quoted, so a literal double-quote needs
// no escaping there — unlike a label value).
func escapeHelp(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}

// formatFloat renders v the way every sample value and histogram bucket
// bound in this package's output is formatted: the shortest round-trippable
// decimal representation (matching what github.com/prometheus/client_golang
// itself produces, and what promtool's parser accepts), with Inf/NaN spelled
// out per the exposition format's own special-token rules.
func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	default:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
}
