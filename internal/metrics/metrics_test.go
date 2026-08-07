package metrics

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestCounterConcurrentIncrements exercises Counter.Add's CAS-retry loop
// under real contention: many goroutines incrementing the same Counter must
// never lose an update (the classic "read-modify-CAS" race a naive
// load-then-store would drop under -race/-count>1). Run with -race to catch
// any unsynchronized access, not just verify the final count.
func TestCounterConcurrentIncrements(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("test_counter", "a counter")
	const goroutines = 50
	const perGoroutine = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				c.Inc()
			}
		}()
	}
	wg.Wait()
	want := float64(goroutines * perGoroutine)
	if got := c.Value(); got != want {
		t.Fatalf("counter value = %v, want %v (lost updates under concurrency)", got, want)
	}
}

// TestCounterVecConcurrentDifferentLabels exercises the more daemon-realistic
// shape: many goroutines incrementing a bounded, small set of label
// combinations (mirroring offshoot_flush_total{result,kind}) concurrently.
func TestCounterVecConcurrentDifferentLabels(t *testing.T) {
	r := NewRegistry()
	v := r.NewCounterVec("test_vec_counter", "a labeled counter", "result", "kind")
	combos := [][2]string{{"ok", "auto"}, {"ok", "manual"}, {"error", "auto"}, {"error", "manual"}}
	const perCombo = 500
	var wg sync.WaitGroup
	for _, combo := range combos {
		combo := combo
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perCombo; i++ {
				v.WithLabelValues(combo[0], combo[1]).Inc()
			}
		}()
	}
	wg.Wait()
	for _, combo := range combos {
		if got := v.WithLabelValues(combo[0], combo[1]).Value(); got != perCombo {
			t.Errorf("combo %v = %v, want %v", combo, got, perCombo)
		}
	}
}

func TestGaugeSetAndAdd(t *testing.T) {
	r := NewRegistry()
	g := r.NewGauge("test_gauge", "a gauge")
	if got := g.Value(); got != 0 {
		t.Fatalf("zero-value gauge = %v, want 0", got)
	}
	g.Set(42.5)
	if got := g.Value(); got != 42.5 {
		t.Fatalf("after Set(42.5) = %v, want 42.5", got)
	}
	g.Add(-10)
	if got := g.Value(); got != 32.5 {
		t.Fatalf("after Add(-10) = %v, want 32.5", got)
	}
}

func TestGaugeVecReset(t *testing.T) {
	r := NewRegistry()
	v := r.NewGaugeVec("test_gauge_vec", "a labeled gauge", "db", "branch")
	v.WithLabelValues("a", "main").Set(100)
	v.WithLabelValues("b", "main").Set(200)

	var buf bytes.Buffer
	if err := r.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `db="a"`) || !strings.Contains(buf.String(), `db="b"`) {
		t.Fatalf("expected both label sets before Reset:\n%s", buf.String())
	}

	v.Reset()
	v.WithLabelValues("a", "main").Set(999)

	buf.Reset()
	if err := r.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if strings.Contains(out, `db="b"`) {
		t.Fatalf("expected db=\"b\" to be gone after Reset:\n%s", out)
	}
	if !strings.Contains(out, `db="a",branch="main"} 999`) {
		t.Fatalf("expected the re-populated db=\"a\" gauge at 999:\n%s", out)
	}
}

// TestHistogramBucketMath checks cumulative bucket counts, the +Inf overflow
// bucket, and _sum/_count against a hand-computed expectation for a known
// set of observations straddling every bucket boundary.
func TestHistogramBucketMath(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogram("test_hist", "a histogram", []float64{1, 5, 10})
	// Observations: 0.5 (bucket<=1), 1 (bucket<=1, boundary inclusive), 3
	// (bucket<=5), 7 (bucket<=10), 100 (overflow to +Inf).
	obs := []float64{0.5, 1, 3, 7, 100}
	var sum float64
	for _, v := range obs {
		h.Observe(v)
		sum += v
	}

	var buf bytes.Buffer
	if err := r.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()

	wantLines := []string{
		`test_hist_bucket{le="1"} 2`,
		`test_hist_bucket{le="5"} 3`,
		`test_hist_bucket{le="10"} 4`,
		`test_hist_bucket{le="+Inf"} 5`,
		`test_hist_count 5`,
	}
	for _, want := range wantLines {
		if !strings.Contains(out, want) {
			t.Errorf("missing line %q in:\n%s", want, out)
		}
	}
	wantSum := "test_hist_sum " + formatFloat(sum)
	if !strings.Contains(out, wantSum) {
		t.Errorf("missing sum line %q in:\n%s", wantSum, out)
	}
}

// TestHistogramAllObservationsBelowFirstBucket and
// TestHistogramAllObservationsAboveLastBucket are the edge cases the
// cumulative-sum loop in NewHistogram's render closure could get wrong at
// the boundaries (index 0 and the +Inf-only case).
func TestHistogramAllObservationsBelowFirstBucket(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogram("test_hist_low", "h", []float64{10, 20})
	h.Observe(1)
	h.Observe(2)
	var buf bytes.Buffer
	r.WritePrometheus(&buf)
	out := buf.String()
	for _, want := range []string{
		`test_hist_low_bucket{le="10"} 2`,
		`test_hist_low_bucket{le="20"} 2`,
		`test_hist_low_bucket{le="+Inf"} 2`,
		`test_hist_low_count 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestHistogramAllObservationsAboveLastBucket(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogram("test_hist_high", "h", []float64{1, 2})
	h.Observe(50)
	h.Observe(75)
	var buf bytes.Buffer
	r.WritePrometheus(&buf)
	out := buf.String()
	for _, want := range []string{
		`test_hist_high_bucket{le="1"} 0`,
		`test_hist_high_bucket{le="2"} 0`,
		`test_hist_high_bucket{le="+Inf"} 2`,
		`test_hist_high_count 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// TestHistogramConcurrentObserve is the histogram's own -race/lost-update
// check, mirroring TestCounterConcurrentIncrements.
func TestHistogramConcurrentObserve(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogram("test_hist_concurrent", "h", DefaultDurationBuckets)
	const goroutines = 50
	const perGoroutine = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				h.Observe(0.02)
			}
		}()
	}
	wg.Wait()
	if got, want := h.total.Load(), uint64(goroutines*perGoroutine); got != want {
		t.Fatalf("total = %d, want %d", got, want)
	}
}

func TestLabelValueEscaping(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{`plain`, `plain`},
		{`has "quotes"`, `has \"quotes\"`},
		{"has\nnewline", `has\nnewline`},
		{`has\backslash`, `has\\backslash`},
		{"mix: \\\"\n", `mix: \\\"\n`},
	}
	for _, c := range cases {
		if got := escapeLabelValue(c.in); got != c.want {
			t.Errorf("escapeLabelValue(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestWritePrometheusLabelEscapingEndToEnd proves the escaping actually
// reaches WritePrometheus's output for a Vec label value, not just the
// escapeLabelValue helper in isolation.
func TestWritePrometheusLabelEscapingEndToEnd(t *testing.T) {
	r := NewRegistry()
	v := r.NewGaugeVec("test_escape", "e", "branch")
	v.WithLabelValues(`agent"1"` + "\n" + `back\slash`).Set(1)
	var buf bytes.Buffer
	if err := r.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	want := `branch="agent\"1\"\nback\\slash"`
	if !strings.Contains(buf.String(), want) {
		t.Fatalf("expected escaped label %q in:\n%s", want, buf.String())
	}
}

func TestDuplicateRegistrationPanics(t *testing.T) {
	r := NewRegistry()
	r.NewCounter("dup", "first")
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic registering a duplicate metric name")
		}
	}()
	r.NewGauge("dup", "second")
}

func TestWithLabelValuesArityMismatchPanics(t *testing.T) {
	r := NewRegistry()
	v := r.NewCounterVec("arity", "a", "one", "two")
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic on wrong label-value arity")
		}
	}()
	v.WithLabelValues("only-one")
}

// TestCollectRunsBeforeRender proves Collect callbacks run synchronously at
// the start of every WritePrometheus call — the mechanism
// offshoot_sessions_open/capture_lag_bytes/durable_age_seconds rely on for
// scrape-time (not continuous) computation.
func TestCollectRunsBeforeRender(t *testing.T) {
	r := NewRegistry()
	g := r.NewGauge("scrape_time_gauge", "g")
	calls := 0
	r.Collect(func() {
		calls++
		g.Set(float64(calls))
	})

	var buf bytes.Buffer
	if err := r.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "scrape_time_gauge 1") {
		t.Fatalf("expected value 1 on first scrape:\n%s", buf.String())
	}
	buf.Reset()
	if err := r.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "scrape_time_gauge 2") {
		t.Fatalf("expected value 2 on second scrape (collector re-ran):\n%s", buf.String())
	}
}

// TestCollectCanCallWithLabelValuesWithoutDeadlock proves a Collect callback
// is free to call WithLabelValues on a Vec from this same Registry — the
// exact pattern the daemon's session-gauge collector uses — without
// deadlocking against Registry.mu. See WritePrometheus's doc comment for why
// this is safe by construction (collectors run with no Registry lock held).
func TestCollectCanCallWithLabelValuesWithoutDeadlock(t *testing.T) {
	r := NewRegistry()
	v := r.NewGaugeVec("collect_vec", "v", "db", "branch")
	r.Collect(func() {
		v.Reset()
		v.WithLabelValues("mydb", "main").Set(7)
	})
	done := make(chan struct{})
	go func() {
		var buf bytes.Buffer
		r.WritePrometheus(&buf)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("WritePrometheus deadlocked calling a Collect callback that uses WithLabelValues")
	}
}

// TestWritePrometheusGolden compares a full exposition dump of a registry
// shaped like offshoot's real locked-metric set (one of every metric kind
// this package supports, with labels and a few observations) against a
// checked-in golden file — the PM Amendment 7 hard-gate's golden-file half.
// A diff here means either a real formatting regression or an intentional
// format change that must update testdata/golden.txt in the same commit.
func TestWritePrometheusGolden(t *testing.T) {
	r := buildGoldenRegistry()
	var buf bytes.Buffer
	if err := r.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	golden, err := os.ReadFile("testdata/golden.txt")
	if err != nil {
		t.Fatal(err)
	}
	if buf.String() != string(golden) {
		t.Fatalf("WritePrometheus output does not match testdata/golden.txt.\n--- got ---\n%s\n--- want ---\n%s",
			buf.String(), string(golden))
	}
}

// buildGoldenRegistry builds a registry exercising every metric kind
// (Counter, Gauge, Histogram, CounterVec, GaugeVec) with representative
// names/help/labels/values, deterministically (no wall-clock, no map
// iteration leaking into ordering), so its WritePrometheus output can be
// pinned in a golden file. Shared by the golden test above and (if
// promtool is on PATH) TestPromtoolCheckMetrics below, so both exercise the
// identical shape.
func buildGoldenRegistry() *Registry {
	r := NewRegistry()

	build := r.NewGaugeVec("offshoot_build_info", "Always 1; identifies the running build.", "version")
	build.WithLabelValues("0.1.3-test").Set(1)

	sessions := r.NewGauge("offshoot_sessions_open", "Number of sessions currently open in this daemon.")
	sessions.Set(3)

	lag := r.NewGaugeVec("offshoot_capture_lag_bytes",
		"WAL bytes committed but not yet applied, for currently open sessions.", "db", "branch")
	lag.WithLabelValues("app", "main").Set(4096)
	lag.WithLabelValues("app", "feature").Set(0)

	flush := r.NewCounterVec("offshoot_flush_total", "Flushes by result and kind.", "result", "kind")
	flush.WithLabelValues("ok", "auto").Add(12)
	flush.WithLabelValues("ok", "manual").Add(3)
	flush.WithLabelValues("error", "auto").Add(1)
	flush.WithLabelValues("error", "manual").Add(0)

	dur := r.NewHistogram("offshoot_flush_duration_seconds", "Flush duration.", DefaultDurationBuckets)
	for _, v := range []float64{0.002, 0.02, 0.2, 2, 20} {
		dur.Observe(v)
	}

	fork := r.NewCounterVec("offshoot_fork_total", "Forks by path.", "path")
	fork.WithLabelValues("fast").Add(9)
	fork.WithLabelValues("slow").Add(1)

	reap := r.NewCounter("offshoot_reap_total", "Branches reaped.")
	reap.Add(2)

	return r
}

// TestPromtoolCheckMetrics is PM Amendment 7's other hard-gate half: piping
// a generated exposition dump into `promtool check metrics` (Prometheus's
// own linter for the exact HELP/TYPE/naming-convention rules this format
// requires). promtool is not part of this module's dependency graph (it's a
// separate binary, not a Go import) and is not assumed present on a
// contributor's machine, so this test SKIPS LOUDLY (not silently) if it
// isn't on PATH — CI installs it explicitly (see .github/workflows/ci.yml's
// metrics-lint job) so the real gate still runs there on every PR.
func TestPromtoolCheckMetrics(t *testing.T) {
	promtool, err := exec.LookPath("promtool")
	if err != nil {
		t.Skip("SKIPPING: promtool not found on PATH — this test only verifies exposition-format " +
			"compliance when promtool is installed (https://github.com/prometheus/prometheus/releases). " +
			"CI installs it explicitly; install it locally to run this check here too.")
	}

	r := buildGoldenRegistry()
	var buf bytes.Buffer
	if err := r.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(promtool, "check", "metrics")
	cmd.Stdin = &buf
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("promtool check metrics failed: %v\noutput:\n%s", err, out)
	}
}
