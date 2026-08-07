package daemon

import (
	"os"
	"os/exec"
	"testing"
	"time"
)

// mkCheckpointAt writes a row to path and checkpoints it under name — a
// daemon-package-local mirror of internal/ops's identical test helper (kept
// separate: this package's tests can't import internal/ops's unexported
// test helpers across the package boundary).
func mkCheckpointAt(t *testing.T, w interface {
	Checkpoint(db, branch, name string, meta map[string]string) (uint64, error)
}, path, name string) {
	t.Helper()
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE IF NOT EXISTS t (v); INSERT INTO t VALUES ('x');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", name, nil); err != nil {
		t.Fatal(err)
	}
}

func chtimeAt(t *testing.T, path string, when time.Time) {
	t.Helper()
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatal(err)
	}
}

// TestJanitorTickEvictsLRUUnderBudgetAndFiresEvictedEvent is Milestone 4
// Task 5's core acceptance scenario, run through the real janitor pass (not
// ops.EvictROCache directly, which internal/ops/rocache_test.go already
// covers in isolation): build three cached checkpoints of known size,
// backdate two of them into an unambiguous LRU order, HIT the oldest one
// (making it MRU via the real CheckoutAt touch-on-hit path) so it should
// survive despite its backdated creation time, set a budget that requires
// evicting exactly one entry, subscribe to the event bus first, run one
// janitor tick, and assert: the correct (untouched, oldest-by-clock) entry
// is gone, the touched + newest entries survive, usage is at/under budget,
// offshoot_ro_cache_bytes/offshoot_ro_cache_evictions_total moved, and an
// "evicted" event carrying db/branch/checkpoint/bytes was published.
func TestJanitorTickEvictsLRUUnderBudgetAndFiresEvictedEvent(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mkCheckpointAt(t, w, path, "v1")
	mkCheckpointAt(t, w, path, "v2")
	mkCheckpointAt(t, w, path, "v3")

	p1, err := w.CheckoutAt("app", "main", "v1", false)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := w.CheckoutAt("app", "main", "v2", false)
	if err != nil {
		t.Fatal(err)
	}
	p3, err := w.CheckoutAt("app", "main", "v3", false)
	if err != nil {
		t.Fatal(err)
	}

	// Backdate all three .db mtimes into an unambiguous creation order:
	// v1 oldest, v2 middle, v3 newest.
	base := time.Now().Add(-time.Hour)
	chtimeAt(t, p1, base)
	chtimeAt(t, p2, base.Add(10*time.Minute))
	chtimeAt(t, p3, base.Add(20*time.Minute))

	// HIT v1 (the oldest by creation) — the real CheckoutAt cache-hit path,
	// which touches its `.last-used` marker to "now": this should make v1
	// the MRU entry despite its backdated .db mtime.
	if _, err := w.CheckoutAt("app", "main", "v1", false); err != nil {
		t.Fatal(err)
	}

	total, _, err := w.ROCacheUsage()
	if err != nil {
		t.Fatal(err)
	}
	fi2, err := os.Stat(p2)
	if err != nil {
		t.Fatal(err)
	}
	// v2 is now the true LRU entry (v1 was just touched to MRU; v3 is
	// newer than v2 by raw creation): force exactly one eviction, of v2.
	budget := total - fi2.Size()
	srv.SetROCacheBudget(budget)

	sc, events := subscribeSocket(t, sock)
	defer sc.Close()

	bytesBefore := srv.metrics.ROCacheBytes.Value()
	evictionsBefore := srv.metrics.ROCacheEvictionsTotal.Value()
	_ = bytesBefore

	srv.janitorTick(time.Hour)

	if _, err := os.Stat(p1); err != nil {
		t.Fatalf("v1 (touched, MRU) must survive: %v", err)
	}
	if _, err := os.Stat(p2); !os.IsNotExist(err) {
		t.Fatalf("v2 (LRU) must be evicted: stat err = %v", err)
	}
	if _, err := os.Stat(p3); err != nil {
		t.Fatalf("v3 (newest by creation) must survive: %v", err)
	}
	if _, err := os.Stat(p2 + ".last-used"); !os.IsNotExist(err) {
		t.Fatalf("v2's marker (none expected, but belt-and-suspenders) must also be gone: stat err = %v", err)
	}

	gotUsage, gotCount, err := w.ROCacheUsage()
	if err != nil {
		t.Fatal(err)
	}
	if gotCount != 2 {
		t.Fatalf("post-tick ro-cache entry count = %d, want 2", gotCount)
	}
	if gotUsage > budget {
		t.Fatalf("post-tick usage = %d, want <= budget %d", gotUsage, budget)
	}

	if got := srv.metrics.ROCacheBytes.Value(); got != float64(gotUsage) {
		t.Fatalf("offshoot_ro_cache_bytes = %v, want %v", got, gotUsage)
	}
	if got := srv.metrics.ROCacheEvictionsTotal.Value(); got != evictionsBefore+1 {
		t.Fatalf("offshoot_ro_cache_evictions_total = %v, want %v", got, evictionsBefore+1)
	}

	ev := waitForEventType(t, events, "evicted", 5*time.Second)
	if ev.DB != "app" || ev.Branch != "main" {
		t.Fatalf("evicted event db/branch = %q/%q, want app/main (event=%+v)", ev.DB, ev.Branch, ev)
	}
	if cp, _ := ev.Detail["checkpoint"].(string); cp != "v2" {
		t.Fatalf("evicted event detail.checkpoint = %v, want \"v2\" (detail=%+v)", ev.Detail["checkpoint"], ev.Detail)
	}
	if bytesVal, ok := ev.Detail["bytes"].(float64); !ok || int64(bytesVal) != fi2.Size() {
		t.Fatalf("evicted event detail.bytes = %v, want %d (detail=%+v)", ev.Detail["bytes"], fi2.Size(), ev.Detail)
	}
}

// TestJanitorTickBudgetZeroNeverEvicts proves budget 0 (the default, unset)
// means unlimited even when checkouts-ro usage is way over any sane real
// budget: the janitor still reports usage (offshoot_ro_cache_bytes moves
// off zero) but evicts nothing at all, ever.
func TestJanitorTickBudgetZeroNeverEvicts(t *testing.T) {
	srv, w := newServer(t)

	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"v1", "v2", "v3", "v4", "v5"} {
		mkCheckpointAt(t, w, path, name)
		if _, err := w.CheckoutAt("app", "main", name, false); err != nil {
			t.Fatalf("checkout-at %s: %v", name, err)
		}
		_ = i
	}

	total, count, err := w.ROCacheUsage()
	if err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Fatalf("setup: count = %d, want 5", count)
	}

	// SetROCacheBudget is never called: roCacheBudget stays at its zero
	// value (unlimited) — the explicit point of this test.
	evictionsBefore := srv.metrics.ROCacheEvictionsTotal.Value()
	srv.janitorTick(time.Hour)

	if got := srv.metrics.ROCacheEvictionsTotal.Value(); got != evictionsBefore {
		t.Fatalf("offshoot_ro_cache_evictions_total moved under budget=0: before=%v after=%v", evictionsBefore, got)
	}
	if got := srv.metrics.ROCacheBytes.Value(); got != float64(total) {
		t.Fatalf("offshoot_ro_cache_bytes = %v, want %v (usage still reported even though nothing is evicted)", got, total)
	}
	gotUsage, gotCount, err := w.ROCacheUsage()
	if err != nil {
		t.Fatal(err)
	}
	if gotUsage != total || gotCount != count {
		t.Fatalf("ro-cache changed under budget=0: before=(%d,%d) after=(%d,%d)", total, count, gotUsage, gotCount)
	}
}

// TestJanitorROCacheEvictionNeverTouchesLeasedWritableCheckout proves the
// "never evict checkouts/" guarantee end to end through the daemon: a real
// session is open (a live lease, a writable checkout under checkouts/) at
// the same time an aggressive ro-cache eviction pass runs (budget=1, forcing
// every ro-cache entry out). The leased session's checkout — same directory
// tree ops.EvictROCache never walks — must survive untouched and the
// session must remain fully functional (a flush after the eviction pass
// still succeeds).
func TestJanitorROCacheEvictionNeverTouchesLeasedWritableCheckout(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK || open.Checkout == "" {
		t.Fatalf("open = %+v", open)
	}
	if out, err := exec.Command("sqlite3", open.Checkout,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	// Also seed a ro-cache entry so the eviction pass has something real to
	// do (proving it acts on checkouts-ro while leaving checkouts/ alone,
	// not merely that there's nothing to evict). A live session is open on
	// this same branch, so the named checkpoint must go through the
	// session's own "flush" op (a live session's checkpoint IS a named
	// flush — see opFlush's doc comment) rather than
	// ops.Workspace.Checkpoint, which operates directly on the raw
	// checkout file and is unsafe/refused against a live session.
	flushV1 := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main", Name: "v1"})
	if !flushV1.OK {
		t.Fatalf("flush --name v1 = %+v", flushV1)
	}
	if _, err := w.CheckoutAt("app", "main", "v1", false); err != nil {
		t.Fatal(err)
	}

	beforeContent, err := os.ReadFile(open.Checkout)
	if err != nil {
		t.Fatal(err)
	}
	fiBefore, err := os.Stat(open.Checkout)
	if err != nil {
		t.Fatal(err)
	}

	srv.SetROCacheBudget(1) // as aggressive as it gets
	srv.janitorTick(time.Hour)

	if evicted := srv.metrics.ROCacheEvictionsTotal.Value(); evicted == 0 {
		t.Fatal("setup expected the aggressive pass to evict the seeded ro-cache entry")
	}

	afterContent, err := os.ReadFile(open.Checkout)
	if err != nil {
		t.Fatalf("leased writable checkout must still exist: %v", err)
	}
	if string(beforeContent) != string(afterContent) {
		t.Fatal("leased writable checkout content changed by an ro-cache eviction pass")
	}
	fiAfter, err := os.Stat(open.Checkout)
	if err != nil {
		t.Fatal(err)
	}
	if fiBefore.Mode().Perm() != fiAfter.Mode().Perm() {
		t.Fatalf("leased writable checkout permissions changed: before=%o after=%o",
			fiBefore.Mode().Perm(), fiAfter.Mode().Perm())
	}

	// The session must still be fully usable: a flush after the eviction
	// pass succeeds exactly as it would have before.
	flush := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main"})
	if !flush.OK {
		t.Fatalf("flush after an aggressive ro-cache eviction pass = %+v, want ok", flush)
	}
}
