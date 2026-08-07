package ops

import (
	"os"
	"os/exec"
	"testing"
	"time"
)

// mkCheckpoint writes a single INSERT to path via the sqlite3 CLI and
// checkpoints it under name — a small helper shared by this file's tests to
// build up several distinct checkpoints on app@main quickly.
func mkCheckpoint(t *testing.T, w *Workspace, path, name string) {
	t.Helper()
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE IF NOT EXISTS t (v); INSERT INTO t VALUES ('x');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", name, nil); err != nil {
		t.Fatal(err)
	}
}

// chtime backdates (or advances) path's mtime to when, for deterministic LRU
// ordering in tests — real wall-clock gaps between fast test calls can be
// smaller than filesystem mtime granularity on some platforms/filesystems,
// so tests that need a controlled order set mtimes explicitly rather than
// relying on real elapsed time between calls.
func chtime(t *testing.T, path string, when time.Time) {
	t.Helper()
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatal(err)
	}
}

func TestROCacheUsageEmptyWhenNoCacheDirectory(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	bytes, count, err := w.ROCacheUsage()
	if err != nil {
		t.Fatal(err)
	}
	if bytes != 0 || count != 0 {
		t.Fatalf("ROCacheUsage on an untouched store = (%d, %d), want (0, 0)", bytes, count)
	}
}

func TestROCacheUsageCountsCachedEntries(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mkCheckpoint(t, w, path, "v1")
	mkCheckpoint(t, w, path, "v2")

	p1, err := w.CheckoutAt("app", "main", "v1", false)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := w.CheckoutAt("app", "main", "v2", false)
	if err != nil {
		t.Fatal(err)
	}
	fi1, err := os.Stat(p1)
	if err != nil {
		t.Fatal(err)
	}
	fi2, err := os.Stat(p2)
	if err != nil {
		t.Fatal(err)
	}
	wantBytes := fi1.Size() + fi2.Size()

	gotBytes, gotCount, err := w.ROCacheUsage()
	if err != nil {
		t.Fatal(err)
	}
	if gotCount != 2 {
		t.Fatalf("count = %d, want 2", gotCount)
	}
	if gotBytes != wantBytes {
		t.Fatalf("bytes = %d, want %d", gotBytes, wantBytes)
	}
}

func TestEvictROCacheBudgetZeroNeverEvicts(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	mkCheckpoint(t, w, path, "v1")
	if _, err := w.CheckoutAt("app", "main", "v1", false); err != nil {
		t.Fatal(err)
	}

	wantBytes, _, err := w.ROCacheUsage()
	if err != nil {
		t.Fatal(err)
	}
	if wantBytes == 0 {
		t.Fatal("test setup produced zero-byte usage; nothing to prove")
	}

	// budget <= 0 (here: 0 and, separately, a large negative value) must
	// never evict, no matter how far "over" any sane budget usage is —
	// the CLI/daemon default and the documented "unlimited" contract.
	for _, budget := range []int64{0, -1, -1000} {
		evicted, usage, err := w.EvictROCache(budget)
		if err != nil {
			t.Fatal(err)
		}
		if len(evicted) != 0 {
			t.Fatalf("budget=%d must never evict, got %+v", budget, evicted)
		}
		if usage != wantBytes {
			t.Fatalf("budget=%d usage = %d, want %d (unchanged)", budget, usage, wantBytes)
		}
	}
	if _, err := os.Stat(w.CheckoutAtPath("app", "main", "v1")); err != nil {
		t.Fatalf("cache file must still exist after budget=0 passes: %v", err)
	}
}

func TestEvictROCacheAlreadyUnderBudgetNeverEvicts(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	mkCheckpoint(t, w, path, "v1")
	if _, err := w.CheckoutAt("app", "main", "v1", false); err != nil {
		t.Fatal(err)
	}
	total, _, err := w.ROCacheUsage()
	if err != nil {
		t.Fatal(err)
	}

	evicted, usage, err := w.EvictROCache(total + 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if len(evicted) != 0 {
		t.Fatalf("usage under budget must not evict, got %+v", evicted)
	}
	if usage != total {
		t.Fatalf("usage = %d, want %d", usage, total)
	}
}

// TestEvictROCacheEvictsOldestFirstUntilUnderBudget builds three cached
// checkpoints, backdates their .db mtimes into a known order (no
// `.last-used` marker involved — the plain mtime-floor case), sets a budget
// that requires evicting exactly the single oldest entry, and asserts: the
// oldest is gone, the other two survive, and reported usage is at or under
// budget.
func TestEvictROCacheEvictsOldestFirstUntilUnderBudget(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	mkCheckpoint(t, w, path, "v1")
	mkCheckpoint(t, w, path, "v2")
	mkCheckpoint(t, w, path, "v3")

	names := []string{"v1", "v2", "v3"}
	paths := map[string]string{}
	for _, n := range names {
		p, err := w.CheckoutAt("app", "main", n, false)
		if err != nil {
			t.Fatal(err)
		}
		paths[n] = p
	}

	// v1 oldest, v2 middle, v3 newest by mtime — no markers exist yet, so
	// this is the pure .db-mtime-floor ordering.
	base := time.Now().Add(-time.Hour)
	chtime(t, paths["v1"], base)
	chtime(t, paths["v2"], base.Add(10*time.Minute))
	chtime(t, paths["v3"], base.Add(20*time.Minute))

	sizes := map[string]int64{}
	var total int64
	for _, n := range names {
		fi, err := os.Stat(paths[n])
		if err != nil {
			t.Fatal(err)
		}
		sizes[n] = fi.Size()
		total += fi.Size()
	}

	// Budget just under total, but large enough that evicting only v1
	// (the oldest) satisfies it — every cached file here is the same
	// content shape, so v1's own size is exactly the gap.
	budget := total - sizes["v1"]

	evicted, usageAfter, err := w.EvictROCache(budget)
	if err != nil {
		t.Fatal(err)
	}
	if len(evicted) != 1 || evicted[0].Checkpoint != "v1" {
		t.Fatalf("evicted = %+v, want exactly v1", evicted)
	}
	if evicted[0].Bytes != sizes["v1"] {
		t.Fatalf("evicted[0].Bytes = %d, want %d", evicted[0].Bytes, sizes["v1"])
	}
	if usageAfter > budget {
		t.Fatalf("usageAfter = %d, want <= budget %d", usageAfter, budget)
	}

	if _, err := os.Stat(paths["v1"]); !os.IsNotExist(err) {
		t.Fatalf("v1 (LRU) must be gone, stat err = %v", err)
	}
	for _, n := range []string{"v2", "v3"} {
		if _, err := os.Stat(paths[n]); err != nil {
			t.Fatalf("%s (MRU) must survive: %v", n, err)
		}
	}

	gotBytes, gotCount, err := w.ROCacheUsage()
	if err != nil {
		t.Fatal(err)
	}
	if gotCount != 2 {
		t.Fatalf("post-eviction count = %d, want 2", gotCount)
	}
	if gotBytes != usageAfter {
		t.Fatalf("post-eviction ROCacheUsage = %d, want %d (EvictROCache's own reported usageAfter)", gotBytes, usageAfter)
	}
}

// TestEvictROCacheRemovesBothDBAndMarker proves an eviction cleans up the
// `.last-used` marker alongside the .db file it belongs to, not just the
// .db itself.
func TestEvictROCacheRemovesBothDBAndMarker(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	mkCheckpoint(t, w, path, "v1")
	mkCheckpoint(t, w, path, "v2")

	p1, err := w.CheckoutAt("app", "main", "v1", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.CheckoutAt("app", "main", "v2", false); err != nil {
		t.Fatal(err)
	}
	// A cache HIT on v1 creates its marker.
	if _, err := w.CheckoutAt("app", "main", "v1", false); err != nil {
		t.Fatal(err)
	}
	marker := p1 + lastUsedSuffix
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("expected a .last-used marker for v1 after a cache hit: %v", err)
	}
	// Make v1 the oldest by LRU clock despite having a marker: backdate the
	// marker itself well before v2's .db mtime.
	chtime(t, marker, time.Now().Add(-time.Hour))

	total, _, err := w.ROCacheUsage()
	if err != nil {
		t.Fatal(err)
	}
	fi1, err := os.Stat(p1)
	if err != nil {
		t.Fatal(err)
	}
	budget := total - fi1.Size()

	evicted, _, err := w.EvictROCache(budget)
	if err != nil {
		t.Fatal(err)
	}
	if len(evicted) != 1 || evicted[0].Checkpoint != "v1" {
		t.Fatalf("evicted = %+v, want exactly v1", evicted)
	}
	if _, err := os.Stat(p1); !os.IsNotExist(err) {
		t.Fatalf("v1's .db must be gone: stat err = %v", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("v1's .last-used marker must be gone alongside its .db: stat err = %v", err)
	}
}

// TestEvictROCacheNeverTouchesWritableCheckout proves the "never evict
// checkouts/" guarantee is structural: even an aggressive eviction pass
// (budget=1, forcing every ro-cache entry out) leaves the branch's
// writable checkout completely untouched — same path, same content, same
// permissions — because EvictROCache only ever walks/removes paths under
// the SEPARATE checkouts-ro tree.
func TestEvictROCacheNeverTouchesWritableCheckout(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mkCheckpoint(t, w, path, "v1")
	if _, err := w.CheckoutAt("app", "main", "v1", false); err != nil {
		t.Fatal(err)
	}

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fiBefore, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	evicted, usageAfter, err := w.EvictROCache(1) // as aggressive as it gets
	if err != nil {
		t.Fatal(err)
	}
	if len(evicted) != 1 {
		t.Fatalf("expected the single ro-cache entry to be evicted under budget=1, got %+v", evicted)
	}
	if usageAfter != 0 {
		t.Fatalf("usageAfter = %d, want 0 (everything evicted)", usageAfter)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("writable checkout must still exist: %v", err)
	}
	if string(before) != string(after) {
		t.Fatal("writable checkout content changed by an ro-cache eviction pass")
	}
	fiAfter, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fiBefore.Mode().Perm() != fiAfter.Mode().Perm() {
		t.Fatalf("writable checkout permissions changed: before=%o after=%o", fiBefore.Mode().Perm(), fiAfter.Mode().Perm())
	}
	if fiAfter.Mode().Perm()&0o200 == 0 {
		t.Fatal("writable checkout must still be writable after an aggressive ro-cache eviction pass")
	}
}

// TestCheckoutAtCacheHitTouchesLastUsedMarker proves the touch-on-HIT
// contract directly: the initial materialize writes no marker at all
// (PM Amendment 11 — "touch-on-HIT", not touch-on-create), and a
// subsequent force=false cache hit creates/updates one.
func TestCheckoutAtCacheHitTouchesLastUsedMarker(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	mkCheckpoint(t, w, path, "v1")

	roPath, err := w.CheckoutAt("app", "main", "v1", false)
	if err != nil {
		t.Fatal(err)
	}
	marker := roPath + lastUsedSuffix
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("a freshly materialized entry must have no marker yet, stat err = %v", err)
	}

	if _, err := w.CheckoutAt("app", "main", "v1", false); err != nil {
		t.Fatal(err)
	}
	mi, err := os.Stat(marker)
	if err != nil {
		t.Fatalf("a cache hit must create the .last-used marker: %v", err)
	}
	if time.Since(mi.ModTime()) > time.Minute {
		t.Fatalf("marker mtime %v is not recent", mi.ModTime())
	}
}

// TestEvictROCacheLastUsedTouchBeatsCreationOrder is the plan's explicit
// acceptance scenario: an OLDER entry (by creation) that gets HIT survives
// eviction, while a NEWER (by creation) entry that is never hit gets
// evicted instead — proving the `.last-used` marker, not raw creation
// order, drives the LRU ranking.
func TestEvictROCacheLastUsedTouchBeatsCreationOrder(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	mkCheckpoint(t, w, path, "v1")
	mkCheckpoint(t, w, path, "v2")

	pOld, err := w.CheckoutAt("app", "main", "v1", false) // created first: "old"
	if err != nil {
		t.Fatal(err)
	}
	pNew, err := w.CheckoutAt("app", "main", "v2", false) // created second: "new"
	if err != nil {
		t.Fatal(err)
	}

	// Backdate both .db mtimes so the un-hit floor ordering is unambiguous
	// (v1 older than v2) before any marker exists.
	base := time.Now().Add(-time.Hour)
	chtime(t, pOld, base)
	chtime(t, pNew, base.Add(10*time.Minute))

	// HIT the OLD entry: real cache-hit path, real touchLastUsed call —
	// its marker mtime is "now", far newer than pNew's backdated .db mtime.
	if _, err := w.CheckoutAt("app", "main", "v1", false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(pOld + lastUsedSuffix); err != nil {
		t.Fatalf("expected v1's marker after the hit: %v", err)
	}

	total, _, err := w.ROCacheUsage()
	if err != nil {
		t.Fatal(err)
	}
	fiOld, err := os.Stat(pOld)
	if err != nil {
		t.Fatal(err)
	}
	// Force exactly one eviction.
	budget := total - fiOld.Size()

	evicted, _, err := w.EvictROCache(budget)
	if err != nil {
		t.Fatal(err)
	}
	if len(evicted) != 1 || evicted[0].Checkpoint != "v2" {
		t.Fatalf("evicted = %+v, want exactly v2 (the un-hit, newer-by-creation entry)", evicted)
	}
	if _, err := os.Stat(pOld); err != nil {
		t.Fatalf("v1 (hit, older by creation) must survive: %v", err)
	}
	if _, err := os.Stat(pNew); !os.IsNotExist(err) {
		t.Fatalf("v2 (never hit, newer by creation) must be evicted: stat err = %v", err)
	}
}
