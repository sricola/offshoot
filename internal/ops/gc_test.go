package ops

import (
	"errors"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

func TestDestroyAndGC(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	w.Checkpoint("app", "main", "v1", nil)
	w.Fork("app", "main", "attempt-1", "", 0, nil)
	aref, _, _ := w.Store.GetRef("app", "attempt-1")

	// Protected destroy requires force.
	if err := w.Destroy("app", "main", false); err == nil {
		t.Fatal("destroying protected main without force must fail")
	}
	if err := w.Destroy("app", "attempt-1", false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.GetRef("app", "attempt-1"); err == nil {
		t.Fatal("ref must be gone")
	}

	// Phase 1: tombstone the now-unreachable lineage; nothing deleted yet.
	tombstoned, deleted, err := w.GC(time.Hour)
	if err != nil || tombstoned != 1 || deleted != 0 {
		t.Fatalf("gc1: %d %d %v", tombstoned, deleted, err)
	}
	if keys, _ := w.Store.B.List(store.LineagePrefix(aref.Lineage)); len(keys) == 0 {
		t.Fatal("phase 1 must not delete data")
	}
	// Phase 2 with zero grace: swept.
	if _, deleted, err = w.GC(0); err != nil || deleted != 1 {
		t.Fatalf("gc2: deleted=%d err=%v", deleted, err)
	}
	if keys, _ := w.Store.B.List(store.LineagePrefix(aref.Lineage)); len(keys) != 0 {
		t.Fatalf("lineage not swept: %v", keys)
	}
	// Live lineages untouched.
	if _, err := w.Checkout("app", "main"); err != nil {
		t.Fatalf("live branch damaged by GC: %v", err)
	}
}

// TestDestroyRefusesLiveLeaseWithoutForce proves Destroy gates on an active
// lease per the design spec's fault matrix: a live holder may still be
// mid-write, so destroying under it without --force must be refused (naming
// the holder), while --force overrides it and an already-expired lease
// never blocks at all.
func TestDestroyRefusesLiveLeaseWithoutForce(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "b1", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AcquireLease("app", "b1", "holder-a", DefaultLeaseTTL); err != nil {
		t.Fatal(err)
	}
	err := w.Destroy("app", "b1", false)
	if err == nil || !strings.Contains(err.Error(), "holder-a") {
		t.Fatalf("destroy under a live lease without force must fail and name the holder, got: %v", err)
	}
	if err := w.Destroy("app", "b1", true); err != nil {
		t.Fatalf("destroy with force must override a live lease: %v", err)
	}
	if _, _, err := w.Store.GetRef("app", "b1"); err == nil {
		t.Fatal("ref must be gone after forced destroy")
	}

	// A branch whose lease has already expired is destroyable without force.
	if _, err := w.Fork("app", "main", "b2", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AcquireLease("app", "b2", "holder-b", time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	if err := w.Destroy("app", "b2", false); err != nil {
		t.Fatalf("destroy of a branch with an expired lease must succeed without force: %v", err)
	}
}

// TestDestroyAndGCOnS3 is TestDestroyAndGC run against the S3 backend
// instead of Local. GC's sweep phase deletes every object under a
// tombstoned lineage's prefix (Store.B.Delete in a loop) and its earlier
// phase persists the tombstone map via an unconditional Store.B.Put — both
// paths that, like Checkpoint's orphan-overwrite path, depend on ops's
// object-storage semantics matching across backends rather than being
// accidents of Local's filesystem behavior. Same assertions as the Local
// version: phase 1 tombstones without deleting, phase 2 with zero grace
// sweeps the data, and live lineages are left untouched throughout.
func TestDestroyAndGCOnS3(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWSOnFakeS3(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	w.Checkpoint("app", "main", "v1", nil)
	w.Fork("app", "main", "attempt-1", "", 0, nil)
	aref, _, _ := w.Store.GetRef("app", "attempt-1")

	// Protected destroy requires force.
	if err := w.Destroy("app", "main", false); err == nil {
		t.Fatal("destroying protected main without force must fail")
	}
	if err := w.Destroy("app", "attempt-1", false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.GetRef("app", "attempt-1"); err == nil {
		t.Fatal("ref must be gone")
	}

	// Phase 1: tombstone the now-unreachable lineage; nothing deleted yet.
	tombstoned, deleted, err := w.GC(time.Hour)
	if err != nil || tombstoned != 1 || deleted != 0 {
		t.Fatalf("gc1: %d %d %v", tombstoned, deleted, err)
	}
	if keys, _ := w.Store.B.List(store.LineagePrefix(aref.Lineage)); len(keys) == 0 {
		t.Fatal("phase 1 must not delete data")
	}
	// Phase 2 with zero grace: swept.
	if _, deleted, err = w.GC(0); err != nil || deleted != 1 {
		t.Fatalf("gc2: deleted=%d err=%v", deleted, err)
	}
	if keys, _ := w.Store.B.List(store.LineagePrefix(aref.Lineage)); len(keys) != 0 {
		t.Fatalf("lineage not swept: %v", keys)
	}
	// Live lineages untouched.
	if _, err := w.Checkout("app", "main"); err != nil {
		t.Fatalf("live branch damaged by GC: %v", err)
	}
}

// TestGCSingleRunDoesNotSweepStonesMintedThisRun guards against a
// mark-and-sweep race: with grace=0, phase 2's cutoff is `time.Now()`
// computed within the same GC call, which every stone phase 1 just minted
// (timestamped moments earlier in the same call) trivially satisfies. Before
// the fix, a single gc(0) invocation could tombstone AND sweep a newly
// unreachable lineage in one run — dangerous because a concurrent fork could
// be mid-flight, having just read a snapshot out of that lineage a moment
// before its old ref went away, without yet having landed its own ref.
// Sweeping must always wait for an independent, later GC run.
func TestGCSingleRunDoesNotSweepStonesMintedThisRun(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	w.Checkpoint("app", "main", "v1", nil)
	w.Fork("app", "main", "attempt-1", "", 0, nil)
	aref, _, _ := w.Store.GetRef("app", "attempt-1")
	if err := w.Destroy("app", "attempt-1", false); err != nil {
		t.Fatal(err)
	}

	tombstoned, deleted, err := w.GC(0)
	if err != nil {
		t.Fatal(err)
	}
	if tombstoned != 1 {
		t.Fatalf("tombstoned = %d, want 1", tombstoned)
	}
	if deleted != 0 {
		t.Fatalf("deleted = %d, want 0: a single gc(0) run must not sweep a stone it just minted", deleted)
	}
	if keys, _ := w.Store.B.List(store.LineagePrefix(aref.Lineage)); len(keys) == 0 {
		t.Fatal("lineage data deleted in the same run it was tombstoned")
	}

	// A second, independent gc(0) run (the stone now ages from a prior run)
	// must sweep it.
	if _, deleted, err = w.GC(0); err != nil || deleted != 1 {
		t.Fatalf("gc2: deleted=%d err=%v", deleted, err)
	}
	if keys, _ := w.Store.B.List(store.LineagePrefix(aref.Lineage)); len(keys) != 0 {
		t.Fatalf("lineage not swept on second run: %v", keys)
	}
}

// TestGCPrunesLegacyLineageKeyedStone: the pre-object-granular tombstone
// format keyed stones by LINEAGE id. Such a key names no real object, so
// phase 2's re-list finds nothing behind it: the stone is pruned without a
// delete and the (live) lineage it once named is untouched. (This test's
// ancestor, TestGCSparesRereferencedLineage, claimed the stone was spared
// "because it is still referenced" — under object granularity what actually
// happens is legacy-stone pruning, which is what is asserted now.)
func TestGCPrunesLegacyLineageKeyedStone(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.tombstone(map[string]string{ref.Lineage: "2000-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if _, deleted, err := w.GC(0); err != nil || deleted != 0 {
		t.Fatalf("a legacy lineage-keyed stone must prune without deleting: deleted=%d err=%v", deleted, err)
	}
	stones, _, err := w.loadTombstones()
	if err != nil {
		t.Fatal(err)
	}
	if _, still := stones[ref.Lineage]; still {
		t.Fatal("legacy lineage-keyed stone must be pruned from gc/tombstones")
	}
	if _, err := w.Checkout("app", "main"); err != nil {
		t.Fatal(err)
	}
}

// ---- Object-granular reachability GC (copy-on-write Task 5) ----

// mustSQL runs a sqlite3 CLI statement against path, failing the test loudly.
func mustSQL(t *testing.T, path, stmt string) {
	t.Helper()
	if out, err := exec.Command("sqlite3", path, stmt).CombinedOutput(); err != nil {
		t.Fatalf("sqlite3 %q: %v: %s", stmt, err, out)
	}
}

// querySQL returns a sqlite3 CLI query's stdout.
func querySQL(t *testing.T, path, stmt string) string {
	t.Helper()
	out, err := exec.Command("sqlite3", path, stmt).Output()
	if err != nil {
		t.Fatalf("sqlite3 %q: %v", stmt, err)
	}
	return string(out)
}

// TestGCKeepsParentAliveViaChildBase is the F3 core: a parent lineage whose
// ref is GONE (branch destroyed) but that a live shared child still bases on
// must never be swept — the child's resolved chain reaches into it, and the
// old single-hop liveLineages mark could not see that.
func TestGCKeepsParentAliveViaChildBase(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (42);")
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "child", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	cref, _, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	if cref.Base == nil {
		t.Fatal("test precondition: the fork must share (Base set)")
	}
	parentLineage := cref.Base.Lineage

	// The parent BRANCH dies; only the child's base pointer keeps its
	// objects alive.
	if err := w.Destroy("app", "main", true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}

	// The child's chain still resolves, its members reach into the parent's
	// lineage, and every member object still exists.
	chain, err := w.Store.Chain(cref.Lineage, cref.HeadTXID)
	if err != nil {
		t.Fatalf("child chain must survive GC of its destroyed parent: %v", err)
	}
	crossesIntoParent := false
	for _, m := range chain {
		if strings.HasPrefix(m.Key, store.LineagePrefix(parentLineage)) {
			crossesIntoParent = true
		}
		if _, _, err := w.Store.B.Get(m.Key); err != nil {
			t.Fatalf("chain member %s swept out from under the live child: %v", m.Key, err)
		}
	}
	if !crossesIntoParent {
		t.Fatal("test precondition: the child's chain must reference the parent's lineage")
	}
	cpath, err := w.Checkout("app", "child")
	if err != nil {
		t.Fatalf("child must materialize after parent destroy + GC: %v", err)
	}
	if got := querySQL(t, cpath, "SELECT v FROM t;"); got != "42\n" {
		t.Fatalf("child content = %q, want \"42\\n\"", got)
	}
}

// TestGCReclaimsDestroyedParentAboveForkPoint: object granularity within a
// partially-shared lineage. A destroyed parent's objects at or below a live
// child's fork point survive; everything else in the parent — its later
// checkpoint's snapshot AND its pre-fork-point init snapshot no ref anchors
// anymore — is reclaimed.
func TestGCReclaimsDestroyedParentAboveForkPoint(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	txidV1, err := w.Checkpoint("app", "main", "v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "child", "v1", 0, nil); err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "INSERT INTO t VALUES (2);")
	txidV2, err := w.Checkpoint("app", "main", "v2", nil)
	if err != nil {
		t.Fatal(err)
	}
	mref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Destroy("app", "main", true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}

	// The parent lineage must be trimmed to exactly the fork-point snapshot:
	// the >base checkpoint snapshot (txidV2) and the pre-fork init snapshot
	// (txid 1, whose init checkpoint died with the ref) are both reclaimed.
	keys, err := w.Store.B.List(store.LineagePrefix(mref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("destroyed parent must keep exactly its fork-point snapshot, has %d objects: %v", len(keys), keys)
	}
	m, ok := store.ParseMemberKey(keys[0])
	if !ok || !m.Snapshot || m.MaxTXID != txidV1 {
		t.Fatalf("surviving object = %v (parsed %+v), want the snapshot at fork point txid %d (txid %d must be gone)",
			keys[0], m, txidV1, txidV2)
	}
	cpath, err := w.Checkout("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	if got := querySQL(t, cpath, "SELECT v FROM t ORDER BY v;"); got != "1\n" {
		t.Fatalf("child content = %q, want the fork-point state \"1\\n\"", got)
	}
}

// TestGCMarkEqualsReadPathResolution is the MARK==READ property test: GC's
// reachable set must equal, exactly, the union over every live ref of the
// read path's own resolution (store.Chain at head and at every checkpoint)
// plus the base.json objects resolution reads along each ref's base spine.
// Any divergence between the mark and the read path can sweep a live member.
func TestGCMarkEqualsReadPathResolution(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "INSERT INTO t VALUES (2);")
	if _, err := w.Checkpoint("app", "main", "v2", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "child", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	cpath, err := w.Checkout("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, cpath, "INSERT INTO t VALUES (3);")
	// The child's own checkpoint writes its divergence snapshot in its own
	// lineage, so the union spans a shared spine AND child-owned objects.
	if _, err := w.Checkpoint("app", "child", "c1", nil); err != nil {
		t.Fatal(err)
	}

	got, err := w.reachableObjects()
	if err != nil {
		t.Fatal(err)
	}

	// Independent recomputation straight off the read path.
	want := map[string]bool{}
	refs, err := w.Store.ListRefs()
	if err != nil {
		t.Fatal(err)
	}
	for db, branches := range refs {
		for _, br := range branches {
			r, _, err := w.Store.GetRef(db, br)
			if err != nil {
				t.Fatal(err)
			}
			spine, err := w.Store.BaseSpine(r.Lineage)
			if err != nil {
				t.Fatal(err)
			}
			for _, l := range append([]string{r.Lineage}, spine...) {
				if _, _, err := w.Store.B.Get(store.BaseKey(l)); err == nil {
					want[store.BaseKey(l)] = true
				}
			}
			txids := map[uint64]bool{r.HeadTXID: true}
			for _, cp := range r.Checkpoints {
				txids[cp.TXID] = true
			}
			for txid := range txids {
				members, err := w.Store.Chain(r.Lineage, txid)
				if err != nil {
					t.Fatalf("read-path chain %s@%d: %v", r.Lineage, txid, err)
				}
				for _, m := range members {
					want[m.Key] = true
				}
			}
		}
	}
	toSorted := func(set map[string]bool) []string {
		out := make([]string, 0, len(set))
		for k := range set {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	if g, w := toSorted(got), toSorted(want); !reflect.DeepEqual(g, w) {
		t.Fatalf("MARK != READ:\n mark: %v\n read: %v", g, w)
	}
}

// TestGCRaceAncestorDestroyedMidForkOfFork is the fault-injection race test:
// a fork-of-fork has read its source ref but not yet landed its own, when
// the source branch is destroyed and GC runs. The two-phase design (phase 1
// only tombstones; the mint-this-run skip forbids a same-run sweep; phase
// 2's re-mark on a later run sees the now-landed child) must protect the
// in-flight fork's source objects.
func TestGCRaceAncestorDestroyedMidForkOfFork(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (7);")
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "b", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	bref, _, err := w.Store.GetRef("app", "b")
	if err != nil {
		t.Fatal(err)
	}
	if bref.Base == nil {
		t.Fatal("test precondition: fork b must share")
	}

	// The in-flight fork-of-fork has read b's ref (bref above). Before its
	// own ref lands, b is destroyed...
	if err := w.Destroy("app", "b", false); err != nil {
		t.Fatal(err)
	}
	// ...and GC runs with zero grace. b's lineage (its base.json — a shared
	// fork's only object) is unreachable right now, but a stone minted this
	// run must never be swept this run.
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.B.Get(store.BaseKey(bref.Lineage)); err != nil {
		t.Fatalf("mint-this-run skip violated: in-flight fork's source base.json swept in the same GC run: %v", err)
	}

	// The in-flight fork lands, exactly as ops.Fork's shared path would have
	// written it: base object first, then the ref.
	childLineage := store.NewLineageID()
	bp := store.BasePointer{Lineage: bref.Lineage, TXID: bref.HeadTXID}
	if err := w.Store.WriteLineageBase(childLineage, bp); err != nil {
		t.Fatal(err)
	}
	child := store.Ref{
		Lineage: childLineage, Epoch: 1, HeadTXID: bref.HeadTXID, HeadEpoch: 1,
		Base: &bp,
	}
	child.SetCheckpoint("fork", store.Checkpoint{TXID: bref.HeadTXID, Epoch: 1, CreatedAt: nowStamp()})
	if _, err := w.Store.PutRef("app", "c", child, ""); err != nil {
		t.Fatal(err)
	}

	// A later GC run: the stone is grace-eligible (grace 0), but the re-mark
	// now sees c's spine reading through b's base.json — it must survive.
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.B.Get(store.BaseKey(bref.Lineage)); err != nil {
		t.Fatalf("re-mark failed to protect a re-referenced object: %v", err)
	}
	cpath, err := w.Checkout("app", "c")
	if err != nil {
		t.Fatalf("fork-of-fork must materialize through its destroyed parent's base.json: %v", err)
	}
	if got := querySQL(t, cpath, "SELECT v FROM t;"); got != "7\n" {
		t.Fatalf("fork-of-fork content = %q, want \"7\\n\"", got)
	}
}

// TestGCMixedStoreV1AndV2Forks: a v1-style full-copy fork (no base.json,
// self-contained lineage — forced via the materialize test hook) and a v2
// shared (base-pointer) fork coexist; reachability handles both uniformly
// when their common parent branch is destroyed.
func TestGCMixedStoreV1AndV2Forks(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (5);")
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	SetForkMaterializeForTest(true)
	t.Cleanup(func() { SetForkMaterializeForTest(false) })
	_, ferr := w.Fork("app", "main", "fullcopy", "", 0, nil)
	SetForkMaterializeForTest(false)
	if ferr != nil {
		t.Fatal(ferr)
	}
	if _, err := w.Fork("app", "main", "shared", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	fref, _, err := w.Store.GetRef("app", "fullcopy")
	if err != nil {
		t.Fatal(err)
	}
	sref, _, err := w.Store.GetRef("app", "shared")
	if err != nil {
		t.Fatal(err)
	}
	if fref.Base != nil || sref.Base == nil {
		t.Fatalf("test precondition: fullcopy must materialize (Base=%+v) and shared must share (Base=%+v)",
			fref.Base, sref.Base)
	}

	if err := w.Destroy("app", "main", true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}

	// The full-copy child is self-contained: its own snapshot must be intact.
	fkeys, err := w.Store.B.List(store.LineagePrefix(fref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	if len(fkeys) == 0 {
		t.Fatal("full-copy child's lineage swept")
	}
	for _, br := range []string{"fullcopy", "shared"} {
		p, err := w.Checkout("app", br)
		if err != nil {
			t.Fatalf("checkout %s after GC: %v", br, err)
		}
		if got := querySQL(t, p, "SELECT v FROM t;"); got != "5\n" {
			t.Fatalf("%s content = %q, want \"5\\n\"", br, got)
		}
	}
}

// TestGCPreservesCheckpointAnchoredSnapshot guards the union-over-checkpoints
// mark: a live ref's OLD checkpoint anchors an older snapshot that the head
// chain no longer needs. A head-only mark would sweep it and break
// Fork(at=cp)/Rollback(to=cp); the checkpoint union must keep it.
func TestGCPreservesCheckpointAnchoredSnapshot(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	txidV1, err := w.Checkpoint("app", "main", "v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "INSERT INTO t VALUES (2);")
	if _, err := w.Checkpoint("app", "main", "v2", nil); err != nil {
		t.Fatal(err)
	}
	mref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	// Everything in a live, fully-checkpointed lineage is reachable: GC must
	// tombstone nothing at all here, let alone sweep.
	tombstoned, _, err := w.GC(0)
	if err != nil {
		t.Fatal(err)
	}
	if tombstoned != 0 {
		t.Fatalf("GC tombstoned %d objects of a live, fully-checkpointed lineage; want 0", tombstoned)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}

	// The old checkpoint's snapshot is still resolvable and forkable.
	if _, err := w.Store.Chain(mref.Lineage, txidV1); err != nil {
		t.Fatalf("old checkpoint's chain must survive GC: %v", err)
	}
	if _, err := w.Fork("app", "main", "old", "v1", 0, nil); err != nil {
		t.Fatal(err)
	}
	opath, err := w.Checkout("app", "old")
	if err != nil {
		t.Fatal(err)
	}
	if got := querySQL(t, opath, "SELECT v FROM t ORDER BY v;"); got != "1\n" {
		t.Fatalf("fork-at-old-checkpoint content = %q, want \"1\\n\"", got)
	}
}

// TestGCKeepsPassThroughLineageBaseJSON guards the spine-vs-member-lineages
// decision: C bases on B bases on A, and B never diverged, so B contributes
// ZERO chain members to C's resolution — but C's resolution READS B's
// base.json to fall through to A. After B's branch is destroyed, B's
// base.json must survive GC via C's base spine, or C can never materialize.
func TestGCKeepsPassThroughLineageBaseJSON(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (9);")
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "b", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "b", "c", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	bref, _, err := w.Store.GetRef("app", "b")
	if err != nil {
		t.Fatal(err)
	}
	cref, _, err := w.Store.GetRef("app", "c")
	if err != nil {
		t.Fatal(err)
	}
	if bref.Base == nil || cref.Base == nil || cref.Base.Lineage != bref.Lineage {
		t.Fatalf("test precondition: want C->B->A spine, got b.Base=%+v c.Base=%+v", bref.Base, cref.Base)
	}
	// Precondition: B is a pure pass-through — C's chain has no member in
	// B's lineage.
	chain, err := w.Store.Chain(cref.Lineage, cref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range chain {
		if strings.HasPrefix(m.Key, store.LineagePrefix(bref.Lineage)) {
			t.Fatalf("test precondition: B must contribute zero members, chain has %s", m.Key)
		}
	}

	if err := w.Destroy("app", "b", false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}

	if _, _, err := w.Store.B.Get(store.BaseKey(bref.Lineage)); err != nil {
		t.Fatalf("pass-through lineage's base.json swept (spine marking broken): %v", err)
	}
	cpath, err := w.Checkout("app", "c")
	if err != nil {
		t.Fatalf("C must materialize through the pass-through B: %v", err)
	}
	if got := querySQL(t, cpath, "SELECT v FROM t;"); got != "9\n" {
		t.Fatalf("C content = %q, want \"9\\n\"", got)
	}
}

// TestGCRescuedStoneGetsFreshGrace guards the review-fix for the grace
// bypass: a stone rescued by phase 2's re-mark (its object became reachable
// again) must be PRUNED, not kept with its original timestamp. Kept, the
// stale stone would let the object's later, legitimate death be swept with
// effectively zero grace — skipping exactly the fork-in-flight window grace
// exists to protect. Pruned, the later death mints a FRESH stone and gets a
// full grace period.
func TestGCRescuedStoneGetsFreshGrace(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "b", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	bref, _, err := w.Store.GetRef("app", "b")
	if err != nil {
		t.Fatal(err)
	}
	if bref.Base == nil {
		t.Fatal("test precondition: fork b must share")
	}
	key := store.BaseKey(bref.Lineage)

	// An old race-window stone for a NOW-REACHABLE object (the shape a
	// caught-mid-flight fork leaves behind once its ref lands).
	if err := w.tombstone(map[string]string{key: "2000-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	// Rescue run: the re-mark sees the object reachable; the stone must be
	// pruned, the object untouched.
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.B.Get(key); err != nil {
		t.Fatalf("rescued object must survive: %v", err)
	}
	stones, _, err := w.loadTombstones()
	if err != nil {
		t.Fatal(err)
	}
	if _, still := stones[key]; still {
		t.Fatal("rescued stone must be pruned, not kept with its stale timestamp")
	}

	// The object later legitimately dies (branch destroyed). The immediately
	// following grace-elapsed run must NOT sweep it: it gets a FRESH stone
	// (full grace from now), never a sweep against the old timestamp.
	if err := w.Destroy("app", "b", false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.B.Get(key); err != nil {
		t.Fatalf("grace bypass: object swept against a stale rescued stone without a fresh grace period: %v", err)
	}
	// And the fresh stone ages normally: a later independent run sweeps it.
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.B.Get(key); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("object must sweep after its fresh grace elapses, Get err = %v", err)
	}
}

// TestGCCompensatingRuleProtectsAboveHeadOrphan guards the review-fix for
// the flush-retry race: a session flush that hit an ambiguous ref-write
// failure re-Puts the SAME (lineage, epoch, txid) object key on retry
// (session/flush.go), and HeadTXID only advances on a successful ref write,
// so that key is always ABOVE the live ref's head — unreachable by
// definition, so reachability alone cannot protect it from the object-
// granular sweep. The compensating rule must: phase 2 never deletes a
// member object above a live ref's head in that ref's own lineage, however
// old its stone; once the branch itself dies, the same object becomes
// sweepable.
func TestGCCompensatingRuleProtectsAboveHeadOrphan(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	head, err := w.Checkpoint("app", "main", "v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	mref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	// The orphan a crashed/ambiguous flush leaves: a segment at head+1 in
	// the live ref's own lineage, with a stone already past any grace.
	orphanKey := store.SegmentKey(mref.Lineage, mref.Epoch, head+1, head+1)
	if err := w.Store.B.Put(orphanKey, []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	if err := w.tombstone(map[string]string{orphanKey: "2000-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}

	// While the branch lives, the orphan must never be deleted, run after run.
	for i := 0; i < 2; i++ {
		if _, _, err := w.GC(0); err != nil {
			t.Fatal(err)
		}
		if _, _, err := w.Store.B.Get(orphanKey); err != nil {
			t.Fatalf("run %d: above-head orphan in a live lineage swept (flush retry would re-create it): %v", i, err)
		}
	}
	// The stone is deliberately KEPT while the rule protects the object.
	stones, _, err := w.loadTombstones()
	if err != nil {
		t.Fatal(err)
	}
	if _, kept := stones[orphanKey]; !kept {
		t.Fatal("protected orphan's stone must be kept until the object is reachable or the branch dies")
	}

	// Branch gone: the lineage drops out of the live-head set and the same
	// stale stone now sweeps.
	if err := w.Destroy("app", "main", true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.B.Get(orphanKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("orphan must be sweepable once its branch is gone, Get err = %v", err)
	}
}

// TestGCCompensatingRuleSweepsFencedEpochOrphan guards the epoch-aware fix
// to the compensating rule: an above-head orphan written under an epoch
// strictly older than the lineage's CURRENT writer generation (Ref.Epoch)
// can never win a future ref CAS — keepHighestEpoch's same-range resolution
// and the CAS'd ref write both key off the current epoch, so a fenced
// writer's stragglers lose deterministically (see lease.go/store.go's
// Epoch/keepHighestEpoch docs) — and so can never become live. Protecting
// such an object forever, as an epoch-blind rule once did, is a bounded
// space leak; this test proves it is now reclaimed once provably fenced.
func TestGCCompensatingRuleSweepsFencedEpochOrphan(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	head, err := w.Checkpoint("app", "main", "v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	mref, metag, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	// The orphan a writer left behind before it was fenced: a segment at
	// head+1, written under the epoch active at the time (mref.Epoch).
	orphanKey := store.SegmentKey(mref.Lineage, mref.Epoch, head+1, head+1)
	if err := w.Store.B.Put(orphanKey, []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	if err := w.tombstone(map[string]string{orphanKey: "2000-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	// Simulate the branch's lease being reclaimed by a new holder (the same
	// Epoch bump AcquireLease performs on a fresh acquisition/reclaim — see
	// lease.go): the writer that left the orphan is now fenced, and the
	// orphan can never win a ref CAS at the new epoch.
	mref.Epoch++
	if _, err := w.Store.PutRef("app", "main", mref, metag); err != nil {
		t.Fatal(err)
	}

	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.B.Get(orphanKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("fenced-epoch orphan must be swept once provably fenced, Get err = %v", err)
	}
}

// TestGCCompensatingRuleProtectsNewerEpochOrphan pins the compensating
// rule's direction-of-safety argument (see reachableObjectsAndHeads and the
// phase-2 predicate's comment): liveHeads' Epoch is the MINIMUM read across
// every ref naming the lineage, so a stale or racing read only ever makes
// the compared-against epoch SMALLER — erring toward protecting. An object
// at an epoch NEWER than the (possibly stale) epoch GC read — e.g. a new
// acquirer bumped Ref.Epoch after GC's own read — must stay protected: only
// a STRICTLY OLDER epoch than what was read is provably fenced under every
// interleaving.
func TestGCCompensatingRuleProtectsNewerEpochOrphan(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	head, err := w.Checkpoint("app", "main", "v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	mref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	// An orphan at an epoch NEWER than the ref's current epoch.
	orphanKey := store.SegmentKey(mref.Lineage, mref.Epoch+1, head+1, head+1)
	if err := w.Store.B.Put(orphanKey, []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	if err := w.tombstone(map[string]string{orphanKey: "2000-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}

	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.B.Get(orphanKey); err != nil {
		t.Fatalf("newer-epoch orphan above head must stay protected: %v", err)
	}
}

// TestGCCompensatingRuleSweepsAtHeadOrphanRegardlessOfEpoch confirms the
// epoch change didn't touch the rule's other gate: it only ever protects
// keys STRICTLY ABOVE a live ref's head (m.MaxTXID > head). A member key at
// or below head is swept once unreachable and past grace no matter what
// epoch it carries — including an epoch newer than the ref's own, which
// would satisfy the epoch half of the predicate on its own.
func TestGCCompensatingRuleSweepsAtHeadOrphanRegardlessOfEpoch(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	head, err := w.Checkpoint("app", "main", "v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	mref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	// A bogus, unreachable member key AT head (not above it), deliberately
	// given an epoch NEWER than the ref's current epoch (mref.Epoch+1
	// guarantees no collision with any real object, since nothing has been
	// written at that epoch). If the above-head gate were broken this would
	// be wrongly protected by the epoch half of the predicate alone.
	orphanKey := store.SegmentKey(mref.Lineage, mref.Epoch+1, 0, head)
	if err := w.Store.B.Put(orphanKey, []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	if err := w.tombstone(map[string]string{orphanKey: "2000-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}

	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.B.Get(orphanKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("at/below-head orphan must sweep regardless of epoch, Get err = %v", err)
	}
}
