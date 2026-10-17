package ops

import (
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// pruneFixture is app@work, an unprotected fork of main carrying the
// checkpoints a, b, c in that order, each at its own txid after a write,
// besides the "fork" checkpoint Fork records at the fork point (the oldest).
func pruneFixture(t *testing.T) *Workspace {
	t.Helper()
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "work", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "work")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := testutil.SQLite3CLI(path, "CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	for _, name := range []string{"a", "b", "c"} {
		if out, err := testutil.SQLite3CLI(path, "INSERT INTO t VALUES ('"+name+"');").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		if _, err := w.Checkpoint("app", "work", name, nil); err != nil {
			t.Fatal(err)
		}
	}
	return w
}

func checkpointNames(t *testing.T, w *Workspace, db, branch string) []string {
	t.Helper()
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for n := range ref.Checkpoints {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func TestPruneCheckpointsByName(t *testing.T) {
	w := pruneFixture(t)
	before, _, _ := w.Store.GetRef("app", "work")

	res, err := w.PruneCheckpoints("app", "work", PruneOptions{Names: []string{"b", "a"}})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "b"}; !reflect.DeepEqual(res.Deleted, want) {
		t.Fatalf("Deleted = %v, want %v (sorted)", res.Deleted, want)
	}
	if res.Kept != 2 {
		t.Fatalf("Kept = %d, want 2", res.Kept)
	}
	if got, want := checkpointNames(t, w, "app", "work"), []string{"c", "fork"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("checkpoints = %v, want %v", got, want)
	}
	after, _, _ := w.Store.GetRef("app", "work")
	if after.HeadTXID != before.HeadTXID || after.Lineage != before.Lineage {
		t.Fatalf("prune moved the head: %d@%s -> %d@%s", before.HeadTXID, before.Lineage, after.HeadTXID, after.Lineage)
	}
	// Pruning is not activity: the TTL clock stays where it was.
	if after.TouchedAt != before.TouchedAt {
		t.Fatalf("prune touched the branch: %q -> %q", before.TouchedAt, after.TouchedAt)
	}
	// A pruned name is gone for every by-name consumer, and free again.
	if _, err := w.Fork("app", "work", "x", "a", 0, nil); err == nil {
		t.Fatal("fork --at a pruned checkpoint must fail")
	}
	if _, err := w.Checkpoint("app", "work", "a", nil); err != nil {
		t.Fatalf("a pruned name must be reusable: %v", err)
	}
}

// An unknown name refuses the whole call: a typo never half-applies.
func TestPruneCheckpointsUnknownNameDeletesNothing(t *testing.T) {
	w := pruneFixture(t)
	_, err := w.PruneCheckpoints("app", "work", PruneOptions{Names: []string{"a", "nope"}})
	if err == nil || !strings.Contains(err.Error(), `"nope"`) {
		t.Fatalf("want an error naming the unknown checkpoint, got %v", err)
	}
	if got, want := checkpointNames(t, w, "app", "work"), []string{"a", "b", "c", "fork"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("checkpoints = %v, want all of %v kept", got, want)
	}
}

func TestPruneCheckpointsKeepNewest(t *testing.T) {
	w := pruneFixture(t)
	res, err := w.PruneCheckpoints("app", "work", PruneOptions{Keep: 2, KeepSet: true})
	if err != nil {
		t.Fatal(err)
	}
	// The two newest by txid survive; "fork" (at the fork point) and a go.
	if want := []string{"a", "fork"}; !reflect.DeepEqual(res.Deleted, want) {
		t.Fatalf("Deleted = %v, want %v", res.Deleted, want)
	}
	if got, want := checkpointNames(t, w, "app", "work"), []string{"b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("checkpoints = %v, want %v", got, want)
	}
	// Keeping at least as many as exist is a no-op, not an error.
	res, err = w.PruneCheckpoints("app", "work", PruneOptions{Keep: 5, KeepSet: true})
	if err != nil || len(res.Deleted) != 0 || res.Kept != 2 {
		t.Fatalf("keep 5 of 2 = %+v, %v; want nothing deleted, 2 kept", res, err)
	}
	// Keep 0 drops them all.
	if res, err = w.PruneCheckpoints("app", "work", PruneOptions{Keep: 0, KeepSet: true}); err != nil || res.Kept != 0 {
		t.Fatalf("keep 0 = %+v, %v", res, err)
	}
	if got := checkpointNames(t, w, "app", "work"); len(got) != 0 {
		t.Fatalf("checkpoints = %v, want none", got)
	}
}

// Order is txid, then created_at, then name, newest first: two checkpoints
// at one txid are told apart deterministically.
func TestPruneKeepOrderBreaksTXIDTies(t *testing.T) {
	cps := map[string]store.Checkpoint{
		"old":   {TXID: 1, CreatedAt: "2026-10-17T00:00:03Z"},
		"tie-a": {TXID: 5, CreatedAt: "2026-10-17T00:00:01Z"},
		"tie-b": {TXID: 5, CreatedAt: "2026-10-17T00:00:01Z"},
		"tie-z": {TXID: 5, CreatedAt: "2026-10-17T00:00:02Z"},
		"new":   {TXID: 9, CreatedAt: "2026-10-17T00:00:00Z"},
	}
	if got, want := newestFirst(cps), []string{"new", "tie-z", "tie-a", "tie-b", "old"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("newestFirst = %v, want %v", got, want)
	}
}

func TestPruneCheckpointsRefusesBadOptions(t *testing.T) {
	w := pruneFixture(t)
	for _, tc := range []struct {
		name string
		opts PruneOptions
	}{
		{"neither", PruneOptions{}},
		{"both", PruneOptions{Names: []string{"a"}, Keep: 1, KeepSet: true}},
		{"negative keep", PruneOptions{Keep: -1, KeepSet: true}},
		{"invalid name", PruneOptions{Names: []string{"../x"}}},
	} {
		if _, err := w.PruneCheckpoints("app", "work", tc.opts); err == nil {
			t.Errorf("%s: want an error", tc.name)
		}
	}
	if got := checkpointNames(t, w, "app", "work"); len(got) != 4 {
		t.Fatalf("a refused call deleted checkpoints: %v", got)
	}
}

// A protected branch's checkpoints are restore points someone chose to
// keep: pruning them needs force, as destroy does.
func TestPruneCheckpointsProtectedNeedsForce(t *testing.T) {
	w := pruneFixture(t)
	if _, err := w.SetProtected("app", "work", true); err != nil {
		t.Fatal(err)
	}
	_, err := w.PruneCheckpoints("app", "work", PruneOptions{Names: []string{"a"}})
	if err == nil || !strings.Contains(err.Error(), "protected") {
		t.Fatalf("unforced prune of a protected branch must refuse, got %v", err)
	}
	if got := checkpointNames(t, w, "app", "work"); len(got) != 4 {
		t.Fatalf("refused prune deleted checkpoints: %v", got)
	}
	if _, err := w.PruneCheckpoints("app", "work", PruneOptions{Names: []string{"a"}, Force: true}); err != nil {
		t.Fatalf("forced prune: %v", err)
	}
}

func TestPruneCheckpointsRefusesClaimedBranch(t *testing.T) {
	w := pruneFixture(t)
	ref, etag, err := w.Store.GetRef("app", "work")
	if err != nil {
		t.Fatal(err)
	}
	ref.Deleting = true
	if _, err := w.Store.PutRef("app", "work", ref, etag); err != nil {
		t.Fatal(err)
	}
	if _, err := w.PruneCheckpoints("app", "work", PruneOptions{Names: []string{"a"}}); err == nil {
		t.Fatal("prune of a branch being destroyed must refuse")
	}
}

// A CAS loss re-reads and recomputes from the fresh ref, so a checkpoint
// recorded concurrently survives a by-name prune of another.
func TestPruneCheckpointsRetriesOnCASLoss(t *testing.T) {
	w := pruneFixture(t)
	raced := false
	pruneBeforePutHook = func() {
		if raced {
			return
		}
		raced = true
		ref, etag, err := w.Store.GetRef("app", "work")
		if err != nil {
			t.Error(err)
			return
		}
		ref.SetCheckpoint("rival", store.Checkpoint{TXID: ref.HeadTXID, Epoch: ref.HeadEpoch})
		if _, err := w.Store.PutRef("app", "work", ref, etag); err != nil {
			t.Error(err)
		}
	}
	defer func() { pruneBeforePutHook = nil }()
	if _, err := w.PruneCheckpoints("app", "work", PruneOptions{Names: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	if !raced {
		t.Fatal("hook never ran")
	}
	if got, want := checkpointNames(t, w, "app", "work"), []string{"b", "c", "fork", "rival"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("checkpoints = %v, want %v", got, want)
	}
}

// Pruning is metadata only; the storage a pruned checkpoint alone kept
// alive is reclaimed by GC, and the head still reads back intact.
func TestPruneThenGCKeepsHeadReadable(t *testing.T) {
	w := pruneFixture(t)
	if _, err := w.PruneCheckpoints("app", "work", PruneOptions{Keep: 0, KeepSet: true}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "work", "after-gc", "", time.Hour, nil); err != nil {
		t.Fatalf("fork of the pruned branch's head after GC: %v", err)
	}
	path, err := w.Checkout("app", "after-gc")
	if err != nil {
		t.Fatal(err)
	}
	out, err := testutil.SQLite3CLI(path, "SELECT group_concat(v) FROM t;").Output()
	if err != nil || strings.TrimSpace(string(out)) != "a,b,c" {
		t.Fatalf("head after prune+GC = %q, %v; want a,b,c", out, err)
	}
}

func TestPruneCheckpointsUnknownBranch(t *testing.T) {
	w := pruneFixture(t)
	_, err := w.PruneCheckpoints("app", "nope", PruneOptions{Keep: 0, KeepSet: true})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
