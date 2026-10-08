package ops

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/sricola/offshoot/internal/store"
)

// recordChainSources installs observeChainSource for the test and returns a
// function reporting (and clearing) the kinds observed so far.
func recordChainSources(t *testing.T) func() []string {
	t.Helper()
	var mu sync.Mutex
	var kinds []string
	observeChainSource = func(kind string) {
		mu.Lock()
		defer mu.Unlock()
		kinds = append(kinds, kind)
	}
	t.Cleanup(func() { observeChainSource = nil })
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := kinds
		kinds = nil
		return out
	}
}

// assertCachedChainIsResolved checks that the chain the checkout's sidecar
// recorded is exactly what Store.Chain resolves for the branch's head:
// keys, order, kinds and epochs.
func assertCachedChainIsResolved(t *testing.T, w *Workspace, path, db, branch string) []store.ChainMember {
	t.Helper()
	ref := refOf(t, w, db, branch)
	got, ok := w.cachedChain(path, ref)
	if !ok {
		rec, _ := readSidecar(path)
		t.Fatalf("no usable recorded chain at %s@%d (sidecar %+v)", ref.Lineage, ref.HeadTXID, rec)
	}
	want, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recorded chain differs from Store.Chain:\n got  %+v\n want %+v", got, want)
	}
	return got
}

// assertHeadIsCheckout materializes db@branch's checkpoint name
// independently and compares it with the checkout byte for byte.
func assertHeadIsCheckout(t *testing.T, w *Workspace, db, branch, name, path string) {
	t.Helper()
	at, err := w.CheckoutAt(db, branch, name, true)
	if err != nil {
		t.Fatalf("checkout-at %s: %v", name, err)
	}
	if !bytes.Equal(readFile(t, at), readFile(t, path)) {
		t.Fatalf("%s: materialized head differs from the checkout", name)
	}
}

// chainCacheSeed seeds app@main and takes a snapshot checkpoint "a" and a
// segment checkpoint "b", returning the checkout path. The sidecar then
// records b's chain: a's snapshot and b's segment.
func chainCacheSeed(t *testing.T, w *Workspace) string {
	t.Helper()
	requireClone(t, w)
	path := seedRows(t, w, "app", 1<<20, 4000)
	if res := mustCheckpointWith(t, w, "app", "main", "a", CheckpointOptions{}); res.Kind != "snapshot" {
		t.Fatalf("a: kind %q, want snapshot", res.Kind)
	}
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	if res := mustCheckpointWith(t, w, "app", "main", "b", CheckpointOptions{}); res.Kind != "segment" {
		t.Fatalf("b: kind %q, want segment", res.Kind)
	}
	return path
}

// editSidecar rewrites path's sidecar JSON through edit, leaving every
// other field exactly as recorded.
func editSidecar(t *testing.T, path string, edit func(map[string]any)) {
	t.Helper()
	raw, err := os.ReadFile(path + ".sum")
	if err != nil {
		t.Fatal(err)
	}
	// UseNumber: the record's uint64 fields (the post-apply checksum) do
	// not survive a float64 round trip.
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		t.Fatal(err)
	}
	edit(m)
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".sum", out, 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertSources(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: chain sources %q, want %q", what, got, want)
	}
}

func TestSecondCheckpointUsesTheRecordedChain(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	path := seedRows(t, w, "app", 1<<20, 4000)
	if res := mustCheckpointWith(t, w, "app", "main", "a", CheckpointOptions{}); res.Kind != "snapshot" {
		t.Fatalf("a: kind %q, want snapshot", res.Kind)
	}
	if got := assertCachedChainIsResolved(t, w, path, "app", "main"); len(got) != 1 || !got[0].Snapshot {
		t.Fatalf("after a snapshot checkpoint the recorded chain is %+v, want the snapshot alone", got)
	}
	sources := recordChainSources(t)
	for i, name := range []string{"b", "c", "d"} {
		mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
		res := mustCheckpointWith(t, w, "app", "main", name, CheckpointOptions{})
		if res.Kind != "segment" {
			t.Fatalf("%s: kind %q, want segment", name, res.Kind)
		}
		assertSources(t, name, sources(), "cache")
		if got := assertCachedChainIsResolved(t, w, path, "app", "main"); len(got) != i+2 {
			t.Fatalf("%s: recorded chain has %d members, want %d", name, len(got), i+2)
		}
		assertHeadIsCheckout(t, w, "app", "main", name, path)
	}
}

func TestCachedChainMissesAfterRepoint(t *testing.T) {
	cases := map[string]func(t *testing.T, w *Workspace){
		"rollback": func(t *testing.T, w *Workspace) {
			if _, err := w.Rollback("app", "main", "a"); err != nil {
				t.Fatal(err)
			}
		},
		"compact": func(t *testing.T, w *Workspace) {
			// Compact cuts a shared base, so main first gets one: a
			// rollback, then a checkpoint on the shared lineage.
			if _, err := w.Rollback("app", "main", "a"); err != nil {
				t.Fatal(err)
			}
			rp := mustCheckout(t, w, "app", "main")
			mustSQL(t, rp, "INSERT INTO t (v) VALUES (randomblob(300));")
			mustCheckpointWith(t, w, "app", "main", "r", CheckpointOptions{})
			if _, err := w.Compact("app", "main"); err != nil {
				t.Fatal(err)
			}
		},
		"promote": func(t *testing.T, w *Workspace) {
			mustFork(t, w, "app", "main", "feat", "")
			fp := mustCheckout(t, w, "app", "feat")
			mustSQL(t, fp, "INSERT INTO t (v) VALUES (randomblob(300));")
			mustCheckpointWith(t, w, "app", "feat", "f", CheckpointOptions{})
			if _, err := w.Promote("app", "feat", "main", true); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, repoint := range cases {
		t.Run(name, func(t *testing.T) {
			w := newWS(t)
			path := chainCacheSeed(t, w)
			before := refOf(t, w, "app", "main")
			repoint(t, w)
			after := refOf(t, w, "app", "main")
			if after.Lineage == before.Lineage {
				t.Fatalf("%s did not change the lineage", name)
			}
			if _, ok := w.cachedChain(path, after); ok {
				t.Fatalf("recorded chain used after %s", name)
			}
			path = mustCheckout(t, w, "app", "main")
			sources := recordChainSources(t)
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			mustCheckpointWith(t, w, "app", "main", "next", CheckpointOptions{})
			assertSources(t, "checkpoint after "+name, sources(), "resolve")
			assertHeadIsCheckout(t, w, "app", "main", "next", path)
		})
	}
}

func TestCachedChainSurvivesGC(t *testing.T) {
	w := newWS(t)
	path := chainCacheSeed(t, w)
	ref := refOf(t, w, "app", "main")
	// A fenced attempt's leftovers: a snapshot and a segment at the head's
	// txid under the lineage's first epoch, which the live head's higher
	// epoch supersedes (store.keepHighestEpoch). Unreachable, so GC sweeps
	// them around the recorded members.
	if ref.HeadEpoch <= 1 {
		t.Fatalf("head epoch %d: the orphans' epoch 1 would not be lower", ref.HeadEpoch)
	}
	orphans := []string{
		store.SnapshotKey(ref.Lineage, 1, ref.HeadTXID),
		store.SegmentKey(ref.Lineage, 1, ref.HeadTXID, ref.HeadTXID),
	}
	for _, key := range orphans {
		if err := w.Store.B.Put(key, []byte("orphan")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, _, err := w.GC(0); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range orphans {
		if _, _, err := w.Store.B.Get(key); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("orphan %s not swept by GC: Get err = %v", key, err)
		}
	}
	sources := recordChainSources(t)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	if res := mustCheckpointWith(t, w, "app", "main", "c", CheckpointOptions{}); res.Kind != "segment" {
		t.Fatalf("c: kind %q, want segment", res.Kind)
	}
	assertSources(t, "checkpoint after GC", sources(), "cache")
	assertCachedChainIsResolved(t, w, path, "app", "main")
	assertHeadIsCheckout(t, w, "app", "main", "c", path)
}

func TestOldSidecarWithoutChainResolves(t *testing.T) {
	w := newWS(t)
	path := chainCacheSeed(t, w)
	editSidecar(t, path, func(m map[string]any) { delete(m, "chain") })
	if _, ok := w.cachedChain(path, refOf(t, w, "app", "main")); ok {
		t.Fatal("a sidecar without a chain answered from the cache")
	}
	sources := recordChainSources(t)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	if res := mustCheckpointWith(t, w, "app", "main", "c", CheckpointOptions{}); res.Kind != "segment" {
		t.Fatalf("c: kind %q, want segment", res.Kind)
	}
	assertSources(t, "checkpoint on an old sidecar", sources(), "resolve")
	assertHeadIsCheckout(t, w, "app", "main", "c", path)
	// The checkpoint records the chain again.
	assertCachedChainIsResolved(t, w, path, "app", "main")
}

func TestCorruptRecordedChainResolves(t *testing.T) {
	cases := map[string]func(chain []any, ref store.Ref) []any{
		"last key dropped": func(chain []any, _ store.Ref) []any { return chain[:len(chain)-1] },
		"middle key dropped": func(chain []any, _ store.Ref) []any {
			return append(append([]any(nil), chain[0]), chain[2:]...)
		},
		"second snapshot": func(chain []any, ref store.Ref) []any {
			return append(append([]any(nil), chain[:2]...), store.SnapshotKey(ref.Lineage, ref.HeadEpoch, ref.HeadTXID))
		},
		"segments out of order": func(chain []any, _ store.Ref) []any {
			return []any{chain[0], chain[2], chain[1], chain[2]}
		},
		"other lineage": func(chain []any, ref store.Ref) []any {
			out := append([]any(nil), chain...)
			out[0] = store.SnapshotKey(ref.Lineage+"x", ref.HeadEpoch, 1)
			return out
		},
		"unparseable key": func(chain []any, _ store.Ref) []any {
			return append(append([]any(nil), chain[:len(chain)-1]...), "data/garbage")
		},
		"no snapshot first": func(chain []any, _ store.Ref) []any { return chain[1:] },
		"empty":             func([]any, store.Ref) []any { return []any{} },
		"last key epoch rewritten": func(chain []any, ref store.Ref) []any {
			last, ok := chain[len(chain)-1].(string)
			if !ok {
				panic("last chain entry is not a string")
			}
			m, ok := store.ParseMemberKey(last)
			if !ok {
				panic("last chain entry does not parse as a member key")
			}
			var rewritten string
			if m.Snapshot {
				rewritten = store.SnapshotKey(ref.Lineage, m.Epoch+1, m.MaxTXID)
			} else {
				rewritten = store.SegmentKey(ref.Lineage, m.Epoch+1, m.MinTXID, m.MaxTXID)
			}
			return append(append([]any(nil), chain[:len(chain)-1]...), rewritten)
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			w := newWS(t)
			path := chainCacheSeed(t, w)
			// A third member, so a middle key can be dropped.
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			if res := mustCheckpointWith(t, w, "app", "main", "b2", CheckpointOptions{}); res.Kind != "segment" {
				t.Fatalf("b2: kind %q, want segment", res.Kind)
			}
			ref := refOf(t, w, "app", "main")
			editSidecar(t, path, func(m map[string]any) {
				chain, ok := m["chain"].([]any)
				if !ok || len(chain) != 3 {
					t.Fatalf("sidecar chain %v, want three keys", m["chain"])
				}
				m["chain"] = corrupt(chain, ref)
			})
			if got, ok := w.cachedChain(path, ref); ok {
				t.Fatalf("a corrupt recorded chain answered from the cache: %+v", got)
			}
			sources := recordChainSources(t)
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			if res := mustCheckpointWith(t, w, "app", "main", "c", CheckpointOptions{}); res.Kind != "segment" {
				t.Fatalf("c: kind %q, want segment", res.Kind)
			}
			assertSources(t, "checkpoint on a corrupt chain", sources(), "resolve")
			assertHeadIsCheckout(t, w, "app", "main", "c", path)
			assertCachedChainIsResolved(t, w, path, "app", "main")
		})
	}
}

func TestForkAtHeadUsesTheRecordedChain(t *testing.T) {
	w := newWS(t)
	path := chainCacheSeed(t, w)
	assertCachedChainIsResolved(t, w, path, "app", "main")
	sources := recordChainSources(t)
	mustFork(t, w, "app", "main", "child", "")
	assertSources(t, "fork at head", sources(), "cache")
	child := mustCheckout(t, w, "app", "child")
	if !bytes.Equal(readFile(t, child), readFile(t, path)) {
		t.Fatal("child checkout differs from the parent's")
	}
	// A fork at an older checkpoint resolves as before, with no shortcut.
	mustFork(t, w, "app", "main", "older", "a")
	assertSources(t, "fork at an older checkpoint", sources())
	// A sidecar whose identity is not the head is not consulted.
	editSidecar(t, path, func(m map[string]any) {
		txid, err := m["txid"].(json.Number).Int64()
		if err != nil {
			t.Fatal(err)
		}
		m["txid"] = txid - 1
	})
	mustFork(t, w, "app", "main", "stale", "")
	assertSources(t, "fork with a stale sidecar", sources(), "resolve")
	stale := mustCheckout(t, w, "app", "stale")
	if !bytes.Equal(readFile(t, stale), readFile(t, child)) {
		t.Fatal("fork with a stale sidecar differs from the fork at head")
	}
}

// TestCheckpointStampRecordsChainOnlyWhenTrusted: the chain rides the
// trusted stamp, and a distrusted stamp records none, so a checkout the
// checkpoint could not vouch for resolves its chain next time.
func TestCheckpointStampRecordsChainOnlyWhenTrusted(t *testing.T) {
	chain := []string{"data/lin/1/snapshot-0000000000000002.ltx"}
	t.Run("trusted", func(t *testing.T) {
		path, _, encSum := stampRaceCheckout(t)
		trusted, err := stampCheckpoint(path, "lin", 1, 2, encSum, true, chain)
		if err != nil || !trusted {
			t.Fatalf("trusted=%v err=%v, want a trusted stamp", trusted, err)
		}
		if rec, _ := readSidecar(path); !reflect.DeepEqual(rec.Chain, chain) {
			t.Fatalf("trusted stamp recorded chain %q, want %q", rec.Chain, chain)
		}
	})
	t.Run("distrusted", func(t *testing.T) {
		path, _, encSum := stampRaceCheckout(t)
		trusted, err := stampCheckpoint(path, "lin", 1, 2, encSum^1, true, chain)
		if err != nil || trusted {
			t.Fatalf("trusted=%v err=%v, want a distrusted stamp", trusted, err)
		}
		if rec, _ := readSidecar(path); len(rec.Chain) != 0 {
			t.Fatalf("distrusted stamp recorded chain %q", rec.Chain)
		}
	})
}

// sharedChildSeed seeds app@main with a snapshot checkpoint "a" and a
// segment checkpoint "b" (chainCacheSeed), forks app@child at main's head
// (a shared child: base pointer at b's txid, zero own objects), checks it
// out and returns the child's checkout path. The child's sidecar records no
// chain yet: a checkout stamps through writeSum, which records none.
func sharedChildSeed(t *testing.T, w *Workspace) string {
	t.Helper()
	chainCacheSeed(t, w)
	if _, err := w.Fork("app", "main", "child", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	ref := refOf(t, w, "app", "child")
	if ref.Base == nil {
		t.Fatal("child has no base pointer; the fork materialized instead of sharing")
	}
	return path
}

// TestSharedChildSecondCheckpointUsesTheRecordedChain: a shared child's
// first segment checkpoint resolves (its sidecar has no chain), records
// the chain it built — main's snapshot and segment, then its own segment —
// and the second checkpoint takes that record although its first keys
// name main's lineage.
func TestSharedChildSecondCheckpointUsesTheRecordedChain(t *testing.T) {
	w := newWS(t)
	path := sharedChildSeed(t, w)
	sources := recordChainSources(t)

	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	if res := mustCheckpointWith(t, w, "app", "child", "c1", CheckpointOptions{}); res.Kind != "segment" {
		t.Fatalf("c1: kind %q, want segment", res.Kind)
	}
	assertSources(t, "first child checkpoint", sources(), "resolve")
	members := assertCachedChainIsResolved(t, w, path, "app", "child")
	child := refOf(t, w, "app", "child")
	if !strings.HasPrefix(members[0].Key, store.LineagePrefix(child.Base.Lineage)) {
		t.Fatalf("recorded chain does not begin on the base lineage: %s", members[0].Key)
	}

	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	if res := mustCheckpointWith(t, w, "app", "child", "c2", CheckpointOptions{}); res.Kind != "segment" {
		t.Fatalf("c2: kind %q, want segment", res.Kind)
	}
	assertSources(t, "second child checkpoint", sources(), "cache")
	assertCachedChainIsResolved(t, w, path, "app", "child")
	assertHeadIsCheckout(t, w, "app", "child", "c2", path)
}

// TestSharedChildAfterOwnSnapshotUsesOwnChain: once the child writes its
// own snapshot (CheckpointOptions{Snapshot: true}), the record is the
// single-lineage shape again and is taken as before.
func TestSharedChildAfterOwnSnapshotUsesOwnChain(t *testing.T) {
	w := newWS(t)
	path := sharedChildSeed(t, w)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	if res := mustCheckpointWith(t, w, "app", "child", "s", CheckpointOptions{Snapshot: true}); res.Kind != "snapshot" {
		t.Fatalf("s: kind %q, want snapshot", res.Kind)
	}
	sources := recordChainSources(t)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	if res := mustCheckpointWith(t, w, "app", "child", "c", CheckpointOptions{}); res.Kind != "segment" {
		t.Fatalf("c: kind %q, want segment", res.Kind)
	}
	assertSources(t, "checkpoint after own snapshot", sources(), "cache")
	members := assertCachedChainIsResolved(t, w, path, "app", "child")
	child := refOf(t, w, "app", "child")
	for _, m := range members {
		if !strings.HasPrefix(m.Key, store.LineagePrefix(child.Lineage)) {
			t.Fatalf("chain after an own snapshot still names another lineage: %s", m.Key)
		}
	}
	assertHeadIsCheckout(t, w, "app", "child", "c", path)
}

// TestPassThroughSpineUsesTheRecordedChain: a fork of a shared child that
// has not diverged (grandchild forked at child's head, child's head still
// at the seam). The fork collapses the base (store.CollapseBase names the
// nearest lineage that owns the seam txid), so the grandchild's base
// pointer names main directly, not the pass-through child: this spine is
// one hop, pinned below. (A child that has diverged is not collapsed past;
// TestTwoHopSpineUsesTheRecordedChain covers that real two-hop spine.)
// After the grandchild's first checkpoint its record spans main's keys
// then its own, with the child contributing none, and the second
// checkpoint takes it.
func TestPassThroughSpineUsesTheRecordedChain(t *testing.T) {
	w := newWS(t)
	sharedChildSeed(t, w)
	if _, err := w.Fork("app", "child", "grandchild", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	main, gcRef := refOf(t, w, "app", "main"), refOf(t, w, "app", "grandchild")
	if gcRef.Base == nil || gcRef.Base.Lineage != main.Lineage {
		t.Fatalf("grandchild base %+v, want main's lineage %s (collapsed past the pass-through child)", gcRef.Base, main.Lineage)
	}
	path, err := w.Checkout("app", "grandchild")
	if err != nil {
		t.Fatal(err)
	}
	sources := recordChainSources(t)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	mustCheckpointWith(t, w, "app", "grandchild", "g1", CheckpointOptions{})
	assertSources(t, "first grandchild checkpoint", sources(), "resolve")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	if res := mustCheckpointWith(t, w, "app", "grandchild", "g2", CheckpointOptions{}); res.Kind != "segment" {
		t.Fatalf("g2: kind %q, want segment", res.Kind)
	}
	assertSources(t, "second grandchild checkpoint", sources(), "cache")
	assertCachedChainIsResolved(t, w, path, "app", "grandchild")
	assertHeadIsCheckout(t, w, "app", "grandchild", "g2", path)
}

// TestTwoHopSpineUsesTheRecordedChain: a shared child that has diverged
// (one segment checkpoint of its own above the seam) is forked at head.
// CollapseBase stops at the child, which owns that txid, so the
// grandchild's base pointer names the child and the spine has two hops:
// grandchild to child, child to main. The grandchild's first checkpoint
// resolves and records a chain on three lineages in order (main's
// snapshot and segment, the child's segment, its own segment); the second
// takes that record. With both ancestor branches destroyed and GC run
// twice, the ancestor members stay reachable through the grandchild's own
// resolution, so a third checkpoint still takes the record.
func TestTwoHopSpineUsesTheRecordedChain(t *testing.T) {
	w := newWS(t)
	path := sharedChildSeed(t, w)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	if res := mustCheckpointWith(t, w, "app", "child", "c1", CheckpointOptions{}); res.Kind != "segment" {
		t.Fatalf("c1: kind %q, want segment", res.Kind)
	}
	if _, err := w.Fork("app", "child", "grandchild", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	main, child, gcRef := refOf(t, w, "app", "main"), refOf(t, w, "app", "child"), refOf(t, w, "app", "grandchild")
	if gcRef.Base == nil || gcRef.Base.Lineage != child.Lineage {
		t.Fatalf("grandchild base %+v, want the child's lineage %s", gcRef.Base, child.Lineage)
	}
	gpath, err := w.Checkout("app", "grandchild")
	if err != nil {
		t.Fatal(err)
	}
	sources := recordChainSources(t)

	mustSQL(t, gpath, "INSERT INTO t (v) VALUES (randomblob(100));")
	mustCheckpointWith(t, w, "app", "grandchild", "g1", CheckpointOptions{})
	assertSources(t, "first grandchild checkpoint", sources(), "resolve")
	members := assertCachedChainIsResolved(t, w, gpath, "app", "grandchild")
	// Each member's position on the spine (0 main, 1 child, 2 grandchild)
	// must never decrease, and all three must appear.
	spine := []string{main.Lineage, child.Lineage, gcRef.Lineage}
	seen, last := map[int]bool{}, 0
	for _, m := range members {
		at := -1
		for i, l := range spine {
			if strings.HasPrefix(m.Key, store.LineagePrefix(l)) {
				at = i
			}
		}
		if at < last {
			t.Fatalf("record key %s is off the spine or out of order: %v", m.Key, members)
		}
		seen[at], last = true, at
	}
	if len(seen) != len(spine) {
		t.Fatalf("record spans %d of the spine's %d lineages: %v", len(seen), len(spine), members)
	}

	mustSQL(t, gpath, "INSERT INTO t (v) VALUES (randomblob(100));")
	if res := mustCheckpointWith(t, w, "app", "grandchild", "g2", CheckpointOptions{}); res.Kind != "segment" {
		t.Fatalf("g2: kind %q, want segment", res.Kind)
	}
	assertSources(t, "second grandchild checkpoint", sources(), "cache")
	assertCachedChainIsResolved(t, w, gpath, "app", "grandchild")
	assertHeadIsCheckout(t, w, "app", "grandchild", "g2", gpath)

	for _, branch := range []string{"child", "main"} {
		if err := w.Destroy("app", branch, true); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, _, err := w.GC(0); err != nil {
			t.Fatal(err)
		}
	}
	mustSQL(t, gpath, "INSERT INTO t (v) VALUES (randomblob(100));")
	if res := mustCheckpointWith(t, w, "app", "grandchild", "g3", CheckpointOptions{}); res.Kind != "segment" {
		t.Fatalf("g3: kind %q, want segment", res.Kind)
	}
	assertSources(t, "grandchild checkpoint after both ancestors destroyed + GC", sources(), "cache")
	assertCachedChainIsResolved(t, w, gpath, "app", "grandchild")
	assertHeadIsCheckout(t, w, "app", "grandchild", "g3", gpath)
}

// TestSharedChildCacheSurvivesBaseDestroyAndGC: with the base branch
// destroyed and GC run twice, the ancestor members the child's record names
// are still reachable through the child's own resolution, so the cache
// keeps hitting and the content is intact.
func TestSharedChildCacheSurvivesBaseDestroyAndGC(t *testing.T) {
	w := newWS(t)
	path := sharedChildSeed(t, w)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	mustCheckpointWith(t, w, "app", "child", "c1", CheckpointOptions{})
	if err := w.Destroy("app", "main", true); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, _, err := w.GC(0); err != nil {
			t.Fatal(err)
		}
	}
	sources := recordChainSources(t)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	if res := mustCheckpointWith(t, w, "app", "child", "c2", CheckpointOptions{}); res.Kind != "segment" {
		t.Fatalf("c2: kind %q, want segment", res.Kind)
	}
	assertSources(t, "child checkpoint after base destroy + GC", sources(), "cache")
	assertCachedChainIsResolved(t, w, path, "app", "child")
	assertHeadIsCheckout(t, w, "app", "child", "c2", path)
}

// TestSharedChildCorruptSeamResolves: a record corrupted at its seam is
// rejected and the checkpoint resolves instead, with the right content.
// Three corruptions: the suffix starts one past the seam (a hole), the
// prefix ends one before the seam (the ancestor half is short), and an
// own key moved in front of a foreign one (interleaved). The first two
// also break the record's own contiguity, so they do not isolate the
// comparison with ref.Base (TestSharedChildSeamMustMatchRefBase does);
// the third isolates the interleaving rule.
func TestSharedChildCorruptSeamResolves(t *testing.T) {
	cases := []struct {
		name string
		edit func(child store.Ref, chain []string) []string
	}{
		{"suffix starts past the seam", func(child store.Ref, chain []string) []string {
			i := len(chain) - 1 // the child's own segment
			m, _ := store.ParseMemberKey(chain[i])
			chain[i] = store.SegmentKey(child.Lineage, m.Epoch, m.MinTXID+1, m.MaxTXID)
			return chain
		}},
		{"prefix ends before the seam", func(child store.Ref, chain []string) []string {
			i := len(chain) - 2 // main's segment, which ends at the seam
			m, _ := store.ParseMemberKey(chain[i])
			chain[i] = store.SegmentKey(child.Base.Lineage, m.Epoch, m.MinTXID, m.MaxTXID-1)
			return chain
		}},
		{"interleaved", func(child store.Ref, chain []string) []string {
			n := len(chain)
			chain[n-1], chain[n-2] = chain[n-2], chain[n-1]
			return chain
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWS(t)
			path := sharedChildSeed(t, w)
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			mustCheckpointWith(t, w, "app", "child", "c1", CheckpointOptions{})
			child := refOf(t, w, "app", "child")
			editSidecar(t, path, func(m map[string]any) {
				raw := m["chain"].([]any)
				chain := make([]string, len(raw))
				for i, k := range raw {
					chain[i] = k.(string)
				}
				chain = tc.edit(child, chain)
				out := make([]any, len(chain))
				for i, k := range chain {
					out[i] = k
				}
				m["chain"] = out
			})
			if _, ok := w.cachedChain(path, child); ok {
				t.Fatal("cachedChain accepted a record whose seam does not match ref.Base")
			}
			sources := recordChainSources(t)
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			mustCheckpointWith(t, w, "app", "child", "c2", CheckpointOptions{})
			assertSources(t, "checkpoint over a corrupt seam", sources(), "resolve")
			assertHeadIsCheckout(t, w, "app", "child", "c2", path)
		})
	}
}

// TestSharedChildSeamMustMatchRefBase: the record is left intact and the
// ref's base pointer is shifted by one either way in memory. The record is
// contiguous, so this is the only case the comparison of its seam with
// ref.Base.TXID alone catches: it ties the record to the fork point the
// ref names, not merely to a well-formed chain.
func TestSharedChildSeamMustMatchRefBase(t *testing.T) {
	w := newWS(t)
	path := sharedChildSeed(t, w)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	mustCheckpointWith(t, w, "app", "child", "c1", CheckpointOptions{})
	child := refOf(t, w, "app", "child")
	if _, ok := w.cachedChain(path, child); !ok {
		t.Fatal("cachedChain rejected the unshifted record")
	}
	for _, tc := range []struct {
		name string
		txid uint64
	}{
		{"base one above the seam", child.Base.TXID + 1},
		{"base one below the seam", child.Base.TXID - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shifted := child
			base := *child.Base
			base.TXID = tc.txid
			shifted.Base = &base
			if _, ok := w.cachedChain(path, shifted); ok {
				t.Fatalf("cachedChain accepted a record whose seam is at %d with ref.Base.TXID %d", child.Base.TXID, tc.txid)
			}
		})
	}
}

// TestForeignKeysWithoutBaseResolve: a record with foreign keys on a ref
// that has no base pointer is rejected, as today.
func TestForeignKeysWithoutBaseResolve(t *testing.T) {
	w := newWS(t)
	path := sharedChildSeed(t, w)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	mustCheckpointWith(t, w, "app", "child", "c1", CheckpointOptions{})
	child := refOf(t, w, "app", "child")
	child.Base = nil
	if _, ok := w.cachedChain(path, child); ok {
		t.Fatal("cachedChain accepted foreign keys on a ref without a base pointer")
	}
}

// TestForkOfSharedChildAtHeadUsesTheRecordedChain: a fork at a shared
// child's head takes the child's record for its share/materialize
// decision instead of listing two lineages.
func TestForkOfSharedChildAtHeadUsesTheRecordedChain(t *testing.T) {
	w := newWS(t)
	path := sharedChildSeed(t, w)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	mustCheckpointWith(t, w, "app", "child", "c1", CheckpointOptions{})
	sources := recordChainSources(t)
	if _, err := w.Fork("app", "child", "grandchild", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	assertSources(t, "fork at a shared child's head", sources(), "cache")
	gc := refOf(t, w, "app", "grandchild")
	if gc.Base == nil {
		t.Fatal("grandchild did not share")
	}
	assertHeadIsCheckout(t, w, "app", "grandchild", "fork", path)
}

// TestForkAfterOwnSnapshotFirstResolvesThenHits: a shared child that wrote
// its own snapshot (its record's foreign run is empty) is forked again at
// head; the grandchild shares the child's lineage, its first checkpoint
// resolves (its checkout's sidecar records no chain) and its second takes
// the record, whose foreign run is now the child's keys.
func TestForkAfterOwnSnapshotFirstResolvesThenHits(t *testing.T) {
	w := newWS(t)
	path := sharedChildSeed(t, w)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	if res := mustCheckpointWith(t, w, "app", "child", "s", CheckpointOptions{Snapshot: true}); res.Kind != "snapshot" {
		t.Fatalf("s: kind %q, want snapshot", res.Kind)
	}
	if _, err := w.Fork("app", "child", "grandchild", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	child, gcRef := refOf(t, w, "app", "child"), refOf(t, w, "app", "grandchild")
	if gcRef.Base == nil || gcRef.Base.Lineage != child.Lineage {
		t.Fatalf("grandchild base %+v, want the child's lineage %s", gcRef.Base, child.Lineage)
	}
	gpath, err := w.Checkout("app", "grandchild")
	if err != nil {
		t.Fatal(err)
	}
	sources := recordChainSources(t)
	mustSQL(t, gpath, "INSERT INTO t (v) VALUES (randomblob(100));")
	mustCheckpointWith(t, w, "app", "grandchild", "g1", CheckpointOptions{})
	assertSources(t, "first grandchild checkpoint", sources(), "resolve")
	mustSQL(t, gpath, "INSERT INTO t (v) VALUES (randomblob(100));")
	if res := mustCheckpointWith(t, w, "app", "grandchild", "g2", CheckpointOptions{}); res.Kind != "segment" {
		t.Fatalf("g2: kind %q, want segment", res.Kind)
	}
	assertSources(t, "second grandchild checkpoint", sources(), "cache")
	members := assertCachedChainIsResolved(t, w, gpath, "app", "grandchild")
	if !strings.HasPrefix(members[0].Key, store.LineagePrefix(child.Lineage)) {
		t.Fatalf("grandchild's record does not begin on the child's lineage: %s", members[0].Key)
	}
	assertHeadIsCheckout(t, w, "app", "grandchild", "g2", gpath)
}

// TestPromoteAtHeadUsesTheRecordedChain: promote resolves the source's
// head; with a valid record on the source's checkout it takes the cache,
// and the promoted target reads the same bytes as a resolve would give.
func TestPromoteAtHeadUsesTheRecordedChain(t *testing.T) {
	w := newWS(t)
	path := sharedChildSeed(t, w)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	mustCheckpointWith(t, w, "app", "child", "c1", CheckpointOptions{})
	sources := recordChainSources(t)
	if _, err := w.PromoteWith("app", "child", "main", PromoteOptions{Force: true, NoBackup: true}); err != nil {
		t.Fatal(err)
	}
	assertSources(t, "promote of a shared child", sources(), "cache")
	assertHeadIsCheckout(t, w, "app", "main", "promote", path)
}

// TestRollbackToHeadUsesTheRecordedChain: a rollback to the head's own
// checkpoint resolves at the head and takes the cache; a rollback below
// the head still lists.
func TestRollbackToHeadUsesTheRecordedChain(t *testing.T) {
	w := newWS(t)
	path := chainCacheSeed(t, w)
	sources := recordChainSources(t)
	if _, err := w.RollbackWith("app", "main", "b", RollbackOptions{NoBackup: true}); err != nil {
		t.Fatal(err)
	}
	assertSources(t, "rollback to the head's checkpoint", sources(), "cache")
	// Rollback keeps every checkpoint at or below the target by name
	// rather than renaming the target to "rollback" (unlike promote, which
	// always resets its target's checkpoint map to {"promote": txid}), so
	// the target's own name ("b") still names the head.
	assertHeadIsCheckout(t, w, "app", "main", "b", path)

	sources = recordChainSources(t)
	if _, err := w.RollbackWith("app", "main", "a", RollbackOptions{NoBackup: true}); err != nil {
		t.Fatal(err)
	}
	// Below the head, RollbackWith calls Store.Chain directly (as Fork's
	// below-floor branch does; see TestForkAtHeadUsesTheRecordedChain's
	// "fork at an older checkpoint" case), which does not go through
	// headChain and so never calls observeChainSource.
	assertSources(t, "rollback below the head", sources())
}
