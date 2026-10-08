package ops

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
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
