// Fork-time snapshot-floor tests. External package (ops_test) because
// building a deep resolved chain requires session's flush cadence (segments,
// not the CLI Checkpoint's always-a-snapshot), and session imports ops — see
// gc_chain_test.go's package doc comment for the cycle rationale.
package ops_test

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"testing"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/session"
	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// TestForkTimeFloorMaterializesDeepChain: a fork whose resolved base chain
// already has >= ops.ForkShareMaxDepth members must NOT share — it
// materializes a fresh snapshot floor in its own lineage (Base nil, no
// base.json), so no fork spine's resolved chain ever exceeds the bound. A
// control fork taken while the chain was still shallow shares as usual.
func TestForkTimeFloorMaterializesDeepChain(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// SnapshotEvery far above the flush count: after the settling flush's
	// forced full snapshot, every flush appends a segment, growing the
	// resolved chain by one per flush.
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if out, err := exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("CREATE TABLE: %v: %s", err, out)
	}
	// Settling flush: the chain's one snapshot.
	if _, err := s.Flush("", nil); err != nil {
		t.Fatal(err)
	}

	// Control: fork while the chain is shallow — must share.
	if _, err := w.Fork("app", "main", "shallow", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	shallowRef, _, err := w.Store.GetRef("app", "shallow")
	if err != nil {
		t.Fatal(err)
	}
	if shallowRef.Base == nil {
		t.Fatal("control fork of a shallow chain must share (Base set)")
	}

	// Grow the chain to >= ForkShareMaxDepth members (snapshot + segments).
	for i := 0; i < ops.ForkShareMaxDepth; i++ {
		if out, err := exec.Command("sqlite3", s.CheckoutPath(),
			fmt.Sprintf("INSERT INTO t VALUES (%d);", i)).CombinedOutput(); err != nil {
			t.Fatalf("INSERT %d: %v: %s", i, err, out)
		}
		if _, err := s.Flush("", nil); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	chain, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) < ops.ForkShareMaxDepth {
		t.Fatalf("test precondition: resolved chain has %d members, want >= %d",
			len(chain), ops.ForkShareMaxDepth)
	}

	// The floor: this fork must materialize, not share.
	if _, err := w.Fork("app", "main", "deep", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	dref, _, err := w.Store.GetRef("app", "deep")
	if err != nil {
		t.Fatal(err)
	}
	if dref.Base != nil {
		t.Fatalf("deep fork must materialize (Base nil), got Base %+v", dref.Base)
	}
	if _, _, err := w.Store.B.Get(store.BaseKey(dref.Lineage)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("deep fork must write no base.json, Get err = %v", err)
	}
	keys, err := w.Store.B.List(store.LineagePrefix(dref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	sawSnapshot := false
	for _, k := range keys {
		if m, ok := store.ParseMemberKey(k); ok && m.Snapshot {
			sawSnapshot = true
		}
	}
	if !sawSnapshot {
		t.Fatalf("deep fork must own a snapshot floor, lineage holds %v", keys)
	}
	// And its resolved chain is exactly that floor: depth reset to one.
	dchain, err := w.Store.Chain(dref.Lineage, dref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(dchain) != 1 || !dchain[0].Snapshot {
		t.Fatalf("deep fork's resolved chain = %d members (snapshot=%v), want exactly its own snapshot",
			len(dchain), len(dchain) > 0 && dchain[0].Snapshot)
	}

	// Functional: the materialized child reads the full state.
	cpath, err := w.Checkout("app", "deep")
	if err != nil {
		t.Fatal(err)
	}
	got, err := exec.Command("sqlite3", cpath, "SELECT count(*) FROM t;").Output()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != fmt.Sprintf("%d\n", ops.ForkShareMaxDepth) {
		t.Fatalf("deep fork content = %q, want %d rows", got, ops.ForkShareMaxDepth)
	}
}

// TestForkFloorTracksConfiguredSnapshotEvery: the fork-time floor's bound
// must be Workspace.SnapshotEvery when set, not the hardcoded default. A
// parent whose resolved chain has >= 4 members forks MATERIALIZED under
// Workspace.SnapshotEvery=4 (its own snapshot, Base nil, no base.json),
// while the SAME parent under the default (SnapshotEvery 0 -> 16) SHARES.
// This is what keeps a daemon configured with a small session cadence from
// minting shared forks whose resolved chains exceed that cadence.
func TestForkFloorTracksConfiguredSnapshotEvery(t *testing.T) {
	testutil.RequireSQLite3(t)
	const cadence = 4
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// Grow main's resolved chain to >= cadence members but well under the
	// default 16: settling snapshot + `cadence` segments = cadence+1.
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if out, err := exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("CREATE TABLE: %v: %s", err, out)
	}
	if _, err := s.Flush("", nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < cadence; i++ {
		if out, err := exec.Command("sqlite3", s.CheckoutPath(),
			fmt.Sprintf("INSERT INTO t VALUES (%d);", i)).CombinedOutput(); err != nil {
			t.Fatalf("INSERT %d: %v: %s", i, err, out)
		}
		if _, err := s.Flush("", nil); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	chain, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) < cadence || len(chain) >= ops.ForkShareMaxDepth {
		t.Fatalf("test precondition: resolved chain has %d members, want >= %d and < %d",
			len(chain), cadence, ops.ForkShareMaxDepth)
	}

	// Control: SnapshotEvery 0 defaults to ForkShareMaxDepth — this chain is
	// under it, so the fork SHARES exactly as before the field existed.
	if _, err := w.Fork("app", "main", "under-default", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	uref, _, err := w.Store.GetRef("app", "under-default")
	if err != nil {
		t.Fatal(err)
	}
	if uref.Base == nil {
		t.Fatal("fork under the default bound must share (Base set)")
	}

	// The configured bound: the same parent, forked with SnapshotEvery=4,
	// trips the floor and MATERIALIZES.
	w.SnapshotEvery = cadence
	if _, err := w.Fork("app", "main", "at-cadence", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	cref, _, err := w.Store.GetRef("app", "at-cadence")
	if err != nil {
		t.Fatal(err)
	}
	if cref.Base != nil {
		t.Fatalf("fork at the configured cadence must materialize (Base nil), got Base %+v", cref.Base)
	}
	if _, _, err := w.Store.B.Get(store.BaseKey(cref.Lineage)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("materialized fork must write no base.json, Get err = %v", err)
	}
	cchain, err := w.Store.Chain(cref.Lineage, cref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(cchain) != 1 || !cchain[0].Snapshot {
		t.Fatalf("materialized fork's resolved chain = %d members, want exactly its own snapshot", len(cchain))
	}

	// Functional: the materialized child reads the full state.
	cpath, err := w.Checkout("app", "at-cadence")
	if err != nil {
		t.Fatal(err)
	}
	got, err := exec.Command("sqlite3", cpath, "SELECT count(*) FROM t;").Output()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != fmt.Sprintf("%d\n", cadence) {
		t.Fatalf("materialized fork content = %q, want %d rows", got, cadence)
	}
}
