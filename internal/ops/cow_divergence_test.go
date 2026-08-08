// Divergence-floor tests for copy-on-write forks (Task 4): bounded
// materialization across a deep spine of nested forks, the shared child's
// own divergence-floor snapshot, and end-to-end parent/child divergence
// isolation. External ops_test package because these need session, which
// imports ops — see gc_chain_test.go's package doc comment.
package ops_test

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/session"
	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// flushInserts runs n rounds of (INSERT one row via the sqlite3 CLI, then
// Flush) against an open session, inserting val each time.
func flushInserts(t *testing.T, s *session.Session, n int, val string) {
	t.Helper()
	for i := 0; i < n; i++ {
		stmt := fmt.Sprintf("INSERT INTO t VALUES (%s);", val)
		if out, err := exec.Command("sqlite3", s.CheckoutPath(), stmt).CombinedOutput(); err != nil {
			t.Fatalf("INSERT %d of %q failed: %v: %s", i, val, err, out)
		}
		if _, err := s.Flush("", nil); err != nil {
			t.Fatalf("flush %d of %q: %v", i, val, err)
		}
	}
}

// TestDeepForkChainStaysBounded is the divergence-floor claim made
// falsifiable: build a spine of nested forks far deeper than
// ForkShareMaxDepth, each level writing a few segments, and assert that
// EVERY level's head still resolves through a bounded chain. Three
// mechanisms must cooperate for this to hold by construction:
//
//   - the fork-time floor (ops.Fork materializes a fresh snapshot when the
//     fork point's fully-resolved chain reaches ForkShareMaxDepth, resetting
//     the spine's depth to one),
//   - the session snapshot cadence (a level that keeps writing self-snapshots
//     after SnapshotEvery flushes, capping its own contribution), and
//   - Chain's divergence anchor (a child with its own covering snapshot
//     resolves wholly in-child).
//
// Bound justification: a resolved chain is at most the shared spine below
// the last materialized floor (< the fork floor's bound, by its own trigger
// condition — here Workspace.SnapshotEvery = 4, set below the way the
// daemon's SetSnapshotEvery sets it, so the fork floor tracks the session
// cadence) plus the head level's own members since its last snapshot
// (< SnapshotEvery by the cadence). SnapshotEvery + SnapshotEvery =
// 4 + 4 = 8 is therefore a safe ceiling a correct implementation can never
// exceed, REGARDLESS of how many levels deep the spine grows — which is
// exactly what makes 40 levels a falsifying probe rather than a tautology.
func TestDeepForkChainStaysBounded(t *testing.T) {
	testutil.RequireSQLite3(t)
	const (
		levels        = 40
		snapshotEvery = 4
		perLevel      = 2 // flushes (one insert each) written at every level
	)
	w := newWS(t)
	// Align the fork-time floor with the session cadence, exactly as the
	// daemon's SetSnapshotEvery does — this is what tightens the bound below
	// from ForkShareMaxDepth+SnapshotEvery (16+4) to SnapshotEvery*2 (4+4).
	w.SnapshotEvery = snapshotEvery
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}

	openAndWrite := func(branch, val string, initSchema bool) {
		t.Helper()
		// Materialize the checkout BEFORE opening the session: a fresh
		// materialize inside Open leaves cleanAtOpen false and the settling
		// flush then writes a full snapshot as the session's first flush,
		// which would self-anchor every level immediately and never grow a
		// shared spine at all. Pre-materializing arms the settling-flush
		// suppression (clean checkout, recorded head checksum), so each
		// level's flushes are SEGMENTS appended over the shared seam — the
		// exact shape whose growth the divergence floor and the fork-time
		// floor must jointly bound.
		if _, err := w.Checkout("app", branch); err != nil {
			t.Fatalf("checkout %s: %v", branch, err)
		}
		s, err := session.Open(context.Background(), session.Options{
			WS: w, DB: "app", Branch: branch, SnapshotEvery: snapshotEvery,
		})
		if err != nil {
			t.Fatalf("open session on %s: %v", branch, err)
		}
		defer s.Close()
		if initSchema {
			if out, err := exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (lvl INTEGER);").CombinedOutput(); err != nil {
				t.Fatalf("CREATE TABLE failed: %v: %s", err, out)
			}
		}
		flushInserts(t, s, perLevel, val)
	}

	branches := []string{"main"}
	openAndWrite("main", "0", true)
	prev := "main"
	for i := 1; i <= levels; i++ {
		name := fmt.Sprintf("f%d", i)
		if _, err := w.Fork("app", prev, name, "", 0, nil); err != nil {
			t.Fatalf("fork %s from %s: %v", name, prev, err)
		}
		openAndWrite(name, fmt.Sprintf("%d", i), false)
		branches = append(branches, name)
		prev = name
	}

	// Every level's head must resolve through a bounded, snapshot-anchored
	// chain — not just the deepest level: a bug that let one mid-spine
	// lineage's segments leak into every descendant would show up at the
	// level where it happens, possibly below the deepest.
	// Fork floor bound (Workspace.SnapshotEvery, set above) + the head
	// level's own members since its last snapshot (< SnapshotEvery).
	const bound = snapshotEvery + snapshotEvery
	maxSeen := 0
	seamCrossed := false
	for _, b := range branches {
		ref, _, err := w.Store.GetRef("app", b)
		if err != nil {
			t.Fatalf("GetRef %s: %v", b, err)
		}
		chain, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
		if err != nil {
			t.Fatalf("chain for %s@%d: %v", b, ref.HeadTXID, err)
		}
		if len(chain) > bound {
			t.Errorf("%s: resolved chain has %d members, bound is %d (fork floor + SnapshotEvery)", b, len(chain), bound)
		}
		if len(chain) == 0 || !chain[0].Snapshot {
			t.Errorf("%s: chain must start at a snapshot, got %+v", b, chain)
		}
		for _, m := range chain {
			if !strings.HasPrefix(m.Key, store.LineagePrefix(ref.Lineage)) {
				seamCrossed = true
			}
		}
		if len(chain) > maxSeen {
			maxSeen = len(chain)
		}
	}
	t.Logf("deepest resolved chain across %d levels: %d members (bound %d)", levels+1, maxSeen, bound)
	// Teeth check: the bound above only guards anything if the settling-flush
	// suppression actually armed and levels appended SEGMENTS over the shared
	// seam. If suppression silently broke, every level's first flush would be
	// its own settle snapshot, every resolved chain would live wholly in its
	// own lineage at 1-2 members, and the bound would hold vacuously. At
	// least one level's head chain must therefore cross the seam — contain a
	// member inherited from an ancestor lineage.
	if !seamCrossed {
		t.Fatal("suppression did not arm anywhere: no level's resolved chain contains an ancestor-lineage member — every level self-anchored on a settle snapshot, so the bound assertion is vacuous")
	}

	// Content correctness at the deepest level: a bounded-but-wrong chain
	// (e.g. one that dropped an ancestor's segments to stay short) must fail
	// here. Every level i inserted its own level number perLevel times, and
	// f40 inherits all of them.
	cpath, err := w.Checkout("app", fmt.Sprintf("f%d", levels))
	if err != nil {
		t.Fatal(err)
	}
	got, err := exec.Command("sqlite3", cpath, "SELECT lvl, COUNT(*) FROM t GROUP BY lvl ORDER BY lvl;").Output()
	if err != nil {
		t.Fatal(err)
	}
	var want strings.Builder
	for i := 0; i <= levels; i++ {
		fmt.Fprintf(&want, "%d|%d\n", i, perLevel)
	}
	if string(got) != want.String() {
		t.Fatalf("deepest level content mismatch:\ngot:\n%s\nwant:\n%s", got, want.String())
	}
}

// TestSharedChildSelfSnapshotsPastCadence proves the divergence floor and
// Chain's divergence anchor cooperate: a session on a SHARED fork (base
// pointer, zero own objects at birth) that flushes more than SnapshotEvery
// times past its fork point must end up with its OWN snapshot object in its
// OWN lineage at a txid above the fork point, and a read above that snapshot
// must resolve wholly in-child — no parent-lineage members in the chain.
func TestSharedChildSelfSnapshotsPastCadence(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (lvl INTEGER);").CombinedOutput(); err != nil {
		t.Fatalf("CREATE TABLE failed: %v: %s", err, out)
	}
	flushInserts(t, s, 1, "0")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	forkTXID, err := w.Fork("app", "main", "child", "", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	cref, _, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	if cref.Base == nil {
		t.Fatal("test precondition: fork of a short chain must SHARE (Base non-nil), or this test proves nothing about shared children")
	}

	// Pre-materialize the child's checkout so the settling-flush suppression
	// arms (see TestDeepForkChainStaysBounded's openAndWrite comment): the
	// child's flushes then start as SEGMENTS over the seam, and the snapshot
	// this test finds below is genuinely the divergence floor's — the
	// SnapshotEvery cadence tripping mid-run — not a settle snapshot that
	// would appear on the very first flush regardless.
	if _, err := w.Checkout("app", "child"); err != nil {
		t.Fatal(err)
	}
	cs, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "child", SnapshotEvery: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	// 6 flushes > SnapshotEvery=4: the cadence must have forced at least one
	// full self-snapshot somewhere in this run.
	flushInserts(t, cs, 6, "1")
	if err := cs.Close(); err != nil {
		t.Fatal(err)
	}

	keys, err := w.Store.B.List(store.LineagePrefix(cref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	var ownSnapshot bool
	for _, k := range keys {
		if m, ok := store.ParseMemberKey(k); ok && m.Snapshot {
			if m.MaxTXID <= forkTXID {
				t.Fatalf("child snapshot at txid %d is at or below the fork point %d — a shared child only ever writes above its fork", m.MaxTXID, forkTXID)
			}
			ownSnapshot = true
		}
	}
	if !ownSnapshot {
		t.Fatalf("divergence floor: child wrote 6 > SnapshotEvery=4 flushes past its fork but has no own snapshot; lineage keys: %v", keys)
	}

	// The anchor: a read at the child's head (above its own snapshot) must
	// resolve wholly within the child lineage — zero parent-lineage keys.
	head, _, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	chain, err := w.Store.Chain(head.Lineage, head.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	prefix := store.LineagePrefix(cref.Lineage)
	for _, m := range chain {
		if !strings.HasPrefix(m.Key, prefix) {
			t.Fatalf("read above the child's own snapshot must not touch the parent lineage, chain member %s is outside %s (chain: %+v)", m.Key, prefix, chain)
		}
	}
	if !chain[0].Snapshot {
		t.Fatalf("chain must start at the child's own snapshot, got %+v", chain[0])
	}
}

// TestSharedChildShortSessionsStayBounded is the divergence floor's
// cross-session teeth, and the test that FAILS without flush.go's
// divergence-floor seeding: the in-memory flush counter restarts at zero
// with every session, and the settling-flush suppression lets each new
// session on a clean checkout keep appending segments directly onto the
// branch's durable chain — so a shared child written through many SHORT
// sessions (each flushing fewer than SnapshotEvery times) would cross
// SnapshotEvery of OWN divergence without ever self-snapshotting, growing
// its resolved chain without bound (measured pre-fix: +2 members per
// 2-flush session, indefinitely). The floor requires the cadence to be a
// property of the BRANCH: seeded from the durable chain's trailing segment
// run, so the bound holds across session restarts exactly as within one
// session.
func TestSharedChildShortSessionsStayBounded(t *testing.T) {
	testutil.RequireSQLite3(t)
	const snapshotEvery = 4
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: snapshotEvery,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (lvl INTEGER);").CombinedOutput(); err != nil {
		t.Fatalf("CREATE TABLE failed: %v: %s", err, out)
	}
	flushInserts(t, s, 1, "0")
	if err := s.Close(); err != nil {
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
		t.Fatal("test precondition: this fork must SHARE (Base non-nil)")
	}

	// 6 sessions x 2 flushes = 12 segments of own divergence if nothing ever
	// snapshots — three times SnapshotEvery. Assert the resolved chain at
	// head after EVERY session, not just the last: the pre-fix failure mode
	// is monotonic growth, visible at whichever round first exceeds the
	// bound. The bound is exactly SnapshotEvery: a resolved chain is one
	// covering snapshot plus the trailing segment run, and the seeded
	// cadence caps that run at SnapshotEvery-1 segments at every flush
	// decision (the parent half contributes only members BELOW the covering
	// snapshot's txid — none, once resolution anchors — or its own trailing
	// segments, which the seed counts too).
	for round := 0; round < 6; round++ {
		// Pre-materialize so the settling-flush suppression arms each round
		// (see TestDeepForkChainStaysBounded's openAndWrite comment) — a
		// settle snapshot on every reopen would mask the counter reset this
		// test exists to catch.
		if _, err := w.Checkout("app", "child"); err != nil {
			t.Fatalf("round %d checkout: %v", round, err)
		}
		cs, err := session.Open(context.Background(), session.Options{
			WS: w, DB: "app", Branch: "child", SnapshotEvery: snapshotEvery,
		})
		if err != nil {
			t.Fatalf("round %d open: %v", round, err)
		}
		flushInserts(t, cs, 2, "1")
		if err := cs.Close(); err != nil {
			t.Fatalf("round %d close: %v", round, err)
		}
		ref, _, err := w.Store.GetRef("app", "child")
		if err != nil {
			t.Fatal(err)
		}
		chain, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
		if err != nil {
			t.Fatalf("round %d chain: %v", round, err)
		}
		if len(chain) > snapshotEvery {
			t.Fatalf("round %d: resolved chain has %d members, bound is SnapshotEvery=%d — the divergence floor must hold across session restarts", round, len(chain), snapshotEvery)
		}
		// Teeth check: this test only guards anything if the settling-flush
		// suppression actually armed, i.e. round 0's two flushes were
		// SEGMENTS over the seam. If suppression silently broke, the first
		// flush would write the child's OWN snapshot and the bound above
		// would hold vacuously at 1-2 members forever. After round 0 (2
		// flushes, seeded trailing run 0, both under the cadence) the
		// covering snapshot must therefore still be the PARENT's — the child
		// lineage must not have produced one yet.
		if round == 0 && strings.HasPrefix(chain[0].Key, store.LineagePrefix(cref.Lineage)) {
			t.Fatalf("suppression did not arm: round 0's covering snapshot %s is already in the child lineage — first flush was a settle snapshot, not a segment, so this test's bound is vacuous", chain[0].Key)
		}
	}

	// Crossing SnapshotEvery of own divergence must have produced an OWN
	// snapshot in the child's lineage (the floor itself, not just its
	// bounded side effect).
	keys, err := w.Store.B.List(store.LineagePrefix(cref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	var ownSnapshot bool
	for _, k := range keys {
		if m, ok := store.ParseMemberKey(k); ok && m.Snapshot {
			ownSnapshot = true
		}
	}
	if !ownSnapshot {
		t.Fatalf("child wrote 12 flushes of own divergence across short sessions but never self-snapshotted; lineage keys: %v", keys)
	}
}

// TestSharedForkDivergenceIsolation is the user-visible promise of
// copy-on-write forks, end to end (deferred finding F2 from Task 3): after a
// SHARED fork, parent and child sessions that each write different rows must
// see only their own writes plus the common fork-point state — in BOTH
// directions. A resolution bug that served the parent's post-fork bytes to
// the child (or vice versa) fails here even if every chain stays bounded.
func TestSharedForkDivergenceIsolation(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v TEXT); INSERT INTO t VALUES ('base');").CombinedOutput(); err != nil {
		t.Fatalf("seed: %v: %s", err, out)
	}
	if _, err := s.Flush("", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
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
		t.Fatal("test precondition: this fork must SHARE (Base non-nil) — a materialized fork's isolation is already covered by TestForkIsIndependentOfParent")
	}

	// Parent diverges: inserts A.
	ps, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", ps.CheckoutPath(), "INSERT INTO t VALUES ('A');").CombinedOutput(); err != nil {
		t.Fatalf("parent insert: %v: %s", err, out)
	}
	if _, err := ps.Flush("", nil); err != nil {
		t.Fatal(err)
	}
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}

	// Child diverges: inserts B.
	csess, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "child", SnapshotEvery: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", csess.CheckoutPath(), "INSERT INTO t VALUES ('B');").CombinedOutput(); err != nil {
		t.Fatalf("child insert: %v: %s", err, out)
	}
	if _, err := csess.Flush("", nil); err != nil {
		t.Fatal(err)
	}
	if err := csess.Close(); err != nil {
		t.Fatal(err)
	}

	dump := func(branch string) string {
		t.Helper()
		p, err := w.Checkout("app", branch)
		if err != nil {
			t.Fatalf("checkout %s: %v", branch, err)
		}
		out, err := exec.Command("sqlite3", p, "SELECT v FROM t ORDER BY v;").Output()
		if err != nil {
			t.Fatalf("dump %s: %v", branch, err)
		}
		return string(out)
	}

	if got, want := dump("main"), "A\nbase\n"; got != want {
		t.Errorf("parent dump = %q, want %q (must contain A and the fork-point state, and must NOT contain the child's B)", got, want)
	}
	if got, want := dump("child"), "B\nbase\n"; got != want {
		t.Errorf("child dump = %q, want %q (must contain B and the fork-point state, and must NOT contain the parent's post-fork A)", got, want)
	}
}
