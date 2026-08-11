package store

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// cpEqual compares only TXID/Epoch, the fields most existing tests care
// about — Checkpoint now carries a Meta map, which makes the struct
// incomparable with ==/!=.
func cpEqual(a, b Checkpoint) bool {
	return a.TXID == b.TXID && a.Epoch == b.Epoch
}

func newStore(t *testing.T) *Store {
	t.Helper()
	b, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &Store{B: b}
}

func TestManifestLifecycle(t *testing.T) {
	s := newStore(t)
	if err := s.CheckManifest(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound before init, got %v", err)
	}
	if err := s.InitManifest(); err != nil {
		t.Fatal(err)
	}
	if err := s.InitManifest(); err == nil {
		t.Fatal("double init must fail")
	}
	if err := s.CheckManifest(); err != nil {
		t.Fatal(err)
	}
}

func TestRefRoundTripAndCAS(t *testing.T) {
	s := newStore(t)
	r := Ref{Schema: 1, Lineage: NewLineageID(), Epoch: 1, HeadTXID: 1}
	r.SetCheckpoint("init", Checkpoint{TXID: 1, Epoch: 1})
	etag, err := s.PutRef("app", "main", r, "")
	if err != nil {
		t.Fatal(err)
	}
	got, gotEtag, err := s.GetRef("app", "main")
	if err != nil || gotEtag != etag {
		t.Fatalf("get: %v", err)
	}
	if got.Lineage != r.Lineage || !cpEqual(got.Checkpoints["init"], Checkpoint{TXID: 1, Epoch: 1}) {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	got.HeadTXID = 2
	if _, err := s.PutRef("app", "main", got, "stale"); !errors.Is(err, ErrCAS) {
		t.Fatalf("want ErrCAS, got %v", err)
	}
	if _, err := s.PutRef("app", "main", got, etag); err != nil {
		t.Fatal(err)
	}
}

func TestListRefs(t *testing.T) {
	s := newStore(t)
	for _, br := range []string{"main", "attempt-1"} {
		r := Ref{Schema: 1, Lineage: NewLineageID(), Epoch: 1, HeadTXID: 1}
		if _, err := s.PutRef("app", br, r, ""); err != nil {
			t.Fatal(err)
		}
	}
	m, err := s.ListRefs()
	if err != nil || len(m["app"]) != 2 || m["app"][0] != "attempt-1" {
		t.Fatalf("m=%v err=%v", m, err)
	}
}

func TestValidateName(t *testing.T) {
	for _, ok := range []string{"app", "attempt-1", "v1.2_x"} {
		if err := ValidateName(ok); err != nil {
			t.Errorf("%q should be valid: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "App", "a/b", "a b", strings.Repeat("x", 129),
		".", "..", "a..b", "..a", "a..", "a...b"} {
		if err := ValidateName(bad); err == nil {
			t.Errorf("%q should be invalid", bad)
		}
	}
}

func TestKeys(t *testing.T) {
	if k := SnapshotKey("abc", 2, 255); k != "data/abc/2/snapshot-00000000000000ff.ltx" {
		t.Fatalf("k=%s", k)
	}
	if id := NewLineageID(); len(id) != 32 || id == NewLineageID() {
		t.Fatalf("lineage id: %s", id)
	}
}

func TestRefV2RoundTrip(t *testing.T) {
	s := newStore(t)
	r := Ref{Schema: RefSchema, Lineage: NewLineageID(), Epoch: 3, HeadTXID: 9, HeadEpoch: 3}
	r.SetCheckpoint("init", Checkpoint{TXID: 1, Epoch: 1})
	r.SetCheckpoint("v1", Checkpoint{TXID: 9, Epoch: 3})
	etag, err := s.PutRef("app", "main", r, "")
	if err != nil {
		t.Fatal(err)
	}
	got, gotEtag, err := s.GetRef("app", "main")
	if err != nil || gotEtag != etag {
		t.Fatalf("get: %v", err)
	}
	if !cpEqual(got.Checkpoints["init"], Checkpoint{TXID: 1, Epoch: 1}) {
		t.Errorf("init checkpoint = %+v", got.Checkpoints["init"])
	}
	if !cpEqual(got.Checkpoints["v1"], Checkpoint{TXID: 9, Epoch: 3}) {
		t.Errorf("v1 checkpoint = %+v", got.Checkpoints["v1"])
	}
	if got.HeadEpoch != 3 {
		t.Errorf("head epoch = %d", got.HeadEpoch)
	}
}

func TestGetRefUpgradesV1(t *testing.T) {
	s := newStore(t)
	// Hand-write a v1 ref exactly as Plan 2 wrote them.
	v1 := `{"schema":1,"lineage":"abc","epoch":2,"head_txid":7,` +
		`"checkpoints":{"init":1,"v1":7},"protected":true}`
	if _, err := s.B.PutIf(RefKey("app", "main"), []byte(v1), ""); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if got.Schema != RefSchema {
		t.Errorf("schema = %d, want %d", got.Schema, RefSchema)
	}
	// Every v1 checkpoint lived under the ref's epoch.
	if !cpEqual(got.Checkpoints["init"], Checkpoint{TXID: 1, Epoch: 2}) {
		t.Errorf("init = %+v, want txid 1 epoch 2", got.Checkpoints["init"])
	}
	if !cpEqual(got.Checkpoints["v1"], Checkpoint{TXID: 7, Epoch: 2}) {
		t.Errorf("v1 = %+v, want txid 7 epoch 2", got.Checkpoints["v1"])
	}
	if got.HeadEpoch != 2 {
		t.Errorf("head epoch = %d, want 2", got.HeadEpoch)
	}
	if !got.Protected || got.Lineage != "abc" || got.HeadTXID != 7 {
		t.Errorf("other fields lost: %+v", got)
	}
}

func TestGetRefRejectsNewerSchema(t *testing.T) {
	s := newStore(t)
	future := `{"schema":99,"lineage":"abc","epoch":1,"head_txid":1}`
	if _, err := s.B.PutIf(RefKey("app", "main"), []byte(future), ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.GetRef("app", "main"); err == nil {
		t.Fatal("a ref from a newer binary must be refused, not guessed at")
	}
}

func TestGetRefRejectsNullCheckpoint(t *testing.T) {
	s := newStore(t)
	// A checkpoint value of JSON null decodes as a no-op into both the
	// Checkpoint struct and the bare uint64 fallback, silently producing
	// {TXID:0} instead of surfacing the malformed ref.
	bad := `{"schema":2,"lineage":"abc","epoch":1,"head_txid":1,"checkpoints":{"v1":null}}`
	if _, err := s.B.PutIf(RefKey("app", "main"), []byte(bad), ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.GetRef("app", "main"); err == nil {
		t.Fatal("a null checkpoint value must error, not silently decode as {TXID:0}")
	}
}

func TestSetCheckpointAllocates(t *testing.T) {
	var r Ref
	r.SetCheckpoint("a", Checkpoint{TXID: 4, Epoch: 2})
	if !cpEqual(r.Checkpoints["a"], Checkpoint{TXID: 4, Epoch: 2}) {
		t.Fatalf("checkpoints = %+v", r.Checkpoints)
	}
}

func TestSegmentKeySortsByMaxTXID(t *testing.T) {
	a := SegmentKey("lin", 1, 2, 5)
	b := SegmentKey("lin", 1, 6, 9)
	if !(a < b) {
		t.Fatalf("segment keys must sort in apply order: %s !< %s", a, b)
	}
	m, ok := ParseMemberKey(a)
	if !ok || m.Snapshot || m.MinTXID != 2 || m.MaxTXID != 5 || m.Epoch != 1 {
		t.Fatalf("round trip failed: %+v ok=%v", m, ok)
	}
	sm, ok := ParseMemberKey(SnapshotKey("lin", 3, 7))
	if !ok || !sm.Snapshot || sm.MaxTXID != 7 || sm.Epoch != 3 {
		t.Fatalf("snapshot round trip failed: %+v ok=%v", sm, ok)
	}
	if _, ok := ParseMemberKey("data/lin/1/not-a-member.txt"); ok {
		t.Fatal("a non-member key must not parse")
	}
}

func TestChainPicksNewestSnapshotThenSegments(t *testing.T) {
	s := newStore(t)
	put := func(k string) {
		if err := s.B.Put(k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	put(SnapshotKey("lin", 1, 1))
	put(SegmentKey("lin", 1, 2, 3))
	put(SegmentKey("lin", 1, 4, 5))
	put(SnapshotKey("lin", 2, 6)) // a later full snapshot, different epoch
	put(SegmentKey("lin", 2, 7, 8))

	// Target after the second snapshot: chain starts there, not at txid 1.
	got, err := s.Chain("lin", 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !got[0].Snapshot || got[0].MaxTXID != 6 || got[1].MaxTXID != 8 {
		t.Fatalf("chain = %+v", got)
	}

	// Target in the middle of the first run: snapshot 1 plus one segment.
	got, err = s.Chain("lin", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !got[0].Snapshot || got[0].MaxTXID != 1 || got[1].MaxTXID != 3 {
		t.Fatalf("chain = %+v", got)
	}

	// Exactly on a snapshot: just that snapshot.
	got, err = s.Chain("lin", 6)
	if err != nil || len(got) != 1 || !got[0].Snapshot {
		t.Fatalf("chain = %+v err=%v", got, err)
	}
}

func TestChainRefusesAHole(t *testing.T) {
	s := newStore(t)
	if err := s.B.Put(SnapshotKey("lin", 1, 1), []byte("x")); err != nil {
		t.Fatal(err)
	}
	// 2..3 present, 4..5 missing, 6..7 present: a hole before the target.
	if err := s.B.Put(SegmentKey("lin", 1, 2, 3), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.B.Put(SegmentKey("lin", 1, 6, 7), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Chain("lin", 7); err == nil {
		t.Fatal("a hole in the chain must be an error")
	}
}

func TestChainRefusesWhenNoSnapshotCoversTarget(t *testing.T) {
	s := newStore(t)
	if err := s.B.Put(SegmentKey("lin", 1, 2, 3), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Chain("lin", 3); err == nil {
		t.Fatal("a chain with no base snapshot must be an error")
	}
}

// TestChainPrefersTheHigherEpochOnSameTXID pins the fencing guarantee on the
// read path: when a fenced writer's orphan shares a TXID with the live
// object, the chain must always resolve to the live (higher-epoch) one.
//
// The pre-fix implementation sorted only by TXID and picked whichever member
// map iteration happened to place last, so a single run could pass by luck.
// Rebuilding the store and re-resolving many times makes a non-deterministic
// implementation fail reliably.
func TestChainPrefersTheHigherEpochOnSameTXID(t *testing.T) {
	for i := 0; i < 50; i++ {
		s := newStore(t)
		// Same txid 5 under a fenced epoch 2 and the live epoch 3.
		if err := s.B.Put(SnapshotKey("lin", 2, 5), []byte("fenced")); err != nil {
			t.Fatal(err)
		}
		if err := s.B.Put(SnapshotKey("lin", 3, 5), []byte("live")); err != nil {
			t.Fatal(err)
		}
		got, err := s.Chain("lin", 5)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 {
			t.Fatalf("chain = %+v, want a single snapshot", got)
		}
		if got[0].Epoch != 3 {
			t.Fatalf("iteration %d: chain resolved to epoch %d (the fenced writer's object), want 3",
				i, got[0].Epoch)
		}
	}
}

// TestChainPrefersTheHigherEpochForSegments is the same guarantee for an
// incremental segment covering an identical range under two epochs.
func TestChainPrefersTheHigherEpochForSegments(t *testing.T) {
	for i := 0; i < 50; i++ {
		s := newStore(t)
		if err := s.B.Put(SnapshotKey("lin", 3, 1), []byte("base")); err != nil {
			t.Fatal(err)
		}
		if err := s.B.Put(SegmentKey("lin", 2, 2, 4), []byte("fenced")); err != nil {
			t.Fatal(err)
		}
		if err := s.B.Put(SegmentKey("lin", 3, 2, 4), []byte("live")); err != nil {
			t.Fatal(err)
		}
		got, err := s.Chain("lin", 4)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 {
			t.Fatalf("chain = %+v, want snapshot + one segment", got)
		}
		if got[1].Epoch != 3 {
			t.Fatalf("iteration %d: segment resolved to epoch %d (the fenced writer's), want 3",
				i, got[1].Epoch)
		}
	}
}

// TestChainPrefersALiveSegmentOverAFencedSnapshotAtTheSameTXID is the
// regression test for a SILENT DATA-LOSS bug in resolution: a fenced writer's
// snapshot at txid T beat the live writer's segment at T, so a committed
// transaction disappeared with no error and no checksum failure.
//
// The real shape: session A holds epoch 2 and flushes a SNAPSHOT at H+1; its
// terminal PutRef fails with an ambiguous non-CAS error, so flush deliberately
// leaves the uploaded object behind, and A dies. Session B acquires the lease
// (epoch 3), the settling-flush suppression clears forceSnapshot, so B's first
// real flush writes a SEGMENT at that same H+1 and wins the ref CAS. Both
// objects now sit at txid H+1 under different epochs.
//
// Pre-fix, keepHighestEpoch keyed on {MinTXID, MaxTXID} and ran SEPARATELY
// over the snapshots and the segments: A's snapshot at H+1 keys as
// {Min=0, Max=H+1} and B's single-txid segment at H+1 keys as
// {Min=H+1, Max=H+1}, so they shared neither a key nor a slice and could
// never collide. resolveSelf then anchored on "newest snapshot with
// MaxTXID <= target" — A's fenced snapshot — and, because its MaxTXID equals
// the target, returned it ALONE. B's row was gone.
//
// Post-fix the collapse runs once over the combined set keyed on MaxTXID, so
// the epoch-3 segment supersedes the epoch-2 snapshot and the chain anchors on
// the real snapshot at H instead. Asserted on exact keys, so there is no
// ambiguity about which object was chosen.
func TestChainPrefersALiveSegmentOverAFencedSnapshotAtTheSameTXID(t *testing.T) {
	const head = 4 // H
	for i := 0; i < 50; i++ {
		s := newStore(t)
		putObj(t, s, SnapshotKey("lin", 2, head))          // the real, live snapshot at H
		putObj(t, s, SnapshotKey("lin", 2, head+1))        // session A's fenced orphan at H+1
		putObj(t, s, SegmentKey("lin", 3, head+1, head+1)) // session B's live segment at H+1

		got, err := s.Chain("lin", head+1)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		want := []string{
			SnapshotKey("lin", 2, head),
			SegmentKey("lin", 3, head+1, head+1),
		}
		gotKeys := memberKeys(got)
		if !slices.Equal(gotKeys, want) {
			t.Fatalf("iteration %d: chain = %v, want %v (the fenced epoch-2 snapshot at %d must never be chosen)",
				i, gotKeys, want, head+1)
		}
	}
}

// TestLiveSnapshotSupersedesAFencedSegmentAtTheSameTXID is the symmetric case
// of the bug above: the fenced writer left a SEGMENT at txid T and the live
// writer wrote a SNAPSHOT at T. The live snapshot must win — the fenced
// segment must not survive into the member set at all.
//
// This asserts on listMembers, not on Chain, deliberately. Chain cannot
// observe this direction of the collision: resolveSelf anchors on the newest
// snapshot with MaxTXID <= target, so a snapshot at T is preferred as the
// anchor whether or not a stale segment at T also survived, and a segment at
// T is skipped by the post-anchor walk (its MaxTXID <= the anchor's). The
// fenced segment is therefore inert for today's resolver but still WRONG to
// keep: it is a superseded writer's object sitting in the set every resolver
// walks, one resolver change away from being applied. Asserting at the member
// level is what gives the symmetric guarantee teeth.
func TestLiveSnapshotSupersedesAFencedSegmentAtTheSameTXID(t *testing.T) {
	const head = 4
	for i := 0; i < 50; i++ {
		s := newStore(t)
		putObj(t, s, SnapshotKey("lin", 2, head))          // the real, live snapshot at H
		putObj(t, s, SegmentKey("lin", 2, head+1, head+1)) // the fenced writer's segment at H+1
		putObj(t, s, SnapshotKey("lin", 3, head+1))        // the live writer's snapshot at H+1

		snapshots, segments, err := s.listMembers("lin")
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		wantSnapshots := []string{SnapshotKey("lin", 2, head), SnapshotKey("lin", 3, head+1)}
		if got := memberKeys(snapshots); !slices.Equal(got, wantSnapshots) {
			t.Fatalf("iteration %d: snapshots = %v, want %v", i, got, wantSnapshots)
		}
		if got := memberKeys(segments); len(got) != 0 {
			t.Fatalf("iteration %d: segments = %v, want none — the epoch-2 segment at %d was "+
				"superseded by the epoch-3 snapshot at the same txid and must not survive",
				i, got, head+1)
		}

		// And the chain itself is the live snapshot alone.
		got, err := s.Chain("lin", head+1)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		want := []string{SnapshotKey("lin", 3, head+1)}
		if gotKeys := memberKeys(got); !slices.Equal(gotKeys, want) {
			t.Fatalf("iteration %d: chain = %v, want %v", i, gotKeys, want)
		}
	}
}

// TestAMultiTXIDSegmentNeverEvictsASnapshotAtTheSameMaxTXID pins the
// deliberate limit on the MaxTXID collapse above.
//
// Collapsing a snapshot and a segment at the same MaxTXID is justified by one
// premise: every segment this binary's writers produce is SINGLE-TXID (the
// sole production ltxio.EncodeSegment caller passes (txid, txid)), so a
// segment's MaxTXID identifies it as completely as its range does. A
// MULTI-txid segment is outside that premise — no writer here can make one, so
// it is a hand-written fixture or corruption — and its MaxTXID does NOT
// identify it. If the collapse applied to it anyway, a stray {Min=0, Max=T}
// segment at any epoch above the live one would EVICT the real snapshot at T
// (which keys as {Min=0, Max=T} too, but is a SNAPSHOT, so the old dedupe
// never compared them) and break
// the chain outright: a loud failure, but a whole branch bricked by an object
// that used to be inert. ops/gc_test.go's
// TestGCCompensatingRuleSweepsAtHeadOrphanRegardlessOfEpoch fabricates
// exactly that key.
//
// So multi-txid segments keep the original {MinTXID, MaxTXID} key and never
// collide with a snapshot — which is safe precisely because they ARE inert:
// resolveSelf prefers a snapshot as the anchor, and the post-anchor walk skips
// any segment whose MaxTXID is at or below the anchor's.
func TestAMultiTXIDSegmentNeverEvictsASnapshotAtTheSameMaxTXID(t *testing.T) {
	for i := 0; i < 50; i++ {
		s := newStore(t)
		putObj(t, s, SnapshotKey("lin", 1, 2))   // the live snapshot at txid 2
		putObj(t, s, SegmentKey("lin", 2, 0, 2)) // a bogus {Min=0, Max=2} segment, HIGHER epoch
		putObj(t, s, SegmentKey("lin", 1, 3, 3)) // the live segment at txid 3

		got, err := s.Chain("lin", 3)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		want := []string{SnapshotKey("lin", 1, 2), SegmentKey("lin", 1, 3, 3)}
		if gotKeys := memberKeys(got); !slices.Equal(gotKeys, want) {
			t.Fatalf("iteration %d: chain = %v, want %v — a multi-txid segment must never "+
				"evict the snapshot at its MaxTXID", i, gotKeys, want)
		}
	}
}

// memberKeys projects a resolved chain to its object keys, so a chain
// assertion names the exact objects rather than a shape.
func memberKeys(members []ChainMember) []string {
	keys := make([]string, 0, len(members))
	for _, m := range members {
		keys = append(keys, m.Key)
	}
	return keys
}

func TestRefTTLFieldsRoundTripAndOldRefsDecode(t *testing.T) {
	s := newStore(t)
	if err := s.InitManifest(); err != nil {
		t.Fatal(err)
	}
	r := Ref{Schema: 2, Lineage: "lin1", Epoch: 1, HeadTXID: 3, HeadEpoch: 1}
	r.TTL = "2h"
	r.Touch(time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC))
	etag, err := s.PutRef("app", "b", r, "")
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := s.GetRef("app", "b")
	if err != nil {
		t.Fatal(err)
	}
	if got.TTL != "2h" || got.TouchedAt != "2026-08-05T12:00:00Z" {
		t.Fatalf("TTL fields did not round-trip: %+v", got)
	}
	// A ref written without the new fields (a Plan-7 ref) must decode with
	// them empty — no TTL means never reaped.
	old := Ref{Schema: 2, Lineage: "lin2", Epoch: 1, HeadTXID: 1, HeadEpoch: 1}
	if _, err := s.PutRef("app", "old", old, ""); err != nil {
		t.Fatal(err)
	}
	gotOld, _, err := s.GetRef("app", "old")
	if err != nil {
		t.Fatal(err)
	}
	if gotOld.TTL != "" || gotOld.TouchedAt != "" || gotOld.Reaping {
		t.Fatalf("plain ref grew TTL state: %+v", gotOld)
	}
	_ = etag
}

// TestRefMetaAndCheckpointFieldsRoundTripAndOldRefsDecode mirrors
// TestRefTTLFieldsRoundTripAndOldRefsDecode for the metadata fields added
// alongside list-databases/metadata (Milestone 3 Task 1): Ref.Meta and
// per-checkpoint CreatedAt/Meta. Same no-schema-bump contract: these are new
// omitempty fields, so an old ref (or an old checkpoint sub-object) must
// decode with them simply absent, not error.
func TestRefMetaAndCheckpointFieldsRoundTripAndOldRefsDecode(t *testing.T) {
	s := newStore(t)
	if err := s.InitManifest(); err != nil {
		t.Fatal(err)
	}
	r := Ref{Schema: 2, Lineage: "lin1", Epoch: 1, HeadTXID: 3, HeadEpoch: 1}
	r.Meta = map[string]string{"eval_run": "42", "git_sha": "abc123"}
	r.SetCheckpoint("cp1", Checkpoint{
		TXID: 3, Epoch: 1,
		CreatedAt: "2026-08-06T12:00:00Z",
		Meta:      map[string]string{"agent": "claude"},
	})
	if _, err := s.PutRef("app", "b", r, ""); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.GetRef("app", "b")
	if err != nil {
		t.Fatal(err)
	}
	if got.Meta["eval_run"] != "42" || got.Meta["git_sha"] != "abc123" {
		t.Fatalf("Ref.Meta did not round-trip: %+v", got.Meta)
	}
	cp, ok := got.Checkpoints["cp1"]
	if !ok {
		t.Fatal("checkpoint cp1 missing after round trip")
	}
	if cp.CreatedAt != "2026-08-06T12:00:00Z" {
		t.Fatalf("checkpoint CreatedAt did not round-trip: %+v", cp)
	}
	if cp.Meta["agent"] != "claude" {
		t.Fatalf("checkpoint Meta did not round-trip: %+v", cp.Meta)
	}

	// A ref written without Meta/CreatedAt (an old ref) must decode with
	// them empty/nil — no metadata means none was ever recorded, not a
	// decode error.
	old := Ref{Schema: 2, Lineage: "lin2", Epoch: 1, HeadTXID: 1, HeadEpoch: 1}
	old.SetCheckpoint("init", Checkpoint{TXID: 1, Epoch: 1})
	if _, err := s.PutRef("app", "old", old, ""); err != nil {
		t.Fatal(err)
	}
	gotOld, _, err := s.GetRef("app", "old")
	if err != nil {
		t.Fatal(err)
	}
	if gotOld.Meta != nil {
		t.Fatalf("plain ref grew Meta: %+v", gotOld.Meta)
	}
	oldCP, ok := gotOld.Checkpoints["init"]
	if !ok {
		t.Fatal("checkpoint init missing after round trip")
	}
	if oldCP.CreatedAt != "" || oldCP.Meta != nil {
		t.Fatalf("plain checkpoint grew CreatedAt/Meta: %+v", oldCP)
	}

	// Decode raw bytes shaped exactly like a ref written by a binary that
	// predates this field (no "meta" key on the ref OR on the checkpoint
	// sub-object at all — not even as an explicit null) — the on-disk
	// tolerant path, not just a Go zero-value round trip through PutRef.
	raw := []byte(`{"schema":2,"lineage":"lin3","epoch":1,"head_txid":1,"head_epoch":1,` +
		`"checkpoints":{"init":{"txid":1,"epoch":1}}}`)
	decoded, err := decodeRef(raw)
	if err != nil {
		t.Fatalf("decodeRef of an old-shaped ref failed: %v", err)
	}
	if decoded.Meta != nil {
		t.Fatalf("decodeRef fabricated Meta for an old-shaped ref: %+v", decoded.Meta)
	}
	rawCP, ok := decoded.Checkpoints["init"]
	if !ok {
		t.Fatal("decodeRef lost the old-shaped ref's checkpoint")
	}
	if rawCP.CreatedAt != "" || rawCP.Meta != nil {
		t.Fatalf("decodeRef fabricated checkpoint CreatedAt/Meta for an old-shaped ref: %+v", rawCP)
	}

	// A v1 (pre-checkpoint-epoch) ref, whose checkpoints are bare numbers
	// rather than objects, must still decode cleanly with the new fields
	// simply absent.
	v1raw := []byte(`{"schema":1,"lineage":"lin4","epoch":1,"head_txid":1,` +
		`"checkpoints":{"init":1}}`)
	v1decoded, err := decodeRef(v1raw)
	if err != nil {
		t.Fatalf("decodeRef of a v1 ref failed: %v", err)
	}
	v1cp, ok := v1decoded.Checkpoints["init"]
	if !ok {
		t.Fatal("decodeRef lost the v1 ref's checkpoint")
	}
	if v1cp.CreatedAt != "" || v1cp.Meta != nil {
		t.Fatalf("decodeRef fabricated CreatedAt/Meta for a v1 ref's checkpoint: %+v", v1cp)
	}
}

// TestBasePointerRoundTrip is copy-on-write Task 1: Ref.Base is a NEW field,
// distinct from the existing Parent breadcrumb, that survives a PutRef ->
// GetRef round trip with its values intact.
func TestBasePointerRoundTrip(t *testing.T) {
	s := newStore(t)
	r := Ref{
		Schema: RefSchema, Lineage: NewLineageID(), Epoch: 1, HeadTXID: 1, HeadEpoch: 1,
		Base: &BasePointer{Lineage: "L", TXID: 42},
	}
	r.SetCheckpoint("init", Checkpoint{TXID: 1, Epoch: 1})
	if _, err := s.PutRef("app", "based", r, ""); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.GetRef("app", "based")
	if err != nil {
		t.Fatal(err)
	}
	if got.Base == nil {
		t.Fatal("want Base set after round trip, got nil")
	}
	if got.Base.Lineage != "L" || got.Base.TXID != 42 {
		t.Fatalf("Base round trip mismatch: %+v", got.Base)
	}
}

// TestDecodeRefOldShapeHasNilBase is copy-on-write Task 1: a ref written by
// a binary that predates the base pointer (no "base" key at all) must
// decode with Base == nil, not a fabricated zero-value pointer — the
// tolerant-decode path, mirroring how TTL/Meta/Reaping are already handled.
func TestDecodeRefOldShapeHasNilBase(t *testing.T) {
	raw := []byte(`{"schema":2,"lineage":"lin5","epoch":1,"head_txid":1,"head_epoch":1,` +
		`"checkpoints":{"init":{"txid":1,"epoch":1}}}`)
	decoded, err := decodeRef(raw)
	if err != nil {
		t.Fatalf("decodeRef: %v", err)
	}
	if decoded.Base != nil {
		t.Fatalf("decodeRef fabricated Base for an old-shaped ref: %+v", decoded.Base)
	}
}

// TestEnsureLayoutV2 is copy-on-write Task 1: EnsureLayoutV2 CAS-bumps an
// existing v1 manifest to v2, and calling it again once already at v2 is a
// no-op, not an error.
func TestEnsureLayoutV2(t *testing.T) {
	s := newStore(t)
	// Simulate a pre-existing v1 store: write a v1 manifest directly, since
	// InitManifest now always writes v2 for brand-new stores.
	v1 := Manifest{LayoutVersion: 1, CreatedAt: time.Now().UTC().Format(time.RFC3339)}
	data, _ := json.Marshal(v1)
	if _, err := s.B.PutIf(manifestKey, data, ""); err != nil {
		t.Fatal(err)
	}

	if err := s.EnsureLayoutV2(); err != nil {
		t.Fatalf("EnsureLayoutV2: %v", err)
	}
	got, _, err := s.B.Get(manifestKey)
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err := json.Unmarshal(got, &m); err != nil {
		t.Fatal(err)
	}
	if m.LayoutVersion != LayoutVersion {
		t.Fatalf("want LayoutVersion %d after bump, got %d", LayoutVersion, m.LayoutVersion)
	}

	// Idempotent: calling it again on an already-v2 manifest must not error
	// or touch the manifest.
	if err := s.EnsureLayoutV2(); err != nil {
		t.Fatalf("EnsureLayoutV2 (already v2): %v", err)
	}
	got2, _, err := s.B.Get(manifestKey)
	if err != nil {
		t.Fatal(err)
	}
	if string(got2) != string(got) {
		t.Fatalf("EnsureLayoutV2 rewrote an already-v2 manifest: before %s, after %s", got, got2)
	}
}

// TestEnsureLayoutV2ConcurrentBump is copy-on-write Task 1: EnsureLayoutV2
// must be CAS-safe under concurrent callers racing to bump the same v1
// manifest — a concurrent bump to v2 is success for every caller, not a
// failure for whoever lost the CAS race.
func TestEnsureLayoutV2ConcurrentBump(t *testing.T) {
	s := newStore(t)
	v1 := Manifest{LayoutVersion: 1, CreatedAt: time.Now().UTC().Format(time.RFC3339)}
	data, _ := json.Marshal(v1)
	if _, err := s.B.PutIf(manifestKey, data, ""); err != nil {
		t.Fatal(err)
	}

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = s.EnsureLayoutV2()
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: EnsureLayoutV2: %v", i, err)
		}
	}

	got, _, err := s.B.Get(manifestKey)
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err := json.Unmarshal(got, &m); err != nil {
		t.Fatal(err)
	}
	if m.LayoutVersion != LayoutVersion {
		t.Fatalf("want LayoutVersion %d after concurrent bump, got %d", LayoutVersion, m.LayoutVersion)
	}
}

// TestCheckManifestRefusesNewerLayout is copy-on-write Task 1: a manifest
// whose LayoutVersion is newer than this binary supports must refuse the
// whole store. This is the exact mechanism a real pre-CoW (v1) binary hits
// against a v2 manifest once LayoutVersion is bumped to 2; the test writes
// LayoutVersion+1 rather than the literal value 2 so it keeps proving "a
// binary refuses a manifest newer than itself" regardless of what the
// current const is.
func TestCheckManifestRefusesNewerLayout(t *testing.T) {
	s := newStore(t)
	m := Manifest{LayoutVersion: LayoutVersion + 1, CreatedAt: time.Now().UTC().Format(time.RFC3339)}
	data, _ := json.Marshal(m)
	if _, err := s.B.PutIf(manifestKey, data, ""); err != nil {
		t.Fatal(err)
	}
	err := s.CheckManifest()
	if err == nil {
		t.Fatal("want error for a manifest newer than this binary supports")
	}
	if !strings.Contains(err.Error(), "newer than this binary supports") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- Copy-on-write base-following resolution (Task 2) ---

// putObj is a small helper for the base-following tests: it writes a
// throwaway body under a snapshot/segment key so Chain can resolve it.
func putObj(t *testing.T, s *Store, key string) {
	t.Helper()
	if err := s.B.Put(key, []byte("x")); err != nil {
		t.Fatal(err)
	}
}

// keysOf extracts the member keys of a chain for prefix assertions.
func keysOf(chain []ChainMember) []string {
	out := make([]string, len(chain))
	for i, m := range chain {
		out[i] = m.Key
	}
	return out
}

// TestLineageBaseAbsentIsNilNil: a lineage with no base object resolves to
// (nil, nil) — the non-shared, pre-CoW path.
func TestLineageBaseAbsentIsNilNil(t *testing.T) {
	s := newStore(t)
	b, err := s.lineageBase("nolin")
	if err != nil {
		t.Fatalf("lineageBase err = %v", err)
	}
	if b != nil {
		t.Fatalf("want nil base for a lineage with no base object, got %+v", b)
	}
}

// TestWriteLineageBaseImmutable: a lineage's base is create-only; a second
// write is refused with ErrCAS, and the first value round-trips.
func TestWriteLineageBaseImmutable(t *testing.T) {
	s := newStore(t)
	if err := s.WriteLineageBase("child", BasePointer{Lineage: "parent", TXID: 3}); err != nil {
		t.Fatalf("first WriteLineageBase err = %v", err)
	}
	got, err := s.lineageBase("child")
	if err != nil {
		t.Fatalf("lineageBase err = %v", err)
	}
	if got == nil || got.Lineage != "parent" || got.TXID != 3 {
		t.Fatalf("round-trip base = %+v", got)
	}
	err = s.WriteLineageBase("child", BasePointer{Lineage: "other", TXID: 9})
	if !errors.Is(err, ErrCAS) {
		t.Fatalf("second write must be ErrCAS, got %v", err)
	}
}

// TestWriteLineageBaseRejectsSelfBase: a base naming the lineage itself is
// refused before any write lands — Chain resolves a based lineage by
// recursing into its base, so a durable self-base would recurse forever.
func TestWriteLineageBaseRejectsSelfBase(t *testing.T) {
	s := newStore(t)
	if err := s.WriteLineageBase("child", BasePointer{Lineage: "child", TXID: 3}); err == nil {
		t.Fatal("self-base must be refused")
	}
	// Nothing durable may have landed: the refusal must happen before the write.
	b, err := s.lineageBase("child")
	if err != nil {
		t.Fatalf("lineageBase err = %v", err)
	}
	if b != nil {
		t.Fatalf("refused self-base must not persist, got %+v", b)
	}
}

// TestBaseKeyNotAChainMember: the base.json object lives under the lineage
// prefix but must never be parsed as a chain member (neither snapshot- nor
// segment-prefixed).
func TestBaseKeyNotAChainMember(t *testing.T) {
	if _, ok := ParseMemberKey(BaseKey("lin")); ok {
		t.Fatalf("BaseKey %q must not parse as a chain member", BaseKey("lin"))
	}
	// And it must live under the lineage prefix so it is swept with the lineage.
	if !strings.HasPrefix(BaseKey("lin"), LineagePrefix("lin")) {
		t.Fatalf("BaseKey %q must live under LineagePrefix %q", BaseKey("lin"), LineagePrefix("lin"))
	}
}

// TestBaseSpine: the ancestor walk GC's mark uses to find every base.json
// resolution would read. Nearest-first order, starting lineage excluded, an
// un-based lineage returns an empty spine, and a (hand-constructed — the
// writer can't produce one) cycle fails loudly instead of looping.
func TestBaseSpine(t *testing.T) {
	s := newStore(t)
	// No base at all.
	spine, err := s.BaseSpine("solo")
	if err != nil || len(spine) != 0 {
		t.Fatalf("BaseSpine(solo) = %v, %v; want empty, nil", spine, err)
	}
	// c -> b -> a, a un-based.
	if err := s.WriteLineageBase("b", BasePointer{Lineage: "a", TXID: 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteLineageBase("c", BasePointer{Lineage: "b", TXID: 5}); err != nil {
		t.Fatal(err)
	}
	spine, err = s.BaseSpine("c")
	if err != nil {
		t.Fatalf("BaseSpine(c) err = %v", err)
	}
	if !reflect.DeepEqual(spine, []string{"b", "a"}) {
		t.Fatalf("BaseSpine(c) = %v, want [b a]", spine)
	}
	spine, err = s.BaseSpine("b")
	if err != nil || !reflect.DeepEqual(spine, []string{"a"}) {
		t.Fatalf("BaseSpine(b) = %v, %v; want [a], nil", spine, err)
	}
	// Cycle x -> y -> x, constructed via raw backend puts (WriteLineageBase
	// cannot build one): the walk must error, not loop.
	for from, to := range map[string]string{"x": "y", "y": "x"} {
		data, _ := json.Marshal(BasePointer{Lineage: to, TXID: 1})
		if err := s.B.Put(BaseKey(from), data); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.BaseSpine("x"); err == nil {
		t.Fatal("BaseSpine over a cycle must error, not loop")
	}
}

// (a) A shared child reading target > base.TXID gets the parent's
// snapshot-anchored chain up to base.TXID, then the child's own segments in
// (base.TXID, target], in apply order and contiguous.
func TestChainBaseConcatenatesChildSegments(t *testing.T) {
	s := newStore(t)
	// Parent P: snapshot@1, segments to txid 3 (the fork point).
	putObj(t, s, SnapshotKey("parent", 1, 1))
	putObj(t, s, SegmentKey("parent", 1, 2, 2))
	putObj(t, s, SegmentKey("parent", 1, 3, 3))
	// Child C bases on P at txid 3, diverges with its own segments 4,5.
	if err := s.WriteLineageBase("child", BasePointer{Lineage: "parent", TXID: 3}); err != nil {
		t.Fatal(err)
	}
	putObj(t, s, SegmentKey("child", 1, 4, 4))
	putObj(t, s, SegmentKey("child", 1, 5, 5))

	got, err := s.Chain("child", 5)
	if err != nil {
		t.Fatalf("Chain err = %v", err)
	}
	want := []string{
		SnapshotKey("parent", 1, 1),
		SegmentKey("parent", 1, 2, 2),
		SegmentKey("parent", 1, 3, 3),
		SegmentKey("child", 1, 4, 4),
		SegmentKey("child", 1, 5, 5),
	}
	if !reflect.DeepEqual(keysOf(got), want) {
		t.Fatalf("chain keys =\n %v\nwant\n %v", keysOf(got), want)
	}
	if !got[0].Snapshot {
		t.Fatalf("resolved chain must still start at a snapshot: %+v", got[0])
	}
	// Contiguity 1..5 with no hole.
	prev := got[0].MaxTXID
	for _, m := range got[1:] {
		if m.MinTXID != prev+1 {
			t.Fatalf("hole at %+v (prevMax=%d)", m, prev)
		}
		prev = m.MaxTXID
	}
	if prev != 5 {
		t.Fatalf("chain reaches %d, want 5", prev)
	}
}

// A shared child that has grown its OWN snapshot (a divergence floor —
// ops.Checkpoint always writes one) must anchor resolution at that snapshot
// for any target it covers: the whole chain lives in the child lineage and
// the base is never consulted. Below the child snapshot, the seam path still
// applies unchanged.
func TestChainChildOwnSnapshotAnchorsResolution(t *testing.T) {
	s := newStore(t)
	// Parent P: snapshot@1 .. segment@3 (the fork point).
	putObj(t, s, SnapshotKey("parent", 1, 1))
	putObj(t, s, SegmentKey("parent", 1, 2, 2))
	putObj(t, s, SegmentKey("parent", 1, 3, 3))
	// Child C bases on P at 3, diverges with segment@4, then its own FULL
	// snapshot@5 (what ops.Checkpoint writes), then segment@6.
	if err := s.WriteLineageBase("child", BasePointer{Lineage: "parent", TXID: 3}); err != nil {
		t.Fatal(err)
	}
	putObj(t, s, SegmentKey("child", 1, 4, 4))
	putObj(t, s, SnapshotKey("child", 1, 5))
	putObj(t, s, SegmentKey("child", 1, 6, 6))

	// At/above the child snapshot: anchored at the child's own snapshot,
	// no parent member anywhere.
	got, err := s.Chain("child", 6)
	if err != nil {
		t.Fatalf("Chain err = %v", err)
	}
	want := []string{
		SnapshotKey("child", 1, 5),
		SegmentKey("child", 1, 6, 6),
	}
	if !reflect.DeepEqual(keysOf(got), want) {
		t.Fatalf("chain keys =\n %v\nwant\n %v", keysOf(got), want)
	}

	// Below the child snapshot: the seam path is unchanged — parent chain to
	// the fork point, then the child's segment.
	got, err = s.Chain("child", 4)
	if err != nil {
		t.Fatalf("Chain err = %v", err)
	}
	want = []string{
		SnapshotKey("parent", 1, 1),
		SegmentKey("parent", 1, 2, 2),
		SegmentKey("parent", 1, 3, 3),
		SegmentKey("child", 1, 4, 4),
	}
	if !reflect.DeepEqual(keysOf(got), want) {
		t.Fatalf("chain keys =\n %v\nwant\n %v", keysOf(got), want)
	}
}

// listErrBackend wraps a real Backend and fails List for one exact prefix
// with a fixed error, leaving every other operation (and every other
// prefix's List) untouched. It exists to prove Chain propagates transient
// backend failures instead of misreporting them.
type listErrBackend struct {
	Backend
	failPrefix string
	err        error
}

func (b listErrBackend) List(prefix string) ([]string, error) {
	if prefix == b.failPrefix {
		return nil, b.err
	}
	return b.Backend.List(prefix)
}

// A transient backend error while resolving a shared child's OWN half must
// propagate as-is — not be swallowed by the chainSelf-first divergence-floor
// probe and re-reported as a bogus seam "hole". Only the sentinel "no
// snapshot covers target" may fall through to the seam path.
func TestChainSharedChildBackendErrorPropagates(t *testing.T) {
	s := newStore(t)
	putObj(t, s, SnapshotKey("parent", 1, 1))
	putObj(t, s, SegmentKey("parent", 1, 2, 2))
	putObj(t, s, SegmentKey("parent", 1, 3, 3))
	if err := s.WriteLineageBase("child", BasePointer{Lineage: "parent", TXID: 3}); err != nil {
		t.Fatal(err)
	}
	putObj(t, s, SegmentKey("child", 1, 4, 4))

	boom := errors.New("transient backend failure")
	s.B = listErrBackend{Backend: s.B, failPrefix: LineagePrefix("child"), err: boom}

	_, err := s.Chain("child", 4)
	if !errors.Is(err, boom) {
		t.Fatalf("Chain must propagate the backend error, got %v", err)
	}
	if err != nil && strings.Contains(err.Error(), "hole") {
		t.Fatalf("backend error misreported as a chain hole: %v", err)
	}
}

// A base-pointer cycle between lineages — constructible only by corrupting
// the store directly (WriteLineageBase is create-only, rejects a self-base,
// and always points at an already-existing lineage) — must make Chain fail
// loudly with a cycle error, never recurse until the stack overflows.
// Mirrors TestBaseSpine's hand-built cycle for the other base walker.
func TestChainBasePointerCycleErrors(t *testing.T) {
	s := newStore(t)
	// x -> y -> x, via raw backend puts (bypassing WriteLineageBase's guards).
	for from, to := range map[string]string{"x": "y", "y": "x"} {
		data, err := json.Marshal(BasePointer{Lineage: to, TXID: 1})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.B.Put(BaseKey(from), data); err != nil {
			t.Fatal(err)
		}
	}
	// Above the fork point (target > base.TXID): the seam path recurses
	// x -> y -> x and must detect the revisit.
	if _, err := s.Chain("x", 5); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("Chain over a base cycle must return a cycle error, got %v", err)
	}
	// At/below the fork point (target <= base.TXID): the resolve-in-base
	// branch recurses the same way and must also be guarded.
	if _, err := s.Chain("x", 1); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("Chain at the fork point over a base cycle must return a cycle error, got %v", err)
	}
	// A hand-corrupted SELF-base (WriteLineageBase refuses to write one)
	// must be caught by the same guard.
	data, err := json.Marshal(BasePointer{Lineage: "z", TXID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.B.Put(BaseKey("z"), data); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Chain("z", 2); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("Chain over a self-base must return a cycle error, got %v", err)
	}
}

// (b) MUTATION test — the never-merge-across-lineages invariant. Child epoch 1
// and parent epoch 2 both carry an object covering the SAME txid range just
// past the fork seam. Chain MUST return the CHILD's bytes for the child's
// range. A naive resolver that unioned both lineages' members BEFORE
// keepHighestEpoch would pick the parent's higher-epoch object — so we assert
// on the returned member's lineage-prefixed Key and epoch, which a union
// implementation fails.
func TestChainNeverMergesAcrossLineages(t *testing.T) {
	s := newStore(t)
	// Parent P: snapshot@1 .. txid 3 (fork point), PLUS its own post-fork
	// divergence at txid 4 under a bumped epoch 2 (the parent's timeline).
	putObj(t, s, SnapshotKey("parent", 1, 1))
	putObj(t, s, SegmentKey("parent", 1, 2, 2))
	putObj(t, s, SegmentKey("parent", 1, 3, 3))
	putObj(t, s, SegmentKey("parent", 2, 4, 4)) // parent's OWN txid 4, epoch 2
	// Child C bases on P at 3, writes its OWN txid 4 under epoch 1.
	if err := s.WriteLineageBase("child", BasePointer{Lineage: "parent", TXID: 3}); err != nil {
		t.Fatal(err)
	}
	putObj(t, s, SegmentKey("child", 1, 4, 4)) // child's txid 4, epoch 1

	got, err := s.Chain("child", 4)
	if err != nil {
		t.Fatalf("Chain err = %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("chain len = %d, want 4: %v", len(got), keysOf(got))
	}
	last := got[len(got)-1]
	if last.MaxTXID != 4 {
		t.Fatalf("last member covers %d, want 4: %+v", last.MaxTXID, last)
	}
	// The load-bearing assertion: the txid-4 member is the CHILD's object
	// (epoch 1), NOT the parent's higher-epoch orphan. A union-then-
	// keepHighestEpoch resolver returns the parent's epoch-2 key here.
	if !strings.HasPrefix(last.Key, LineagePrefix("child")) {
		t.Fatalf("txid-4 member is not the child's object: %q (union bug picks parent's epoch-2 object)", last.Key)
	}
	if last.Epoch != 1 {
		t.Fatalf("txid-4 member epoch = %d, want child epoch 1", last.Epoch)
	}
	if last.Key != SegmentKey("child", 1, 4, 4) {
		t.Fatalf("txid-4 member = %q, want %q", last.Key, SegmentKey("child", 1, 4, 4))
	}
	// The base half members must all be the parent's (no child key sneaks in).
	for _, m := range got[:3] {
		if !strings.HasPrefix(m.Key, LineagePrefix("parent")) {
			t.Fatalf("base-half member is not the parent's: %q", m.Key)
		}
	}
}

// (c) Fenced-orphan: the parent bumps to epoch 2 AFTER the fork, leaving a
// higher-epoch object at the fork txid. Resolution within the parent half must
// pick the highest epoch (fencing) and the child still resolves — no
// regression.
func TestChainFencedParentOrphanAtForkPoint(t *testing.T) {
	s := newStore(t)
	putObj(t, s, SnapshotKey("parent", 1, 1))
	putObj(t, s, SegmentKey("parent", 1, 2, 2))
	putObj(t, s, SegmentKey("parent", 1, 3, 3)) // original fork-point object
	putObj(t, s, SegmentKey("parent", 2, 3, 3)) // parent bumped epoch, rewrote txid 3
	if err := s.WriteLineageBase("child", BasePointer{Lineage: "parent", TXID: 3}); err != nil {
		t.Fatal(err)
	}
	putObj(t, s, SegmentKey("child", 1, 4, 4))

	got, err := s.Chain("child", 4)
	if err != nil {
		t.Fatalf("Chain err = %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("chain len = %d, want 4: %v", len(got), keysOf(got))
	}
	// The fork-txid (3) member must be the higher-epoch (live) parent object.
	var forkMember *ChainMember
	for i := range got {
		if got[i].MaxTXID == 3 {
			forkMember = &got[i]
		}
	}
	if forkMember == nil || forkMember.Epoch != 2 {
		t.Fatalf("fork-point member must be parent epoch 2: %+v", forkMember)
	}
	if forkMember.Key != SegmentKey("parent", 2, 3, 3) {
		t.Fatalf("fork-point member = %q, want %q", forkMember.Key, SegmentKey("parent", 2, 3, 3))
	}
}

// (d) Fork-of-fork transitive: C bases on B at Tc=4, B bases on A at Tb=2,
// Tc > Tb. C at target=5 resolves A's snapshot + A's (.,2] + B's (2,4] segs +
// C's (4,5] segs, all contiguous, each member in its own lineage.
func TestChainForkOfForkTransitive(t *testing.T) {
	s := newStore(t)
	// A: snapshot@1, segment txid 2 (Tb).
	putObj(t, s, SnapshotKey("a", 1, 1))
	putObj(t, s, SegmentKey("a", 1, 2, 2))
	// B bases on A at 2, has its own txids 3,4 (Tc=4).
	if err := s.WriteLineageBase("b", BasePointer{Lineage: "a", TXID: 2}); err != nil {
		t.Fatal(err)
	}
	putObj(t, s, SegmentKey("b", 1, 3, 3))
	putObj(t, s, SegmentKey("b", 1, 4, 4))
	// C bases on B at 4, has its own txid 5.
	if err := s.WriteLineageBase("c", BasePointer{Lineage: "b", TXID: 4}); err != nil {
		t.Fatal(err)
	}
	putObj(t, s, SegmentKey("c", 1, 5, 5))

	got, err := s.Chain("c", 5)
	if err != nil {
		t.Fatalf("Chain err = %v", err)
	}
	want := []string{
		SnapshotKey("a", 1, 1),
		SegmentKey("a", 1, 2, 2),
		SegmentKey("b", 1, 3, 3),
		SegmentKey("b", 1, 4, 4),
		SegmentKey("c", 1, 5, 5),
	}
	if !reflect.DeepEqual(keysOf(got), want) {
		t.Fatalf("chain keys =\n %v\nwant\n %v", keysOf(got), want)
	}
	if !got[0].Snapshot {
		t.Fatalf("chain must start at A's snapshot: %+v", got[0])
	}
}

// (e) target <= base.TXID resolves purely in the base lineage; the child
// contributes nothing.
func TestChainTargetAtOrBelowBaseResolvesInBase(t *testing.T) {
	s := newStore(t)
	putObj(t, s, SnapshotKey("parent", 1, 1))
	putObj(t, s, SegmentKey("parent", 1, 2, 2))
	putObj(t, s, SegmentKey("parent", 1, 3, 3))
	if err := s.WriteLineageBase("child", BasePointer{Lineage: "parent", TXID: 3}); err != nil {
		t.Fatal(err)
	}
	// A child object that must NOT appear when reading at/below the fork point.
	putObj(t, s, SegmentKey("child", 1, 4, 4))

	// target == base.TXID
	got, err := s.Chain("child", 3)
	if err != nil {
		t.Fatalf("Chain err = %v", err)
	}
	want := []string{
		SnapshotKey("parent", 1, 1),
		SegmentKey("parent", 1, 2, 2),
		SegmentKey("parent", 1, 3, 3),
	}
	if !reflect.DeepEqual(keysOf(got), want) {
		t.Fatalf("chain keys = %v, want %v", keysOf(got), want)
	}
	for _, m := range got {
		if strings.HasPrefix(m.Key, LineagePrefix("child")) {
			t.Fatalf("child key leaked into an at/below-fork read: %q", m.Key)
		}
	}
	// target < base.TXID
	got, err = s.Chain("child", 2)
	if err != nil {
		t.Fatalf("Chain err (target<base) = %v", err)
	}
	if len(got) != 2 || got[len(got)-1].MaxTXID != 2 {
		t.Fatalf("chain at target 2 = %v", keysOf(got))
	}
}

// (f) Destroyed-intermediate: build the base records and objects for C -> B ->
// A with NO refs created at all, and assert Chain still resolves fully. This
// proves resolution follows the DURABLE per-lineage base object, never a ref —
// the reason the base pointer is a per-lineage object rather than Ref.Base.
func TestChainResolvesWithoutAnyRefs(t *testing.T) {
	s := newStore(t)
	// A -> B -> C, no PutRef anywhere in this test.
	putObj(t, s, SnapshotKey("a", 1, 1))
	putObj(t, s, SegmentKey("a", 1, 2, 2))
	if err := s.WriteLineageBase("b", BasePointer{Lineage: "a", TXID: 2}); err != nil {
		t.Fatal(err)
	}
	putObj(t, s, SegmentKey("b", 1, 3, 3))
	if err := s.WriteLineageBase("c", BasePointer{Lineage: "b", TXID: 3}); err != nil {
		t.Fatal(err)
	}
	putObj(t, s, SegmentKey("c", 1, 4, 4))

	// Sanity: no refs exist.
	refs, err := s.ListRefs()
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 {
		t.Fatalf("test must create no refs, got %v", refs)
	}

	got, err := s.Chain("c", 4)
	if err != nil {
		t.Fatalf("Chain err = %v", err)
	}
	want := []string{
		SnapshotKey("a", 1, 1),
		SegmentKey("a", 1, 2, 2),
		SegmentKey("b", 1, 3, 3),
		SegmentKey("c", 1, 4, 4),
	}
	if !reflect.DeepEqual(keysOf(got), want) {
		t.Fatalf("chain keys = %v, want %v", keysOf(got), want)
	}
}

// TestChainBaseErrorsOnChildHole: a shared child whose own segment run has a
// hole past the fork point errors loudly rather than returning a gapped chain.
func TestChainBaseErrorsOnChildHole(t *testing.T) {
	s := newStore(t)
	putObj(t, s, SnapshotKey("parent", 1, 1))
	putObj(t, s, SegmentKey("parent", 1, 2, 2))
	if err := s.WriteLineageBase("child", BasePointer{Lineage: "parent", TXID: 2}); err != nil {
		t.Fatal(err)
	}
	// Child has txid 3 and 5, but not 4: a hole.
	putObj(t, s, SegmentKey("child", 1, 3, 3))
	putObj(t, s, SegmentKey("child", 1, 5, 5))
	if _, err := s.Chain("child", 5); err == nil {
		t.Fatal("a hole in the child's own segment run must be an error")
	}
}
