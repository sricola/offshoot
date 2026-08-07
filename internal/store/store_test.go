package store

import (
	"errors"
	"strings"
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
