package store

import (
	"errors"
	"strings"
	"testing"
)

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
	r.SetCheckpoint("init", 1, 1)
	etag, err := s.PutRef("app", "main", r, "")
	if err != nil {
		t.Fatal(err)
	}
	got, gotEtag, err := s.GetRef("app", "main")
	if err != nil || gotEtag != etag {
		t.Fatalf("get: %v", err)
	}
	if got.Lineage != r.Lineage || got.Checkpoints["init"] != (Checkpoint{TXID: 1, Epoch: 1}) {
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
	r.SetCheckpoint("init", 1, 1)
	r.SetCheckpoint("v1", 9, 3)
	etag, err := s.PutRef("app", "main", r, "")
	if err != nil {
		t.Fatal(err)
	}
	got, gotEtag, err := s.GetRef("app", "main")
	if err != nil || gotEtag != etag {
		t.Fatalf("get: %v", err)
	}
	if got.Checkpoints["init"] != (Checkpoint{TXID: 1, Epoch: 1}) {
		t.Errorf("init checkpoint = %+v", got.Checkpoints["init"])
	}
	if got.Checkpoints["v1"] != (Checkpoint{TXID: 9, Epoch: 3}) {
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
	if got.Checkpoints["init"] != (Checkpoint{TXID: 1, Epoch: 2}) {
		t.Errorf("init = %+v, want txid 1 epoch 2", got.Checkpoints["init"])
	}
	if got.Checkpoints["v1"] != (Checkpoint{TXID: 7, Epoch: 2}) {
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

func TestSetCheckpointAllocates(t *testing.T) {
	var r Ref
	r.SetCheckpoint("a", 4, 2)
	if r.Checkpoints["a"] != (Checkpoint{TXID: 4, Epoch: 2}) {
		t.Fatalf("checkpoints = %+v", r.Checkpoints)
	}
}
