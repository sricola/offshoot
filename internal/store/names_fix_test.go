package store

import (
	"errors"
	"testing"
)

// TestValidateNameRejectsLocalBackendReservedShapes: the local backend
// keeps a per-key flock file at "<key>.lock" (unlinked on release) and
// temp files named "<base>.tmp-*", and its List hides both shapes. A ref
// whose own name ends in ".lock" or contains ".tmp-" would therefore be
// invisible to ListRefs (so GC would sweep its lineage) and deletable by a
// sibling's CAS release. Such names are refused up front, on every
// backend, so the two backends never disagree on which names exist.
// Verified against the pre-fix code, which accepted all four.
func TestValidateNameRejectsLocalBackendReservedShapes(t *testing.T) {
	for _, name := range []string{"main.lock", ".lock", "exp.tmp-1", "a.tmp-"} {
		if err := ValidateName(name); err == nil {
			t.Errorf("ValidateName(%q) = nil, want a refusal (local backend reserved shape)", name)
		}
	}
	for _, name := range []string{"lock", "main-lock", "mainlock", "tmp-1", "x.tmpx", "x.tmp", "a.lockfile"} {
		if err := ValidateName(name); err != nil {
			t.Errorf("ValidateName(%q) = %v, want accepted", name, err)
		}
	}
}

// TestLocalListRefsShowsEveryValidRefAfterASiblingCAS: on the local
// backend, a CAS write to one ref must never make another ref disappear
// from ListRefs — the symptom the reserved-shape rule above prevents.
func TestLocalListRefsShowsEveryValidRefAfterASiblingCAS(t *testing.T) {
	b, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := &Store{B: b}
	for _, br := range []string{"main", "main-lock", "x.tmp"} {
		if _, err := s.PutRef("app", br, Ref{Lineage: "l" + br, Epoch: 1}, ""); err != nil {
			t.Fatal(err)
		}
	}
	ref, etag, err := s.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	ref.HeadTXID++
	if _, err := s.PutRef("app", "main", ref, etag); err != nil {
		t.Fatal(err)
	}
	refs, err := s.ListRefs()
	if err != nil {
		t.Fatal(err)
	}
	if got := refs["app"]; len(got) != 3 {
		t.Fatalf("ListRefs after a sibling CAS = %v, want all three refs", got)
	}
	for _, br := range []string{"main-lock", "x.tmp"} {
		if _, _, err := s.GetRef("app", br); err != nil {
			t.Errorf("GetRef(app@%s) after a sibling CAS: %v", br, err)
		}
	}
	if _, _, err := s.GetRef("app", "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("sanity: missing ref = %v", err)
	}
}
