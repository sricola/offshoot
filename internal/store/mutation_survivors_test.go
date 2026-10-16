package store

import (
	"reflect"
	"testing"
)

// TestValidateNameCharsetBoundaries pins the ends of every accepted range
// in docs/reference.md's naming rule ([a-z0-9-_.]): the first and last
// letter and digit are accepted, their neighbours outside the ranges are
// not. gremlins reported the `<= 'z'` and `<= '9'` comparisons as
// surviving a boundary mutation because no test used those characters.
func TestValidateNameCharsetBoundaries(t *testing.T) {
	for _, name := range []string{"a", "z", "0", "9", "az09-_.", "zz", "99"} {
		if err := ValidateName(name); err != nil {
			t.Errorf("ValidateName(%q) = %v, want accepted", name, err)
		}
	}
	for _, name := range []string{"`", "{", "/", ":", "Z", "A", "a\tb"} {
		if err := ValidateName(name); err == nil {
			t.Errorf("ValidateName(%q) = nil, want rejected (outside [a-z0-9-_.])", name)
		}
	}
}

// TestChainChildHalfIgnoresSegmentsAtOrBelowTheForkPoint pins the one hard
// rule in resolveRange (docs/architecture.md "Resolution never merges
// across lineages"): a child's own segments wholly at or below its fork
// point belong to the base half and are skipped, including one whose
// MaxTXID equals the fork txid exactly. Without the `<=` such a leftover
// (a fenced writer's object, say) reads as a hole before the child's real
// first segment. gremlins reported the boundary as surviving.
func TestChainChildHalfIgnoresSegmentsAtOrBelowTheForkPoint(t *testing.T) {
	s := newStore(t)
	putObj(t, s, SnapshotKey("parent", 1, 1))
	putObj(t, s, SegmentKey("parent", 1, 2, 2))
	putObj(t, s, SegmentKey("parent", 1, 3, 3))
	if err := s.WriteLineageBase("child", BasePointer{Lineage: "parent", TXID: 3}); err != nil {
		t.Fatal(err)
	}
	// Leftovers at and below the fork point in the child's own lineage.
	putObj(t, s, SegmentKey("child", 1, 3, 3))
	putObj(t, s, SegmentKey("child", 1, 2, 3))
	putObj(t, s, SegmentKey("child", 1, 4, 4))

	got, err := s.Chain("child", 4)
	if err != nil {
		t.Fatalf("Chain err = %v", err)
	}
	want := []string{
		SnapshotKey("parent", 1, 1),
		SegmentKey("parent", 1, 2, 2),
		SegmentKey("parent", 1, 3, 3),
		SegmentKey("child", 1, 4, 4),
	}
	if !reflect.DeepEqual(keysOf(got), want) {
		t.Fatalf("chain keys =\n %v\nwant\n %v", keysOf(got), want)
	}
}
