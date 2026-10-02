package ops

import (
	"testing"
	"time"
)

// TestGCPruneDoesNotClobberAConcurrentPass: GC used to persist its whole
// in-memory tombstone list with an unconditional Put at the end of a pass,
// so a concurrent GC or janitor pass that had pruned a stone (after
// deleting its object) or added one since this pass loaded the list was
// overwritten: the pruned stone came back with its OLD timestamp (a key a
// fork in flight could recreate would then sit under an already expired
// stone), and the new stone vanished until a later pass re-derived it.
// The prune is now a compare-and-swap of exactly this pass's removals.
func TestGCPruneDoesNotClobberAConcurrentPass(t *testing.T) {
	w := newWS(t)
	old := time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano)
	// Two stale stones for keys that do not exist as objects: a pass prunes
	// such stones (their object is already gone). Our pass will see both at
	// load; the "concurrent" pass removes B and adds C in the window.
	if err := w.tombstone(map[string]string{
		"data/l1/1/snapshot-0000000000000001.ltx": old,
		"data/l2/1/snapshot-0000000000000001.ltx": old,
	}); err != nil {
		t.Fatal(err)
	}
	gcBeforePruneForTest = func() {
		if err := w.pruneTombstones([]string{"data/l2/1/snapshot-0000000000000001.ltx"}); err != nil {
			t.Fatal(err)
		}
		if err := w.tombstone(map[string]string{"data/l3/1/snapshot-0000000000000001.ltx": time.Now().UTC().Format(time.RFC3339Nano)}); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { gcBeforePruneForTest = nil })

	if _, _, err := w.GC(time.Hour); err != nil {
		t.Fatal(err)
	}
	stones, _, err := w.loadTombstones()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stones["data/l1/1/snapshot-0000000000000001.ltx"]; ok {
		t.Fatal("our own prune of l1 was not persisted")
	}
	if _, ok := stones["data/l2/1/snapshot-0000000000000001.ltx"]; ok {
		t.Fatal("the concurrent pass's prune of l2 was clobbered: the stone came back with its old timestamp")
	}
	if _, ok := stones["data/l3/1/snapshot-0000000000000001.ltx"]; !ok {
		t.Fatal("the concurrent pass's new stone l3 was clobbered")
	}
}
