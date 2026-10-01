package ops

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// kindsSince returns the checkout-source kinds recorded after the first n.
func kindsSince(kinds *[]string, n int) []string {
	return append([]string(nil), (*kinds)[n:]...)
}

// assertRefreshed checks a refreshed writable checkout byte-for-byte
// against want (an independent Export), and that its sidecar is clean for
// the branch's current head (see assertCheckoutMatches).
func assertRefreshed(t *testing.T, w *Workspace, db, branch, path string, want []byte) {
	t.Helper()
	if !bytes.Equal(readFile(t, path), want) {
		t.Fatalf("%s: refreshed checkout differs from an independent export", branch)
	}
	assertCheckoutMatches(t, w, db, branch, path)
}

// A rollback's checkout refresh goes through the by-chain cache: rolling
// back to the same checkpoint twice resolves the same chain both times, so
// the second refresh clones the entry the first one built.
func TestRollbackRefreshClonesFromByChainCache(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	path := seedRows(t, w, "app", 8<<20, 4000)
	mustCheckpointWith(t, w, "app", "main", "a", CheckpointOptions{})
	want := exportBytes(t, w, "app", "main", "a")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	mustCheckpointWith(t, w, "app", "main", "b", CheckpointOptions{})
	mustCheckout(t, w, "app", "main")
	kinds := recordKinds(t)

	for i, name := range []string{"b2", "b3"} {
		n := len(*kinds)
		res, err := w.RollbackWith("app", "main", "a", RollbackOptions{NoBackup: true})
		if err != nil {
			t.Fatal(err)
		}
		got := kindsSince(kinds, n)
		if len(got) != 1 {
			t.Fatalf("rollback %d: checkout sources %v, want exactly one refresh", i+1, got)
		}
		if i == 1 && got[0] != "clone" {
			t.Fatalf("second rollback to a: refresh source %q, want clone (the first rollback cached a's chain)", got[0])
		}
		assertRefreshed(t, w, "app", "main", res.Path, want)
		// Diverge again so the next rollback has something to undo.
		mustSQL(t, res.Path, "INSERT INTO t (v) VALUES (randomblob(100));")
		mustCheckpointWith(t, w, "app", "main", name, CheckpointOptions{})
	}
}

// A promote's checkout refresh goes through the by-chain cache: promoting
// the same head twice lands the same chain, so the second refresh is a
// clone, never a fresh decode.
func TestPromoteRefreshClonesFromByChainCache(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 8<<20)
	mustFork(t, w, "app", "main", "attempt", "seed")
	ap := mustCheckout(t, w, "app", "attempt")
	mustSQL(t, ap, "INSERT INTO t (v) VALUES (randomblob(100));")
	if _, err := w.Checkpoint("app", "attempt", "done", nil); err != nil {
		t.Fatal(err)
	}
	want := exportBytes(t, w, "app", "attempt", "")
	mp := mustCheckout(t, w, "app", "main")
	kinds := recordKinds(t)

	for i := 0; i < 2; i++ {
		n := len(*kinds)
		if _, err := w.PromoteWith("app", "attempt", "main", PromoteOptions{Force: true, NoBackup: true}); err != nil {
			t.Fatal(err)
		}
		got := kindsSince(kinds, n)
		if len(got) != 1 {
			t.Fatalf("promote %d: checkout sources %v, want exactly one refresh", i+1, got)
		}
		if i == 1 && got[0] == "materialize" {
			t.Fatalf("second promote of an identical head: refresh source %q, want a by-chain clone", got[0])
		}
		assertRefreshed(t, w, "app", "main", mp, want)
	}
}

// Compact's checkout refresh goes through materializeFromChain (which
// leaves a by-chain entry for the compacted chain), and the compacted
// checkout's bytes are unchanged.
func TestCompactRefreshUsesByChainCache(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 8<<20)
	mustFork(t, w, "app", "main", "child", "seed")
	cp := mustCheckout(t, w, "app", "child")
	mustSQL(t, cp, "INSERT INTO t (v) VALUES (randomblob(100));")
	if _, err := w.Checkpoint("app", "child", "c1", nil); err != nil {
		t.Fatal(err)
	}
	before := readFile(t, cp)
	kinds := recordKinds(t)
	if _, err := w.Compact("app", "child"); err != nil {
		t.Fatal(err)
	}
	if len(*kinds) != 1 {
		t.Fatalf("compact: checkout sources %v, want exactly one refresh through materializeFromChain", *kinds)
	}
	if !bytes.Equal(readFile(t, cp), before) {
		t.Fatal("compact changed the checkout's bytes")
	}
	assertRefreshed(t, w, "app", "child", cp, exportBytes(t, w, "app", "child", ""))
	if _, err := os.Stat(w.byChainPath("app", headChainID(t, w, "app", "child"))); err != nil {
		t.Fatalf("compact's refresh left no by-chain entry for the compacted chain: %v", err)
	}
}

// On a filesystem that cannot clone, a refresh degrades to a plain
// materialize and leaves no by-chain area behind.
func TestRefreshDegradesWhenReflinkUnsupported(t *testing.T) {
	w := newWS(t)
	reflinkUnsupportedForTest = true
	t.Cleanup(func() { reflinkUnsupportedForTest = false })
	path := seedRows(t, w, "app", 1<<20, 4000)
	mustCheckpointWith(t, w, "app", "main", "a", CheckpointOptions{})
	want := exportBytes(t, w, "app", "main", "a")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	mustCheckpointWith(t, w, "app", "main", "b", CheckpointOptions{})
	kinds := recordKinds(t)
	res, err := w.RollbackWith("app", "main", "a", RollbackOptions{NoBackup: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(*kinds, ","); got != "materialize" {
		t.Fatalf("checkout sources = %q, want one materialize", got)
	}
	assertRefreshed(t, w, "app", "main", res.Path, want)
	if _, err := os.Stat(filepath.Join(w.roCacheRoot(), "app", byChainDir)); !os.IsNotExist(err) {
		t.Fatalf("by-chain directory exists on a filesystem that cannot clone: %v", err)
	}
}
