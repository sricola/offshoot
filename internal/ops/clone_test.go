package ops

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/ops/reflink"
	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// requireClone skips unless the workspace's filesystem supports
// copy-on-write clones: the by-chain fast path is a capability, and on a
// non-CoW filesystem (ext4, tmpfs) TestClonePathDegradesWhenUnsupported's
// behaviour is the only correct one.
func requireClone(t *testing.T, w *Workspace) {
	t.Helper()
	src := filepath.Join(w.Root, "clone-probe")
	if err := os.WriteFile(src, []byte("probe"), 0o644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(src)
	dst := src + ".clone"
	err := reflink.Clone(dst, src)
	os.Remove(dst)
	if errors.Is(err, reflink.ErrUnsupported) {
		t.Skip("workspace filesystem does not support reflink/clonefile")
	}
	if err != nil {
		t.Fatal(err)
	}
}

// seedDB creates db, writes roughly size bytes of rows on main, and
// checkpoints them as "seed".
func seedDB(t *testing.T, w *Workspace, db string, size int) {
	t.Helper()
	testutil.RequireSQLite3(t)
	if err := w.Create(db); err != nil {
		t.Fatal(err)
	}
	path := mustCheckout(t, w, db, "main")
	const blob = 4000
	mustSQL(t, path, fmt.Sprintf(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);"+
			"WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < %d) "+
			"INSERT INTO t (v) SELECT randomblob(%d) FROM n;", size/blob, blob))
	if _, err := w.Checkpoint(db, "main", "seed", nil); err != nil {
		t.Fatal(err)
	}
}

func mustFork(t *testing.T, w *Workspace, db, src, branch, at string) {
	t.Helper()
	if _, err := w.Fork(db, src, branch, at, 0, nil); err != nil {
		t.Fatalf("fork %s -> %s: %v", src, branch, err)
	}
}

func mustCheckout(t *testing.T, w *Workspace, db, branch string) string {
	t.Helper()
	path, err := w.Checkout(db, branch)
	if err != nil {
		t.Fatalf("checkout %s@%s: %v", db, branch, err)
	}
	return path
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// exportBytes is an independent materialization of db@branch at checkpoint
// ("" = head) through Export, which never consults the by-chain cache.
func exportBytes(t *testing.T, w *Workspace, db, branch, checkpoint string) []byte {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "export.db")
	if err := w.Export(db, branch, checkpoint, dst, false); err != nil {
		t.Fatal(err)
	}
	return readFile(t, dst)
}

func refOf(t *testing.T, w *Workspace, db, branch string) store.Ref {
	t.Helper()
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

// headChainID resolves db@branch's head chain and returns its chainID.
func headChainID(t *testing.T, w *Workspace, db, branch string) string {
	t.Helper()
	ref := refOf(t, w, db, branch)
	members, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	return chainID(members)
}

// assertCheckoutMatches checks a writable checkout byte-for-byte against an
// independent Export, that its sidecar is clean for the branch's own
// identity, that the recorded post-apply checksum equals a full
// ltxio.ChecksumDatabase rescan, and that the recorded chain_id is the head
// chain's.
func assertCheckoutMatches(t *testing.T, w *Workspace, db, branch, path string) {
	t.Helper()
	if !bytes.Equal(readFile(t, path), exportBytes(t, w, db, branch, "")) {
		t.Fatalf("%s: checkout differs from an independent export", branch)
	}
	ref := refOf(t, w, db, branch)
	if st, _ := checkoutState(path, ref); st != "clean" {
		t.Fatalf("%s: sidecar state %q, want clean", branch, st)
	}
	rec, ok := readSidecar(path)
	if !ok {
		t.Fatalf("%s: no readable sidecar", branch)
	}
	if rec.Lineage != ref.Lineage || rec.Epoch != ref.HeadEpoch || rec.TXID != ref.HeadTXID {
		t.Fatalf("%s: sidecar identity (%s,%d,%d), want the branch's own (%s,%d,%d)",
			branch, rec.Lineage, rec.Epoch, rec.TXID, ref.Lineage, ref.HeadEpoch, ref.HeadTXID)
	}
	sum, err := ltxio.ChecksumDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	if rec.PostApplyChecksum != sum {
		t.Fatalf("%s: recorded post-apply checksum %016x, ChecksumDatabase %016x", branch, rec.PostApplyChecksum, sum)
	}
	if want := headChainID(t, w, db, branch); rec.ChainID != want {
		t.Fatalf("%s: recorded chain_id %q, want %q", branch, rec.ChainID, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o200 == 0 {
		t.Fatalf("%s: writable checkout has mode %v", branch, info.Mode())
	}
}

// recordKinds installs observeCheckoutSource for the rest of the test.
func recordKinds(t *testing.T) *[]string {
	t.Helper()
	var kinds []string
	observeCheckoutSource = func(k string) { kinds = append(kinds, k) }
	t.Cleanup(func() { observeCheckoutSource = nil })
	return &kinds
}

func TestByChainDirIsNotAValidName(t *testing.T) {
	if err := store.ValidateName(byChainDir); err == nil {
		t.Fatalf("by-chain directory %q passes store.ValidateName; it could collide with a database/branch name", byChainDir)
	}
}

func TestForkCheckoutsCloneFromByChainCache(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 8<<20)
	kinds := recordKinds(t)
	want := exportBytes(t, w, "app", "main", "seed")
	for i := 0; i < 5; i++ {
		br := fmt.Sprintf("a%d", i)
		mustFork(t, w, "app", "main", br, "seed")
		path := mustCheckout(t, w, "app", br)
		if got := readFile(t, path); !bytes.Equal(got, want) {
			t.Fatalf("%s: cloned checkout differs from seed export", br)
		}
		assertCheckoutMatches(t, w, "app", br, path)
	}
	// first child materializes (and populates), the rest clone
	if (*kinds)[0] != "materialize" || strings.Count(strings.Join(*kinds, ","), "clone") != 4 {
		t.Fatalf("checkout sources = %v", *kinds)
	}
	entry := w.byChainPath("app", headChainID(t, w, "app", "a0"))
	info, err := os.Stat(entry)
	if err != nil {
		t.Fatalf("by-chain entry not populated: %v", err)
	}
	if info.Mode().Perm() != byChainEntryMode {
		t.Fatalf("by-chain entry mode %v, want %v", info.Mode().Perm(), byChainEntryMode)
	}
	if !bytes.Equal(readFile(t, entry), want) {
		t.Fatal("by-chain entry differs from seed export")
	}
}

func TestByChainEntriesAreEvictedAndRecreated(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	_, before, err := w.ROCacheUsage()
	if err != nil {
		t.Fatal(err)
	}
	kinds := recordKinds(t)
	mustFork(t, w, "app", "main", "b0", "seed")
	mustCheckout(t, w, "app", "b0")
	id := headChainID(t, w, "app", "b0")
	entry := w.byChainPath("app", id)
	if _, err := os.Stat(entry); err != nil {
		t.Fatalf("by-chain entry not populated: %v", err)
	}
	if _, n, err := w.ROCacheUsage(); err != nil || n != before+1 {
		t.Fatalf("ROCacheUsage count = %d (err %v), want %d: the by-chain entry counts", n, err, before+1)
	}

	evicted, usage, err := w.EvictROCache(1)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range evicted {
		if e.DB == "app" && e.Branch == byChainDir && e.Checkpoint == id {
			found = true
		}
	}
	if !found || len(evicted) != before+1 || usage != 0 {
		t.Fatalf("evicted = %+v (usage after %d), want every entry including by-chain %s", evicted, usage, id)
	}
	for _, p := range []string{entry, entry + ".sum", entry + lastUsedSuffix} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("%s survived eviction: %v", p, err)
		}
	}

	mustFork(t, w, "app", "main", "b1", "seed")
	assertCheckoutMatches(t, w, "app", "b1", mustCheckout(t, w, "app", "b1"))
	mustFork(t, w, "app", "main", "b2", "seed")
	assertCheckoutMatches(t, w, "app", "b2", mustCheckout(t, w, "app", "b2"))
	if got := strings.Join(*kinds, ","); got != "materialize,materialize,clone" {
		t.Fatalf("checkout sources = %s, want materialize,materialize,clone", got)
	}
}

func TestClonePathDegradesWhenUnsupported(t *testing.T) {
	w := newWS(t)
	reflinkUnsupportedForTest = true
	t.Cleanup(func() { reflinkUnsupportedForTest = false })
	seedDB(t, w, "app", 1<<20)
	kinds := recordKinds(t)
	for i := 0; i < 3; i++ {
		br := fmt.Sprintf("d%d", i)
		mustFork(t, w, "app", "main", br, "seed")
		assertCheckoutMatches(t, w, "app", br, mustCheckout(t, w, "app", br))
	}
	if got := strings.Join(*kinds, ","); got != "materialize,materialize,materialize" {
		t.Fatalf("checkout sources = %s, want every checkout materialized", got)
	}
	if _, err := os.Stat(filepath.Join(w.roCacheRoot(), "app", byChainDir)); !os.IsNotExist(err) {
		t.Fatalf("by-chain directory exists on a filesystem that cannot clone: %v", err)
	}
	if _, err := os.Stat(w.roCacheRoot()); !os.IsNotExist(err) {
		t.Fatalf("checkouts-ro exists on a filesystem that cannot clone, with no --at checkout: %v", err)
	}
	// A --at checkout still lands in checkouts-ro/<db>, which the probe
	// must not remove from under it.
	at, err := w.CheckoutAt("app", "main", "seed", false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readFile(t, at), exportBytes(t, w, "app", "main", "seed")) {
		t.Fatal("checkout-at differs from an export on a filesystem that cannot clone")
	}
}

// TestByChainEntryWithoutUsableSidecarIsAMiss: an entry whose .sum predates
// chain_id (or is missing, or whose .db vanished under a concurrent
// eviction) is never an error, only "no fast path".
func TestByChainEntryWithoutUsableSidecarIsAMiss(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	mustFork(t, w, "app", "main", "o0", "seed")
	mustCheckout(t, w, "app", "o0")
	entry := w.byChainPath("app", headChainID(t, w, "app", "o0"))
	rec, ok := readSidecar(entry)
	if !ok {
		t.Fatal("by-chain entry has no readable sidecar")
	}
	kinds := recordKinds(t)

	// Old-format sidecar: no chain_id.
	if err := StampSum(entry, rec.Hash, "", 0, 0, rec.PostApplyChecksum, ""); err != nil {
		t.Fatal(err)
	}
	mustFork(t, w, "app", "main", "o1", "seed")
	assertCheckoutMatches(t, w, "app", "o1", mustCheckout(t, w, "app", "o1"))

	// o1's materialize re-populated the entry; now evict the .db alone, as
	// if an eviction landed between the sidecar read and the clone.
	if err := os.Remove(entry); err != nil {
		t.Fatal(err)
	}
	mustFork(t, w, "app", "main", "o2", "seed")
	assertCheckoutMatches(t, w, "app", "o2", mustCheckout(t, w, "app", "o2"))

	if got := strings.Join(*kinds, ","); got != "materialize,materialize" {
		t.Fatalf("checkout sources = %s, want both to materialize", got)
	}
}

// appendHandSegment writes one single-txid segment on db@branch built from
// the checkout's current state plus stmt, and advances the ref — the shape
// a session flush on a shared fork produces.
func appendHandSegment(t *testing.T, w *Workspace, db, branch, stmt string) {
	t.Helper()
	cur := w.CheckoutPath(db, branch)
	next := filepath.Join(t.TempDir(), "next.db")
	if err := copyFile(cur, next); err != nil {
		t.Fatal(err)
	}
	mustSQL(t, next, stmt)
	ref, etag, err := w.Store.GetRef(db, branch)
	if err != nil {
		t.Fatal(err)
	}
	txid := ref.HeadTXID + 1
	pageSize, commit, changed := changedPagesForTest(t, cur, next)
	pre, err := ltxio.ChecksumDatabase(cur)
	if err != nil {
		t.Fatal(err)
	}
	post, err := ltxio.ChecksumDatabase(next)
	if err != nil {
		t.Fatal(err)
	}
	var seg bytes.Buffer
	if err := ltxio.EncodeSegment(pageSize, commit, txid, txid, pre, post, changed, &seg); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Store.B.PutIf(store.SegmentKey(ref.Lineage, ref.Epoch, txid, txid), seg.Bytes(), ""); err != nil {
		t.Fatal(err)
	}
	ref.HeadTXID = txid
	ref.HeadEpoch = ref.Epoch
	if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
		t.Fatal(err)
	}
}

func TestDivergedChildClonesPrefixAndAppliesSegments(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 2<<20)
	mustFork(t, w, "app", "main", "c", "seed")
	mustCheckout(t, w, "app", "c") // materializes the base chain and populates its entry
	appendHandSegment(t, w, "app", "c", "INSERT INTO t (v) VALUES (randomblob(9000));")

	ref := refOf(t, w, "app", "c")
	members, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) < 2 || members[len(members)-1].Snapshot {
		t.Fatalf("child head chain = %+v, want the base chain plus a segment", members)
	}

	path := w.CheckoutPath("app", "c")
	for _, p := range []string{path, path + ".sum"} {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	kinds := recordKinds(t)
	assertCheckoutMatches(t, w, "app", "c", mustCheckout(t, w, "app", "c"))
	if got := strings.Join(*kinds, ","); got != "clone+segments" {
		t.Fatalf("checkout sources = %s, want clone+segments", got)
	}

	// The full chain was populated too: a second fork at c's head clones.
	full := w.byChainPath("app", chainID(members))
	if !bytes.Equal(readFile(t, full), exportBytes(t, w, "app", "c", "")) {
		t.Fatal("full-chain by-chain entry differs from an export of c's head")
	}
	mustFork(t, w, "app", "c", "c2", "")
	assertCheckoutMatches(t, w, "app", "c2", mustCheckout(t, w, "app", "c2"))
	if got := strings.Join(*kinds, ","); got != "clone+segments,clone" {
		t.Fatalf("checkout sources = %s, want clone+segments,clone", got)
	}
}

// TestCheckoutAtSharesTheByChainCache: CheckoutAt populates the by-chain
// area on a miss, a writable checkout of the same chain then clones from
// it, and a CheckoutAt of a different branch's identical chain clones too.
func TestCheckoutAtSharesTheByChainCache(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	kinds := recordKinds(t)
	want := exportBytes(t, w, "app", "main", "seed")

	ro, err := w.CheckoutAt("app", "main", "seed", false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readFile(t, ro), want) {
		t.Fatal("checkout-at differs from seed export")
	}
	mustFork(t, w, "app", "main", "e0", "seed")
	assertCheckoutMatches(t, w, "app", "e0", mustCheckout(t, w, "app", "e0"))

	ref, etag, err := w.Store.GetRef("app", "e0")
	if err != nil {
		t.Fatal(err)
	}
	ref.SetCheckpoint("same", headCheckpoint(ref))
	if _, err := w.Store.PutRef("app", "e0", ref, etag); err != nil {
		t.Fatal(err)
	}
	ro2, err := w.CheckoutAt("app", "e0", "same", false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readFile(t, ro2), want) {
		t.Fatal("cloned checkout-at differs from seed export")
	}
	info, err := os.Stat(ro2)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o444 {
		t.Fatalf("checkout-at mode %v, want 0444", info.Mode().Perm())
	}
	if got := strings.Join(*kinds, ","); got != "materialize,clone,clone" {
		t.Fatalf("checkout sources = %s, want materialize,clone,clone", got)
	}
}

// TestMaterializeMissBuildsImmutableEntryFirst pins the populate order: on
// a materialize miss the entry is built first and the writable checkout is
// a clone of it, so the entry's recorded hash and checksum describe the
// entry's own bytes (not the checkout's), and a write to the checkout
// afterwards never reaches the entry a later fork clones.
func TestMaterializeMissBuildsImmutableEntryFirst(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	kinds := recordKinds(t)
	mustFork(t, w, "app", "main", "m0", "seed")
	path := mustCheckout(t, w, "app", "m0")
	id := headChainID(t, w, "app", "m0")
	entry := w.byChainPath("app", id)

	if !bytes.Equal(readFile(t, path), readFile(t, entry)) {
		t.Fatal("writable checkout and its by-chain entry differ")
	}
	assertEntryRecordMatchesItself := func() {
		t.Helper()
		rec, ok := readSidecar(entry)
		if !ok || rec.ChainID != id {
			t.Fatalf("entry sidecar = %+v (ok %v), want chain_id %s", rec, ok, id)
		}
		hash, err := fileSum(entry)
		if err != nil {
			t.Fatal(err)
		}
		sum, err := ltxio.ChecksumDatabase(entry)
		if err != nil {
			t.Fatal(err)
		}
		if rec.Hash != hash || rec.PostApplyChecksum != sum {
			t.Fatalf("entry record (hash %s, checksum %016x) != entry's own bytes (hash %s, checksum %016x)",
				rec.Hash, rec.PostApplyChecksum, hash, sum)
		}
	}
	assertEntryRecordMatchesItself()

	// A write to the fresh checkout lands in the checkout's own extents only.
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(5000));")
	assertEntryRecordMatchesItself()
	mustFork(t, w, "app", "main", "m1", "seed")
	assertCheckoutMatches(t, w, "app", "m1", mustCheckout(t, w, "app", "m1"))
	if got := strings.Join(*kinds, ","); got != "materialize,clone" {
		t.Fatalf("checkout sources = %s, want materialize,clone", got)
	}
}

// byChainIDs lists db's by-chain entry ids (the <id>.db files only).
func byChainIDs(t *testing.T, w *Workspace, db string) map[string]bool {
	t.Helper()
	entries, err := byChainEntries(db, filepath.Join(w.roCacheRoot(), db, byChainDir))
	if err != nil {
		t.Fatal(err)
	}
	ids := make(map[string]bool, len(entries))
	for _, e := range entries {
		ids[e.Checkpoint] = true
	}
	return ids
}

// TestByChainAreaIsBoundedByDefault pins ruling I1: with no
// -ro-cache-budget and no janitor, a run of checkout misses over more than
// DefaultByChainMaxEntries distinct chains never leaves more than that many
// entries, the survivors are the most recently used ones, a staging file of
// an entry being built is never touched, and a checkout of an evicted chain
// still works (it rebuilds the entry).
func TestByChainAreaIsBoundedByDefault(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	requireClone(t, w)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path := mustCheckout(t, w, "app", "main")
	mustSQL(t, path, "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);")

	// An entry mid-build: a staging file, old enough to rank first.
	byChain := filepath.Join(w.roCacheRoot(), "app", byChainDir)
	stage := filepath.Join(byChain, strings.Repeat("0", 64)+".db.stage-test")
	if err := os.WriteFile(stage, []byte("building"), 0o600); err != nil {
		t.Fatal(err)
	}
	chtime(t, stage, time.Now().Add(-time.Hour))

	const bound = DefaultByChainMaxEntries
	var order []string // chain ids in populate order
	var headOf []string
	for i := 0; i < bound+8; i++ {
		mustSQL(t, path, fmt.Sprintf("INSERT INTO t (v) VALUES (randomblob(%d));", 500+i))
		name := fmt.Sprintf("c%d", i)
		if _, err := w.Checkpoint("app", "main", name, nil); err != nil {
			t.Fatal(err)
		}
		// Drop the checkout so the next CheckoutProven is a by-chain miss
		// that builds this head's entry.
		for _, p := range []string{path, path + ".sum", shadowPath(path)} {
			os.Remove(p)
		}
		res, err := w.CheckoutProven("app", "main")
		if err != nil {
			t.Fatal(err)
		}
		if res.Path != path {
			t.Fatalf("checkout path %s, want %s", res.Path, path)
		}
		id := headChainID(t, w, "app", "main")
		order = append(order, id)
		headOf = append(headOf, name)
		ids := byChainIDs(t, w, "app")
		if len(ids) > bound {
			t.Fatalf("after %d populates: %d by-chain entries, bound %d", i+1, len(ids), bound)
		}
		if !ids[id] {
			t.Fatalf("after %d populates: the entry just built was pruned", i+1)
		}
	}

	ids := byChainIDs(t, w, "app")
	if len(ids) != bound {
		t.Fatalf("%d entries survive, want exactly the bound %d", len(ids), bound)
	}
	for _, id := range order[len(order)-bound:] {
		if !ids[id] {
			t.Fatalf("a most recently used entry %s was pruned", id)
		}
	}
	evicted := order[0]
	if ids[evicted] {
		t.Fatalf("the least recently used entry %s survived", evicted)
	}
	for _, p := range []string{".db", ".db.sum", ".db" + lastUsedSuffix} {
		if _, err := os.Stat(filepath.Join(byChain, evicted+p)); !os.IsNotExist(err) {
			t.Fatalf("evicted entry's %s survived: %v", p, err)
		}
	}
	if _, err := os.Stat(stage); err != nil {
		t.Fatalf("a staging file was pruned: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the writable checkout is gone: %v", err)
	}

	// A branch whose head is the evicted chain rebuilds it on checkout.
	kinds := recordKinds(t)
	mustFork(t, w, "app", "main", "old", headOf[0])
	if got := headChainID(t, w, "app", "old"); got != evicted {
		t.Fatalf("fork at %s resolves chain %s, want the evicted %s", headOf[0], got, evicted)
	}
	oldPath := mustCheckout(t, w, "app", "old")
	assertCheckoutMatches(t, w, "app", "old", oldPath)
	if len(*kinds) != 1 || (*kinds)[0] == "clone" {
		t.Fatalf("checkout of an evicted chain: sources %v, want a rebuild", *kinds)
	}
	ids = byChainIDs(t, w, "app")
	if !ids[evicted] || len(ids) != bound {
		t.Fatalf("after the rebuild: entry present %v, %d entries (want present, %d)", ids[evicted], len(ids), bound)
	}
}

// TestByChainAndShadowModesArePrivate pins ruling I4: a by-chain entry is
// 0400, the checkouts-ro, checkouts-ro/<db> and by-chain directories are
// 0700 (a pre-existing wider one is tightened), a CheckoutAt file stays
// 0444 inside them, and a shadow is 0600 after refreshShadow.
func TestByChainAndShadowModesArePrivate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX modes are not enforced on windows")
	}
	w := newWS(t)
	requireClone(t, w)
	// A wider directory left by an earlier version is tightened.
	if err := os.MkdirAll(filepath.Join(w.roCacheRoot(), "app"), 0o755); err != nil {
		t.Fatal(err)
	}
	seedDB(t, w, "app", 1<<20)
	mustFork(t, w, "app", "main", "b", "seed")
	path := mustCheckout(t, w, "app", "b")
	entry := w.byChainPath("app", headChainID(t, w, "app", "b"))

	mode := func(p string) os.FileMode {
		t.Helper()
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		return fi.Mode().Perm()
	}
	if m := mode(entry); m != 0o400 {
		t.Fatalf("by-chain entry mode %v, want 0400", m)
	}
	for _, d := range []string{w.roCacheRoot(), filepath.Join(w.roCacheRoot(), "app"), filepath.Dir(entry)} {
		if m := mode(d); m != 0o700 {
			t.Fatalf("%s mode %v, want 0700", d, m)
		}
	}
	at, err := w.CheckoutAt("app", "main", "seed", false)
	if err != nil {
		t.Fatal(err)
	}
	if m := mode(at); m != 0o444 {
		t.Fatalf("checkout-at mode %v, want 0444", m)
	}

	// The shadow: widen the checkout, then refresh — the shadow is 0600
	// whatever mode its clone source had.
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	refreshShadow(path)
	if rec, ok := readSidecar(path); !ok || !rec.Shadow {
		t.Fatalf("refreshShadow recorded no shadow: %+v", rec)
	}
	if m := mode(shadowPath(path)); m != 0o600 {
		t.Fatalf("shadow mode %v, want 0600", m)
	}
	if _, err := os.Stat(shadowPath(path) + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("shadow temp left behind: %v", err)
	}
}
