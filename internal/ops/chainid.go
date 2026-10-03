package ops

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sricola/offshoot/internal/dbfile"
	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/reflink"
	"github.com/sricola/offshoot/internal/store"
)

// chainID names the content a resolved chain materializes to: the hex
// SHA-256 of its member keys joined by "\n". members is store.Chain's
// output — already epoch-collapsed with base pointers followed — so two
// targets with equal chainIDs apply the same immutable objects in the same
// order and are byte-identical by construction. A fresh shared fork's head
// resolves to exactly its parent's chain at the fork point, which is what
// makes its checkout a clone of the parent's rather than a fresh decode.
func chainID(members []store.ChainMember) string {
	keys := make([]string, len(members))
	for i, m := range members {
		keys[i] = m.Key
	}
	sum := sha256.Sum256([]byte(strings.Join(keys, "\n")))
	return hex.EncodeToString(sum[:])
}

// observeCheckoutSource, when non-nil, is told how each by-chain-aware
// materialization produced its file: "clone" (a by-chain entry for the
// whole chain), "clone+segments" (an entry for a prefix, plus the remaining
// segments applied), or "materialize" (from the store, as before this cache
// existed). Test-only seam, nil in production (mirrors
// roCacheEvictPreRemoveHook).
var observeCheckoutSource func(kind string)

// reflinkUnsupportedForTest forces every by-chain clone to report
// reflink.ErrUnsupported, so the degrade-to-materialize path is testable on
// a clone-capable filesystem. Test-only; restore it after use.
var reflinkUnsupportedForTest bool

func cloneFile(dst, src string) error {
	if reflinkUnsupportedForTest {
		return reflink.ErrUnsupported
	}
	return reflink.Clone(dst, src)
}

// byChainEntryMode is a by-chain entry's mode: owner read-only, like the
// store's own 0600 objects and the writable checkouts (never world- or
// group-readable). The directories above it are 0700 (mkdirPrivate).
const byChainEntryMode os.FileMode = 0o400

// DefaultByChainMaxEntries bounds how many by-chain entries one database
// keeps: after every populate, CheckoutProven and CheckoutAt prune that
// database's by-chain area to this many entries, least recently used first
// (see pruneByChain). Without a bound every checkout miss on a
// clone-capable filesystem would leave one full physical copy behind for
// good, since only the daemon janitor evicts and -ro-cache-budget defaults
// to unlimited. Eviction is always safe: an entry is re-creatable content
// keyed by chain identity, and a later miss on an evicted chain simply
// rebuilds it (from a cached prefix or the store). 64 covers a deep MCTS
// spine plus its live siblings with room to spare; a workload with more
// distinct live chains per database than that pays a re-materialize on a
// miss, which is exactly the cost without this cache. -ro-cache-budget's
// byte accounting still applies on top when set.
const DefaultByChainMaxEntries = 64

// mkdirPrivate creates each directory (in order, parents first) with mode
// 0700, and tightens one that already exists with a wider mode. Cache
// content derived from the store is no more readable than the store's own
// 0600 objects.
func mkdirPrivate(dirs ...string) error {
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
		if fi, err := os.Stat(d); err == nil && fi.Mode().Perm() != 0o700 {
			if err := os.Chmod(d, 0o700); err != nil {
				return err
			}
		}
	}
	return nil
}

// pruneByChain removes db's least recently used by-chain entries (the LRU
// clock is rocache's lruClock: the .last-used marker a clone or prefix hit
// touches, else the entry's own mtime, which is when its content was
// written) until at most max remain, each with its .sum sidecar and marker.
// It only ever sees <chainID>.db files in the by-chain directory: staging
// files of an entry still being built (<id>.db.stage-*) and probe files
// are not entries, and the writable checkouts live in a separate tree.
// Best-effort: an entry that another process evicts or rebuilds
// concurrently is harmless either way, since a clone of it that loses the
// race falls through to rebuilding (see materializeFromChain).
func (w *Workspace) pruneByChain(db string, max int) {
	entries, err := byChainEntries(db, w.byChainDir(db))
	if err != nil || len(entries) <= max {
		return
	}
	// It runs after the materialize whose reclaim has already happened, so
	// it reclaims the entries it removes itself.
	defer reclaimStranded(w.byChainDir(db))
	sort.Slice(entries, func(i, j int) bool {
		if !entries[i].LastUsed.Equal(entries[j].LastUsed) {
			return entries[i].LastUsed.Before(entries[j].LastUsed)
		}
		return entries[i].Path < entries[j].Path
	})
	for _, e := range entries[:len(entries)-max] {
		if err := os.Remove(e.Path); err != nil && !os.IsNotExist(err) {
			continue
		}
		os.Remove(e.Path + ".sum")
		os.Remove(e.Path + lastUsedSuffix)
	}
}

// byChainDir is db's by-chain area: its entries, their sidecars, and the
// stage and probe files of builds in progress.
func (w *Workspace) byChainDir(db string) string {
	return filepath.Join(w.roCacheRoot(), db, byChainDir)
}

// reclaimStranded closes the dbfile descriptors this process can no longer
// reach by path, re-checking only paths (files or directories) and closing
// any other orphan nothing pins any more. Each materialization renames a
// new inode over its target, a by-chain build renames its stage file away,
// and a prune or destroy removes files, so each one would otherwise leave a
// descriptor holding a full unlinked copy of a database on disk. Anything
// still pinned (an engine or a read on the old inode) waits for a later
// pass. It runs where a strand is made, not just on the daemon's janitor
// tick, because a long-lived process may have no janitor: offshoot mcp
// reaps and checks out on its own, and serve -reap-every 0 runs none. It is
// scoped (dbfile.EvictStrandedAt) because it runs on every checkout, and a
// full re-check would stat every checkout this process has cached.
func reclaimStranded(paths ...string) { dbfile.EvictStrandedAt(paths...) }

// chainPlacement is what materializeFromChain wrote at its destination.
type chainPlacement struct {
	kind     string // see observeCheckoutSource
	chainID  string
	checksum uint64 // LTX post-apply checksum of the content
	// hash is the content's SHA-256 when known without reading dst: the
	// by-chain entry's recorded hash, since dst is a clone of it (identical
	// bytes). "" only on the no-cache fallback, where dst must be hashed.
	hash string
}

// materializeFromChain writes the content of the resolved chain members
// (lineage lineage, database db) at dst, via a temp file renamed into
// place — never in place. dst is always either a clone of an immutable
// by-chain entry or, where cloning is impossible, a plain materialize:
//
//  1. an entry for the whole chain exists: clone it to dst ("clone");
//  2. otherwise, when the filesystem can clone, build the whole-chain entry
//     first — from the longest cached prefix plus ltxio.ApplySegments of
//     the remaining members ("clone+segments"), or from the store
//     ("materialize") — and then clone it to dst;
//  3. otherwise materialize from the store straight into dst, exactly as
//     before this cache existed ("materialize", no entry).
//
// Building the entry before dst exists is what keeps the cache honest: an
// entry's bytes, hash and checksum all describe a file that was 0400
// before it was ever visible under its entry name, and nothing reads the
// writable checkout to create one — a write racing into a fresh checkout
// can corrupt that checkout only, never another branch's future clone.
//
// Any failure of a fast path (clone unsupported, an entry evicted between
// its sidecar read and the clone, a prefix apply that does not verify)
// falls through to the next; only the final direct materialize's errors
// are returned. mode is dst's mode after cloning (an entry is 0400; a writable
// checkout wants 0600 like a materialized temp file; a checkouts-ro file
// keeps 0444, inside 0700 directories).
func (w *Workspace) materializeFromChain(db, lineage string, members []store.ChainMember, dst string, mode os.FileMode) (chainPlacement, error) {
	// dst is renamed over, and a by-chain build renames its stage file away.
	defer reclaimStranded(dst, w.byChainDir(db))
	id := chainID(members)
	if entry, rec, ok := w.byChainEntry(db, id); ok {
		if err := cloneIntoPlace(entry, dst, mode); err == nil {
			touchLastUsed(entry)
			return chainPlacement{kind: "clone", chainID: id, checksum: rec.PostApplyChecksum, hash: rec.Hash}, nil
		}
	}
	if w.canCloneByChain(db, dst) {
		// Any failure here — including a store error, which the fallback
		// below then reports — or an entry evicted before the clone, falls
		// back to materializing dst directly.
		if kind, rec, err := w.buildByChainEntry(db, lineage, members, id); err == nil {
			if err := cloneIntoPlace(w.byChainPath(db, id), dst, mode); err == nil {
				return chainPlacement{kind: kind, chainID: id, checksum: rec.PostApplyChecksum, hash: rec.Hash}, nil
			}
		}
	}
	checksum, err := w.materializeMembersAt(lineage, members, dst)
	if err != nil {
		return chainPlacement{}, err
	}
	return chainPlacement{kind: "materialize", chainID: id, checksum: checksum}, nil
}

// refreshFromChain lands ref's head in the writable checkout at path the
// way CheckoutProven does — Rollback, Promote and Compact refresh through
// it: the head's chain is resolved once and handed to materializeFromChain,
// so the bytes are a clone of a by-chain entry when the chain (or a prefix
// of it) is cached, and a plain materialize where the filesystem cannot
// clone. The source is reported to observeCheckoutSource, and db's by-chain
// area is pruned after a populate. It returns the post-apply checksum and
// chain ID for the caller's stamp; the caller still stamps with writeSum,
// whose fingerprint sandwich guards a write racing the hash, and then
// refreshes the shadow.
func (w *Workspace) refreshFromChain(db string, ref store.Ref, path string) (uint64, string, error) {
	members, err := w.resolveChain(ref, headCheckpoint(ref), path)
	if err != nil {
		return 0, "", err
	}
	placed, err := w.materializeFromChain(db, ref.Lineage, members, path, 0o600)
	if err != nil {
		return 0, "", err
	}
	if observeCheckoutSource != nil {
		observeCheckoutSource(placed.kind)
	}
	if placed.kind != "clone" { // a populate may have added an entry
		w.pruneByChain(db, DefaultByChainMaxEntries)
	}
	return placed.checksum, placed.chainID, nil
}

// canCloneByChain reports whether a file in db's by-chain directory can be
// cloned into dst's directory, by cloning a tiny probe file across exactly
// that pair (a few syscalls, next to a materialize). When it cannot, the
// by-chain directory and the checkouts-ro/<db> and checkouts-ro directories
// above it are removed again when this left them empty (unless dst itself
// lives there, as a CheckoutAt file does), so a non-CoW filesystem carries
// no trace of the cache beyond the files it was asked for.
func (w *Workspace) canCloneByChain(db, dst string) bool {
	dbDir := filepath.Join(w.roCacheRoot(), db)
	dir := filepath.Join(dbDir, byChainDir)
	if err := mkdirPrivate(w.roCacheRoot(), dbDir, dir); err != nil {
		return false
	}
	ok := false
	defer func() {
		if !ok {
			// Each only succeeds while empty, innermost first — and never
			// dst's own directory (CheckoutAt's checkouts-ro/<db>), which
			// the caller is about to write into.
			os.Remove(dir)
			if filepath.Clean(filepath.Dir(dst)) != dbDir {
				os.Remove(dbDir)
				os.Remove(w.roCacheRoot())
			}
		}
	}()
	probe, err := os.CreateTemp(dir, "probe-*")
	if err != nil {
		return false
	}
	probe.Close()
	defer os.Remove(probe.Name())
	tmp, err := cloneToTemp(probe.Name(), dst)
	if err != nil {
		return false
	}
	os.Remove(tmp)
	ok = true
	return true
}

// buildByChainEntry creates the entry for chain id: its content is written
// to a staging file in the by-chain directory — applied onto the longest
// cached prefix entry when one verifies, else materialized from the store —
// made 0400, hashed, renamed to the entry name, and only then given the
// .sum sidecar that makes byChainEntry accept it. Returns how the content
// was produced ("clone+segments" or "materialize") and the entry's record.
func (w *Workspace) buildByChainEntry(db, lineage string, members []store.ChainMember, id string) (string, sumRecord, error) {
	entry := w.byChainPath(db, id)
	f, err := os.CreateTemp(filepath.Dir(entry), filepath.Base(entry)+".stage-*")
	if err != nil {
		return "", sumRecord{}, err
	}
	stage := f.Name()
	f.Close()
	defer os.Remove(stage) // a no-op once renamed to the entry

	kind := "materialize"
	checksum, applied := w.applyLongestPrefix(db, members, stage)
	if applied {
		kind = "clone+segments"
	} else if checksum, err = w.materializeMembersAt(lineage, members, stage); err != nil {
		return "", sumRecord{}, err
	}
	if err := os.Chmod(stage, byChainEntryMode); err != nil {
		return "", sumRecord{}, err
	}
	hash, err := fileSum(stage)
	if err != nil {
		return "", sumRecord{}, err
	}
	if err := os.Rename(stage, entry); err != nil {
		return "", sumRecord{}, err
	}
	if err := StampSum(entry, hash, "", 0, 0, checksum, id); err != nil {
		return "", sumRecord{}, err
	}
	return kind, sumRecord{Hash: hash, PostApplyChecksum: checksum, ChainID: id}, nil
}

// applyLongestPrefix finds the longest proper prefix of members with a
// by-chain entry and applies the remaining members onto a copy of it at
// dst (see ltxio.ApplySegments). ok=false when there is no such entry or
// its apply does not verify; a shorter prefix is not tried then, as it is
// no better than the store.
func (w *Workspace) applyLongestPrefix(db string, members []store.ChainMember, dst string) (checksum uint64, ok bool) {
	for k := len(members) - 1; k >= 1; k-- {
		entry, rec, found := w.byChainEntry(db, chainID(members[:k]))
		if !found {
			continue
		}
		readers, closeAll := w.memberReaders(members[k:])
		defer closeAll()
		_, checksum, err := ltxio.ApplySegments(entry, rec.PostApplyChecksum, readers, dst)
		if err != nil {
			return 0, false
		}
		touchLastUsed(entry)
		return checksum, true
	}
	return 0, false
}

// byChainEntry returns the by-chain entry for id when both its file and a
// sidecar naming exactly this chain (with a checksum) are present. A
// missing, old-format (no chain_id) or unreadable sidecar is a miss, never
// an error.
func (w *Workspace) byChainEntry(db, id string) (string, sumRecord, bool) {
	entry := w.byChainPath(db, id)
	rec, ok := readSidecar(entry)
	if !ok || rec.ChainID != id || rec.PostApplyChecksum == 0 {
		return "", sumRecord{}, false
	}
	return entry, rec, true
}

// cloneIntoPlace clones src to a temp file in dst's directory, sets mode,
// and renames it over dst, removing dst's stale -wal/-shm siblings as
// ltxio's finalizeDestination does.
func cloneIntoPlace(src, dst string, mode os.FileMode) error {
	tmp, err := cloneToTemp(src, dst)
	if err != nil {
		return err
	}
	if err := os.Chmod(tmp, mode); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return err
	}
	os.Remove(dst + "-wal")
	os.Remove(dst + "-shm")
	return nil
}

// cloneToTemp clones src to a fresh, not-yet-existing temp path next to
// dst and returns it.
func cloneToTemp(src, dst string) (string, error) {
	f, err := os.CreateTemp(filepath.Dir(dst), filepath.Base(dst)+".tmp-*")
	if err != nil {
		return "", err
	}
	tmp := f.Name()
	f.Close()
	if err := os.Remove(tmp); err != nil {
		return "", err
	}
	if err := cloneFile(tmp, src); err != nil {
		os.Remove(tmp)
		return "", err
	}
	return tmp, nil
}
