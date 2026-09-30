package ops

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"

	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/ops/reflink"
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

// chainPlacement is what materializeFromChain wrote at its destination.
type chainPlacement struct {
	kind     string // see observeCheckoutSource
	chainID  string
	checksum uint64 // LTX post-apply checksum of the content
	// hash is the content's SHA-256 when known without reading the file
	// (a whole-chain clone carries its entry's recorded hash: identical
	// bytes); "" when the content was freshly written and must be hashed.
	hash string
}

// materializeFromChain writes the content of the resolved chain members
// (ref lineage lineage, database db) into dst via a temp file renamed into
// place — never in place — preferring, in order:
//
//  1. a by-chain entry for the whole chain: cloned, chmod'd to mode,
//     renamed over dst;
//  2. a by-chain entry for the longest proper prefix: ltxio.ApplySegments
//     from it with only the remaining members fetched from the store;
//  3. a full materialize from the store, exactly as before this cache.
//
// Any failure of a fast path (clone unsupported, an entry evicted between
// its sidecar read and the clone, a prefix apply that does not verify)
// falls through to the next; only the full materialize's own errors are
// returned. dst's stale -wal/-shm siblings are removed after a rename, as
// ltxio's finalizeDestination does. mode applies to a cloned file only
// (an entry is 0444; a writable checkout wants 0600 like a materialized
// temp file, a checkouts-ro file keeps 0444).
func (w *Workspace) materializeFromChain(db, lineage string, members []store.ChainMember, dst string, mode os.FileMode) (chainPlacement, error) {
	id := chainID(members)
	if entry, rec, ok := w.byChainEntry(db, id); ok {
		if err := cloneIntoPlace(entry, dst, mode); err == nil {
			touchLastUsed(entry)
			return chainPlacement{kind: "clone", chainID: id, checksum: rec.PostApplyChecksum, hash: rec.Hash}, nil
		}
	}
	for k := len(members) - 1; k >= 1; k-- {
		entry, rec, ok := w.byChainEntry(db, chainID(members[:k]))
		if !ok {
			continue
		}
		if checksum, err := w.applyMembersOnto(entry, rec.PostApplyChecksum, members[k:], dst); err == nil {
			touchLastUsed(entry)
			return chainPlacement{kind: "clone+segments", chainID: id, checksum: checksum}, nil
		}
		break // the longest cached prefix failed; a shorter one is no better than the store
	}
	checksum, err := w.materializeMembersAt(lineage, members, dst)
	if err != nil {
		return chainPlacement{}, err
	}
	return chainPlacement{kind: "materialize", chainID: id, checksum: checksum}, nil
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
// and renames it over dst.
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

// applyMembersOnto applies the segment members onto a copy of start (whose
// checksum is startChecksum) and renames the result over dst — see
// ltxio.ApplySegments.
func (w *Workspace) applyMembersOnto(start string, startChecksum uint64, segments []store.ChainMember, dst string) (uint64, error) {
	readers, closeAll := w.memberReaders(segments)
	defer closeAll()
	_, checksum, err := ltxio.ApplySegments(start, startChecksum, readers, dst)
	return checksum, err
}

// populateByChain records src (content of chain id, with the given SHA-256
// and post-apply checksum) as the by-chain entry for id: cloned into the
// entry directory, chmod 0444, renamed into place, then its sidecar
// written. Best-effort and silent — the checkout it follows has already
// succeeded — and a no-op where the filesystem cannot clone, so a non-CoW
// filesystem never pays a second full write (and gets no by-chain
// directory at all).
//
// src is a file this process has just renamed into place (a fresh writable
// checkout, or a checkouts-ro file): no SQLite connection anywhere holds
// its new inode yet, so the clone's brief open of it (FICLONE on Linux)
// cannot drop anyone's POSIX locks.
func (w *Workspace) populateByChain(db, id, src, hash string, checksum uint64) {
	entry := w.byChainPath(db, id)
	dir := filepath.Dir(entry)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	tmp, err := cloneToTemp(src, entry)
	if err != nil {
		os.Remove(dir) // only succeeds while empty: leave no trace of a failed populate
		return
	}
	if err := os.Chmod(tmp, 0o444); err != nil {
		os.Remove(tmp)
		return
	}
	if err := os.Rename(tmp, entry); err != nil {
		os.Remove(tmp)
		return
	}
	_ = StampSum(entry, hash, "", 0, 0, checksum, id)
}
