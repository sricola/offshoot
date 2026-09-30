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
// entry's bytes, hash and checksum all describe a file that was 0444
// before it was ever visible under its entry name, and nothing reads the
// writable checkout to create one — a write racing into a fresh checkout
// can corrupt that checkout only, never another branch's future clone.
//
// Any failure of a fast path (clone unsupported, an entry evicted between
// its sidecar read and the clone, a prefix apply that does not verify)
// falls through to the next; only the final direct materialize's errors
// are returned. mode is dst's mode after cloning (an entry is 0444; a writable
// checkout wants 0600 like a materialized temp file; a checkouts-ro file
// keeps 0444).
func (w *Workspace) materializeFromChain(db, lineage string, members []store.ChainMember, dst string, mode os.FileMode) (chainPlacement, error) {
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

// canCloneByChain reports whether a file in db's by-chain directory can be
// cloned into dst's directory, by cloning a tiny probe file across exactly
// that pair (a few syscalls, next to a materialize). When it cannot, the
// by-chain directory is removed again if this left it empty, so a non-CoW
// filesystem carries no trace of the cache.
func (w *Workspace) canCloneByChain(db, dst string) bool {
	dir := filepath.Join(w.roCacheRoot(), db, byChainDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false
	}
	ok := false
	defer func() {
		if !ok {
			os.Remove(dir) // only succeeds while empty
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
// made 0444, hashed, renamed to the entry name, and only then given the
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
	if err := os.Chmod(stage, 0o444); err != nil {
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
