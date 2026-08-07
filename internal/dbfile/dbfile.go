// Package dbfile hands out read-only descriptors on SQLite database files
// that are opened at most once per process and, deliberately, NEVER closed.
//
// # Why this package exists
//
// POSIX advisory locks (fcntl F_SETLK), which is what SQLite uses on unix,
// are keyed by (process, inode) rather than by file descriptor. The
// consequence is the single most dangerous footgun in this codebase:
//
//	Closing ANY descriptor a process holds on a file releases EVERY lock
//	that process holds on that file — including locks taken by a completely
//	unrelated descriptor, in unrelated code, on a connection this code has
//	never heard of.
//
// SQLite holds a SHARED lock on the main database file for a WAL-mode
// connection's entire lifetime and tracks that lock's state in process
// memory. So a stray open/close of a database file anywhere in this process
// silently drops the kernel-side lock of every SQLite connection this
// process has on it, while SQLite still believes it holds them — and
// therefore never re-acquires them. Nothing returns an error. The capture
// engine, for instance, then runs completely unlocked while reporting
// healthy: the next foreign writer to exit wins the exclusive lock its
// close-time cleanup wants, checkpoints the WAL into the main database and
// unlinks -wal/-shm, and the WAL reader sees an empty file for the rest of
// the session.
//
// Reading a database file's raw bytes therefore cannot be done with an
// ordinary os.Open/defer Close. It must be done through a descriptor that
// outlives every SQLite connection that could possibly be co-resident with
// it — which, since this package cannot know what else the process has open
// or when it will close, means a descriptor that is never closed at all.
//
// # What this costs
//
// This is the unattractive half of the trade and it is worse than "one fd per
// distinct database file". Retention scales with SESSION OPENS and with
// checkout deletions — both of which are ordinary runtime events, not rare
// user-initiated ones. Do not reason about it as bounded by the number of
// distinct checkouts.
//
// Two ways a descriptor becomes stranded, i.e. retained for the life of the
// process while referring to an inode nothing can reach any more. A stranded
// descriptor pins not just a file descriptor but the unlinked inode's disk —
// a FULL COPY of that database — until the process exits:
//
//   - Re-materialization. ops.Checkout materializes unconditionally, and
//     ltxio.Materialize writes a temp file and os.Renames it over the
//     checkout path. So every session.Open gives that path a brand-new inode
//     and strands the descriptor for the previous one. Measured: 2 stranded
//     descriptors after 5 session open/close cycles on a SINGLE branch.
//
//   - Deletion. The janitor's automatic reap, and Destroy, os.Remove the
//     checkout path (see ops/gc.go). handle() stats the path before it gets
//     as far as revalidating the cached descriptor, so once the path is gone
//     it returns early on ENOENT and the map entry is never revisited — the
//     descriptor, and the deleted database's disk, linger for the life of the
//     process with nothing that can ever reclaim them.
//
// This is accepted deliberately, because the alternative is not a smaller
// leak but silent data loss: closing at the wrong moment unlocks a live
// capture engine and loses every subsequent write, with no error anywhere.
// A bounded-but-real disk cost is the better failure mode. It is a genuine
// cost, though, not a rounding error, and it should not stay this way.
//
// Tracked follow-ups, in order of how much they buy:
//
//   - MITIGATED for real now, including the daemon's default config:
//     ops.Checkout skips materialization when checkoutState reports the
//     checkout is already clean and current, instead of re-materializing
//     regardless. This originally only genuinely removed the per-session-
//     open stranding above for at-rest/CLI-style reopen patterns, because
//     nothing kept the checkout's `.sum` sidecar current across a daemon
//     session's own writes: the settling flush (internal/session's first
//     auto-flush after open) advances the branch ref's head txid, but no
//     clean Session.Close rewrote the sidecar to match, so a session that
//     outlived its own settling flush left the sidecar stale the moment it
//     closed and the NEXT session.Open re-materialized anyway. Two ledgered
//     follow-ups have since landed and close that gap: (1) the settling
//     flush itself is now skipped, but only when BOTH Open's checkout was
//     already proven clean-and-current at the moment Open checked AND the
//     LTX checksum recorded in that SAME checkout's .sum sidecar — never
//     fetched fresh from the store; an earlier, since-reverted version of
//     this fix did fetch it fresh on every Open, which meant downloading
//     the entire head object whenever it happened to be a full snapshot,
//     defeating the point for exactly the read-only-reopen case this
//     targets — exactly matches what the checkout actually contains once
//     the session's real startup rebase finishes running (rebaseline's
//     checksum-suppression, see internal/session/session.go's rebaseline
//     doc comment for why the second check is load-bearing, not redundant:
//     Open can return before its own startup rebase finishes, so a write
//     landing in that window needs the checksum comparison to be caught at
//     all). When both hold, a read-only reopen of an unmodified checkout
//     never even reaches a flush that could go stale, and Open pays no
//     store call beyond the two tiny ref-metadata reads it always needed.
//     (2) a clean Session.Close now refreshes the
//     sidecar itself (Session.commitSidecarRefresh), but ONLY by reusing the
//     capture engine's OWN post-shutdown fingerprint — persisted only once
//     its shutdown fully verifies the checkout's WAL was cleanly and
//     completely folded in (capture.State.Clean/MainHash) — never an
//     independently re-derived one; a session that DID write still leaves
//     the next reopen clean, but shutdown's own unmet verification (a
//     foreign write racing its final checkpoint, most directly) leaves the
//     sidecar unstamped rather than risk fingerprinting content shutdown
//     itself refused to vouch for. Further real limits, all deliberately
//     conservative rather than risking a false "clean": a Close after a
//     failed flush, a fenced session, or a session that ever took a
//     mid-session rebase-on-divergence (its replica's provenance is no
//     longer a straight line back to the checkout Open seeded it from — see
//     Session.singleStartupRebase) does not refresh the sidecar, so that one
//     reopen still pays the full re-materialize-and-strand cost above.
//     Outside those cases, the daemon's default config now stays flat on
//     reopen the same way at-rest/CLI reopen already did. One accepted,
//     ledgered tradeoff of the clean-skip mechanism itself (not new here,
//     just now reachable via this path too): a clean-and-current checkout
//     is served without ever consulting the object store's chain, so a
//     chain corrupted after the sidecar was stamped goes undetected until
//     something else forces a re-materialize — see docs/status.md's
//     "Clean-and-current checkout served without chain validation" row.
//   - Reclaim map entries for paths that no longer exist, so deletion stops
//     being permanent. (Closing the stranded descriptor is the part that
//     needs care: it is only safe once nothing in the process can still hold
//     SQLite locks on that inode.)
//   - Remove the need to read these files raw at all — snapshots through
//     SQLite's online backup API, fingerprints through a cumulative checksum
//     the writer maintains. That is Plan 2's direction (see the capture
//     engine's hashSrc doc comment) and retires this package entirely.
//
// Until then, this package is the single chokepoint: raw reads of a live
// SQLite database file go through here, or they are a bug.
//
// # What is NOT covered
//
// Only the main database file matters for this hazard in practice, but that
// is a statement about what this codebase opens, not about SQLite: SQLite
// takes its WAL locks (read marks, write, checkpoint, recovery) as fcntl
// locks on the -shm descriptor, so closing a stray descriptor on a -shm file
// would drop those exactly the same way. Nothing here opens -shm, which is
// the only reason it is safe. The -wal file is not fcntl-locked by SQLite,
// so wal.Reader's per-poll open/close of it is genuinely safe — verified
// empirically, not assumed.
package dbfile

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	mu      sync.Mutex
	handles = map[string]*os.File{}
)

// Reader returns a reader over the whole current contents of the SQLite
// database file at path, backed by a descriptor this package keeps open for
// the life of the process (see the package doc for why it is never closed).
//
// The returned *io.SectionReader is safe to use while other callers hold
// readers on the same file: it carries its own cursor and addresses the file
// through ReadAt, so it never disturbs — and is never disturbed by — the
// shared descriptor's own file offset. It is a snapshot of the file's length
// at the moment of this call; callers wanting to observe a subsequent
// extension must call Reader again.
//
// A missing file is reported as an ordinary error (os.IsNotExist applies);
// no descriptor is cached for it.
func Reader(path string) (*io.SectionReader, error) {
	f, size, err := handle(path)
	if err != nil {
		return nil, err
	}
	return io.NewSectionReader(f, 0, size), nil
}

// handle returns the process-wide descriptor for path, opening it on first
// use, and its current size.
//
// The cached descriptor is validated against the path on every call rather
// than trusted indefinitely: a checkout that has been re-materialized
// (write-temp-then-rename) leaves the cached descriptor pointing at the old,
// now-unlinked inode, and hashing that would silently answer a question
// about a file that no longer exists. On a mismatch the stale entry is
// dropped and a fresh descriptor opened — but the stale one is still not
// closed, because some SQLite connection in this process may hold locks on
// that old inode and closing is exactly what must never happen.
func handle(path string) (*os.File, int64, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, 0, err
	}
	// Stat the path (not the descriptor) first, so a cached descriptor can be
	// checked for still naming the same inode.
	want, err := os.Stat(abs)
	if err != nil {
		return nil, 0, err
	}

	mu.Lock()
	defer mu.Unlock()

	if f, ok := handles[abs]; ok {
		if have, err := f.Stat(); err == nil && os.SameFile(have, want) {
			return f, have.Size(), nil
		}
		// Different inode at the same path now. Drop the mapping; never close.
		delete(handles, abs)
	}

	f, err := os.Open(abs)
	if err != nil {
		return nil, 0, err
	}
	// Cache before anything else can fail, so a later error cannot strand an
	// uncached descriptor that we are also not allowed to close.
	handles[abs] = f
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
