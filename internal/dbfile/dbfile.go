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
//   - MITIGATED, not removed: ops.Checkout now skips materialization when
//     checkoutState reports the checkout is already clean and current,
//     instead of re-materializing regardless. This genuinely removes the
//     per-session-open stranding above for at-rest/CLI-style reopen
//     patterns — anything that closes (or never opens under a daemon at
//     all) before the checkout's `.sum` sidecar goes stale. It does NOT
//     remove it for the daemon's own default config: the settling flush
//     (internal/session's first auto-flush after open, landing ~30s later
//     under `-flush-every`'s default) advances the branch ref's head txid,
//     but no clean Session.Close rewrites the sidecar to match — only
//     Checkout/Checkpoint/Rollback/Promote do (see ops.go's writeSum call
//     sites) — so a session that outlives its own settling flush leaves the
//     sidecar stale the moment it closes, and the NEXT session.Open on that
//     branch re-materializes exactly as before this mitigation landed. Two
//     ledgered follow-ups, neither built this pass: suppressing the
//     settling flush's own re-baseline cost when content is provably
//     unchanged (docs/status.md's "Settling-flush checksum-compare
//     suppression" row), and refreshing the sidecar on a clean Close so
//     reopen-after-settling stays clean too (docs/status.md's "Sidecar
//     refresh on clean Close" row).
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
