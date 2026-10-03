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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Inode identifies a file independently of any path naming it.
type Inode struct{ Dev, Ino uint64 }

// entry is one descriptor this package holds. path, f and ino never change
// once created; lastUse is guarded by mu.
type entry struct {
	path    string // absolute path it was opened at
	f       *os.File
	ino     Inode
	lastUse uint64
}

var (
	mu sync.Mutex
	// live holds one descriptor per absolute path, on the inode that path
	// named when the descriptor was opened.
	live = map[string]*entry{}
	// orphans holds descriptors whose path has since been renamed over or
	// removed, by inode. They stay referenced until closed explicitly: a
	// dropped *os.File is closed by its finalizer at the next GC, whether or
	// not anything still relies on its inode's locks.
	orphans = map[Inode][]*entry{}
	// pins counts open Sections and Holds per inode.
	pins = map[Inode]int{}
	// unidentified holds descriptors whose inode could not be read right
	// after opening. With no inode they cannot be checked against pins, so
	// they are never closed. Expected to stay empty.
	unidentified []*os.File
	clock        uint64 // LRU clock, advanced by touchLocked
)

// Section is a pinned reader over one database file. It reads through
// ReadAt with its own cursor, so concurrent Sections over one file never
// disturb each other or the shared descriptor's offset. Its length is the
// file's size when Reader returned it.
type Section struct {
	*io.SectionReader
	ino  Inode
	once sync.Once
}

// Close unpins the file. It never closes the descriptor (see the package
// doc), and calling it more than once is harmless.
func (s *Section) Close() error {
	s.once.Do(func() { unpin(s.ino) })
	return nil
}

// Reader returns a pinned reader over the whole current contents of the
// SQLite database file at path, backed by this package's cached descriptor
// for path. The caller must Close it: until then nothing can close the
// descriptor under it. To observe a later extension of the file, call
// Reader again.
//
// A missing file is reported as an ordinary error (os.IsNotExist applies),
// and a descriptor cached for that path is orphaned, not closed.
func Reader(path string) (*Section, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	// Stat the path, not the descriptor, so a cached descriptor can be
	// checked for still naming the same inode.
	want, statErr := os.Stat(abs)

	mu.Lock()
	defer mu.Unlock()
	if statErr != nil {
		if os.IsNotExist(statErr) {
			orphanLocked(abs)
		}
		return nil, statErr
	}
	wantIno, err := inodeOf(want)
	if err != nil {
		return nil, err
	}
	e := live[abs]
	if e != nil && e.ino != wantIno {
		// Re-materialized: the path names a new inode now. Hashing the old
		// one would answer a question about a file that no longer exists,
		// and closing it is unsafe while anything pins its inode.
		orphanLocked(abs)
		e = nil
	}
	if e == nil {
		if e, err = openLocked(abs); err != nil {
			return nil, err
		}
	}
	fi, err := e.f.Stat()
	if err != nil {
		return nil, err
	}
	pins[e.ino]++
	touchLocked(e)
	return &Section{SectionReader: io.NewSectionReader(e.f, 0, fi.Size()), ino: e.ino}, nil
}

// openLocked opens abs and caches it. The inode comes from the opened
// descriptor, not the caller's earlier stat: a rename landing in between can
// then only make the entry look stale to the next sweep, never let it borrow
// another inode's pins.
func openLocked(abs string) (*entry, error) {
	f, err := os.Open(abs)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err == nil {
		var ino Inode
		if ino, err = inodeOf(fi); err == nil {
			e := &entry{path: abs, f: f, ino: ino}
			live[abs] = e
			return e, nil
		}
	}
	unidentified = append(unidentified, f)
	return nil, err
}

// orphanLocked moves abs's cached descriptor, if any, to the orphan set.
func orphanLocked(abs string) {
	if e := live[abs]; e != nil {
		delete(live, abs)
		orphans[e.ino] = append(orphans[e.ino], e)
	}
}

func touchLocked(e *entry) {
	clock++
	e.lastUse = clock
}

func unpin(ino Inode) {
	mu.Lock()
	defer mu.Unlock()
	if pins[ino] <= 1 {
		delete(pins, ino)
	} else {
		pins[ino]--
	}
}

// PinsAt reports how many pins cover the inode path names right now. For
// tests and diagnostics.
func PinsAt(path string) (int, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	ino, err := inodeOf(fi)
	if err != nil {
		return 0, err
	}
	mu.Lock()
	defer mu.Unlock()
	return pins[ino], nil
}

// EntryInfo describes one descriptor this package holds.
type EntryInfo struct {
	Path   string
	Inode  Inode
	Orphan bool // its path was renamed over or removed
	Pins   int  // pins on its inode
}

// Entries lists every descriptor this package holds, sorted by path with a
// path's live entry before its orphans. For tests and diagnostics.
func Entries() []EntryInfo {
	mu.Lock()
	defer mu.Unlock()
	var out []EntryInfo
	for _, e := range live {
		out = append(out, EntryInfo{Path: e.path, Inode: e.ino, Pins: pins[e.ino]})
	}
	for _, es := range orphans {
		for _, e := range es {
			out = append(out, EntryInfo{Path: e.path, Inode: e.ino, Orphan: true, Pins: pins[e.ino]})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		return !out[i].Orphan && out[j].Orphan
	})
	return out
}

// ErrReplaced reports that a held path was renamed over, or removed, before
// the SQLite connection opened on it was verified (see Hold).
var ErrReplaced = errors.New("dbfile: path was replaced while a SQLite open held it")

// HoldHookForTest, when non-nil, runs inside Hold with the absolute path,
// after the pin is taken and before Hold returns: where a test renames a
// fresh file over the path to land a re-materialize between a Hold and the
// connection it guards, or records which paths were held. Test-only and
// process-global; set it and restore nil with t.Cleanup.
var HoldHookForTest func(path string)

// Hold pins the inode path names now, on behalf of an in-process SQLite open
// of that file. Take it BEFORE sql.Open, and release it only AFTER every
// connection from that open is closed: defer release() before deferring the
// database's Close, so it runs last on every return path. database/sql opens
// lazily, so once the first real connection exists (db.Conn), call
// Verify(path, ino). If the path was renamed over in between, that
// connection is on a file this pin does not cover. Every materialization
// renames a fresh inode over the path, so an inode never reappears there,
// and Hold, Conn, then Verify is sufficient.
//
// Hold opens no descriptor. It touches path's cached descriptor, if that
// still names the held inode, for Evict's least-recently-used order. release
// is safe to call more than once.
func Hold(path string) (release func(), ino Inode, err error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, Inode{}, err
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return nil, Inode{}, err
	}
	if ino, err = inodeOf(fi); err != nil {
		return nil, Inode{}, err
	}
	mu.Lock()
	pins[ino]++
	if e := live[abs]; e != nil && e.ino == ino {
		touchLocked(e)
	}
	mu.Unlock()
	var once sync.Once
	release = func() { once.Do(func() { unpin(ino) }) }
	if HoldHookForTest != nil {
		HoldHookForTest(abs)
	}
	return release, ino, nil
}

// Verify reports whether path still names ino, the inode a Hold pinned.
// Call it right after the held open's first real connection (db.Conn): on
// ErrReplaced, close the connection, release the hold and return the error.
// A path that is gone wraps both ErrReplaced and the stat error.
func Verify(path string, ino Inode) error {
	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrReplaced, path, err)
	}
	got, err := inodeOf(fi)
	if err != nil {
		return err
	}
	if got != ino {
		return fmt.Errorf("%w: %s", ErrReplaced, path)
	}
	return nil
}
