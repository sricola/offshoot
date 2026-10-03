// Package dbfile hands out read-only descriptors on SQLite database files,
// cached one per path, and closes a descriptor only when nothing in this
// process can be relying on the locks of the inode it names.
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
// ordinary os.Open/defer Close. It must go through a descriptor whose close
// is ordered against every SQLite connection in this process that could
// hold locks on the same inode. This package is that ordering.
//
// # Pins
//
// A pin is a count on an inode (device and inode number, never a path:
// locks belong to the inode, and two paths can name one). Two kinds of
// caller take one:
//
//   - Reader returns a Section over the cached descriptor, pinned until the
//     caller closes it, so a read in progress never has its descriptor
//     closed under it. Section.CloneTo makes a copy-on-write clone through
//     that same descriptor, for a caller that would otherwise open the file
//     to clone it.
//   - Hold is taken by every in-process SQLite open of a file this package
//     may cache, BEFORE sql.Open, and released only AFTER the connection is
//     closed: defer the release first, so it runs last on every return
//     path. The open never creates the file (NoCreateDSN, or a mode=ro URI),
//     so a path removed after the Hold fails to open rather than coming back
//     as an empty database.
//
// A Hold covers its path as well as its inode, because the pin alone cannot
// cover the connection. database/sql connects lazily, and the connect itself
// takes SHARED: go-sqlite3 runs a schema-reading PRAGMA on every new
// connection, and a WAL-mode connection keeps that lock for its whole life.
// A re-materialize that renames a fresh inode over the path between the Hold
// and the connect leaves the connection locking an inode the pin does not
// cover, before the caller can check anything. A lock dropped there is not
// one connection's problem either: SQLite tracks lock state per inode for
// the whole process, so the next connection here on that inode takes SHARED
// by bumping a count, with no fcntl, and then runs unlocked however
// carefully it was pinned and verified. Every inode the held open can land
// on is one its path named while the Hold was active, so while any Hold on a
// path is active, nothing cached under that path is closed, whatever inode
// it names. Verify, called right after the first real connection (db.Conn),
// then tells the caller whether that connection is on the file the path
// names now (ErrReplaced if not): a file renamed over is no longer the
// checkout, and working on it would be wrong even though it is now safe.
//
// Closing a descriptor is safe when nothing pins its inode and no Hold is
// active on its path: no read is using it and no in-process SQLite
// connection is open on that inode, so the close has no locks to drop. That
// premise holds only while every in-process SQLite open takes a Hold, and
// while no checkout inode has a second name (offshoot never hard-links one).
// sites_test.go fails on any sql.Open, sql.OpenDB or go-sqlite3 driver use
// that is not on its reviewed list, and on a pinned one that does not defer
// its release before the open. Another process's connections are
// unaffected: POSIX locks are per process.
//
// # Orphans and eviction
//
// A cached descriptor is orphaned when its path is renamed over (every
// checkout re-materialization, and every by-chain cache build's stage
// file) or removed (destroy, reap, cache eviction). Reader notices this
// when next asked for that path, and EvictStranded re-checks every cached
// path, because a deleted checkout is never asked for again. Until closed,
// an orphan pins the unlinked inode's disk: a full copy of the database.
//
//   - EvictStranded closes every unpinned orphan, re-checking every cached
//     path first. The daemon's janitor calls it every tick, which also
//     catches what evicting the read-only cache strands.
//   - EvictStrandedAt re-checks only the paths its caller names, then
//     closes every unpinned orphan the same way. ops calls it after every
//     materialization, by-chain prune and destroy, where those strands are
//     made, so a process with no janitor reclaims the strands it makes
//     itself, at a cost that does not grow with the number of cached
//     checkouts. A checkout another process removes or replaces is found
//     only by the full EvictStranded: offshoot mcp's reaper runs it on
//     every tick too, and serve -reap-every 0 keeps such descriptors until
//     restart.
//   - Evict(keep) closes unpinned cached descriptors, least recently used
//     first, until at most keep remain (serve -fd-budget).
//
// A pinned orphan stays open: it means a session or a read outlived its
// file. So does an orphan whose path a Hold is active on, whatever its
// inode: a session open across two re-materializations of its checkout
// keeps the middle file's descriptor. For a while that is normal. If it
// persists, it is a pin leak, which ReadStats().StrandedPinned
// (offshoot_dbfile_stranded_pinned) reports, counting both.
//
// A close is decided under the registry lock: nothing pins the inode and no
// Hold is active on the path, so the entry is dropped from the registry and
// its path and inode are marked closing. The close itself then runs with the
// lock released (closeMarked): the last close of an unlinked file frees its
// blocks inside close(2), tens of milliseconds per GiB, and every Reader,
// Hold and release in the process takes that lock. A Hold waits for every
// close in flight on its path or inode before it returns, so no connection
// it guards can take a lock on that inode while such a close is running,
// and a close decided after the Hold skips its path and inode.
//
// Follow-ups, in order of how much they buy:
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
//     reopen still pays a full re-materialize (and orphans the old
//     descriptor until EvictStranded closes it). Outside those cases, the
//     daemon's default config now stays flat on reopen the same way
//     at-rest/CLI reopen already did. One accepted,
//     ledgered tradeoff of the clean-skip mechanism itself (not new here,
//     just now reachable via this path too): a clean-and-current checkout
//     is served without ever consulting the object store's chain, so a
//     chain corrupted after the sidecar was stamped goes undetected until
//     something else forces a re-materialize — see docs/status.md's
//     "Clean-and-current checkout served without chain validation" row.
//   - Remove the need to read these files raw at all — snapshots through
//     SQLite's online backup API, fingerprints through a cumulative checksum
//     the writer maintains. That is Plan 2's direction (see the capture
//     engine's hashSrc doc comment) and retires this package entirely.
//
// Until then, this package is the single chokepoint: raw reads of a live
// SQLite database file go through here, or they are a bug. Two exceptions
// are known:
//
//   - reflink.Clone opens and closes its source on Linux, so a checkout is
//     cloned only through Section.CloneTo, never through reflink.Clone.
//   - ops.CheckpointWith raw-opens and closes the checkout in three places:
//     its snapshot encode (ltxio.EncodeSnapshot), its segment diff
//     (diffPages) and its stamp's checksum (ltxio.ChecksumDatabase). Only
//     the CLI and offshoot mcp call it, and the daemon must never (see its
//     doc comment), so no session's capture engine shares its process. That
//     is the only containment. In offshoot mcp, the reaper goroutine runs
//     beside tool calls, so a TTL fork reaped while it is being checkpointed
//     has destroy's quiesce connection open on the checkout, and any of
//     those closes drops that connection's locks. The checkpoint's branch
//     lease narrows this to its stamp: from its acquire to its head write
//     the lease keeps a reap off the branch, and the head write stamps the
//     activity clock, so only a fork whose TTL runs out during the stamp
//     is reaped under it. This predates the package and is left in place:
//     the connection only folds the WAL of a branch that is being
//     destroyed.
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
	"strings"
	"sync"

	"github.com/sricola/offshoot/internal/reflink"
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
	// held counts active Holds per absolute path. While a path is held,
	// nothing cached under it is closed, whatever inode it names (see the
	// package doc's Pins).
	held = map[string]int{}
	// closingPath and closingIno count closes in flight: descriptors already
	// dropped from the registry and decided closable, whose Close has not
	// returned yet. Hold waits for both to clear on its path and inode, and
	// closeDone is broadcast each time one does.
	closingPath = map[string]int{}
	closingIno  = map[Inode]int{}
	closeDone   = sync.NewCond(&mu)
	// unidentified holds descriptors whose inode could not be read right
	// after opening. With no inode they cannot be checked against pins, so
	// nothing closes them. Expected to stay empty.
	unidentified []*os.File
	clock        uint64 // LRU clock, advanced by touchLocked
	// Cumulative closes, for metrics: orphans closed by EvictStranded and
	// cached descriptors closed by Evict.
	evictedStranded, evictedBudget uint64
)

// Section is a pinned reader over one database file. It reads through
// ReadAt with its own cursor, so concurrent Sections over one file never
// disturb each other or the shared descriptor's offset. Its length is the
// file's size when Reader returned it.
type Section struct {
	*io.SectionReader
	f    *os.File
	ino  Inode
	once sync.Once
}

// Close unpins the file. It never closes the descriptor (see the package
// doc), and calling it more than once is harmless.
func (s *Section) Close() error {
	s.once.Do(func() { unpin(s.ino) })
	return nil
}

// CloneTo makes dst a copy-on-write clone of the file, through the pinned
// descriptor (reflink.CloneFrom), or returns reflink.ErrUnsupported without
// creating dst. Use it, never reflink.Clone, to clone a file an in-process
// SQLite connection may have open: Clone opens and closes its own
// descriptor on the file on Linux, and that close drops the connection's
// locks. Call it before Close.
func (s *Section) CloneTo(dst string) error { return reflink.CloneFrom(dst, s.f) }

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
	return &Section{SectionReader: io.NewSectionReader(e.f, 0, fi.Size()), f: e.f, ino: e.ino}, nil
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
	decLocked(pins, ino)
}

// decLocked decrements m[k], deleting the key at zero so the maps stay as
// small as what is actually pinned, held or closing.
func decLocked[K comparable](m map[K]int, k K) {
	if m[k] <= 1 {
		delete(m, k)
	} else {
		m[k]--
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
	Held   int  // active Holds on its path, which keep it open whatever its inode
}

// Entries lists every descriptor this package holds, sorted by path with a
// path's live entry before its orphans. For tests and diagnostics.
func Entries() []EntryInfo {
	mu.Lock()
	defer mu.Unlock()
	var out []EntryInfo
	for _, e := range live {
		out = append(out, EntryInfo{Path: e.path, Inode: e.ino, Pins: pins[e.ino], Held: held[e.path]})
	}
	for _, es := range orphans {
		for _, e := range es {
			out = append(out, EntryInfo{Path: e.path, Inode: e.ino, Orphan: true, Pins: pins[e.ino], Held: held[e.path]})
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

// VerifyHookForTest, when non-nil, runs inside Verify with the absolute
// path once the held open's first connection is known to be on the held
// inode: the one moment a test can see that connection open, where it
// checks that the pin is still in place. Test-only and process-global, like
// HoldHookForTest.
var VerifyHookForTest func(path string)

// ReleaseHookForTest, when non-nil, runs with the held absolute path the
// first time a Hold's release is called, just before the pin is dropped:
// where a test checks that the connection the Hold guards is already
// closed. A pin that is in place at Verify and dropped only after the close
// covers the connection's whole life, since the Hold came before sql.Open.
// Test-only and process-global, like HoldHookForTest.
var ReleaseHookForTest func(path string)

// Hold pins the inode path names now, and the path itself, on behalf of an
// in-process SQLite open of that file. Take it BEFORE sql.Open, and release
// it only AFTER every connection from that open is closed: defer release()
// before deferring the database's Close, so it runs last on every return
// path. Until then nothing this package caches under path is closed,
// whatever inode it names, so the connection's locks are safe even if a
// re-materialize lands before database/sql's lazy connect (see the package
// doc's Pins). Once the first real connection exists (db.Conn), call
// Verify(path, ino): ErrReplaced means the connection is on a file the path
// no longer names. Open with NoCreateDSN (or a mode=ro URI): Hold fails on a
// path that is already missing, and an open that cannot create keeps one
// removed after the Hold from coming back empty.
//
// Hold opens no descriptor. Before it returns, it waits for any close of a
// descriptor under path or on the inode that is still in flight. It touches
// path's cached descriptor, if that still names the held inode, for Evict's
// least-recently-used order. release is safe to call more than once.
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
	// A close decided before this Hold may still be running. The connection
	// this Hold guards must not take its first lock until it is done, or the
	// close would drop that lock. Closes decided from here on skip the path
	// and the inode.
	for closingPath[abs] > 0 || closingIno[ino] > 0 {
		closeDone.Wait()
	}
	held[abs]++
	pins[ino]++
	if e := live[abs]; e != nil && e.ino == ino {
		touchLocked(e)
	}
	mu.Unlock()
	var once sync.Once
	release = func() {
		once.Do(func() {
			if ReleaseHookForTest != nil {
				ReleaseHookForTest(abs)
			}
			mu.Lock()
			decLocked(held, abs)
			decLocked(pins, ino)
			mu.Unlock()
		})
	}
	if HoldHookForTest != nil {
		HoldHookForTest(abs)
	}
	return release, ino, nil
}

// Verify reports whether path still names ino, the inode a Hold pinned.
// Call it right after the held open's first real connection (db.Conn): on
// ErrReplaced, close the connection, release the hold and return the error.
// A path that is gone wraps both ErrReplaced and the stat error.
//
// It compares inode numbers, and Hold keeps no descriptor on the held inode.
// On a filesystem that hands a freed inode number straight to a new file,
// two re-materializations between the Hold and Verify could therefore let
// Verify pass for a connection on the file renamed in between. The Hold's
// path still keeps that connection's locks safe; only Verify's "this is the
// file the path names" answer can be wrong, and only in that double race.
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
	if VerifyHookForTest != nil {
		if abs, err := filepath.Abs(path); err == nil {
			VerifyHookForTest(abs)
		}
	}
	return nil
}

// NoCreateDSN returns a go-sqlite3 data source name that opens path
// read-write but never creates it, followed by params (go-sqlite3's own
// _-prefixed options, which SQLite ignores). Every held open uses it.
// SQLite's default open creates a missing file, so a checkout removed
// between Hold's stat and the lazy connection would come back as an empty
// database at its path, which Verify could then only report, not undo.
//
// The form is a SQLite URI filename with mode=rw, which governs only the
// main database file (-wal and -shm are created as usual, and a
// write-protected file still falls back to a read-only open). The path is
// escaped so the URI names exactly that file: '?' and '#' would end it, '%'
// would start an escape, and a leading "//" would read as an authority.
func NoCreateDSN(path, params string) string {
	var b strings.Builder
	b.WriteString("file:")
	for i := 0; i < len(path); i++ {
		c := path[i]
		if c == '%' || c == '?' || c == '#' || (i == 1 && c == '/' && path[0] == '/') {
			fmt.Fprintf(&b, "%%%02X", c)
			continue
		}
		b.WriteByte(c)
	}
	b.WriteString("?mode=rw")
	if params != "" {
		b.WriteString("&" + params)
	}
	return b.String()
}

// EvictStranded closes every orphaned descriptor whose inode nothing pins
// and reports how many it closed. It first re-checks every cached path:
// a deleted checkout is never asked for again, so Reader alone would never
// notice it is gone.
func EvictStranded() int { return evictStranded(nil) }

// EvictStrandedAt is EvictStranded for a caller that knows which paths it
// has just renamed over or removed: the re-check stats only the cached
// descriptors at or under paths (each a file or a directory), so its cost
// does not grow with every checkout this process has cached. It still closes
// every unpinned orphan, wherever it is: closing one needs no stat, and an
// orphan that was still pinned when its own path was reclaimed (a session
// outliving a re-materialize) is closed by whichever pass comes next. ops
// calls it where it makes strands; the daemon's janitor and offshoot mcp's
// reaper run the full EvictStranded, which also catches paths removed by
// anything else, another process included.
func EvictStrandedAt(paths ...string) int {
	var roots []string
	for _, p := range paths {
		if abs, err := filepath.Abs(p); err == nil {
			roots = append(roots, abs)
		}
	}
	sweep(func(p string) bool {
		for _, r := range roots {
			if p == r || strings.HasPrefix(p, r+string(filepath.Separator)) {
				return true
			}
		}
		return false
	})
	return closeOrphans(nil)
}

func evictStranded(in func(string) bool) int {
	sweep(in)
	return closeOrphans(in)
}

// closeOrphans closes the orphans whose path in accepts (every one when in
// is nil), unless their inode is pinned or their path held, and reports how
// many.
func closeOrphans(in func(string) bool) int {
	mu.Lock()
	var victims []*entry
	for ino, es := range orphans {
		if pins[ino] > 0 {
			continue // a pinned orphan is a session or read outliving its file; see ReadStats
		}
		kept := es[:0]
		for _, e := range es {
			if (in != nil && !in(e.path)) || held[e.path] > 0 {
				kept = append(kept, e)
				continue
			}
			victims = append(victims, e)
		}
		if len(kept) == 0 {
			delete(orphans, ino)
		} else {
			orphans[ino] = kept
		}
	}
	evictedStranded += uint64(len(victims))
	markClosingLocked(victims)
	mu.Unlock()
	closeMarked(victims)
	return len(victims)
}

// markClosingLocked marks each of es, already dropped from the registry, as
// closing, so a Hold on its path or inode waits for closeMarked.
func markClosingLocked(es []*entry) {
	for _, e := range es {
		closingPath[e.path]++
		closingIno[e.ino]++
	}
}

// closeMarked closes descriptors markClosingLocked marked, with mu NOT held
// (see the package doc's "Orphans and eviction"), clearing each mark once
// its close returns. Every close of a registry descriptor goes through here.
// closeHookForTest runs just before each close.
func closeMarked(es []*entry) {
	for _, e := range es {
		if closeHookForTest != nil {
			closeHookForTest(e)
		}
		e.f.Close()
		mu.Lock()
		decLocked(closingPath, e.path)
		decLocked(closingIno, e.ino)
		closeDone.Broadcast()
		mu.Unlock()
	}
}

var closeHookForTest func(e *entry)

// sweep orphans every cached descriptor whose path is gone or names another
// inode now. The stats run outside mu so a slow filesystem never stalls
// readers. A path cannot come to name a stale entry's inode again: that
// inode stays allocated while the descriptor is open, and nothing in
// offshoot ever renames an old checkout file back into place.
func sweep(in func(string) bool) {
	mu.Lock()
	cands := make([]*entry, 0, len(live))
	for _, e := range live {
		if in == nil || in(e.path) {
			cands = append(cands, e)
		}
	}
	mu.Unlock()
	for _, e := range cands {
		fi, err := os.Stat(e.path)
		stale := os.IsNotExist(err)
		if err == nil {
			if ino, ierr := inodeOf(fi); ierr == nil && ino != e.ino {
				stale = true
			}
		}
		if !stale {
			continue
		}
		mu.Lock()
		if live[e.path] == e {
			orphanLocked(e.path)
		}
		mu.Unlock()
	}
}

// Stats is a point-in-time view of the registry, for metrics and tests.
type Stats struct {
	Cached          int    // live descriptors, one per path: what Evict bounds
	Orphaned        int    // descriptors whose path was renamed over or removed
	Unidentified    int    // opened but never identified; never closable (expected 0)
	Pins            int    // outstanding pins, summed over inodes
	StrandedPinned  int    // orphans kept open: their inode is pinned, or a Hold is active on their path
	EvictedStranded uint64 // orphans closed, ever
	EvictedBudget   uint64 // cached descriptors closed by Evict, ever
}

// Descriptors is every descriptor this package holds open.
func (s Stats) Descriptors() int { return s.Cached + s.Orphaned + s.Unidentified }

// ReadStats returns the registry's current counts.
func ReadStats() Stats {
	mu.Lock()
	defer mu.Unlock()
	s := Stats{
		Cached:          len(live),
		Unidentified:    len(unidentified),
		EvictedStranded: evictedStranded,
		EvictedBudget:   evictedBudget,
	}
	for ino, es := range orphans {
		s.Orphaned += len(es)
		for _, e := range es {
			if pins[ino] > 0 || held[e.path] > 0 {
				s.StrandedPinned++
			}
		}
	}
	for _, n := range pins {
		s.Pins += n
	}
	return s
}

// Evict closes cached descriptors whose inode nothing pins and whose path
// nothing holds, least recently used first (Reader and Hold both count as a
// use), until at most keep remain, and reports how many it closed. A pinned
// descriptor is never closed, so more pinned descriptors than keep (more
// open sessions than serve -fd-budget) leave the cache above keep. Evict(0)
// closes every unpinned cached descriptor, and a negative keep counts as 0.
// The daemon's "0 means unlimited" lives in the janitor, which does not call
// Evict at all then. Orphans are EvictStranded's job.
func Evict(keep int) int { return evict(keep, nil) }

func evict(keep int, in func(string) bool) int {
	if keep < 0 {
		keep = 0
	}
	mu.Lock()
	var scoped []*entry
	for _, e := range live {
		if in == nil || in(e.path) {
			scoped = append(scoped, e)
		}
	}
	excess := len(scoped) - keep
	if excess <= 0 {
		mu.Unlock()
		return 0
	}
	sort.Slice(scoped, func(i, j int) bool { return scoped[i].lastUse < scoped[j].lastUse })
	var victims []*entry
	for _, e := range scoped {
		if len(victims) == excess {
			break
		}
		if pins[e.ino] > 0 || held[e.path] > 0 {
			continue
		}
		delete(live, e.path)
		victims = append(victims, e)
	}
	evictedBudget += uint64(len(victims))
	markClosingLocked(victims)
	mu.Unlock()
	closeMarked(victims)
	return len(victims)
}

// EvictUnder closes every unpinned descriptor, cached or orphaned, whose
// path lies under dir, and reports how many. It exists for tests that share
// a process with SQLite connections that take no Hold (test code is outside
// sites_test.go's inventory): scoping the eviction to the test's own
// directory leaves everyone else's descriptors alone.
func EvictUnder(dir string) int {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return 0
	}
	prefix := abs + string(filepath.Separator)
	in := func(p string) bool { return strings.HasPrefix(p, prefix) }
	return evictStranded(in) + evict(0, in)
}
