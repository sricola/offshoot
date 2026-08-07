// Milestone 4 Task 5: the checkouts-ro disk budget. CheckoutAt (export.go)
// materializes cache files under <Root>/checkouts-ro/<db>/<branch>@<cp>.db
// with no ongoing relationship to the store — nothing here EVER writes to
// <Root>/checkouts (the writable, leased tree); see EvictROCache's doc
// comment for why that guarantee is structural, not a runtime check.
package ops

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// lastUsedSuffix names CheckoutAt's LRU marker file: <cachefile>.last-used,
// alongside <cachefile> itself. Its own mtime, not the .db file's, is the
// LRU clock's primary signal — see touchLastUsed's doc comment for why a
// separate marker exists at all rather than trusting the .db's mtime
// directly.
const lastUsedSuffix = ".last-used"

// touchLastUsed is CheckoutAt's touch-on-HIT: called from the force=false
// cache-hit fast path (see CheckoutAt in export.go) every time a caller is
// served an already-materialized cache file without touching the store.
// This is PM Amendment 11's pre-decided LRU clock, and the reason it exists
// as a separate marker file rather than reusing the .db file's own mtime:
// materializeAt's rename-into-place is the LAST thing that touches the .db
// file's mtime, so a cache file's mtime only ever reflects when it was
// CREATED (or force-recreated) — every subsequent cache HIT is a pure read
// (os.Stat + handing the path back), which does not and must not touch the
// .db file itself (this package's own doc comments elsewhere are explicit
// that a read-only cache file is never opened for writing again). Without a
// separate marker, "least recently used" would collapse to "least recently
// created" — exactly wrong for a cache whose whole point is that a
// checkpoint hit repeatedly stays hot.
//
// Best-effort: a failure here (a permissions problem, a full disk, a
// concurrent rmdir of the parent) does not fail the cache-hit call that
// triggered it — the caller already has a valid, readable cache file in
// hand, and refusing to serve it over a marker-touch failure would be a far
// worse outcome than a slightly stale LRU ranking. An entry whose marker
// never gets touched simply falls back to ITS OWN creation-time floor (see
// lruClock) — a real, if less precise, ranking signal, not a crash.
//
// Races against a concurrent janitor eviction pass: a touch landing here
// while EvictROCache is mid-pass on the SAME entry is exactly the TOCTOU
// EvictROCache's own doc comment addresses — a touch that lands before its
// re-guard check (immediately before that entry's os.Remove) DOES spare it
// this pass; one landing in the microscopic window after that check but
// before the actual unlink does not, but is still SAFE, not corrupting —
// this cache has been "always safe to rm -rf, worst case it rebuilds"
// since CheckoutAt's own doc comment first made that promise, and a touch
// racing an eviction is just a narrower instance of the identical
// contract, not a new risk this function introduces.
func touchLastUsed(dbPath string) {
	marker := dbPath + lastUsedSuffix
	f, err := os.OpenFile(marker, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	f.Close()
	now := time.Now()
	_ = os.Chtimes(marker, now, now)
}

// roCacheEntry is one materialized checkouts-ro cache file, as enumerated
// by roCacheEntries.
type roCacheEntry struct {
	DB, Branch, Checkpoint string
	Path                   string // the .db cache file's full path
	Bytes                  int64
	// LastUsed is this entry's LRU clock reading — see lruClock.
	LastUsed time.Time
}

// ROCacheEviction describes one checkouts-ro entry EvictROCache actually
// removed — the record the daemon's janitor pass turns into a log line, an
// offshoot_ro_cache_evictions_total increment, and an "evicted" event (see
// internal/daemon/events.go's Event.Type doc comment). DB/Branch/Checkpoint
// identify what was evicted; Bytes is exactly how much that eviction freed
// (the .db file's size — see roCacheEntries' doc comment on why the tiny
// .last-used marker isn't counted separately).
type ROCacheEviction struct {
	DB, Branch, Checkpoint string
	Bytes                  int64
}

// roCacheRoot is checkouts-ro's fixed location — the SAME tree
// CheckoutAtPath computes cache files under, one directory up (db-level,
// not db+branch+checkpoint-level).
func (w *Workspace) roCacheRoot() string {
	return filepath.Join(w.Root, "checkouts-ro")
}

// splitCacheFilename reverses CheckoutAtPath's branch+"@"+checkpoint+".db"
// filename construction. Unambiguous because store.ValidateName's charset
// excludes '@' from both branch and checkpoint (CheckoutAt validates both
// before ever constructing the path — see its own doc comment), so exactly
// one '@' is ever present in a filename this package itself wrote; ok=false
// for anything else (no '@' at all, or multiple), which roCacheEntries
// treats as "not one of ours" and skips rather than failing the whole scan
// — a stray file dropped into checkouts-ro by something outside this
// package should not make every other cache entry unreadable.
func splitCacheFilename(nameNoExt string) (branch, checkpoint string, ok bool) {
	i := strings.IndexByte(nameNoExt, '@')
	if i < 0 {
		return "", "", false
	}
	if strings.IndexByte(nameNoExt[i+1:], '@') >= 0 {
		return "", "", false // more than one '@': not a name this package wrote
	}
	return nameNoExt[:i], nameNoExt[i+1:], true
}

// lruClock is the LRU ranking BOTH ROCacheUsage's callers and EvictROCache
// use for one cache file: the `.last-used` marker's mtime when present
// (touchLastUsed's touch-on-hit — see its doc comment), falling back to the
// .db file's OWN mtime (dbInfo, already stat'd by the caller during the
// directory walk — no extra syscall) for an entry that was materialized but
// has never since been served as a cache HIT. That fallback is the
// documented "floor": a freshly materialized, never-hit entry ranks by its
// creation time, exactly as if it had been "used" once at creation — never
// older than that, and never newer than a marker actually says.
func lruClock(dbPath string, dbInfo os.FileInfo) time.Time {
	if mi, err := os.Stat(dbPath + lastUsedSuffix); err == nil {
		return mi.ModTime()
	}
	return dbInfo.ModTime()
}

// roCacheEntries walks checkouts-ro and returns one roCacheEntry per cached
// .db file (never the .last-used markers themselves, and never anything
// under the SEPARATE writable checkouts/ tree — see this file's package doc
// comment). A missing checkouts-ro directory (nothing has ever been cached)
// is not an error: it returns an empty slice, matching "no cache, no
// usage."
//
// Errors mid-walk are NOT swallowed the way an unrecognized filename is
// (see splitCacheFilename's doc comment): an os.ReadDir/Stat failure other
// than the file having vanished out from under a concurrent eviction
// (os.IsNotExist, harmless — treated as "not there to report," matching
// this package's Reap/GC precedent for a ref disappearing mid-listing)
// propagates, since it signals something actually wrong with the
// filesystem this whole computation depends on, not merely one stray file.
func (w *Workspace) roCacheEntries() ([]roCacheEntry, error) {
	root := w.roCacheRoot()
	dbDirs, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []roCacheEntry
	for _, dbDir := range dbDirs {
		if !dbDir.IsDir() {
			continue
		}
		db := dbDir.Name()
		dir := filepath.Join(root, db)
		files, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue // removed concurrently since the outer ReadDir
			}
			return nil, err
		}
		for _, f := range files {
			name := f.Name()
			if !strings.HasSuffix(name, ".db") {
				continue // skip .last-used markers and anything else
			}
			branch, checkpoint, ok := splitCacheFilename(strings.TrimSuffix(name, ".db"))
			if !ok {
				continue // not a filename this package ever wrote; ignore
			}
			info, err := f.Info()
			if err != nil {
				if os.IsNotExist(err) {
					continue // removed concurrently since ReadDir
				}
				return nil, err
			}
			path := filepath.Join(dir, name)
			out = append(out, roCacheEntry{
				DB: db, Branch: branch, Checkpoint: checkpoint,
				Path: path, Bytes: info.Size(),
				LastUsed: lruClock(path, info),
			})
		}
	}
	return out, nil
}

// ROCacheUsage reports checkouts-ro's total current usage — bytes summed
// across every cached .db file (the dominant, and only counted, cost; a
// `.last-used` marker is a near-empty sentinel file and is not counted
// separately, though it IS removed alongside its .db on eviction, see
// EvictROCache) and the number of cache entries. Read-only: never touches,
// let alone evicts, anything. Backs `offshoot status`'s ro-cache summary
// line and is what the daemon janitor also computes each pass to drive
// offshoot_ro_cache_bytes (see internal/daemon/server.go's janitorTick).
func (w *Workspace) ROCacheUsage() (bytes int64, count int, err error) {
	entries, err := w.roCacheEntries()
	if err != nil {
		return 0, 0, err
	}
	for _, e := range entries {
		bytes += e.Bytes
	}
	return bytes, len(entries), nil
}

// EvictROCache computes checkouts-ro's total current usage and, if it
// exceeds budget, evicts entries — oldest by the LRU clock (lruClock)
// first — until usage is at or under budget. budget <= 0 means unlimited
// (the CLI/daemon default): EvictROCache still computes and returns usage
// (so a caller can report/gauge it — "not evicting anything" is itself a
// true, useful statement a budget-less pass makes every time, mirroring
// GCBacklog's own always-set-even-at-zero treatment in janitorTick), but
// NEVER removes anything in this case.
//
// Writable checkouts/ is never evicted — by construction, not by a runtime
// check this function performs: roCacheEntries only ever walks
// checkouts-ro (a SEPARATE directory tree from checkouts/, both in location
// and in filename shape — see CheckoutAtPath's own doc comment), and this
// function only ever removes paths that walk returned. There is no code
// path here that can construct, or be handed, a path under checkouts/ at
// all, so "never evict a leased/writable checkout" holds structurally, the
// same way CheckoutAt's own doc comment already describes for the
// materialize side of this same separation.
//
// Ties in the LRU clock (equal timestamps — e.g. two entries with no
// marker, materialized in the same walk-granularity instant) are broken by
// path, purely for deterministic test behavior; production eviction order
// among genuine ties has no other meaningful tiebreak.
//
// Each eviction removes BOTH the .db cache file and its `.last-used`
// marker (if any — a never-hit entry has none, and os.Remove's
// IsNotExist is treated as success either way, not an error). If a
// os.Remove fails for a reason OTHER than "already gone," eviction stops
// immediately and returns everything evicted so far alongside that error —
// matching Reap/GC's own partial-progress-plus-first-error convention
// elsewhere in this package (see gc.go's GC and reap.go's Reap) — rather
// than losing track of real, already-completed deletions by discarding
// them on a later failure.
//
// # TOCTOU: a concurrent CheckoutAt call racing this same pass
//
// roCacheEntries snapshots every entry's size and LastUsed once, up front;
// everything after that (the sort, and each os.Remove below) runs against
// that snapshot, not a live view. A CheckoutAt cache HIT on some entry E,
// landing anywhere between that snapshot and E's own removal below, is a
// real, expected race — CheckoutAt callers and the janitor share no lock —
// and this function is fail-safe about it in two complementary ways:
//
//  1. Every removal is re-guarded immediately before it happens: if E's
//     `.last-used` marker's mtime is at or after passStart (this call's own
//     start time, captured before the snapshot), a touch landed DURING this
//     very pass — E is spared outright, not evicted this round, and the
//     loop moves on to the next-oldest candidate instead. A budget this
//     tight, under this much concurrent touch pressure, may occasionally
//     leave usageAfter still over budget when every remaining candidate is
//     freshly spared this way — self-heals on the NEXT pass, which
//     re-snapshots fresh recency data from scratch (a fresh passStart, a
//     fresh roCacheEntries walk).
//  2. Even a touch that lands in the microscopic window between that guard
//     check and the os.Remove call itself (impossible to close without a
//     lock this codebase deliberately doesn't take here — CheckoutAt and
//     the janitor are meant to run lock-free against each other, matching
//     the "checkouts-ro is always safe to rm -rf" contract this whole cache
//     already operates under) is still SAFE, just not saved: E is deleted
//     this pass despite the near-simultaneous touch, and — this is the
//     part that makes it safe rather than merely tolerable — a caller that
//     already has E's cache file open (or opens it moments later, having
//     gotten its path from that same CheckoutAt call before the unlink)
//     keeps reading it without error or corruption, POSIX unlink-of-an-
//     open-file semantics being exactly what "safe to rm -rf at any time"
//     already promised. A caller that has NOT yet opened it sees the file
//     gone and — per CheckoutAt's own doc comment — simply re-calls
//     CheckoutAt, which re-materializes it from the store.
//
// See CheckoutAt's doc comment (export.go) for the caller-facing half of
// this same contract, and touchLastUsed's doc comment for why case 1 above
// is the guard, not the whole story.
func (w *Workspace) EvictROCache(budget int64) (evicted []ROCacheEviction, usageAfter int64, err error) {
	passStart := time.Now()
	entries, err := w.roCacheEntries()
	if err != nil {
		return nil, 0, err
	}
	var total int64
	for _, e := range entries {
		total += e.Bytes
	}
	if budget <= 0 || total <= budget {
		return nil, total, nil
	}

	sort.Slice(entries, func(i, j int) bool {
		if !entries[i].LastUsed.Equal(entries[j].LastUsed) {
			return entries[i].LastUsed.Before(entries[j].LastUsed)
		}
		return entries[i].Path < entries[j].Path
	})

	usageAfter = total
	for _, e := range entries {
		if usageAfter <= budget {
			break
		}
		if roCacheEvictPreRemoveHook != nil {
			roCacheEvictPreRemoveHook(e.Path) // test-only; see its own doc comment
		}
		// Race guard (TOCTOU case 1 in this function's own doc comment): a
		// marker touched at/after passStart means a CheckoutAt cache hit
		// landed during THIS pass, after roCacheEntries's snapshot above —
		// spare e rather than evict something that was just proven "in use"
		// more recently than this pass even started.
		if mi, statErr := os.Stat(e.Path + lastUsedSuffix); statErr == nil && !mi.ModTime().Before(passStart) {
			continue
		}
		if rmErr := os.Remove(e.Path); rmErr != nil && !os.IsNotExist(rmErr) {
			return evicted, usageAfter, rmErr
		}
		if rmErr := os.Remove(e.Path + lastUsedSuffix); rmErr != nil && !os.IsNotExist(rmErr) {
			return evicted, usageAfter, rmErr
		}
		usageAfter -= e.Bytes
		evicted = append(evicted, ROCacheEviction{DB: e.DB, Branch: e.Branch, Checkpoint: e.Checkpoint, Bytes: e.Bytes})
	}
	return evicted, usageAfter, nil
}

// roCacheEvictPreRemoveHook, when non-nil, is called with each candidate's
// .db path immediately before EvictROCache's own race-guard check for it —
// purely a test seam (mirrors export.go's checkoutAtChmod / daemon's
// openDelay convention elsewhere in this codebase) so a test can inject a
// touchLastUsed call at EXACTLY the TOCTOU window described in
// EvictROCache's doc comment (case 1: a touch landing after the snapshot,
// before this candidate's removal) deterministically, rather than relying
// on real goroutine-scheduling races. nil (a no-op) in production.
var roCacheEvictPreRemoveHook func(path string)
