package ops

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

const tombstoneKey = "gc/tombstones"

func (w *Workspace) Destroy(db, branch string, force bool) error {
	if err := store.ValidateName(db); err != nil {
		return err
	}
	if err := store.ValidateName(branch); err != nil {
		return err
	}
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		return err
	}
	if ref.Protected && !force {
		return fmt.Errorf("ops: %s@%s is protected; use --force", db, branch)
	}
	// Per the fault matrix, destroying a branch under an active lease
	// requires --force: a live holder may still be mid-write. An expired (or
	// unparseable — fail open, matching the store layer's reclaim policy for
	// corrupt expiries) lease does not block; it's already reclaimable by
	// anyone.
	if ref.LeaseHolder != "" && !force {
		exp, perr := time.Parse(time.RFC3339Nano, ref.LeaseExpiry)
		if perr == nil && time.Now().Before(exp) {
			return fmt.Errorf("ops: %s@%s has a live lease held by %q until %s; use --force",
				db, branch, ref.LeaseHolder, ref.LeaseExpiry)
		}
	}
	path := w.CheckoutPath(db, branch)
	if _, err := os.Stat(path); err == nil {
		if err := quiesce(path); err != nil {
			return fmt.Errorf("ops: checkout in use; close connections before destroy: %w", err)
		}
	}
	if err := w.Store.DeleteRef(db, branch); err != nil {
		return err
	}
	os.Remove(path)
	os.Remove(path + "-wal")
	os.Remove(path + "-shm")
	os.Remove(path + ".sum")
	return nil
}

func (w *Workspace) loadTombstones() (map[string]string, string, error) {
	data, etag, err := w.Store.B.Get(tombstoneKey)
	if errors.Is(err, store.ErrNotFound) {
		return map[string]string{}, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	m := map[string]string{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, "", fmt.Errorf("ops: corrupt tombstone list: %w", err)
	}
	return m, etag, nil
}

func (w *Workspace) tombstone(m map[string]string) error {
	cur, etag, err := w.loadTombstones()
	if err != nil {
		return err
	}
	for k, v := range m {
		cur[k] = v
	}
	data, _ := json.Marshal(cur)
	if _, err := w.Store.B.PutIf(tombstoneKey, data, etag); err != nil {
		return fmt.Errorf("ops: gc tombstone update lost a race (retry): %w", err)
	}
	return nil
}

// liveLineages returns every lineage referenced by any ref.
func (w *Workspace) liveLineages() (map[string]bool, error) {
	refs, err := w.Store.ListRefs()
	if err != nil {
		return nil, err
	}
	live := map[string]bool{}
	for db, branches := range refs {
		for _, br := range branches {
			r, _, err := w.Store.GetRef(db, br)
			if err != nil {
				return nil, err
			}
			live[r.Lineage] = true
		}
	}
	return live, nil
}

// allLineages lists every lineage present under data/.
func (w *Workspace) allLineages() (map[string]bool, error) {
	keys, err := w.Store.B.List("data/")
	if err != nil {
		return nil, err
	}
	all := map[string]bool{}
	for _, k := range keys {
		parts := strings.Split(k, "/")
		if len(parts) >= 2 {
			all[parts[1]] = true
		}
	}
	return all, nil
}

func (w *Workspace) GC(grace time.Duration) (tombstoned, deleted int, err error) {
	live, err := w.liveLineages()
	if err != nil {
		return 0, 0, err
	}
	all, err := w.allLineages()
	if err != nil {
		return 0, 0, err
	}
	stones, _, err := w.loadTombstones()
	if err != nil {
		return 0, 0, err
	}

	// Phase 1: tombstone unreachable lineages not already marked.
	newStones := map[string]string{}
	for lineage := range all {
		if !live[lineage] {
			if _, marked := stones[lineage]; !marked {
				newStones[lineage] = time.Now().UTC().Format(time.RFC3339Nano)
				tombstoned++
			}
		}
	}
	if len(newStones) > 0 {
		if err := w.tombstone(newStones); err != nil {
			return tombstoned, 0, err
		}
		// Reload stones after tombstone to get the newly marked entries
		stones, _, err = w.loadTombstones()
		if err != nil {
			return tombstoned, 0, err
		}
	}

	// Phase 2: sweep stones older than grace that are STILL unreachable
	// (re-list refs after the grace check — a fork could have re-referenced).
	cutoff := time.Now().Add(-grace)
	for lineage, markedAt := range stones {
		if _, mintedThisRun := newStones[lineage]; mintedThisRun {
			// A stone minted in phase 1 above is timestamped `time.Now()`,
			// which trivially satisfies a grace=0 cutoff computed moments
			// later in this same call. Without this skip, one gc(0) call
			// could tombstone AND sweep a lineage in a single run — e.g. one
			// a fork is mid-flight copying a snapshot out of, in the narrow
			// window between the fork's read and its own ref landing. Sweeps
			// must always wait for a later, independent GC run.
			continue
		}
		ts, perr := time.Parse(time.RFC3339Nano, markedAt)
		if perr != nil || !ts.Before(cutoff) {
			continue
		}
		liveNow, err := w.liveLineages()
		if err != nil {
			return tombstoned, deleted, err
		}
		if liveNow[lineage] {
			continue // re-referenced during grace; keep the stone for review
		}
		keys, err := w.Store.B.List(store.LineagePrefix(lineage))
		if err != nil {
			return tombstoned, deleted, err
		}
		for _, k := range keys {
			if err := w.Store.B.Delete(k); err != nil {
				return tombstoned, deleted, err
			}
		}
		deleted++
		delete(stones, lineage)
	}
	// Persist the pruned stone list. This is an unconditional Put, not a
	// CAS: it can clobber phase-1 additions from a concurrent GC run that
	// landed after we loaded `stones` above. That's safe in only one
	// direction — the clobbered lineage isn't lost, it just isn't tombstoned
	// yet; the next GC run's phase 1 re-derives it from liveLineages/
	// allLineages and re-marks it with a fresh timestamp, at worst delaying
	// its eventual sweep by one grace period. It can never cause a live or
	// still-within-grace lineage to be deleted early.
	data, _ := json.Marshal(stones)
	if err := w.Store.B.Put(tombstoneKey, data); err != nil {
		return tombstoned, deleted, err
	}
	return tombstoned, deleted, nil
}
