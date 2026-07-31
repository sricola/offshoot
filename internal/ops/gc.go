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
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		return err
	}
	if ref.Protected && !force {
		return fmt.Errorf("ops: %s@%s is protected; use --force", db, branch)
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
	if etag == "" {
		_, err = w.Store.B.PutIf(tombstoneKey, data, "")
	} else {
		_, err = w.Store.B.PutIf(tombstoneKey, data, etag)
	}
	return err
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
	// Persist the pruned stone list (best-effort CAS; conflict = another GC
	// ran concurrently, which is safe — stones are re-derived each run).
	data, _ := json.Marshal(stones)
	w.Store.B.Put(tombstoneKey, data)
	return tombstoned, deleted, nil
}
