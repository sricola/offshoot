package ops

import (
	"errors"
	"fmt"
	"maps"
	"sort"

	"github.com/sricola/offshoot/internal/store"
)

// PruneOptions selects which of a branch's named checkpoints
// PruneCheckpoints removes: exactly the Names given, or (KeepSet) all but
// the Keep newest. Exactly one of the two must be set.
type PruneOptions struct {
	Names   []string
	Keep    int
	KeepSet bool
	// Force allows pruning a protected branch, which otherwise refuses:
	// its checkpoints are restore points someone chose to keep.
	Force bool
}

// PruneResult reports what PruneCheckpoints removed (sorted) and how many
// checkpoints the branch still has.
type PruneResult struct {
	Deleted []string
	Kept    int
}

// pruneBeforePutHook, when set (tests only), runs after a prune has
// computed its new ref and before it writes it.
var pruneBeforePutHook func()

// PruneCheckpoints removes named checkpoints from db@branch's ref. It is a
// metadata write only: the head, lineage, lease and activity clock are
// left alone (pruning is not activity, like SetProtected), and no object is
// deleted here — storage that only a pruned checkpoint kept reachable is
// reclaimed by the next GC pass after its grace. Because the ref's
// checkpoint map is decoded and rewritten on every checkpoint, keeping it
// small is also what keeps checkpoints on a long-lived branch fast.
//
// All-or-nothing: a name the branch does not have refuses the whole call,
// so a typo never half-applies. CAS-retried, recomputing from the fresh ref
// each time, so a checkpoint recorded concurrently is never dropped by a
// by-name prune and is ranked by a keep prune. Safe beside a live session:
// a session flush and an at-rest checkpoint both rebuild their head write
// from a fresh re-read after a CAS loss, so neither puts a pruned entry
// back. Refuses a branch a reaper or a Destroy has claimed.
func (w *Workspace) PruneCheckpoints(db, branch string, opts PruneOptions) (PruneResult, error) {
	if err := store.ValidateName(db); err != nil {
		return PruneResult{}, err
	}
	if err := store.ValidateName(branch); err != nil {
		return PruneResult{}, err
	}
	switch {
	case opts.KeepSet && len(opts.Names) > 0:
		return PruneResult{}, errors.New("ops: prune takes checkpoint names or a keep count, not both")
	case !opts.KeepSet && len(opts.Names) == 0:
		return PruneResult{}, errors.New("ops: prune needs checkpoint names or a keep count")
	case opts.KeepSet && opts.Keep < 0:
		return PruneResult{}, fmt.Errorf("ops: prune keep count must be 0 or more, got %d", opts.Keep)
	}
	for _, n := range opts.Names {
		if err := store.ValidateName(n); err != nil {
			return PruneResult{}, err
		}
	}
	for {
		ref, etag, err := w.Store.GetRef(db, branch)
		if err != nil {
			return PruneResult{}, err
		}
		if ref.Reaping || ref.Deleting {
			return PruneResult{}, fmt.Errorf("ops: %s@%s is being removed; too late to prune its checkpoints", db, branch)
		}
		if ref.Protected && !opts.Force {
			return PruneResult{}, fmt.Errorf("ops: %s@%s is protected; use --force", db, branch)
		}
		drop := map[string]bool{}
		if opts.KeepSet {
			for _, n := range newestFirst(ref.Checkpoints)[min(opts.Keep, len(ref.Checkpoints)):] {
				drop[n] = true
			}
		} else {
			for _, n := range opts.Names {
				if _, ok := ref.Checkpoints[n]; !ok {
					return PruneResult{}, errNoCheckpoint(db, branch, n, ref)
				}
				drop[n] = true
			}
		}
		res := PruneResult{Deleted: make([]string, 0, len(drop)), Kept: len(ref.Checkpoints) - len(drop)}
		for n := range drop {
			res.Deleted = append(res.Deleted, n)
		}
		sort.Strings(res.Deleted)
		if len(drop) == 0 {
			return res, nil
		}
		ref.Checkpoints = maps.Clone(ref.Checkpoints)
		for n := range drop {
			delete(ref.Checkpoints, n)
		}
		if pruneBeforePutHook != nil {
			pruneBeforePutHook()
		}
		if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
			if errors.Is(err, store.ErrCAS) {
				continue
			}
			return PruneResult{}, err
		}
		return res, nil
	}
}

// newestFirst orders checkpoint names newest first: by txid, then
// created_at (RFC 3339 UTC, so it sorts as a string), then name ascending
// so equal entries still order deterministically.
func newestFirst(cps map[string]store.Checkpoint) []string {
	names := make([]string, 0, len(cps))
	for n := range cps {
		names = append(names, n)
	}
	sort.Slice(names, func(i, j int) bool {
		a, b := cps[names[i]], cps[names[j]]
		if a.TXID != b.TXID {
			return a.TXID > b.TXID
		}
		if a.CreatedAt != b.CreatedAt {
			return a.CreatedAt > b.CreatedAt
		}
		return names[i] < names[j]
	})
	return names
}
