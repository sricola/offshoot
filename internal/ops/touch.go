package ops

import (
	"errors"
	"fmt"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

// Touch resets a branch's activity clock, and optionally sets (ttl > 0) or
// clears (ttl == 0) its TTL; a nil ttl keeps the current TTL. CAS-retried;
// refuses a branch a reaper has already claimed.
func (w *Workspace) Touch(db, branch string, ttl *time.Duration, now time.Time) (store.Ref, error) {
	if err := store.ValidateName(db); err != nil {
		return store.Ref{}, err
	}
	if err := store.ValidateName(branch); err != nil {
		return store.Ref{}, err
	}
	for {
		ref, etag, err := w.Store.GetRef(db, branch)
		if err != nil {
			return store.Ref{}, err
		}
		if ref.Reaping {
			return store.Ref{}, fmt.Errorf("ops: %s@%s is being reaped; too late to touch", db, branch)
		}
		if ttl != nil {
			if *ttl > 0 {
				ref.TTL = ttl.String()
			} else {
				ref.TTL = ""
			}
		}
		ref.Touch(now)
		if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
			if errors.Is(err, store.ErrCAS) {
				continue
			}
			return store.Ref{}, err
		}
		return ref, nil
	}
}
