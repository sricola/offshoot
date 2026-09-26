package ops

import (
	"errors"
	"fmt"

	"github.com/sricola/offshoot/internal/store"
)

// SetProtected sets or clears db@branch's protected flag. A protected
// branch refuses unforced destroy and promote-onto, is never reaped, and —
// through an MCP server without -allow-force — cannot be forced by an
// agent at all. CAS-retried like Touch; refuses a branch a reaper or a
// Destroy has already claimed, since flipping the flag under either would
// race the claim's own outcome. Deliberately does NOT call Touch: a
// protection flip is not activity, so the activity clock (TouchedAt) and
// the TTL deadline it anchors are left exactly as they were — unprotecting
// a stale, TTL'd branch must not incidentally grant it a fresh TTL window.
func (w *Workspace) SetProtected(db, branch string, protected bool) (store.Ref, error) {
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
		if ref.Reaping || ref.Deleting {
			return store.Ref{}, fmt.Errorf("ops: %s@%s is being removed; too late to change its protection", db, branch)
		}
		if ref.Protected == protected {
			return ref, nil
		}
		ref.Protected = protected
		if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
			if errors.Is(err, store.ErrCAS) {
				continue
			}
			return store.Ref{}, err
		}
		return ref, nil
	}
}
