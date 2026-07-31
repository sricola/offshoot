package session

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

// renewLoop keeps the session's lease fresh. It closes s.renewDone exactly
// once, on every exit path, so Close can join it before releasing the lease
// (see Close's comment for why that join matters) — mirroring how runEngine
// closes engDone for the capture goroutine.
//
// Losing the lease is terminal: the session's epoch is dead, so anything it
// wrote afterwards would land in a fenced prefix — it must stop rather than
// keep serving.
func (s *Session) renewLoop(ctx context.Context, every, ttl time.Duration) {
	defer close(s.renewDone)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		// Renew with ttl to keep the lease alive, but allow for potential
		// expiration between renewal attempts to enable fencing detection.
		l, err := s.ws.RenewLease(s.Lease(), ttl)
		if err != nil {
			switch {
			case errors.Is(err, store.ErrLeaseLost):
				// Fenced: someone else reclaimed the branch out from under us.
				s.fail(fmt.Errorf("%w: %v", ErrFenced, err))
				return
			case errors.Is(err, store.ErrNotFound):
				// Gone: the branch itself was destroyed (e.g. `destroy
				// --force` on a live session). There is no ref left to renew
				// against, so this is terminal too, not something a later
				// tick could ever recover from.
				s.fail(fmt.Errorf("session: branch %s@%s no longer exists: %w", s.db, s.branch, err))
				return
			default:
				// A transient store error is neither of the above; try again
				// next tick.
				continue
			}
		}
		s.mu.Lock()
		s.lease = l
		s.mu.Unlock()
	}
}
