package session

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

// renewLoop keeps the session's lease fresh. Losing the lease is terminal:
// the session's epoch is dead, so anything it wrote afterwards would land in
// a fenced prefix — it must stop rather than keep serving.
func (s *Session) renewLoop(ctx context.Context, every, ttl time.Duration) {
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
			if errors.Is(err, store.ErrLeaseLost) {
				s.fail(fmt.Errorf("%w: %v", ErrFenced, err))
				return
			}
			// A transient store error is not fencing; try again next tick.
			continue
		}
		s.mu.Lock()
		s.lease = l
		s.mu.Unlock()
	}
}
