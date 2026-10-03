package session

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sricola/offshoot/internal/store"
)

// renewLoop keeps the session's lease fresh. It closes s.renewDone exactly
// once, on every exit path, so Close can join it before releasing the lease
// (see Close's comment for why that join matters) — mirroring how runEngine
// closes engDone for the capture goroutine.
//
// Losing the lease is terminal: the session's epoch is dead, so anything it
// wrote afterwards would land in a fenced prefix — it must stop rather than
// keep serving.
//
// It runs on its own context (renewCancel), not the capture engine's, so
// Close can keep the lease live through the engine's shutdown and the
// sidecar stamp and stop renewal immediately before the release; fail
// cancels both contexts, so a failed session stops renewing as before.
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

// releaseAttempts and releaseRetryPause bound Close's lease release. A
// release can lose its compare-and-swap to a write that leaves the lease
// ours — touch and protect both stamp the ref while a session is live — or
// hit a transient store error, and a later attempt gets past either.
// Without the retry, one failure left the branch leased until the lease
// expired. releaseRetryPause is a var so tests can shrink it.
const releaseAttempts = 3

var releaseRetryPause = 50 * time.Millisecond

// releaseLease releases l through release, retrying up to releaseAttempts
// times with a pause that grows by releaseRetryPause each time. Every
// attempt re-reads the ref and releases only while it still names l's
// holder and epoch (store.ReleaseLease), so an attempt that finds the lease
// gone — ErrLeaseLost, which is also what a release that landed but
// reported an error reads back as — counts as released. ErrNotFound (the
// branch was destroyed) returns at once: no attempt can succeed.
func releaseLease(release func(store.Lease) error, l store.Lease) error {
	var err error
	for attempt := 0; attempt < releaseAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * releaseRetryPause)
		}
		err = release(l)
		switch {
		case err == nil, errors.Is(err, store.ErrLeaseLost):
			return nil
		case errors.Is(err, store.ErrNotFound):
			return err
		}
	}
	return err
}
