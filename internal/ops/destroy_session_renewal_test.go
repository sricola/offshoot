package ops_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/session"
	"github.com/sricola/offshoot/internal/store"
)

// renewalOverClaimBeforeDelete holds the first delete of refKey, once
// armed, until a write of refKey that carries a destroy claim and a lease
// expiry other than the claim's own lands: a renewal over the claim while
// Destroy quiesces. It is installed before the session opens, so the
// session's renewals go through it.
type renewalOverClaimBeforeDelete struct {
	store.Backend
	refKey  string
	armed   atomic.Bool
	mu      sync.Mutex
	claim   string // the claim's DeletingAt, once one is written
	expiry  string // the lease expiry the claim was written with
	renewed atomic.Bool
	once    sync.Once
	reached atomic.Bool
}

func (b *renewalOverClaimBeforeDelete) PutIf(key string, data []byte, ifMatch string) (string, error) {
	etag, err := b.Backend.PutIf(key, data, ifMatch)
	if err != nil || key != b.refKey || !b.armed.Load() {
		return etag, err
	}
	var r store.Ref
	if json.Unmarshal(data, &r) != nil || !r.Deleting {
		return etag, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.claim != r.DeletingAt:
		b.claim, b.expiry = r.DeletingAt, r.LeaseExpiry
	case r.LeaseExpiry != b.expiry:
		b.renewed.Store(true)
	}
	return etag, err
}

func (b *renewalOverClaimBeforeDelete) DeleteIf(key, ifMatch string) error {
	if key == b.refKey && b.armed.Load() {
		b.once.Do(func() {
			b.reached.Store(true)
			deadline := time.Now().Add(5 * time.Second)
			for !b.renewed.Load() && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
	return b.Backend.(store.ConditionalDeleter).DeleteIf(key, ifMatch)
}

// TestForceDestroyEndsASessionThatRenewsOverItsClaim: `destroy --force` on
// a branch a live daemon session holds, with one of the session's lease
// renewals landing over the destroy's claim before its delete, as it does
// whenever the destroy's quiesce spans a renewal tick. The renewal leaves
// the claim set, the destroy still deletes the branch, and the session's
// next renewal finds the branch gone and ends the session.
func TestForceDestroyEndsASessionThatRenewsOverItsClaim(t *testing.T) {
	w := newWS(t)
	leaseSeededMain(t, w)
	b := &renewalOverClaimBeforeDelete{Backend: w.Store.B, refKey: store.RefKey("app", "main")}
	w.Store.B = b
	s, err := session.Open(context.Background(), session.Options{
		// Renewals every 250 ms: often enough that one lands inside the held
		// delete at once, and far enough apart that the delete's re-read and
		// second attempt fit between two of them, even under -race.
		WS: w, DB: "app", Branch: "main", LeaseTTL: 3 * time.Second, RenewEvery: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	b.armed.Store(true)
	var derr error
	for i := 0; i < 20; i++ {
		// Only a renewal landing between the destroy's read and its claim,
		// winning the claim's compare-and-swap, is retried here.
		if derr = w.Destroy("app", "main", true); derr == nil || b.reached.Load() || !errors.Is(derr, store.ErrCAS) {
			break
		}
	}
	if !b.reached.Load() {
		t.Fatalf("the destroy never reached its delete: %v", derr)
	}
	if !b.renewed.Load() {
		t.Fatal("no session renewal landed over the destroy's claim")
	}
	if derr != nil {
		t.Fatalf("destroy --force with the session renewing over its claim: %v", derr)
	}
	if _, _, err := w.Store.GetRef("app", "main"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the destroyed branch came back: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for s.Err() == nil && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if s.Err() == nil {
		t.Fatal("the session is still running on a destroyed branch")
	}
	t.Logf("the session ended with: %v", s.Err())
	if _, _, err := w.Store.GetRef("app", "main"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the destroyed branch came back after the session ended: %v", err)
	}
}
