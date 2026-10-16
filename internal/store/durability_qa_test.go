package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestV1LayoutIsReadAndWrittenWithoutABump pins docs/stability.md#Layout
// version ("newer binaries read older layouts") and docs/architecture.md
// "v0.2.x reads and writes a v1 store unchanged": against a manifest
// seeded at LayoutVersion 1, CheckManifest accepts the store, refs are
// created, CAS-updated, read back and listed, a lineage's chain resolves,
// and afterwards the manifest's bytes are exactly what was seeded — still
// layout 1. Only a shared fork (EnsureLayoutV2) may bump it.
//
// Mutation verified against: CheckManifest calling EnsureLayoutV2
// (store.go) — the manifest then reads layout 2 after the attach.
func TestV1LayoutIsReadAndWrittenWithoutABump(t *testing.T) {
	s := newStore(t)
	seed, _ := json.Marshal(Manifest{LayoutVersion: 1, CreatedAt: "2025-01-01T00:00:00Z"})
	if _, err := s.B.PutIf(manifestKey, seed, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckManifest(); err != nil {
		t.Fatalf("CheckManifest on a v1 store: %v", err)
	}

	lineage := NewLineageID()
	r := Ref{Lineage: lineage, Epoch: 1, HeadTXID: 1}
	r.SetCheckpoint("init", Checkpoint{TXID: 1, Epoch: 1})
	etag, err := s.PutRef("app", "main", r, "")
	if err != nil {
		t.Fatalf("PutRef (create) on a v1 store: %v", err)
	}
	got, gotEtag, err := s.GetRef("app", "main")
	if err != nil {
		t.Fatalf("GetRef on a v1 store: %v", err)
	}
	if gotEtag != etag || got.Lineage != lineage || got.Schema != RefSchema || got.HeadTXID != 1 {
		t.Fatalf("GetRef returned etag %q ref %+v, want etag %q lineage %s schema %d head 1", gotEtag, got, etag, lineage, RefSchema)
	}
	got.HeadTXID = 2
	etag2, err := s.PutRef("app", "main", got, etag)
	if err != nil {
		t.Fatalf("PutRef (CAS update) on a v1 store: %v", err)
	}
	if etag2 == etag {
		t.Fatal("a CAS update that changed the ref must move the etag")
	}
	if again, _, _ := s.GetRef("app", "main"); again.HeadTXID != 2 {
		t.Fatalf("CAS update did not land: HeadTXID = %d, want 2", again.HeadTXID)
	}

	snap, seg := SnapshotKey(lineage, 1, 1), SegmentKey(lineage, 1, 2, 2)
	for _, k := range []string{snap, seg} {
		if err := s.B.Put(k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	chain, err := s.Chain(lineage, 2)
	if err != nil {
		t.Fatalf("Chain on a v1 store: %v", err)
	}
	if len(chain) != 2 || chain[0].Key != snap || chain[1].Key != seg {
		t.Fatalf("Chain = %+v, want [%s %s]", chain, snap, seg)
	}
	refs, err := s.ListRefs()
	if err != nil {
		t.Fatalf("ListRefs on a v1 store: %v", err)
	}
	if want := map[string][]string{"app": {"main"}}; !reflect.DeepEqual(refs, want) {
		t.Fatalf("ListRefs = %v, want %v", refs, want)
	}
	if err := s.CheckManifest(); err != nil {
		t.Fatalf("CheckManifest after writes on a v1 store: %v", err)
	}

	after, _, err := s.B.Get(manifestKey)
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err := json.Unmarshal(after, &m); err != nil {
		t.Fatal(err)
	}
	if m.LayoutVersion != 1 {
		t.Fatalf("manifest layout_version = %d after non-fork operations, want 1 (no bump)", m.LayoutVersion)
	}
	if !bytes.Equal(after, seed) {
		t.Fatalf("manifest bytes changed: seeded %s, now %s", seed, after)
	}
}

// TestLeaseClockSkewIsFencedNotSilent pins docs/limitations.md "Lease
// expiry is judged on the caller's wall clock" together with
// docs/architecture.md#Invariants §3 (a stale writer is fenced by the epoch
// bump): holder A acquires at `now` with TTL T; holder B, whose clock reads
// now+T+1s, reclaims the lease and gets a BUMPED epoch; A, whose own clock
// still says the lease has T/2 left, is refused with ErrLeaseLost naming
// the new holder and epoch, and its refused renew writes nothing. Skew can
// steal a lease, but never silently share one.
//
// Mutation verified against: RenewLease skipping its holder/epoch check
// (lease.go) — A's renew then succeeds and extends B's lease.
func TestLeaseClockSkewIsFencedNotSilent(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	const ttl = time.Minute

	a, err := s.AcquireLease("app", "main", "daemon-a", ttl, now)
	if err != nil {
		t.Fatal(err)
	}
	if a.Epoch != 2 {
		t.Fatalf("A's epoch = %d, want 2", a.Epoch)
	}

	skewed := now.Add(ttl + time.Second) // B's clock: 1 s past A's expiry
	b, err := s.AcquireLease("app", "main", "daemon-b", ttl, skewed)
	if err != nil {
		t.Fatalf("B at a clock past A's expiry must reclaim the lease: %v", err)
	}
	if b.Epoch != a.Epoch+1 {
		t.Fatalf("B's epoch = %d, want %d (a reclaim bumps the epoch)", b.Epoch, a.Epoch+1)
	}
	ref, etagAfterB, err := s.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	wantExpiry := skewed.Add(ttl).UTC().Format(time.RFC3339Nano)
	if ref.LeaseHolder != "daemon-b" || ref.Epoch != 3 || ref.LeaseExpiry != wantExpiry {
		t.Fatalf("ref after B's reclaim = holder %q epoch %d expiry %q, want daemon-b 3 %q",
			ref.LeaseHolder, ref.Epoch, ref.LeaseExpiry, wantExpiry)
	}

	// A, on its own clock, believes it has half its TTL left.
	_, err = s.RenewLease(a, ttl, now.Add(ttl/2))
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("A's renew after B's reclaim: err = %v, want ErrLeaseLost", err)
	}
	if want := `app@main now held by "daemon-b" at epoch 3`; !strings.Contains(err.Error(), want) {
		t.Fatalf("A's refusal must name the new holder and epoch: got %q, want it to contain %q", err, want)
	}
	again, etagAfterRenew, err := s.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if etagAfterRenew != etagAfterB || again.LeaseHolder != "daemon-b" || again.Epoch != 3 || again.LeaseExpiry != wantExpiry {
		t.Fatalf("a refused renew must write nothing: ref = holder %q epoch %d expiry %q etag %q, want daemon-b 3 %q %q",
			again.LeaseHolder, again.Epoch, again.LeaseExpiry, etagAfterRenew, wantExpiry, etagAfterB)
	}
}

// TestLocalRejectedPutIfLeavesContentUntouched pins docs/architecture.md
// #Invariants §2-3 ("the loser gets ErrCAS", create-only over an existing
// key fails) on the Local backend, with the half TestLocalCreateOnly does
// not assert: after a refused create-only put and after a refused
// stale-etag CAS, the key's content and etag are byte-identical to before,
// the on-disk file is too, and no temp file is left beside it. A stale
// etag against an ABSENT key is refused as well and creates nothing.
//
// Mutation verified against: Local.PutIf falling through to write when the
// "key exists" / "etag mismatch" preconditions fail (local.go).
func TestLocalRejectedPutIfLeavesContentUntouched(t *testing.T) {
	root := t.TempDir()
	b, err := NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	const key = "refs/a/main"
	etag1, err := b.PutIf(key, []byte("v1"), "")
	if err != nil {
		t.Fatal(err)
	}
	onDisk := filepath.Join(root, "refs", "a", "main")

	check := func(step string) {
		t.Helper()
		data, etag, err := b.Get(key)
		if err != nil || string(data) != "v1" || etag != etag1 {
			t.Fatalf("after %s: Get = %q etag %q err %v, want \"v1\" etag %q nil", step, data, etag, err, etag1)
		}
		raw, err := os.ReadFile(onDisk)
		if err != nil || string(raw) != "v1" {
			t.Fatalf("after %s: on-disk file = %q err %v, want \"v1\"", step, raw, err)
		}
		entries, err := os.ReadDir(filepath.Dir(onDisk))
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if strings.Contains(e.Name(), ".tmp-") {
				t.Fatalf("after %s: temp file left behind: %s", step, e.Name())
			}
		}
	}

	if _, err := b.PutIf(key, []byte("v2-create"), ""); !errors.Is(err, ErrCAS) {
		t.Fatalf("create-only over an existing key: err = %v, want ErrCAS", err)
	}
	check("a refused create-only put")

	if _, err := b.PutIf(key, []byte("v2-stale"), "stale-etag"); !errors.Is(err, ErrCAS) {
		t.Fatalf("stale-etag CAS: err = %v, want ErrCAS", err)
	}
	check("a refused stale-etag CAS")

	if _, err := b.PutIf("refs/a/absent", []byte("x"), etag1); !errors.Is(err, ErrCAS) {
		t.Fatalf("etag CAS against an absent key: err = %v, want ErrCAS", err)
	}
	if _, _, err := b.Get("refs/a/absent"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a refused CAS on an absent key must create nothing: Get err = %v, want ErrNotFound", err)
	}
	check("a refused CAS on an absent key")
}

// TestValidateNamePinsDocumentedRule pins docs/reference.md "Naming rules":
// 1-128 characters, charset [a-z0-9-_.], never starting with "-", never
// exactly "." or ".." or containing "..", and a bad name fails with
// "store: invalid name ...". Every rejected name's error carries that
// prefix and the offending name.
//
// Mutation verified against: dropping the leading-dash check (store.go) —
// "-x" is then accepted.
func TestValidateNamePinsDocumentedRule(t *testing.T) {
	accept := []string{
		"a.b", "_", "0", "app", "attempt-1", "v1.2_x",
		strings.Repeat("a", 128),
	}
	for _, name := range accept {
		if err := ValidateName(name); err != nil {
			t.Errorf("ValidateName(%q) = %v, want accepted", name, err)
		}
	}
	reject := []string{
		strings.Repeat("a", 129), "-x", "-", ".", "..", "a..b", "A", "a b", "a/b", "",
	}
	for _, name := range reject {
		err := ValidateName(name)
		if err == nil {
			t.Errorf("ValidateName(%q) = nil, want rejected", name)
			continue
		}
		if !strings.HasPrefix(err.Error(), "store: invalid name ") {
			t.Errorf("ValidateName(%q) = %q, want the documented \"store: invalid name ...\" prefix", name, err)
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("%q", name)) {
			t.Errorf("ValidateName(%q) = %q, want the error to quote the name", name, err)
		}
	}
}
