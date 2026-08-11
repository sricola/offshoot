package store_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/store"
)

// This file pins the single-shot S3 timeout layer (the pass-2 availability
// residual): every buffered single-shot RPC runs under a singleShotDeadline-
// bounded context via the one shared singleShotCtx helper (s3.go) — the flat
// singleShotRPCTimeout base, size-scaled at the 1 MiB/s floor for calls with
// a known payload — and GetReader's stream is guarded by a per-Read progress
// watchdog (watchdogReader). Because the
// buffered calls all wrap uniformly through singleShotCtx, one mid-body test
// (Get — the call whose deadline must outlive the SDK call into ReadAll) and
// one stalled-response test (PutIf — a call that never sees headers at all)
// pin the whole class; the remaining call sites are the same three lines by
// inspection.

// waitOrFatal receives from ch or fails the test after d — a hung call is
// exactly the bug these tests exist to rule out, so no receive may be
// unbounded.
func waitOrFatal[T any](t *testing.T, ch <-chan T, d time.Duration, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(d):
		t.Fatalf("%s did not return within %v — the timeout it exists to prove is not firing", what, d)
		panic("unreachable")
	}
}

// TestS3GetSingleShotTimeoutMidBodyStall pins the hard half of the
// single-shot fix: the transport's response-header timeout is useless once
// headers HAVE arrived, and Get's io.ReadAll of the body runs after the SDK
// call has already returned — so the per-call deadline must stay alive
// through the ReadAll, and a body that stalls mid-transfer must fail the
// call with context.DeadlineExceeded instead of blocking it forever.
func TestS3GetSingleShotTimeoutMidBodyStall(t *testing.T) {
	t.Cleanup(store.SetSingleShotRPCTimeoutForTest(100 * time.Millisecond))

	b, f := newFakeBackedWithFake(t)
	payload := bytes.Repeat([]byte("stall!"), 8192) // ~48 KiB
	f.Seed("data/x/stalled-get.ltx", payload)
	// Headers plus 1 KiB arrive fine; the rest never does.
	f.SetGetBodyStall("data/x/stalled-get.ltx", 1024)

	errc := make(chan error, 1)
	go func() {
		_, _, err := b.Get("data/x/stalled-get.ltx")
		errc <- err
	}()
	err := waitOrFatal(t, errc, 5*time.Second, "Get against a mid-body stall")
	if err == nil {
		t.Fatal("Get whose body stalls past singleShotRPCTimeout must fail, not succeed")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want a chain wrapping context.DeadlineExceeded (the per-call deadline firing during the body read)", err)
	}
	if errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err = %v, a timeout must never masquerade as ErrNotFound", err)
	}
}

// TestS3PutIfSingleShotTimeoutStalledResponse pins the other half at a
// representative write: a backend that accepts the request and then never
// responds at all (no headers — the fake parks the request until the client
// abandons it) fails PutIf with context.DeadlineExceeded within the
// (shrunken) window instead of hanging, and never as ErrCAS — flush.go
// treats any non-CAS error as a hard failure and must not mistake a timeout
// for a lost CAS race. Every other buffered call wraps through the same
// singleShotCtx helper, so this one test covers the class.
func TestS3PutIfSingleShotTimeoutStalledResponse(t *testing.T) {
	t.Cleanup(store.SetSingleShotRPCTimeoutForTest(100 * time.Millisecond))

	b, f := newFakeBackedWithFake(t)
	f.SetRequestStall(func(method, key string) bool {
		return method == http.MethodPut && key == "data/x/stalled-put.ltx"
	})

	errc := make(chan error, 1)
	go func() {
		_, err := b.PutIf("data/x/stalled-put.ltx", []byte("payload"), "")
		errc <- err
	}()
	err := waitOrFatal(t, errc, 5*time.Second, "PutIf against a stalled response")
	if err == nil {
		t.Fatal("PutIf whose response stalls past singleShotRPCTimeout must fail, not hang or succeed")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want a chain wrapping context.DeadlineExceeded (the per-call deadline firing)", err)
	}
	if errors.Is(err, store.ErrCAS) {
		t.Fatalf("err = %v, a timeout must surface as a plain error, never store.ErrCAS", err)
	}
}

// TestS3GetReaderWatchdogKillsStalledProducer pins GetReader's progress
// watchdog: the stream has NO total deadline (that would kill legitimate
// long reads of large objects), but a single Read that blocks for
// readProgressTimeout with zero bytes delivered must fail with the
// recognizable "read stalled" error (wrapping context.Canceled — the
// watchdog cancels the request context, which is what unblocks the Read)
// instead of blocking forever, and Close afterwards must not hang.
func TestS3GetReaderWatchdogKillsStalledProducer(t *testing.T) {
	t.Cleanup(store.SetReadProgressTimeoutForTest(100 * time.Millisecond))

	b, f := newFakeBackedWithFake(t)
	payload := bytes.Repeat([]byte("stall!"), 8192) // ~48 KiB
	f.Seed("data/x/stalled-stream.ltx", payload)
	// Headers plus 1 KiB arrive, then the producer stalls forever.
	f.SetGetBodyStall("data/x/stalled-stream.ltx", 1024)

	rg := b.(store.ReaderGetter)
	r, _, err := rg.GetReader("data/x/stalled-stream.ltx")
	if err != nil {
		t.Fatalf("GetReader: headers arrived fine, open must succeed: %v", err)
	}

	type result struct {
		n   int
		err error
	}
	resc := make(chan result, 1)
	go func() {
		n, err := io.ReadAll(r)
		resc <- result{len(n), err}
	}()
	res := waitOrFatal(t, resc, 5*time.Second, "reading a stalled GetReader stream")
	if res.err == nil {
		t.Fatal("a Read stalled past readProgressTimeout must fail, not succeed or hang")
	}
	if !errors.Is(res.err, context.Canceled) {
		t.Fatalf("err = %v, want a chain wrapping context.Canceled (the watchdog canceling the request)", res.err)
	}
	if !strings.Contains(res.err.Error(), "read stalled") {
		t.Fatalf("err = %v, want the recognizable %q watchdog message", res.err, "read stalled")
	}
	if res.n != 1024 {
		t.Fatalf("read %d bytes before the stall, want exactly the 1024 the fake delivered", res.n)
	}

	// Close after a watchdog fire must return promptly (both paths just
	// cancel — idempotent); its error, if any, is uninteresting.
	closec := make(chan error, 1)
	go func() { closec <- r.Close() }()
	waitOrFatal(t, closec, 5*time.Second, "Close after a watchdog fire")
}

// TestS3SingleShotDeadlineScalesWithSize pins singleShotDeadline's
// size-scaling arithmetic: a call with a known payload earns transfer time
// at the same pessimistic 1 MiB/s floor multipartRPCTimeout's own 15
// minutes is derived from, ON TOP of the flat base — a just-under-5-GiB
// below-threshold single PutObject needs ~85 minutes at that floor, which
// the flat base alone (15 min, an effective ~5.7 MiB/s throughput floor)
// would kill on every attempt, permanently. Size-0 calls (List pages,
// Delete, HeadObject, buffered Get of small metadata objects) get exactly
// the flat base.
func TestS3SingleShotDeadlineScalesWithSize(t *testing.T) {
	const flat = 15 * time.Minute // production singleShotRPCTimeout, not overridden here

	if d := store.SingleShotDeadlineForTest(0); d != flat {
		t.Fatalf("singleShotDeadline(0) = %v, want the flat base %v", d, flat)
	}
	// 5 GiB at the 1 MiB/s floor is 5120s ≈ 85.3 min of transfer allowance;
	// the total must carry at least that ON TOP of nothing — i.e. strictly
	// dominate the ~85 min a worst-case legitimate transfer needs.
	fiveGiB := int64(5) << 30
	if d := store.SingleShotDeadlineForTest(fiveGiB); d < 85*time.Minute {
		t.Fatalf("singleShotDeadline(5GiB) = %v, want >= 85m (5 GiB at the 1 MiB/s floor)", d)
	}
	if d, want := store.SingleShotDeadlineForTest(fiveGiB), flat+5120*time.Second; d != want {
		t.Fatalf("singleShotDeadline(5GiB) = %v, want exactly base+size/floor = %v", d, want)
	}
	// A small payload (every test fixture, most metadata writes) rounds to
	// a 0s size component: the flat base — and therefore the test hook that
	// replaces it — is effectively the whole deadline.
	if d := store.SingleShotDeadlineForTest(64 * 1024); d != flat {
		t.Fatalf("singleShotDeadline(64KiB) = %v, want the flat base %v (sub-floor payloads add nothing)", d, flat)
	}
}

// TestS3GetReaderSlowConsumerNotKilled pins the watchdog's arm-only-during-
// Read design: a consumer that pauses BETWEEN Reads for several multiples of
// readProgressTimeout — against a perfectly healthy, fast producer — must
// never be killed, because the timer is only armed while a Read is actually
// blocked inside the body. A regression that leaves the timer armed across
// the gaps fails every read after the first pause.
func TestS3GetReaderSlowConsumerNotKilled(t *testing.T) {
	t.Cleanup(store.SetReadProgressTimeoutForTest(50 * time.Millisecond))

	b, f := newFakeBackedWithFake(t)
	payload := bytes.Repeat([]byte("ok"), 2048) // 4 KiB, delivered instantly
	f.Seed("data/x/slow-consumer.ltx", payload)

	rg := b.(store.ReaderGetter)
	r, _, err := rg.GetReader("data/x/slow-consumer.ltx")
	if err != nil {
		t.Fatalf("GetReader: %v", err)
	}
	defer r.Close()

	var got []byte
	buf := make([]byte, 512)
	for {
		// Pause 3x the watchdog window before every Read: with the timer
		// armed only during Reads this is invisible; armed across gaps it
		// would fire during the very first pause.
		time.Sleep(150 * time.Millisecond)
		n, rerr := r.Read(buf)
		got = append(got, buf[:n]...)
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			t.Fatalf("slow consumer killed after %d bytes: %v (the watchdog must only bound blocking Reads, never consumer pauses)", len(got), rerr)
		}
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("slow consumer read %d bytes, want %d byte-identical", len(got), len(payload))
	}
}
