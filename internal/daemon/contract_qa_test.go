package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/session"
	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// ---- docs/reference.md#eventing: every documented type reaches a socket
// subscriber with its documented detail keys -------------------------------

// detailKeys returns ev.Detail's keys sorted, for exact-set assertions.
func detailKeys(ev Event) []string {
	keys := make([]string, 0, len(ev.Detail))
	for k := range ev.Detail {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

// TestEventFencedReachesSocketSubscriberWithDocumentedDetail pins
// docs/reference.md#eventing row `fenced`: "A session is fenced out by a
// lease it no longer holds" carries detail `cause`, `holder`, `epoch`, where
// holder/epoch name the same lease the session's own `session_opened`
// reported. The session is opened directly (as branchstate_test.go's
// TestOpBranchesReportsErrorForFencedSession does) so the test controls
// LeaseTTL/RenewEvery; the steal is deadline-polled rather than slept.
// Mutation verified against: removing `case "fenced"` from
// sessionEventType (events.go).
func TestEventFencedReachesSocketSubscriberWithDocumentedDetail(t *testing.T) {
	srv, w := newServer(t)
	sc, events := subscribeSocket(t, srv.SocketPath())
	defer sc.Close()

	sess, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", Holder: "session-qa-fenced",
		LeaseTTL: 100 * time.Millisecond, RenewEvery: 400 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sess.Close() })

	opened := waitForEventType(t, events, "session_opened", 10*time.Second)
	if opened.DB != "app" || opened.Branch != "main" {
		t.Fatalf("session_opened = %+v", opened)
	}
	if opened.Detail["holder"] != "session-qa-fenced" {
		t.Fatalf("session_opened holder = %v, want session-qa-fenced", opened.Detail["holder"])
	}

	// Steal the lease once it lapses: AcquireLease loses the CAS race with
	// ErrLeaseHeld until the 100 ms TTL has passed, so poll it to success.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err = w.AcquireLease("app", "main", "thief", ops.DefaultLeaseTTL)
		if err == nil {
			break
		}
		if !errors.Is(err, store.ErrLeaseHeld) {
			t.Fatalf("rival acquire: unexpected error %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("rival acquire never succeeded within 5s; last error %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	fenced := waitForEventType(t, events, "fenced", 10*time.Second)
	if fenced.V != eventSchemaVersion || fenced.DB != "app" || fenced.Branch != "main" {
		t.Fatalf("fenced event = %+v", fenced)
	}
	if _, err := time.Parse(time.RFC3339, fenced.TS); err != nil {
		t.Fatalf("fenced ts %q is not RFC3339: %v", fenced.TS, err)
	}
	if got, want := detailKeys(fenced), []string{"cause", "epoch", "holder"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("fenced detail keys = %v, want %v (detail %v)", got, want, fenced.Detail)
	}
	cause, _ := fenced.Detail["cause"].(string)
	if !strings.Contains(cause, session.ErrFenced.Error()) {
		t.Fatalf("fenced cause = %q, want it to carry %q", cause, session.ErrFenced.Error())
	}
	if fenced.Detail["holder"] != "session-qa-fenced" {
		t.Fatalf("fenced holder = %v, want session-qa-fenced", fenced.Detail["holder"])
	}
	if fenced.Detail["epoch"] != opened.Detail["epoch"] {
		t.Fatalf("fenced epoch = %v, want the session_opened epoch %v", fenced.Detail["epoch"], opened.Detail["epoch"])
	}
	if sess.Err() == nil {
		t.Fatal("the session must be terminally failed once fenced has been published")
	}
}

// TestEventFlushFailedReachesSocketSubscriberWithDocumentedDetail pins
// docs/reference.md#eventing row `flush_failed`: "A flush fails" carries
// detail `kind`, `error`, `duration_seconds`. The failure is injected with
// lifecycle_test.go's failNRefPutIf (installed before any session opens,
// armed only after the open has settled) and triggered by a manual `flush`
// op, so kind must be "manual". Mutation verified against: removing
// `case "flush-failed"` from sessionEventType (events.go).
func TestEventFlushFailedReachesSocketSubscriberWithDocumentedDetail(t *testing.T) {
	srv, w := newServer(t)
	fp := &failNRefPutIf{Backend: w.Store.B}
	w.Store.B = fp
	srv.SetFlushEvery(0)
	sock := srv.SocketPath()
	sc, events := subscribeSocket(t, sock)
	defer sc.Close()

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	waitForEventType(t, events, "session_opened", 10*time.Second)
	sqliteExec(t, open.Checkout, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")

	fp.arm(1 << 30) // fail every ref write until this test has seen the failure
	resp := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main"})
	if resp.OK || !strings.Contains(resp.Error, "injected transient ref failure") {
		fp.arm(0)
		t.Fatalf("flush under an injected ref failure = %+v, want a refusal naming the injected failure", resp)
	}
	ev := waitForEventType(t, events, "flush_failed", 10*time.Second)
	fp.arm(0)

	if ev.V != eventSchemaVersion || ev.DB != "app" || ev.Branch != "main" {
		t.Fatalf("flush_failed event = %+v", ev)
	}
	if got, want := detailKeys(ev), []string{"duration_seconds", "error", "kind"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("flush_failed detail keys = %v, want %v (detail %v)", got, want, ev.Detail)
	}
	if ev.Detail["kind"] != "manual" {
		t.Fatalf("flush_failed kind = %v, want manual", ev.Detail["kind"])
	}
	errText, _ := ev.Detail["error"].(string)
	if !strings.Contains(errText, "injected transient ref failure on refs/app/main") {
		t.Fatalf("flush_failed error = %q, want the injected ref failure on refs/app/main", errText)
	}
	dur, ok := ev.Detail["duration_seconds"].(float64)
	if !ok || dur < 0 {
		t.Fatalf("flush_failed duration_seconds = %v (%T), want a non-negative number", ev.Detail["duration_seconds"], ev.Detail["duration_seconds"])
	}

	// The failure was transient: the next flush lands and the subscriber
	// sees the matching `flushed` for the same session.
	ok2 := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main"})
	if !ok2.OK {
		t.Fatalf("flush after disarming = %+v, want ok", ok2)
	}
	flushed := waitForEventType(t, events, "flushed", 10*time.Second)
	if flushed.Detail["kind"] != "manual" || flushed.Detail["txid"] != float64(ok2.TXID) {
		t.Fatalf("flushed after recovery = %+v, want kind manual txid %d", flushed, ok2.TXID)
	}
}

// TestEventReapedReachesSocketSubscriberWithNoDetail pins
// docs/reference.md#eventing row `reaped`: "The janitor destroys a branch
// whose TTL expired" carries db/branch and no detail at all. The branch's
// TTL is backdated via PutRef (as lifecycle_test.go's janitor tests do) and
// the janitor pass is driven directly with janitorTick(0). Mutation
// verified against: deleting the `s.events.publish(newEvent("reaped", …))`
// call in janitorTick (janitor.go).
func TestEventReapedReachesSocketSubscriberWithNoDetail(t *testing.T) {
	srv, w := newServer(t)
	sc, events := subscribeSocket(t, srv.SocketPath())
	defer sc.Close()

	if _, err := w.Fork("app", "main", "doomed", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	ref, etag, err := w.Store.GetRef("app", "doomed")
	if err != nil {
		t.Fatal(err)
	}
	ref.TTL = "1h"
	ref.TouchedAt = time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano)
	if _, err := w.Store.PutRef("app", "doomed", ref, etag); err != nil {
		t.Fatal(err)
	}

	srv.janitorTick(0)

	if _, _, err := w.Store.GetRef("app", "doomed"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("doomed must be reaped by the tick, GetRef err = %v", err)
	}
	ev := waitForEventType(t, events, "reaped", 10*time.Second)
	if ev.V != eventSchemaVersion || ev.DB != "app" || ev.Branch != "doomed" {
		t.Fatalf("reaped event = %+v, want db app branch doomed", ev)
	}
	if _, err := time.Parse(time.RFC3339, ev.TS); err != nil {
		t.Fatalf("reaped ts %q is not RFC3339: %v", ev.TS, err)
	}
	if len(ev.Detail) != 0 {
		t.Fatalf("reaped detail = %v, want none", ev.Detail)
	}
	// Exactly one reaped event per destroyed branch: main survived, so no
	// second one may follow.
	select {
	case extra, ok := <-events:
		if ok && extra.Type == "reaped" {
			t.Fatalf("a second reaped event arrived: %+v", extra)
		}
	case <-time.After(200 * time.Millisecond):
	}
}

// ---- docs/reference.md#-http: method and content-type contract ----------

// TestHTTPMethodAndContentTypeContract pins docs/reference.md#-http's
// route table: each route accepts exactly its documented method (anything
// else is 405 with an Allow header naming the one method), POST /rpc
// without Content-Type application/json is 400, a malformed JSON body is
// 400, and the server keeps answering valid requests afterwards.
// Mutation verified against: removing handleMetrics' method guard
// (http.go) — POST /metrics then answers 200.
func TestHTTPMethodAndContentTypeContract(t *testing.T) {
	_, _, base, token := newHTTPServer(t)
	auth := "Bearer " + token

	do := func(method, path, contentType, body string) *http.Response {
		t.Helper()
		var rd io.Reader
		if body != "" {
			rd = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, base+path, rd)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", auth)
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	readBody := func(resp *http.Response) string {
		t.Helper()
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	wrongMethod := []struct {
		method, path, allow string
	}{
		{http.MethodGet, "/rpc", http.MethodPost},
		{http.MethodPost, "/metrics", http.MethodGet},
		{http.MethodPost, "/healthz", http.MethodGet},
		{http.MethodPost, "/events", http.MethodGet},
	}
	for _, c := range wrongMethod {
		resp := do(c.method, c.path, "application/json", `{"op":"dbs"}`)
		body := readBody(resp)
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("%s %s: status = %d, want 405 (body %q)", c.method, c.path, resp.StatusCode, body)
		}
		if got := resp.Header.Get("Allow"); got != c.allow {
			t.Fatalf("%s %s: Allow = %q, want %q", c.method, c.path, got, c.allow)
		}
		if strings.TrimSpace(body) != "method not allowed" {
			t.Fatalf("%s %s: body = %q, want \"method not allowed\"", c.method, c.path, body)
		}
	}

	resp := do(http.MethodPost, "/rpc", "text/plain", `{"op":"dbs"}`)
	if body := readBody(resp); resp.StatusCode != http.StatusBadRequest || strings.TrimSpace(body) != "content-type must be application/json" {
		t.Fatalf("POST /rpc text/plain: status = %d body = %q, want 400 \"content-type must be application/json\"", resp.StatusCode, body)
	}
	resp = do(http.MethodPost, "/rpc", "", `{"op":"dbs"}`)
	if body := readBody(resp); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST /rpc without Content-Type: status = %d body = %q, want 400", resp.StatusCode, body)
	}
	resp = do(http.MethodPost, "/rpc", "application/json", `{`)
	if body := readBody(resp); resp.StatusCode != http.StatusBadRequest || strings.TrimSpace(body) != "invalid JSON request" {
		t.Fatalf("POST /rpc with body `{`: status = %d body = %q, want 400 \"invalid JSON request\"", resp.StatusCode, body)
	}

	// The server (and the keep-alive connection pool) still answers.
	ok := decodeResponse(t, httpRPC(t, base, auth, Request{Op: "dbs"}))
	if !ok.OK || len(ok.Databases) != 1 || ok.Databases[0] != "app" {
		t.Fatalf("dbs after the rejected requests = %+v, want ok with [app]", ok)
	}
	resp = do(http.MethodGet, "/metrics", "", "")
	if body := readBody(resp); resp.StatusCode != http.StatusOK || !strings.Contains(body, "offshoot_sessions_open") {
		t.Fatalf("GET /metrics after the rejected requests: status = %d, want 200 with the sessions gauge", resp.StatusCode)
	}
}

// ---- server.go maxSocketRequestBytes: the cap is enforced per request --

// socketRequestOfSize builds a `status` request whose encoded length is
// exactly n bytes, padding an ignored "pad" string field.
func socketRequestOfSize(t *testing.T, n int) []byte {
	t.Helper()
	const prefix, suffix = `{"op":"status","pad":"`, `"}`
	padLen := n - len(prefix) - len(suffix)
	if padLen < 0 {
		t.Fatalf("request size %d is too small to encode", n)
	}
	b := make([]byte, 0, n)
	b = append(b, prefix...)
	for i := 0; i < padLen; i++ {
		b = append(b, 'x')
	}
	b = append(b, suffix...)
	if len(b) != n {
		t.Fatalf("built %d bytes, want %d", len(b), n)
	}
	return b
}

// TestSocketOverCapRequestIsRefusedAndUnderCapStillWorks pins
// server.go's maxSocketRequestBytes contract (reference.md documents no
// number; the constant's doc comment is the spec): one request may occupy
// at most 16 MiB. A request of exactly cap−1 bytes is answered and the
// same connection then serves a plain request (the cap re-arms per
// request); a request of cap+1 bytes is refused — the daemon stops reading
// and closes that connection, so the client's decode fails — and a fresh
// connection is answered normally afterwards. Mutation verified against:
// replacing handle()'s `io.LimitedReader` with the bare connection
// (server.go) — the over-cap request is then answered ok.
func TestSocketOverCapRequestIsRefusedAndUnderCapStillWorks(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	// The over-cap connection runs concurrently with the under-cap one:
	// each costs the daemon a 16 MiB JSON scan (~10 s under -race), and
	// the two connections are handled by independent goroutines.
	type overResult struct {
		resp Response
		err  error
	}
	overDone := make(chan overResult, 1)
	over, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer over.Close()
	go func() {
		// The daemon stops reading at the cap and closes the connection;
		// the write may itself fail part-way (EPIPE/ECONNRESET) once it
		// does. Either way the decode must not yield a response.
		over.SetDeadline(time.Now().Add(60 * time.Second))
		_, _ = over.Write(socketRequestOfSize(t, maxSocketRequestBytes+1))
		var r overResult
		r.err = json.NewDecoder(over).Decode(&r.resp)
		overDone <- r
	}()

	under, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer under.Close()
	if _, err := under.Write(socketRequestOfSize(t, maxSocketRequestBytes-1)); err != nil {
		t.Fatalf("writing the cap-1 request: %v", err)
	}
	dec := json.NewDecoder(under)
	var r Response
	if err := dec.Decode(&r); err != nil {
		t.Fatalf("a request of exactly cap-1 bytes must be answered, decode: %v", err)
	}
	if !r.OK {
		t.Fatalf("cap-1 request = %+v, want ok", r)
	}
	if _, err := under.Write([]byte(`{"op":"status"}` + "\n")); err != nil {
		t.Fatalf("second request on the same connection: write: %v", err)
	}
	if err := dec.Decode(&r); err != nil || !r.OK {
		t.Fatalf("second request on the same connection = %+v err=%v, want ok (the cap is per request)", r, err)
	}

	var overRes overResult
	select {
	case overRes = <-overDone:
	case <-time.After(90 * time.Second):
		t.Fatal("the over-cap connection was neither answered nor closed within 90s")
	}
	if overRes.err == nil {
		t.Fatalf("a request of cap+1 bytes was answered %+v, want the connection ended without a response", overRes.resp)
	}

	fresh := call(t, sock, Request{Op: "status"})
	if !fresh.OK {
		t.Fatalf("status on a fresh connection after the over-cap request = %+v, want ok", fresh)
	}
}

// ---- reference.md#session-open Errors: exact double-open refusal --------

// TestDoubleOpenIsRefusedWithExactMessage tightens server_test.go's
// TestServerRefusesDoubleOpen: reference.md#session-open documents the
// refusal of a branch already open in this daemon, and server.go's opOpen
// words it "daemon: <db>@<branch> is already open here". The exact text
// is pinned so a refusal for any other reason cannot pass. Mutation
// verified against: rewording the message in opOpen (server.go).
func TestDoubleOpenIsRefusedWithExactMessage(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	first := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !first.OK {
		t.Fatalf("first open = %+v", first)
	}
	second := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if second.OK {
		t.Fatalf("second open = %+v, want a refusal", second)
	}
	if second.Error != "daemon: app@main is already open here" {
		t.Fatalf("second open error = %q, want %q", second.Error, "daemon: app@main is already open here")
	}
	// The refusal must not have disturbed the first session.
	st := getStatus(t, sock, "app", "main")
	if st.State != SessionStateOpen || st.Checkout != first.Checkout {
		t.Fatalf("status after the refused double open = %+v, want the first session untouched", st)
	}
}

// ---- protocol.go SessionInfo: capture_lag_bytes and durable_age are live --

// TestStatusCaptureLagIsALiveValue tightens lifecycle_test.go's
// TestStatusReportsDurableAgeFlushErrorAndCaptureLag: protocol.go
// documents `capture_lag_bytes` as "capture.Engine.Lag(): WAL bytes
// committed by writers but not yet applied to this session's replica".
// With auto-flush off, a writer that keeps committing must at some sampled
// moment show a positive lag through `status`, and once the writer stops
// the lag must return to exactly 0 — a constant could satisfy neither both.
// Mutation verified against: deleting `CaptureLag: sess.CaptureLag()` from
// opStatus (server.go).
func TestStatusCaptureLagIsALiveValue(t *testing.T) {
	srv, _ := newServer(t)
	srv.SetFlushEvery(0)
	sock := srv.SocketPath()

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	sqliteExec(t, open.Checkout, "CREATE TABLE t (v);")

	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		const batch = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 256) " +
			"INSERT INTO t SELECT randomblob(2048) FROM c;"
		for {
			select {
			case <-stop:
				return
			default:
			}
			// Busy errors (the engine's own short write-side locks) are
			// simply retried on the next loop iteration.
			_ = testutil.SQLite3CLI(open.Checkout, batch).Run()
		}
	}()
	stopWriter := func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
		<-writerDone
	}
	defer stopWriter()

	seen := pollStatus(t, sock, "app", "main", time.Now().Add(20*time.Second), func(in SessionInfo) bool {
		return in.CaptureLag > 0
	})
	if seen.CaptureLag <= 0 {
		t.Fatalf("capture_lag_bytes never went positive under a continuous writer: %+v", seen)
	}
	if seen.State != SessionStateOpen || seen.Error != "" {
		t.Fatalf("session under load must stay open and healthy: %+v", seen)
	}

	stopWriter()
	settled := pollStatus(t, sock, "app", "main", time.Now().Add(20*time.Second), func(in SessionInfo) bool {
		return in.CaptureLag == 0
	})
	if settled.CaptureLag != 0 {
		t.Fatalf("capture_lag_bytes must return to 0 once the engine catches up: %+v", settled)
	}
}

// TestStatusDurableAgeIsAParseableDurationThatGrows tightens the same old
// test's DurableAge assertion: protocol.go documents `durable_age` as the
// age of `last_flush_at`, rendered by opStatus as a Go duration rounded to
// the second. With auto-flush off, two status calls separated by a
// deadline-polled wait must parse and the second must be strictly larger
// while `last_flush_at` stays identical. Mutation verified against:
// deleting the `info.DurableAge = …` assignment in opStatus (server.go).
func TestStatusDurableAgeIsAParseableDurationThatGrows(t *testing.T) {
	srv, w := newServer(t)
	srv.SetFlushEvery(0)
	sock := srv.SocketPath()

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	sqliteExec(t, open.Checkout, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	// Flush may race capture; retry until the row is durable — the same
	// pattern as TestServerOpenFlushStatusClose.
	var txid uint64
	deadline := time.Now().Add(10 * time.Second)
	for txid == 0 {
		if resp := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main"}); resp.OK {
			ref, _, err := w.Store.GetRef("app", "main")
			if err != nil {
				t.Fatal(err)
			}
			if ref.HeadTXID == resp.TXID {
				txid = resp.TXID
			}
		}
		if txid == 0 {
			if time.Now().After(deadline) {
				t.Fatal("flush never became durable within 10s")
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	first := getStatus(t, sock, "app", "main")
	if first.LastFlushAt == "" {
		t.Fatalf("last_flush_at must be set after a flush: %+v", first)
	}
	if _, err := time.Parse(time.RFC3339, first.LastFlushAt); err != nil {
		t.Fatalf("last_flush_at %q is not RFC3339: %v", first.LastFlushAt, err)
	}
	firstAge, err := time.ParseDuration(first.DurableAge)
	if err != nil {
		t.Fatalf("durable_age %q is not a parseable duration: %v", first.DurableAge, err)
	}
	if firstAge < 0 || firstAge != firstAge.Round(time.Second) {
		t.Fatalf("durable_age %q must be a non-negative whole-second duration", first.DurableAge)
	}

	later := pollStatus(t, sock, "app", "main", time.Now().Add(10*time.Second), func(in SessionInfo) bool {
		age, err := time.ParseDuration(in.DurableAge)
		return err == nil && age > firstAge
	})
	laterAge, err := time.ParseDuration(later.DurableAge)
	if err != nil {
		t.Fatalf("durable_age %q is not a parseable duration: %v", later.DurableAge, err)
	}
	if laterAge <= firstAge {
		t.Fatalf("durable_age did not grow: first %q, later %q", first.DurableAge, later.DurableAge)
	}
	if later.LastFlushAt != first.LastFlushAt {
		t.Fatalf("last_flush_at changed with no flush in between: %q -> %q", first.LastFlushAt, later.LastFlushAt)
	}
	if later.DurableTXID != txid {
		t.Fatalf("durable_txid = %d, want %d (no flush happened in between)", later.DurableTXID, txid)
	}
}
