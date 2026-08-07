package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
)

// testHTTPToken is a fixed, obviously-a-test-fixture token used by every
// test in this file that needs a real, explicit (not auto-generated)
// token — long enough to look like a real GenerateToken output but
// deliberately distinguishable from one in test failure output.
const testHTTPToken = "test-fixture-token-0123456789abcdef0123456789abcdef"

// newHTTPServer starts a daemon (newServer's unix-socket daemon) and layers
// an HTTP listener on top via StartHTTP, bound to an OS-assigned loopback
// port ("127.0.0.1:0") with testHTTPToken as the Bearer token. Returns the
// server, its workspace, the HTTP base URL, and the token.
func newHTTPServer(t *testing.T) (srv *Server, w *ops.Workspace, base, token string) {
	t.Helper()
	s, ws := newServer(t)
	if err := s.StartHTTP(HTTPConfig{
		Addr:  "127.0.0.1:0",
		Token: testHTTPToken,
		Log:   io.Discard,
	}); err != nil {
		t.Fatalf("StartHTTP: %v", err)
	}
	return s, ws, "http://" + s.HTTPAddr(), testHTTPToken
}

// httpRPC POSTs req as JSON to base+"/rpc" with the given Authorization
// header value (verbatim, empty means omit the header entirely) and
// returns the raw *http.Response for status-code assertions plus the
// decoded body bytes (nil if the body isn't read here). Callers that need
// a decoded Response call decodeResponse on the returned body.
func httpRPC(t *testing.T, base, authHeader string, req Request) *http.Response {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	httpReq, err := http.NewRequest(http.MethodPost, base+"/rpc", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if authHeader != "" {
		httpReq.Header.Set("Authorization", authHeader)
	}
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func decodeResponse(t *testing.T, resp *http.Response) Response {
	t.Helper()
	defer resp.Body.Close()
	var out Response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding Response body: %v", err)
	}
	return out
}

// ---- auth ----

func TestHTTPAuthRejectsNoHeader(t *testing.T) {
	_, _, base, _ := newHTTPServer(t)
	resp := httpRPC(t, base, "", Request{Op: "dbs"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (no Authorization header)", resp.StatusCode)
	}
}

func TestHTTPAuthRejectsWrongToken(t *testing.T) {
	_, _, base, _ := newHTTPServer(t)
	resp := httpRPC(t, base, "Bearer not-the-right-token", Request{Op: "dbs"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (wrong token)", resp.StatusCode)
	}
}

func TestHTTPAuthRejectsMalformedHeader(t *testing.T) {
	_, _, base, token := newHTTPServer(t)
	for _, h := range []string{
		token,                       // no "Bearer " scheme at all
		"Basic " + token,            // wrong scheme
		"Bearer",                    // scheme with no token, no trailing space
		"Bearer" + token,            // missing the separating space
		"Bearer " + token + "extra", // right prefix, wrong (longer) token
	} {
		resp := httpRPC(t, base, h, Request{Op: "dbs"})
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("Authorization %q: status = %d, want 401", h, resp.StatusCode)
		}
	}
}

func TestHTTPAuthAccepts(t *testing.T) {
	_, _, base, token := newHTTPServer(t)
	resp := httpRPC(t, base, "Bearer "+token, Request{Op: "dbs"})
	out := decodeResponse(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if !out.OK {
		t.Fatalf("response = %+v, want ok", out)
	}
}

// TestHTTPAuthAcceptsCaseInsensitiveBearerScheme pins checkAuth's documented
// case-insensitive scheme matching (RFC 7235: auth-scheme names are
// case-insensitive) as a POSITIVE assertion — TestHTTPAuthRejectsMalformedHeader
// only covers scheme-related REJECTIONS, so without this test a future
// change that accidentally made scheme matching case-SENSITIVE would pass
// every existing test while quietly breaking any client that sends a
// lowercase "bearer" (some HTTP libraries normalize scheme names to
// lowercase automatically).
func TestHTTPAuthAcceptsCaseInsensitiveBearerScheme(t *testing.T) {
	_, _, base, token := newHTTPServer(t)
	for _, scheme := range []string{"bearer", "BEARER", "BeArEr"} {
		resp := httpRPC(t, base, scheme+" "+token, Request{Op: "dbs"})
		out := decodeResponse(t, resp)
		if resp.StatusCode != http.StatusOK || !out.OK {
			t.Errorf("scheme %q: status=%d response=%+v, want 200/ok", scheme, resp.StatusCode, out)
		}
	}
}

// ---- rpc parity with the unix socket ----

// TestHTTPRPCParityWithSocket exercises a create/branches pair each
// direction (create over one transport, list over the other) and asserts
// the two transports agree: both dispatch through the exact same
// Server.dispatch, so a db created via HTTP must be visible via the unix
// socket's "branches" op and vice versa, with identical BranchInfo shape.
func TestHTTPRPCParityWithSocket(t *testing.T) {
	srv, _, base, token := newHTTPServer(t)
	sock := srv.SocketPath()

	// Created over HTTP, read back over the unix socket.
	createResp := decodeResponse(t, httpRPC(t, base, "Bearer "+token, Request{Op: "create", DB: "httpdb"}))
	if !createResp.OK {
		t.Fatalf("create over http = %+v", createResp)
	}
	viaSocket := call(t, sock, Request{Op: "branches", DB: "httpdb"})
	if !viaSocket.OK || len(viaSocket.Branches) != 1 || viaSocket.Branches[0].Branch != "main" {
		t.Fatalf("branches over socket after http create = %+v", viaSocket)
	}

	// Created over the unix socket, read back over HTTP.
	sockCreate := call(t, sock, Request{Op: "create", DB: "sockdb"})
	if !sockCreate.OK {
		t.Fatalf("create over socket = %+v", sockCreate)
	}
	viaHTTP := decodeResponse(t, httpRPC(t, base, "Bearer "+token, Request{Op: "branches", DB: "sockdb"}))
	if !viaHTTP.OK || len(viaHTTP.Branches) != 1 || viaHTTP.Branches[0].Branch != "main" {
		t.Fatalf("branches over http after socket create = %+v", viaHTTP)
	}

	// Same db, queried both ways, must report byte-identical BranchInfo
	// (module the two responses being fetched at slightly different
	// instants isn't a factor here since nothing mutates "httpdb" in
	// between).
	bothA := call(t, sock, Request{Op: "branches", DB: "httpdb"})
	bothB := decodeResponse(t, httpRPC(t, base, "Bearer "+token, Request{Op: "branches", DB: "httpdb"}))
	if len(bothA.Branches) != 1 || len(bothB.Branches) != 1 {
		t.Fatalf("expected exactly one branch each side: socket=%+v http=%+v", bothA.Branches, bothB.Branches)
	}
	if !reflect.DeepEqual(bothA.Branches[0], bothB.Branches[0]) {
		t.Fatalf("socket and http disagree on httpdb's branches: socket=%+v http=%+v",
			bothA.Branches[0], bothB.Branches[0])
	}
}

// ---- healthz ----

func TestHTTPHealthzUnauthenticated(t *testing.T) {
	_, _, base, _ := newHTTPServer(t)
	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (healthz needs no auth)", resp.StatusCode)
	}
	var out struct {
		OK       bool `json:"ok"`
		Sessions int  `json:"sessions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if !out.OK {
		t.Fatalf("healthz body = %+v, want ok=true", out)
	}
}

func TestHTTPHealthzReportsOpenSessionCount(t *testing.T) {
	srv, _, base, _ := newHTTPServer(t)
	sock := srv.SocketPath()

	get := func() int {
		resp, err := http.Get(base + "/healthz")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct {
			Sessions int `json:"sessions"`
		}
		json.NewDecoder(resp.Body).Decode(&out)
		return out.Sessions
	}

	if n := get(); n != 0 {
		t.Fatalf("sessions before any open = %d, want 0", n)
	}
	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	if n := get(); n != 1 {
		t.Fatalf("sessions after open = %d, want 1", n)
	}
	call(t, sock, Request{Op: "close", DB: "app", Branch: "main"})
	if n := get(); n != 0 {
		t.Fatalf("sessions after close = %d, want 0", n)
	}
}

// ---- non-loopback bind validation ----

func TestValidateHTTPBind(t *testing.T) {
	cases := []struct {
		name             string
		addr             string
		allowNonLoopback bool
		hasToken         bool
		wantErr          bool
		wantSubstr       string
	}{
		{"loopback ipv4 no ack no token ok", "127.0.0.1:8080", false, false, false, ""},
		{"loopback ipv6 no ack no token ok", "[::1]:8080", false, false, false, ""},
		{"localhost name no ack no token ok", "localhost:8080", false, false, false, ""},
		{"non-loopback no ack refused", "0.0.0.0:8080", false, false, true, "-http-allow-non-loopback"},
		{"non-loopback ack no token refused", "0.0.0.0:8080", true, false, true, "explicit -token or OFFSHOOT_TOKEN"},
		{"non-loopback ack with token ok", "0.0.0.0:8080", true, true, false, ""},
		{"all-interfaces empty host treated non-loopback", ":8080", true, true, false, ""},
		{"all-interfaces empty host no ack refused", ":8080", false, false, true, "-http-allow-non-loopback"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidateHTTPBind(c.addr, c.allowNonLoopback, c.hasToken)
			if c.wantErr && err == nil {
				t.Fatalf("ValidateHTTPBind(%q, %v, %v) = nil, want error", c.addr, c.allowNonLoopback, c.hasToken)
			}
			if !c.wantErr && err != nil {
				t.Fatalf("ValidateHTTPBind(%q, %v, %v) = %v, want nil", c.addr, c.allowNonLoopback, c.hasToken, err)
			}
			if c.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), c.wantSubstr)) {
				t.Fatalf("error = %v, want substring %q", err, c.wantSubstr)
			}
		})
	}
}

// TestValidateHTTPBindErrorsAreDistinct pins that the two non-loopback
// failure modes (missing ack flag vs missing explicit token) produce
// DIFFERENT error text, not a single generic refusal — an operator (or a
// test) must be able to tell which one they hit.
func TestValidateHTTPBindErrorsAreDistinct(t *testing.T) {
	noAck := ValidateHTTPBind("0.0.0.0:8080", false, false)
	noToken := ValidateHTTPBind("0.0.0.0:8080", true, false)
	if noAck == nil || noToken == nil {
		t.Fatalf("expected both to error: noAck=%v noToken=%v", noAck, noToken)
	}
	if noAck.Error() == noToken.Error() {
		t.Fatalf("the two non-loopback refusal errors must be distinct, both were: %q", noAck.Error())
	}
}

// TestStartHTTPRefusesNonLoopbackWithoutAckOrToken exercises the same
// guard end to end through StartHTTP itself (not just the pure
// ValidateHTTPBind function), proving a caller cannot bypass validation by
// calling StartHTTP directly.
func TestStartHTTPRefusesNonLoopbackWithoutAckOrToken(t *testing.T) {
	srv, _ := newServer(t)
	err := srv.StartHTTP(HTTPConfig{Addr: "0.0.0.0:0", Token: testHTTPToken})
	if err == nil {
		t.Fatal("StartHTTP on a non-loopback address without -http-allow-non-loopback must error")
	}
	if !strings.Contains(err.Error(), "-http-allow-non-loopback") {
		t.Fatalf("unexpected error: %v", err)
	}

	srv2, _ := newServer(t)
	err2 := srv2.StartHTTP(HTTPConfig{Addr: "0.0.0.0:0", Token: testHTTPToken, AllowNonLoopback: true, AutoGenerated: true})
	if err2 == nil {
		t.Fatal("StartHTTP on a non-loopback address with an auto-generated token must error")
	}
	if !strings.Contains(err2.Error(), "explicit -token or OFFSHOOT_TOKEN") {
		t.Fatalf("unexpected error: %v", err2)
	}
	if err.Error() == err2.Error() {
		t.Fatalf("the two refusals must be distinct")
	}
}

// TestStartHTTPRejectsShortToken pins the minHTTPTokenLen guard: an
// explicit token under 16 characters is refused outright, since
// TokenFingerprint's 8-character prefix would otherwise reveal half (or,
// for anything 8 chars or under, ALL) of a short token in ongoing
// status/log output.
func TestStartHTTPRejectsShortToken(t *testing.T) {
	srv, _ := newServer(t)
	err := srv.StartHTTP(HTTPConfig{Addr: "127.0.0.1:0", Token: "short-token"})
	if err == nil {
		t.Fatal("StartHTTP with a 11-character token must be refused")
	}
	if !strings.Contains(err.Error(), "at least") {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv.HTTPAddr() != "" {
		t.Fatal("a rejected StartHTTP call must not leave a listener bound")
	}
}

// TestStartHTTPConcurrentCallsOnlyOneWins is the TOCTOU regression test for
// StartHTTP's already-called guard: the first check (before the slow,
// unlocked net.Listen call) is not enough on its own to prevent two
// concurrent StartHTTP calls from both passing it and racing to bind their
// own listener — only the SECOND check, taken again under s.mu after both
// listeners are already bound, can catch that. Without it, the loser's
// httpSrv/httpToken/httpAddr write could silently clobber the winner's
// (or vice versa, depending on scheduling), leaving the daemon serving on
// a listener/token combination neither caller can predict, with the
// loser's own listener leaked (never closed). This asserts the outcome a
// caller can actually observe: exactly one of the two calls succeeds, the
// other gets the "already called" error, and HTTPAddr() afterward reports
// a real, working listener.
func TestStartHTTPConcurrentCallsOnlyOneWins(t *testing.T) {
	srv, _ := newServer(t)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- srv.StartHTTP(HTTPConfig{Addr: "127.0.0.1:0", Token: testHTTPToken, Log: io.Discard})
		}()
	}
	wg.Wait()
	close(errs)

	var succeeded, failed int
	var failErr error
	for err := range errs {
		if err == nil {
			succeeded++
		} else {
			failed++
			failErr = err
		}
	}
	if succeeded != 1 || failed != 1 {
		t.Fatalf("concurrent StartHTTP calls: %d succeeded, %d failed, want exactly 1 and 1", succeeded, failed)
	}
	if !strings.Contains(failErr.Error(), "already called") {
		t.Fatalf("loser's error = %v, want \"already called\"", failErr)
	}
	if srv.HTTPAddr() == "" {
		t.Fatal("the winner's listener must be reachable via HTTPAddr()")
	}

	// The winner's listener must actually work end to end.
	resp, err := http.Get("http://" + srv.HTTPAddr() + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz on the winning listener: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want 200", resp.StatusCode)
	}
}

// ---- token generation / fingerprint / redaction ----

func TestGenerateTokenIsHighEntropyAndUnique(t *testing.T) {
	a, err := GenerateToken()
	if err != nil {
		t.Fatal(err)
	}
	b, err := GenerateToken()
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 64 {
		t.Fatalf("token length = %d, want 64 (32 bytes hex-encoded)", len(a))
	}
	if a == b {
		t.Fatal("two GenerateToken calls produced the same token")
	}
}

func TestTokenFingerprint(t *testing.T) {
	long := "0123456789abcdef"
	if fp := TokenFingerprint(long); fp != "01234567" {
		t.Fatalf("fingerprint of %q = %q, want first 8 chars", long, fp)
	}
	short := "abc"
	if fp := TokenFingerprint(short); fp != short {
		t.Fatalf("fingerprint of a <=8-char token must be the token itself, got %q", fp)
	}
}

// TestHTTPTokenRedaction is the grep-the-log-output test the task
// description calls for: with an EXPLICIT (non-auto-generated) token,
// StartHTTP's log output must show the fingerprint but must NEVER contain
// the full token anywhere.
func TestHTTPTokenRedaction(t *testing.T) {
	srv, _ := newServer(t)
	var log bytes.Buffer
	if err := srv.StartHTTP(HTTPConfig{Addr: "127.0.0.1:0", Token: testHTTPToken, Log: &log}); err != nil {
		t.Fatal(err)
	}
	out := log.String()
	if strings.Contains(out, testHTTPToken) {
		t.Fatalf("log output contains the full token (must never, for an explicit token):\n%s", out)
	}
	fp := TokenFingerprint(testHTTPToken)
	if !strings.Contains(out, fp) {
		t.Fatalf("log output does not contain the token fingerprint %q:\n%s", fp, out)
	}
}

// TestHTTPAutoGeneratedTokenPrintedOnceThenOnlyFingerprint proves the OTHER
// half of PM Amendment 10: a genuinely auto-generated token IS printed in
// full, but exactly once (the startup line), and nowhere else — in
// particular, repeated log lines after startup (simulated here by calling
// the logging path a second time via a fresh StartHTTP on a second server)
// must never repeat the full token, only its fingerprint.
func TestHTTPAutoGeneratedTokenPrintedOnceThenOnlyFingerprint(t *testing.T) {
	srv, _ := newServer(t)
	token, err := GenerateToken()
	if err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	if err := srv.StartHTTP(HTTPConfig{
		Addr: "127.0.0.1:0", Token: token, AutoGenerated: true, Log: &log,
	}); err != nil {
		t.Fatal(err)
	}
	out := log.String()
	if n := strings.Count(out, token); n != 1 {
		t.Fatalf("auto-generated token appears %d times in startup log, want exactly 1:\n%s", n, out)
	}
	fp := TokenFingerprint(token)
	if !strings.Contains(out, fp) {
		t.Fatalf("startup log missing token fingerprint %q:\n%s", fp, out)
	}
}

// ---- body size limit ----

// TestHTTPRPCBodySizeLimit posts an oversized body (over maxRPCBodyBytes)
// and asserts a 413-class response, then proves the server is still
// usable afterward (the oversized request did not wedge or crash it).
func TestHTTPRPCBodySizeLimit(t *testing.T) {
	_, _, base, token := newHTTPServer(t)

	// A syntactically-plausible but oversized JSON body: a "meta" map with
	// one very large value, comfortably past maxRPCBodyBytes (1MiB).
	big := strings.Repeat("x", maxRPCBodyBytes+1024)
	body := fmt.Sprintf(`{"op":"create","db":"x","meta":{"k":"%s"}}`, big)

	req, err := http.NewRequest(http.MethodPost, base+"/rpc", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}

	// Connection/server intact: an ordinary request right after succeeds.
	ok := decodeResponse(t, httpRPC(t, base, "Bearer "+token, Request{Op: "dbs"}))
	if !ok.OK {
		t.Fatalf("request after oversized body = %+v, want ok (server must still be usable)", ok)
	}
}

// ---- pprof behind auth ----

func TestHTTPPprofBehindAuth(t *testing.T) {
	_, _, base, token := newHTTPServer(t)

	resp, err := http.Get(base + "/debug/pprof/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /debug/pprof/ without auth: status = %d, want 401", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodGet, base+"/debug/pprof/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/pprof/ with auth: status = %d, want 200", resp2.StatusCode)
	}

	req3, _ := http.NewRequest(http.MethodGet, base+"/debug/pprof/cmdline", nil)
	req3.Header.Set("Authorization", "Bearer "+token)
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/pprof/cmdline with auth: status = %d, want 200", resp3.StatusCode)
	}
}

// failingResponseWriter is a minimal http.ResponseWriter whose Write always
// fails — used by TestHandleMetricsLogsWritePrometheusErrorsToConfiguredLog
// to force handleMetrics's WritePrometheus error path deterministically
// (a real HTTP round trip has no reliable way to make the server's own
// io.WriteString to the connection fail on demand).
type failingResponseWriter struct {
	h http.Header
}

func (f *failingResponseWriter) Header() http.Header {
	if f.h == nil {
		f.h = http.Header{}
	}
	return f.h
}
func (f *failingResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("simulated write failure")
}
func (f *failingResponseWriter) WriteHeader(int) {}

// TestHandleMetricsLogsWritePrometheusErrorsToConfiguredLog pins that
// handleMetrics's error-path logging goes to HTTPConfig.Log (s.httpLog),
// not a bare os.Stderr — a caller/test that redirected StartHTTP's logging
// (e.g. to a buffer, specifically to keep assertions off real stderr)
// must see this line too.
func TestHandleMetricsLogsWritePrometheusErrorsToConfiguredLog(t *testing.T) {
	srv, _ := newServer(t)
	var log bytes.Buffer
	if err := srv.StartHTTP(HTTPConfig{Addr: "127.0.0.1:0", Token: testHTTPToken, Log: &log}); err != nil {
		t.Fatal(err)
	}
	log.Reset() // drop StartHTTP's own startup lines; only handleMetrics's line matters below

	fw := &failingResponseWriter{}
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	srv.handleMetrics(fw, req)

	out := log.String()
	if !strings.Contains(out, "offshoot: http: /metrics:") {
		t.Fatalf("handleMetrics's WritePrometheus error was not logged to the configured Log writer:\n%s", out)
	}
}

// ---- concurrent scrapes (T2 review carried item) ----

var (
	metricsSessionsOpenRE = regexp.MustCompile(`(?m)^offshoot_sessions_open (\S+)$`)
	metricsCaptureLagRE   = regexp.MustCompile(`(?m)^offshoot_capture_lag_bytes\{`)
)

// TestConcurrentMetricsScrapesWithSessionChurn is the end-to-end (real
// HTTP, real daemon, real sessions) half of the T2-review carried item:
// many goroutines scrape GET /metrics concurrently while OTHER goroutines
// continuously open/close sessions on several branches. Every single scrape
// response must be a complete, self-consistent snapshot: since
// collectSessionGauges (metrics.go) always emits exactly one
// offshoot_capture_lag_bytes sample per counted session, in the SAME
// collector run that sets offshoot_sessions_open to that same count, the
// two must agree in every response body — any mismatch means a scrape
// observed another concurrent scrape's collector mid-Reset/repopulate (see
// internal/metrics's Registry.scrapeMu doc comment). Run with -race.
func TestConcurrentMetricsScrapesWithSessionChurn(t *testing.T) {
	srv, w, base, token := newHTTPServer(t)
	sock := srv.SocketPath()

	branches := []string{"c1", "c2", "c3", "c4"}
	for _, b := range branches {
		if _, err := w.Fork("app", "main", b, "", 0, nil); err != nil {
			t.Fatalf("fork %s: %v", b, err)
		}
	}

	stop := make(chan struct{})
	var churnWG sync.WaitGroup
	for _, b := range branches {
		b := b
		churnWG.Add(1)
		go func() {
			defer churnWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				call(t, sock, Request{Op: "open", DB: "app", Branch: b})
				time.Sleep(2 * time.Millisecond)
				call(t, sock, Request{Op: "close", DB: "app", Branch: b})
			}
		}()
	}

	var scrapeWG sync.WaitGroup
	mismatches := make(chan string, 4096)
	const scrapers = 8
	const perScraper = 40
	for i := 0; i < scrapers; i++ {
		scrapeWG.Add(1)
		go func() {
			defer scrapeWG.Done()
			for j := 0; j < perScraper; j++ {
				req, err := http.NewRequest(http.MethodGet, base+"/metrics", nil)
				if err != nil {
					mismatches <- err.Error()
					continue
				}
				req.Header.Set("Authorization", "Bearer "+token)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					mismatches <- err.Error()
					continue
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					mismatches <- err.Error()
					continue
				}
				if resp.StatusCode != http.StatusOK {
					mismatches <- fmt.Sprintf("status %d:\n%s", resp.StatusCode, body)
					continue
				}
				out := string(body)
				m := metricsSessionsOpenRE.FindStringSubmatch(out)
				if m == nil {
					mismatches <- "missing offshoot_sessions_open sample:\n" + out
					continue
				}
				n, err := strconv.ParseFloat(m[1], 64)
				if err != nil {
					mismatches <- "unparseable offshoot_sessions_open: " + m[1]
					continue
				}
				lines := len(metricsCaptureLagRE.FindAllString(out, -1))
				if int(n) != lines {
					mismatches <- fmt.Sprintf(
						"torn scrape: offshoot_sessions_open=%v but %d offshoot_capture_lag_bytes samples",
						n, lines)
				}
			}
		}()
	}
	scrapeWG.Wait()
	close(stop)
	churnWG.Wait()

	close(mismatches)
	for m := range mismatches {
		t.Error(m)
	}
}

// ---- shutdown ordering / in-flight safety ----

// TestHTTPShutdownResponseIsNotChunkedAndReadAllUnmarshalsCleanly is the
// direct regression test for IMPORTANT-1 (review fold-in round 2): the
// EARLIER fix's own proof (TestHTTPShutdownRespondsBeforeClosingRequestingConn)
// reads the response via json.Decoder, which stops as soon as it has
// decoded one complete JSON value and NEVER reads a chunked body's
// terminating "0\r\n\r\n" trailer — so that test could not have failed
// against the pre-fix (streamed json.Encoder + Flush(), no declared
// Content-Length -> forced chunked encoding) code, even though the bug was
// real. The actual victim is a client that reads the FULL body first (e.g.
// io.ReadAll, or any ReadAll-then-json.Unmarshal pattern), which — on a
// body missing its trailer — surfaces io.ErrUnexpectedEOF even when the
// JSON payload itself had fully landed.
//
// This test targets that exact class, on the "shutdown" op specifically
// (it exercises the respond-then-shutdown-trigger window handleRPC's doc
// comment describes, the narrowest and most relevant case): it reads the
// response with io.ReadAll, not json.Decoder, and separately asserts the
// response's FRAMING headers directly (resp.ContentLength >= 0, no
// "chunked" in resp.TransferEncoding). The framing assertion is the
// deterministic half — Go's http.Client always reports ContentLength == -1
// and a "chunked" TransferEncoding for a response sent without a declared
// Content-Length, on every run, regardless of whether the timing race that
// makes the trailer actually go missing happens to fire on this
// particular run (reviewer reported not reproducing the raw EOF in 3000
// iterations locally) — so this is what actually regresses reliably if
// handleRPC's Content-Length-framed single Write is ever reverted back to
// streaming json.Encoder + Flush().
//
// Verified: reverting handleRPC's fix locally (restoring the
// json.NewEncoder(w).Encode(resp) + Flush() path with no Content-Length)
// makes this test fail every time on the framing assertion
// (ContentLength == -1, TransferEncoding == ["chunked"]) — see this
// task's report for the exact before/after run. Restored, this test
// passes.
func TestHTTPShutdownResponseIsNotChunkedAndReadAllUnmarshalsCleanly(t *testing.T) {
	_, _, base, token := newHTTPServer(t)

	body, err := json.Marshal(Request{Op: "shutdown"})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, base+"/rpc", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Framing: must be Content-Length-declared, never chunked. This is the
	// part that fails DETERMINISTICALLY (every run, not just under a
	// timing race) against the pre-fix streamed-Encoder-plus-Flush code.
	if resp.ContentLength < 0 {
		t.Fatalf("response ContentLength = %d, want >= 0 (negative means Go's client saw chunked "+
			"Transfer-Encoding rather than a declared Content-Length)", resp.ContentLength)
	}
	for _, te := range resp.TransferEncoding {
		if strings.EqualFold(te, "chunked") {
			t.Fatalf("response TransferEncoding = %v, must not include \"chunked\"", resp.TransferEncoding)
		}
	}

	// The actual victim scenario: read the FULL body via io.ReadAll (never
	// json.Decoder, which would mask exactly this failure mode — see this
	// test's own doc comment), then unmarshal what was read.
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("io.ReadAll(resp.Body) = %v (a ReadAll-then-Unmarshal client would see this on a body "+
			"missing its chunked trailer, even though the JSON payload itself had fully landed)", err)
	}
	var out Response
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("json.Unmarshal(%q) = %v", raw, err)
	}
	if !out.OK {
		t.Fatalf("shutdown response = %+v, want OK", out)
	}
}

// TestHTTPShutdownRespondsBeforeClosingRequestingConn is the HTTP analog of
// server_test.go's TestShutdownRespondsBeforeClosingRequestingConn: an
// op=shutdown request's own response must be fully written before
// Shutdown's http.Server.Close() call can tear down the connection it
// arrived on. Uses httpShutdownRespondDelay the same way the socket test
// uses shutdownRespondDelay, to force the losing interleaving
// deterministically instead of relying on real scheduling luck.
func TestHTTPShutdownRespondsBeforeClosingRequestingConn(t *testing.T) {
	_, _, base, token := newHTTPServer(t)

	entered := make(chan struct{})
	release := make(chan struct{})
	httpShutdownRespondDelay = func() {
		close(entered)
		<-release
	}
	defer func() { httpShutdownRespondDelay = nil }()

	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	go func() {
		body, _ := json.Marshal(Request{Op: "shutdown"})
		req, err := http.NewRequest(http.MethodPost, base+"/rpc", bytes.NewReader(body))
		if err != nil {
			done <- result{err: err}
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		done <- result{resp: resp, err: err}
	}()

	<-entered // handle has dispatched "shutdown" and is about to write its response
	time.Sleep(200 * time.Millisecond)
	close(release)

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("shutdown request failed: %v (the daemon must finish writing the shutdown "+
				"response before Shutdown's http.Server.Close() can touch this connection)", r.err)
		}
		defer r.resp.Body.Close()
		var out Response
		if err := json.NewDecoder(r.resp.Body).Decode(&out); err != nil {
			t.Fatalf("decoding shutdown response: %v", err)
		}
		if !out.OK {
			t.Fatalf("shutdown response = %+v, want OK", out)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the shutdown response")
	}
}

// TestHTTPShutdownWhileRequestsInFlightIsSafe hammers the HTTP server with
// concurrent requests while Shutdown runs concurrently: the only
// requirement is no panic and no hang — every in-flight request either
// completes with a real response or fails with a clean connection error.
// Run with -race.
func TestHTTPShutdownWhileRequestsInFlightIsSafe(t *testing.T) {
	srv, _, base, token := newHTTPServer(t)

	// Deliberately not using httpRPC/decodeResponse here: those call
	// t.Fatal on any request-level error, but a request failing outright
	// (connection refused/reset once Shutdown closes the listener) is an
	// EXPECTED outcome of this test, not a failure — only a panic or hang
	// (which the race detector / test timeout would catch on their own) is.
	doOne := func() {
		body, _ := json.Marshal(Request{Op: "dbs"})
		req, err := http.NewRequest(http.MethodPost, base+"/rpc", bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				doOne()
			}
		}()
	}

	// Let some requests get going, then shut down concurrently.
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	close(stop)
	wg.Wait()
}

// ---- path-trick matrix (routing/auth-bypass attempts) ----

// TestHTTPPathTrickMatrixNeverBypassesAuth is a reviewer-probed matrix of
// path variants aimed at reaching a Bearer-protected handler (/metrics,
// /debug/pprof) WITHOUT ever satisfying requireAuth — case variation,
// dot-segments, double slashes, a percent-encoded slash, and a trailing
// ";" — plus one no-trailing-slash pprof variant that exercises
// http.ServeMux's own subtree-redirect behavior. Every request here is
// sent with NO Authorization header via a client that follows redirects
// (http.Get's default), so the only acceptable final outcomes are 401
// (the request eventually reached a requireAuth-wrapped handler, and was
// correctly rejected) or 404 (the mux never matched a registered pattern
// at all) — 200 would mean an auth bypass, and anything else would be a
// surprise worth understanding before trusting this routing. Pinning the
// ACTUAL observed status per variant (not just "not 200") means a future
// Go version's ServeMux behavior change shows up as a targeted test
// failure here instead of an unnoticed regression.
func TestHTTPPathTrickMatrixNeverBypassesAuth(t *testing.T) {
	_, _, base, _ := newHTTPServer(t)

	cases := []struct {
		path string
		want int
	}{
		{"/RPC", http.StatusNotFound},                           // case-sensitive: /RPC != /rpc
		{"//metrics", http.StatusUnauthorized},                  // net/http's own path-cleaning collapses "//" -> "/", redirects to "/metrics", then hits auth
		{"/./metrics", http.StatusUnauthorized},                 // net/http cleans "/./metrics" -> "/metrics", redirects, then hits auth
		{"/a/../metrics", http.StatusUnauthorized},              // cleans to "/metrics", redirects, then hits auth
		{"/healthz/../metrics", http.StatusUnauthorized},        // cleans to "/metrics" (NOT a healthz bypass), redirects, then hits auth
		{"/healthz%2f..%2fmetrics", http.StatusNotFound},        // %2f is NOT decoded into a path separator for routing; literal segment, no match
		{"/debug/pprof/../../metrics", http.StatusUnauthorized}, // cleans to "/metrics", redirects, then hits auth
		{"/metrics;", http.StatusNotFound},                      // literal distinct path; ";" is not a segment/query separator here
		{"/debug/pprof", http.StatusUnauthorized},               // subtree pattern "/debug/pprof/" redirects the no-slash form, then hits auth
	}

	for _, c := range cases {
		t.Run(c.path, func(t *testing.T) {
			resp, err := http.Get(base + c.path)
			if err != nil {
				t.Fatalf("GET %s: %v", c.path, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("GET %s with NO Authorization header returned 200 (auth bypass!): body=%s", c.path, body)
			}
			if resp.StatusCode != c.want {
				t.Fatalf("GET %s = %d, want %d (still not a bypass, but the routing behavior this test "+
					"pins has changed — re-verify by hand before updating the expectation)",
					c.path, resp.StatusCode, c.want)
			}
		})
	}
}
