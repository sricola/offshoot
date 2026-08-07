// HTTP listener: Milestone 4 Task 3. Opt-in (StartHTTP is never called
// unless `offshoot serve -http ADDR` asks for it — see cmd/offshoot's serve
// command), single-token auth, and the same op dispatch the unix socket
// already uses (Server.dispatch), so a client sees byte-identical behavior
// whether it talks JSON-over-unix-socket or JSON-over-POST.
//
// Threat model (see docs/reference.md and docs/operations.md for the full
// statement): the token is a shared secret for a single-tenant, same-host-
// or-trusted-network deployment, not a multi-tenant isolation boundary.
// Anyone holding the token can do everything any daemon client can do —
// exactly as anyone able to open the unix socket already can. Loopback
// binds need no acknowledgment; a non-loopback bind requires BOTH
// -http-allow-non-loopback (an explicit "yes, this leaves localhost") AND
// an explicit token (auto-generate-and-print is a loopback-only
// convenience — see ValidateHTTPBind).
package daemon

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"
)

// maxRPCBodyBytes bounds a single POST /rpc request body. Every Request
// field is a short string, a small string->string Meta map (itself capped
// by ops.ValidateMeta), or a bool/duration string — 1MiB is generous for
// any legitimate request and small enough that a misbehaving or malicious
// client can't tie up daemon memory/CPU decoding an arbitrarily large body.
const maxRPCBodyBytes = 1 << 20 // 1MiB

// HTTP timeouts. Justification for each (PM Amendment 6 asks for explicit,
// sane values, not stdlib's unbounded defaults):
//
//   - httpReadHeaderTimeout guards against a slow/incomplete-headers client
//     (classic slowloris) tying up a connection indefinitely; 5s is ample
//     for headers on a local/trusted-network link and short enough that a
//     stalled handshake is shed quickly.
//
//   - httpReadTimeout bounds the time to read the full request, including
//     body — /rpc's body is capped at maxRPCBodyBytes above, so 30s is
//     generous headroom even over a slow link, not a tight fit.
//
//   - httpWriteTimeout bounds total handler time (net/http.Server.WriteTimeout
//     starts at the end of header read and covers the ENTIRE handler,
//     response included — see net/http's doc comment). This has to
//     accommodate /debug/pprof/profile and /trace, whose default capture
//     window is 30s; 90s leaves headroom for that default capture plus
//     write time without inviting a truly runaway handler to hold a
//     connection forever. An operator requesting a longer profile via
//     `?seconds=N` beyond this budget will see the connection cut — a
//     documented tradeoff (docs/reference.md), not a bug: request a
//     shorter window, or profile out-of-band.
//
//     *** MILESTONE 4 TASK 4a WARNING ***: this same 90s WriteTimeout will
//     hard-cut a long-lived SSE `GET /events` stream (Task 4a's planned
//     addition to this same http.Server/mux) at 90 seconds — WriteTimeout
//     has no concept of "a stream that's still legitimately sending, just
//     slowly," it only sees total handler wall-clock time. Task 4a's
//     /events handler MUST clear the write deadline for its own connection
//     before it starts streaming, e.g.
//     `http.NewResponseController(w).SetWriteDeadline(time.Time{})` — do
//     NOT raise this constant instead: every OTHER handler on this
//     http.Server (/rpc, /metrics, /debug/pprof/*) still wants the 90s
//     bound, and per-connection deadline overrides are exactly the
//     mechanism net/http provides for a single long-lived handler to opt
//     out without changing the server-wide default. See
//     https://pkg.go.dev/net/http#ResponseController.SetWriteDeadline.
//
//   - httpIdleTimeout closes idle keep-alive connections after 2 minutes so
//     a client that opens many connections and goes quiet doesn't pin
//     server-side resources indefinitely.
const (
	httpReadHeaderTimeout = 5 * time.Second
	httpReadTimeout       = 30 * time.Second
	httpWriteTimeout      = 90 * time.Second
	httpIdleTimeout       = 2 * time.Minute
)

// httpShutdownRespondDelay, when non-nil, is invoked by handleRPC after
// dispatching a "shutdown" request but before writing/flushing that
// request's HTTP response. It exists purely for tests — the HTTP analog of
// shutdownRespondDelay (see that var's doc comment and
// TestShutdownRespondsBeforeClosingRequestingConn in server_test.go for the
// unix-socket original this mirrors): it lets a test force the
// interleaving that would expose a bug where Shutdown's http.Server.Close()
// call could tear down this very connection before this response has
// actually been written. Nil (the default) is a no-op in production.
var httpShutdownRespondDelay func()

// HTTPConfig configures StartHTTP.
type HTTPConfig struct {
	// Addr is the "host:port" to bind, e.g. "127.0.0.1:8080". Validated by
	// ValidateHTTPBind (called internally by StartHTTP — a caller does not
	// need to call it separately, though cmd/offshoot's serve command does
	// so too, to fail fast before it even attempts a token/listener setup).
	Addr string
	// Token is the Bearer token every request but /healthz must present.
	// Must be non-empty.
	Token string
	// AllowNonLoopback mirrors -http-allow-non-loopback: an explicit
	// acknowledgment that Addr may be reachable beyond localhost.
	AllowNonLoopback bool
	// AutoGenerated marks Token as having been generated by this process
	// (GenerateToken) rather than supplied explicitly (-token/
	// OFFSHOOT_TOKEN) — controls whether StartHTTP prints the token in full
	// exactly once (loopback only; see ValidateHTTPBind) versus never
	// printing it at all beyond its fingerprint.
	AutoGenerated bool
	// Log receives StartHTTP's startup line(s) (and, if AutoGenerated, the
	// one-time full-token line). Defaults to os.Stderr if nil — tests pass
	// a buffer instead so they can assert on exactly what was logged
	// without racing real stderr.
	Log io.Writer
}

// GenerateToken returns a fresh, high-entropy hex-encoded token (32 random
// bytes / 256 bits, 64 hex characters) — the "auto-generated" token source
// in Global Constraints + PM Amendment 10, used when neither -token nor
// OFFSHOOT_TOKEN was given for a loopback bind.
func GenerateToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("daemon: generating http token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// TokenFingerprint is the only form of a token ever safe to put in ongoing
// status/log output (PM Amendment 10: "status may show a token fingerprint
// (first 8 chars), never the token") — the token's own first 8 characters.
// A token of 8 characters or fewer is returned in full: there is no way to
// show "a prefix" of it that isn't the whole thing, so an operator who
// configures a token that short has, by that choice, made it fully visible
// wherever a fingerprint would otherwise appear — offshoot does not enforce
// a minimum token length.
func TokenFingerprint(token string) string {
	if len(token) <= 8 {
		return token
	}
	return token[:8]
}

// ValidateHTTPBind enforces the non-loopback bind rules from Global
// Constraints + PM Amendment 10, BEFORE any listener is created, so a
// misconfigured bind fails loudly at startup instead of quietly serving an
// under-acknowledged (or auto-token'd) endpoint beyond localhost. Two
// DISTINCT error cases, worded differently so a caller (and a test) can
// tell them apart:
//
//   - non-loopback + !allowNonLoopback: the operator never acknowledged
//     exposing this beyond localhost at all.
//   - non-loopback + allowNonLoopback + !hasExplicitToken: acknowledging
//     non-loopback exposure is not, by itself, permission to rely on the
//     auto-generated, printed-once-to-stderr token — that source is a
//     loopback-only convenience (Global Constraints); a non-loopback bind
//     requires the operator to have supplied the token themselves.
//
// A loopback bind needs neither: matches Global Constraints' "serve -http
// 127.0.0.1:PORT binds loopback" needing no ack flag or explicit token.
func ValidateHTTPBind(addr string, allowNonLoopback, hasExplicitToken bool) error {
	loopback, err := isLoopbackAddr(addr)
	if err != nil {
		return fmt.Errorf("daemon: -http %q: %w", addr, err)
	}
	if loopback {
		return nil
	}
	if !allowNonLoopback {
		return fmt.Errorf(
			"daemon: -http %q binds a non-loopback address; this requires -http-allow-non-loopback "+
				"(an explicit acknowledgment that this endpoint will be reachable beyond localhost)", addr)
	}
	if !hasExplicitToken {
		return fmt.Errorf(
			"daemon: -http %q with -http-allow-non-loopback also requires an explicit -token or "+
				"OFFSHOOT_TOKEN; the auto-generated, printed-once token is a loopback-only convenience", addr)
	}
	return nil
}

// isLoopbackAddr reports whether addr's host (a "host:port" pair, as
// net.Listen("tcp", addr) accepts) is a loopback address. An empty host
// (":8080", "all interfaces") is NOT loopback — that is exactly the
// non-loopback case ValidateHTTPBind exists to gate.
func isLoopbackAddr(addr string) (bool, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false, fmt.Errorf("invalid address %q (want host:port): %w", addr, err)
	}
	if host == "" {
		return false, nil
	}
	if strings.EqualFold(host, "localhost") {
		return true, nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		// A hostname other than "localhost": resolving it here would add a
		// real DNS dependency (and non-determinism — it could resolve
		// differently by the time the listener actually binds) to what
		// should be a synchronous, local startup check. Fail CLOSED: treat
		// anything that isn't a literal loopback IP or "localhost" as
		// non-loopback, requiring the ack flag + explicit token, rather
		// than risk waving through a name that resolves off-host.
		return false, nil
	}
	return ip.IsLoopback(), nil
}

// StartHTTP starts the opt-in HTTP listener: POST /rpc, GET /metrics, GET
// /healthz, GET /debug/pprof/* (Milestone 4 Task 3). Returns once the
// listener is bound (so a caller sees a bind failure — e.g. address already
// in use — synchronously); request serving happens on a background
// goroutine. Safe to call at most once per Server (a second call returns an
// error rather than silently replacing the first listener/token).
//
// cfg.Addr/AllowNonLoopback/Token are re-validated here via
// ValidateHTTPBind even though cmd/offshoot's serve command already
// validates before calling this — StartHTTP is part of this package's
// public surface (a test, or an embedder, can call it directly), and this
// guard must not be bypassable by skipping the CLI.
//
// s.httpSrv/s.httpToken are written under s.mu, AFTER the listener is
// already bound but BEFORE the serving goroutine starts, mirroring
// StartJanitor's closing-check-then-Add pattern: if Shutdown raced this
// call and already set s.closing, StartHTTP backs out (closing the
// just-bound listener) instead of leaving a listener running past
// Shutdown's point of no return. Once set, every HTTP request handler goroutine
// this daemon spawns is guaranteed (by the happens-before edge from `go
// httpSrv.Serve(ln)` itself, in the same way SetVersion/SetFlushEvery's doc
// comments describe for their own single-writer-before-Serve fields) to see
// the token this call wrote — no request-time lock needed to read it.
const minHTTPTokenLen = 16

func (s *Server) StartHTTP(cfg HTTPConfig) error {
	if cfg.Token == "" {
		return fmt.Errorf("daemon: StartHTTP: token must not be empty")
	}
	// A short token makes TokenFingerprint's 8-character prefix reveal a
	// large (or, below 8, total) fraction of the secret. GenerateToken's
	// own output (64 hex chars) is always well over this bound, so this
	// only ever constrains an operator-supplied -token/OFFSHOOT_TOKEN.
	if len(cfg.Token) < minHTTPTokenLen {
		return fmt.Errorf(
			"daemon: StartHTTP: token must be at least %d characters (a short token makes the "+
				"8-character status/log fingerprint reveal too large a fraction of the secret)",
			minHTTPTokenLen)
	}
	if err := ValidateHTTPBind(cfg.Addr, cfg.AllowNonLoopback, !cfg.AutoGenerated); err != nil {
		return err
	}

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return fmt.Errorf("daemon: shutting down")
	}
	if s.httpSrv != nil {
		s.mu.Unlock()
		return fmt.Errorf("daemon: StartHTTP already called")
	}
	s.mu.Unlock()

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("daemon: http listen %s: %w", cfg.Addr, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/rpc", s.requireAuth(s.handleRPC))
	mux.HandleFunc("/metrics", s.requireAuth(s.handleMetrics))
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/debug/pprof/", s.requireAuth(pprof.Index))
	mux.HandleFunc("/debug/pprof/cmdline", s.requireAuth(pprof.Cmdline))
	mux.HandleFunc("/debug/pprof/profile", s.requireAuth(pprof.Profile))
	mux.HandleFunc("/debug/pprof/symbol", s.requireAuth(pprof.Symbol))
	mux.HandleFunc("/debug/pprof/trace", s.requireAuth(pprof.Trace))

	httpSrv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	logw := cfg.Log
	if logw == nil {
		logw = os.Stderr
	}

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		ln.Close()
		return fmt.Errorf("daemon: shutting down")
	}
	// Re-check httpSrv != nil here too (TOCTOU guard): the FIRST check
	// above ran before the (slow, unlocked) net.Listen call below it, so
	// two concurrent StartHTTP calls can both pass that first check, both
	// bind a listener, and race to get here. Without this second check,
	// the loser's listener/token would silently overwrite the winner's —
	// exactly the kind of leaked-listener, wrong-token-in-effect bug a
	// caller would have no way to detect. This mirrors opOpen's own
	// reserve-before-slow-call-then-recheck pattern (see its doc comment)
	// for the identical reason: the lock is not held across the slow I/O.
	if s.httpSrv != nil {
		s.mu.Unlock()
		ln.Close()
		return fmt.Errorf("daemon: StartHTTP already called")
	}
	s.httpSrv = httpSrv
	s.httpToken = cfg.Token
	s.httpAddr = ln.Addr().String()
	s.httpLog = logw
	s.mu.Unlock()
	if cfg.AutoGenerated {
		fmt.Fprintf(logw,
			"offshoot: generated HTTP token for %s — printed ONCE, treat this line (and your "+
				"terminal scrollback/shell history) as sensitive: %s\n", cfg.Addr, cfg.Token)
	}
	fmt.Fprintf(logw, "offshoot: http listening on %s (bearer auth, token fingerprint %s)\n",
		cfg.Addr, TokenFingerprint(cfg.Token))

	go func() {
		if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(logw, "offshoot: http: %v\n", err)
		}
	}()
	return nil
}

// HTTPAddr returns the actual bound address of the HTTP listener ("" if
// StartHTTP has not been called) — useful for tests that bind to
// "127.0.0.1:0" and need the OS-assigned port.
func (s *Server) HTTPAddr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.httpSrv == nil {
		return ""
	}
	return s.httpAddr
}

// requireAuth wraps next so it only runs for a request presenting the
// correct Bearer token, matching Global Constraints: "every HTTP request
// requires Authorization: Bearer <token> except /healthz" (handleHealthz is
// simply never wrapped with this). Comparison is constant-time (PM
// Amendment 3) via checkAuth/crypto/subtle — see checkAuth's doc comment.
func (s *Server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.checkAuth(r) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="offshoot"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

const bearerPrefix = "Bearer "

// checkAuth reports whether r carries a well-formed "Authorization: Bearer
// <token>" header whose token matches s.httpToken. The scheme name
// ("Bearer") is matched case-insensitively (RFC 7235's auth-scheme tokens
// are case-insensitive); the TOKEN ITSELF is compared with
// crypto/subtle.ConstantTimeCompare, never Go's built-in == or
// strings.Compare, specifically so a byte-by-byte early-exit comparison
// can never leak (via response timing) how many leading bytes of a guessed
// token were correct — PM Amendment 3's "not auth theater" requirement.
// Presented and configured tokens of different lengths are rejected
// immediately without calling ConstantTimeCompare (which requires equal-
// length inputs) — this leaks LENGTH, not content, which
// crypto/subtle-style constant-time comparison has never claimed to hide
// (it defends the token's bytes, not its length) and offshoot's tokens are
// a fixed, known length (GenerateToken) or operator-chosen with no secrecy
// expectation on length itself.
func (s *Server) checkAuth(r *http.Request) bool {
	h := r.Header.Get("Authorization")
	if len(h) < len(bearerPrefix) || !strings.EqualFold(h[:len(bearerPrefix)], bearerPrefix) {
		return false
	}
	presented := h[len(bearerPrefix):]
	if len(presented) != len(s.httpToken) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(presented), []byte(s.httpToken)) == 1
}

// handleRPC is POST /rpc: the same Request/Response JSON and
// Server.dispatch the unix socket uses (handle, in server.go), one op per
// POST. Body size is capped at maxRPCBodyBytes via http.MaxBytesReader;
// Content-Type must be application/json.
//
// The "shutdown" op gets the HTTP analog of handle's own fix (see that
// function's doc comment and shutdownRespondDelay): dispatch runs, then
// this response is fully WRITTEN — as a single w.Write of a pre-marshaled,
// Content-Length-framed body, not a streamed json.Encoder+Flush (see
// below for why that distinction is load-bearing) — and only THEN — never
// before — does this handler trigger Shutdown (in a fresh goroutine,
// exactly like handle does). Shutdown's eventual http.Server.Close() call
// (see Shutdown's doc comment in server.go) can therefore never race ahead
// of this response actually being written: by the time Close() runs, this
// handler has already returned, and nothing about the response is still
// "owed" at that point (see below) — so Close() tearing the connection
// down cannot truncate it.
//
// Content-Length framing, not chunked: an EARLIER version of this handler
// wrote the response by streaming json.Encoder straight to w and then
// calling Flush — but w's Header never declared a Content-Length, so that
// combination forces HTTP chunked Transfer-Encoding, and Go's net/http
// only emits the terminating "0\r\n\r\n" chunk when the HANDLER RETURNS
// (or the ResponseWriter is otherwise finalized), which happens AFTER the
// `go s.Shutdown(...)` call below for the "shutdown" op. Flush() only
// guarantees the JSON body bytes reach the connection before Shutdown's
// Close() can run; it does NOT guarantee the chunked terminator does too.
// A client reading via json.Decoder never noticed (it stops as soon as it
// has decoded one complete JSON value, never waiting on the chunk
// trailer), but a client reading via ReadAll-then-Unmarshal — a
// legitimate, common pattern — could observe io.ErrUnexpectedEOF if
// Close() won that narrow race, EVEN THOUGH the actual JSON payload had
// already been fully sent. Marshaling to a byte slice up front and setting
// Content-Length before the single w.Write below sidesteps chunking
// entirely: once exactly Content-Length bytes have been written, the
// response is complete per HTTP framing with nothing left owed at handler
// return, whether or not the handler returns via the "shutdown" path.
func (s *Server) handleRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
		http.Error(w, "content-type must be application/json", http.StatusBadRequest)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRPCBodyBytes)
	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "invalid JSON request", http.StatusBadRequest)
		return
	}

	resp := s.dispatch(req)

	if req.Op == "shutdown" && httpShutdownRespondDelay != nil {
		httpShutdownRespondDelay() // test hook; nil (a no-op) in production
	}

	// Response is a plain, self-constructed struct (strings/ints/bools/
	// slices only) — Marshal failing here would be a logic bug, not a real
	// runtime condition (unlike the unix socket's Encode, which writes
	// straight to the wire and can fail on an already-gone client). Report
	// what we can and still let "shutdown" proceed below regardless —
	// matching handle's own "order matters more than outcome" reasoning in
	// server.go.
	body, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "internal error encoding response", http.StatusInternalServerError)
	} else {
		body = append(body, '\n')
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body) // best-effort; see handle's identical reasoning in server.go
	}
	if req.Op == "shutdown" {
		go s.Shutdown(context.Background())
	}
}

// handleMetrics is GET /metrics: the T2 registry's Prometheus text
// exposition (Server.WritePrometheus, metrics.go) — concurrent-scrape-safe
// via Registry.scrapeMu (internal/metrics/metrics.go); see that field's doc
// comment for why serializing WritePrometheus itself, rather than the
// daemon's collector, is the chosen fix for the carried-over T2 review
// item. Requires auth (Global Constraints: everything but /healthz does) —
// metric labels include branch names, which this daemon does not consider
// public.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if err := s.WritePrometheus(w); err != nil {
		// Headers (and possibly some body bytes) may already be on the
		// wire; there is no clean HTTP error left to send at this point,
		// so this is logged server-side only, never folded into the
		// response body. Uses the SAME writer StartHTTP's own startup
		// lines went to (HTTPConfig.Log, defaulting to os.Stderr there) —
		// not a bare os.Stderr here — so a caller/test that redirected
		// StartHTTP's logging (e.g. to a buffer, to keep assertions off
		// real stderr) sees this line too, instead of it silently
		// escaping to the real stderr regardless.
		s.mu.Lock()
		logw := s.httpLog
		s.mu.Unlock()
		if logw == nil {
			logw = os.Stderr
		}
		fmt.Fprintf(logw, "offshoot: http: /metrics: %v\n", err)
	}
}

// handleHealthz is GET /healthz: the one endpoint Global Constraints
// exempts from auth entirely, so a load balancer / orchestrator liveness
// probe needs no token. Body shape is fixed by the plan:
// {"ok":true,"sessions":N}.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		OK       bool `json:"ok"`
		Sessions int  `json:"sessions"`
	}{OK: true, Sessions: s.sessionCount()})
}
