package daemon

import (
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// flipByte returns s with the byte at index i replaced by a different
// printable byte, so the result has exactly the same length as s and
// differs from it in exactly one position.
func flipByte(s string, i int) string {
	b := []byte(s)
	if b[i] == 'a' {
		b[i] = 'b'
	} else {
		b[i] = 'a'
	}
	return string(b)
}

// TestHTTPAuthRejectsEqualLengthWrongToken pins operations.md
// "#httpauth-threat-model": "The token is a shared secret, checked in
// constant time" and SECURITY.md "Threat model" / HTTP: every request but
// GET /healthz must carry the right Bearer token, "compared with
// crypto/subtle.ConstantTimeCompare". Every pre-existing wrong-token test
// used a token of a DIFFERENT length, so only the length short-circuit in
// checkAuth was ever exercised; this one presents tokens of exactly the
// configured length that differ in the last byte and, separately, in the
// first byte, on POST /rpc, a direct GET /metrics (no redirect) and GET
// /events. Mutation verified against: http.go checkAuth's final
// `return subtle.ConstantTimeCompare(...) == 1` replaced with `return
// true` (every wrong-token case below then gets 200 instead of 401).
func TestHTTPAuthRejectsEqualLengthWrongToken(t *testing.T) {
	_, _, base, token := newHTTPServer(t)
	last := flipByte(token, len(token)-1)
	first := flipByte(token, 0)
	for _, w := range []string{last, first} {
		if len(w) != len(token) || w == token {
			t.Fatalf("test bug: wrong token %q must be same length as and differ from %q", w, token)
		}
	}

	get := func(path, auth string) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, base+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", auth)
		// No redirects: the audit found /metrics auth only ever covered
		// through a //metrics -> /metrics redirect.
		c := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	for name, wrong := range map[string]string{"last-byte": last, "first-byte": first} {
		resp := httpRPC(t, base, "Bearer "+wrong, Request{Op: "status"})
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("POST /rpc with %s-flipped equal-length token: status = %d, want 401", name, resp.StatusCode)
		}
		if got := resp.Header.Get("WWW-Authenticate"); got != `Bearer realm="offshoot"` {
			t.Errorf("POST /rpc with %s-flipped token: WWW-Authenticate = %q", name, got)
		}
		if got := get("/metrics", "Bearer "+wrong); got != http.StatusUnauthorized {
			t.Errorf("GET /metrics with %s-flipped equal-length token: status = %d, want 401", name, got)
		}
		if got := get("/events", "Bearer "+wrong); got != http.StatusUnauthorized {
			t.Errorf("GET /events with %s-flipped equal-length token: status = %d, want 401", name, got)
		}
	}

	// Positive controls: the exact token is accepted on the same routes,
	// so a 401 above is the comparison refusing, not the route being down.
	resp := httpRPC(t, base, "Bearer "+token, Request{Op: "status"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /rpc with the right token: status = %d, want 200", resp.StatusCode)
	}
	if got := get("/metrics", "Bearer "+token); got != http.StatusOK {
		t.Fatalf("GET /metrics with the right token: status = %d, want 200", got)
	}
}

// TestDaemonUnforcedPromoteOntoProtectedRefused pins architecture.md
// invariant 7 / reference.md "#offshoot-protect": "A protected branch
// refuses unforced destroy and unforced promote --onto it" — over the
// daemon's "promote" op, which must pass req.Force through unchanged
// (operations.md: HTTP/socket "adds a transport, not a privilege level").
// Unforced: refused, main's head untouched, and no safety fork minted
// (ops.go: "an unforced promote onto a protected target mints nothing").
// Forced: succeeds and the <target>-pre-promote backup appears. Mutation
// verified against: ops.go PromoteWith's `if tgt.Protected && !force`
// refusal removed (the unforced call then returns ok=true).
func TestDaemonUnforcedPromoteOntoProtectedRefused(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	if r := call(t, sock, Request{Op: "fork", DB: "app", Branch: "main", Name: "exp"}); !r.OK {
		t.Fatalf("fork = %+v", r)
	}
	before, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if !before.Protected {
		t.Fatal("main must be protected by default")
	}

	r := call(t, sock, Request{Op: "promote", DB: "app", Branch: "exp", Name: "main"})
	if r.OK {
		t.Fatal("unforced promote onto protected main must be refused")
	}
	if !strings.Contains(r.Error, "app@main is protected") {
		t.Fatalf("refusal must name the protected target: %q", r.Error)
	}
	after, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if after.Lineage != before.Lineage || after.HeadTXID != before.HeadTXID || after.HeadEpoch != before.HeadEpoch {
		t.Fatalf("refused promote moved main: before=%+v after=%+v", before, after)
	}
	br := call(t, sock, Request{Op: "branches", DB: "app"})
	if !br.OK {
		t.Fatalf("branches = %+v", br)
	}
	names := make([]string, 0, len(br.Branches))
	for _, b := range br.Branches {
		names = append(names, b.Branch)
	}
	if got := strings.Join(names, ","); got != "exp,main" {
		t.Fatalf("a refused promote must mint no safety fork: branches = %q, want exp,main", got)
	}

	fr := call(t, sock, Request{Op: "promote", DB: "app", Branch: "exp", Name: "main", Force: true})
	if !fr.OK {
		t.Fatalf("forced promote onto protected main = %+v, want ok", fr)
	}
	br = call(t, sock, Request{Op: "branches", DB: "app"})
	names = names[:0]
	for _, b := range br.Branches {
		names = append(names, b.Branch)
	}
	if got := strings.Join(names, ","); got != "exp,main,main-pre-promote" {
		t.Fatalf("forced promote must leave the safety fork: branches = %q", got)
	}
}

// TestDaemonHasNoProtectOp pins reference.md "#offshoot-protect": protect
// and unprotect "are CLI-only by design" and status.md's op list, which
// has no protect/unprotect: the daemon's dispatch must answer both with
// its unknown-op error, verbatim, and leave the flag alone. Fails the day
// someone adds a "protect" op to server.go's dispatch.
func TestDaemonHasNoProtectOp(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	if r := call(t, sock, Request{Op: "fork", DB: "app", Branch: "main", Name: "exp"}); !r.OK {
		t.Fatalf("fork = %+v", r)
	}
	for _, op := range []string{"protect", "unprotect"} {
		for _, branch := range []string{"main", "exp"} {
			r := call(t, sock, Request{Op: op, DB: "app", Branch: branch})
			if r.OK {
				t.Fatalf("%s must not be a daemon op, got ok=true", op)
			}
			if want := `daemon: unknown op "` + op + `"`; r.Error != want {
				t.Fatalf("%s error = %q, want %q", op, r.Error, want)
			}
		}
	}
	main, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	exp, _, err := w.Store.GetRef("app", "exp")
	if err != nil {
		t.Fatal(err)
	}
	if !main.Protected || exp.Protected {
		t.Fatalf("flags changed through a non-op: main=%v exp=%v", main.Protected, exp.Protected)
	}
}

// TestDefaultSocketDirIs0700 pins SECURITY.md "Threat model" / Local
// socket: the socket is created "under a 0700 cache directory". The cache
// root is pointed at a fresh temp dir (HOME for darwin's
// ~/Library/Caches, XDG_CACHE_HOME for linux), OFFSHOOT_SOCKET is cleared
// so the env override cannot short-circuit, and the parent of the
// returned path is stat'ed. Mutation verified against: client.go
// DefaultSocketPath's os.MkdirAll(dir, 0o700) changed to 0o755.
func TestDefaultSocketDirIs0700(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX modes are not enforced on windows")
	}
	home := t.TempDir()
	t.Setenv("OFFSHOOT_SOCKET", "")
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(home, "xdg-cache"))

	p, err := DefaultSocketPath("s3://bucket/prefix")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(p, home+string(filepath.Separator)) {
		t.Fatalf("socket path %q is not under the redirected cache root %q", p, home)
	}
	dir := filepath.Dir(p)
	if filepath.Base(dir) != "offshoot" {
		t.Fatalf("socket dir = %q, want an offshoot/ directory", dir)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Fatalf("%s is not a directory", dir)
	}
	if m := fi.Mode().Perm(); m != 0o700 {
		t.Fatalf("socket directory %s mode = %v, want 0700", dir, m)
	}
}
