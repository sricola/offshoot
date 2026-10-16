package main

import (
	"bufio"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/daemon"
)

// qaFreeLoopbackAddr asks the OS for a free 127.0.0.1 port and returns it
// as host:port. `serve -http 127.0.0.1:0` would bind an OS-assigned port
// too, but its "http listening on" line reports the REQUESTED address, so
// a test has no way to learn the bound port from outside the process.
func qaFreeLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// serveHTTPForTest runs `serve -socket SOCK -http ADDR extra...` on a
// goroutine, following main_test.go's serve pattern (short socket dir,
// daemon.Running poll, `session shutdown` to stop, serveDone drained with
// a deadline), with os.Stderr redirected to a pipe for serve's lifetime so
// the daemon's one "http listening on … (token fingerprint …)" line can be
// asserted on. os.Stderr is restored only after run has returned, so no
// daemon goroutine ever reads the variable concurrently with the restore.
// It returns the HTTP base URL and a func that returns everything the
// daemon wrote to stderr so far.
func serveHTTPForTest(t *testing.T, store, httpAddr string, extra ...string) (base string, stderr func() string) {
	t.Helper()
	sockDir, err := os.MkdirTemp("", "offshoot-cli-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "qa.sock")

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	origStderr := os.Stderr
	os.Stderr = w
	var mu sync.Mutex
	var captured strings.Builder
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			mu.Lock()
			captured.WriteString(sc.Text())
			captured.WriteByte('\n')
			mu.Unlock()
		}
	}()

	args := append([]string{"-store", store, "serve", "-socket", sock, "-http", httpAddr}, extra...)
	serveDone := make(chan error, 1)
	go func() { serveDone <- run(args) }()

	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		run([]string{"-store", store, "session", "shutdown", "-socket", sock})
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("serve returned: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("serve did not exit after session shutdown")
		}
		w.Close()
		os.Stderr = origStderr
		<-readerDone
		r.Close()
	}
	t.Cleanup(stop)

	deadline := time.Now().Add(10 * time.Second)
	for !daemon.Running(sock) {
		select {
		case err := <-serveDone:
			serveDone <- err
			stop()
			t.Fatalf("serve exited before coming up: %v\nstderr:\n%s", err, captured.String())
		default:
		}
		if time.Now().After(deadline) {
			stop()
			t.Fatalf("daemon never came up\nstderr:\n%s", captured.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	return "http://" + httpAddr, func() string {
		mu.Lock()
		defer mu.Unlock()
		return captured.String()
	}
}

// getMetricsStatus GETs base+"/metrics" with the given Authorization
// header and returns the status code.
func getMetricsStatus(t *testing.T, base, authorization string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", authorization)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// TestServeTokenFileIsWhitespaceTrimmed pins docs/reference.md#-http
// "Token": "-token-file PATH (the token read from a file, whitespace
// trimmed)". A file holding "  tok-0123456789abcdef \n" configures the
// token "tok-0123456789abcdef": that token authenticates on /metrics
// (200), the untrimmed bytes do not (401), and the one startup line
// fingerprints the trimmed token. Mutation verified against: dropping
// strings.TrimSpace from the -token-file read in cmdServe (main.go).
func TestServeTokenFileIsWhitespaceTrimmed(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	const trimmed = "tok-0123456789abcdef"
	const raw = "  " + trimmed + " \n"
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}

	base, stderr := serveHTTPForTest(t, dir, qaFreeLoopbackAddr(t), "-token-file", tokenFile)

	if got := getMetricsStatus(t, base, "Bearer "+trimmed); got != http.StatusOK {
		t.Fatalf("GET /metrics with the trimmed token: status = %d, want 200", got)
	}
	// The untrimmed file bytes (HTTP strips a header value's trailing
	// whitespace, so the leading spaces are what reach the daemon).
	if got := getMetricsStatus(t, base, "Bearer "+strings.TrimRight(raw, " \n")); got != http.StatusUnauthorized {
		t.Fatalf("GET /metrics with the untrimmed token bytes: status = %d, want 401", got)
	}
	if got := getMetricsStatus(t, base, "Bearer "+trimmed+"x"); got != http.StatusUnauthorized {
		t.Fatalf("GET /metrics with a wrong token: status = %d, want 401", got)
	}

	out := stderr()
	want := "(bearer auth, token fingerprint " + daemon.TokenFingerprint(trimmed) + ")"
	if !strings.Contains(out, "offshoot: http listening on ") || !strings.Contains(out, want) {
		t.Fatalf("startup line must fingerprint the trimmed token %q; stderr:\n%s", want, out)
	}
	if strings.Contains(out, trimmed) {
		t.Fatalf("an explicit token must never be printed in full; stderr:\n%s", out)
	}
	if strings.Contains(out, "generated HTTP token") {
		t.Fatalf("-token-file must suppress the auto-generated token; stderr:\n%s", out)
	}
}

// TestServeTokenAndTokenFileAreMutuallyExclusive pins
// docs/reference.md#serve's `-token TOKEN | -token-file PATH` usage: both
// together is a startup error, refused before any socket or listener is
// created. Mutation verified against: deleting the mutual-exclusion check
// in cmdServe (main.go) — serve then starts and the test's deadline-bounded
// wait sees it running.
func TestServeTokenAndTokenFileAreMutuallyExclusive(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("tok-0123456789abcdef\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sockDir, err := os.MkdirTemp("", "offshoot-cli-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "qa.sock")

	done := make(chan error, 1)
	go func() {
		done <- run([]string{"-store", dir, "serve", "-socket", sock, "-http", "127.0.0.1:0",
			"-token", "tok-fedcba9876543210", "-token-file", tokenFile})
	}()
	select {
	case err := <-done:
		if err == nil || err.Error() != "-token and -token-file are mutually exclusive" {
			t.Fatalf("serve -token with -token-file: err = %v, want %q", err, "-token and -token-file are mutually exclusive")
		}
	case <-time.After(10 * time.Second):
		run([]string{"-store", dir, "session", "shutdown", "-socket", sock})
		t.Fatal("serve with both -token and -token-file started serving instead of refusing")
	}
	if _, err := os.Stat(sock); err == nil {
		t.Fatal("the refusal must happen before the socket is created")
	}
}

// TestServeTokenFileEmptyIsRefusedNamingTheFile pins cmdServe's
// -token-file read: a file that is empty after trimming is a startup
// error naming the file ("-token-file PATH is empty"), not an auto-
// generated token or an empty one. Mutation verified against: dropping
// strings.TrimSpace (a whitespace-only file then passes as a token).
func TestServeTokenFileEmptyIsRefusedNamingTheFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	for _, content := range []string{"", "  \n\t\n"} {
		tokenFile := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(tokenFile, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		sockDir, err := os.MkdirTemp("", "offshoot-cli-test-*")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(sockDir) })
		sock := filepath.Join(sockDir, "qa.sock")

		done := make(chan error, 1)
		go func() {
			done <- run([]string{"-store", dir, "serve", "-socket", sock, "-http", "127.0.0.1:0", "-token-file", tokenFile})
		}()
		select {
		case err := <-done:
			want := "-token-file " + tokenFile + " is empty"
			if err == nil || err.Error() != want {
				t.Fatalf("serve -token-file with content %q: err = %v, want %q", content, err, want)
			}
		case <-time.After(10 * time.Second):
			run([]string{"-store", dir, "session", "shutdown", "-socket", sock})
			t.Fatalf("serve -token-file with content %q started serving instead of refusing", content)
		}
	}
}
