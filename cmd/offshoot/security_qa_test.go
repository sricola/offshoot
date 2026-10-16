package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/daemon"
	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// TestCLIUnforcedPromoteOntoProtectedRefused pins reference.md
// "#offshoot-protect": "A protected branch refuses unforced destroy and
// unforced promote --onto it" through `offshoot promote` itself: the
// unforced run errors naming the protected target, main's ref is
// byte-for-byte unchanged and no main-pre-promote safety fork exists;
// `--force` then succeeds and prints the promoted line. Mutation verified
// against: ops.go PromoteWith's `if tgt.Protected && !force` refusal
// removed (the unforced run then returns nil).
func TestCLIUnforcedPromoteOntoProtectedRefused(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	call(t, dir, "init")
	call(t, dir, "create", "app")
	call(t, dir, "fork", "app", "exp")
	w, err := ops.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	before, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	err = run([]string{"-store", dir, "promote", "app@exp", "--onto", "main"})
	if err == nil {
		t.Fatal("unforced promote onto protected main must be refused")
	}
	if !strings.Contains(err.Error(), "app@main is protected") {
		t.Fatalf("refusal must name the protected target: %v", err)
	}
	after, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if after.Lineage != before.Lineage || after.HeadTXID != before.HeadTXID || after.HeadEpoch != before.HeadEpoch {
		t.Fatalf("refused promote moved main: before=%+v after=%+v", before, after)
	}
	if _, _, err := w.Store.GetRef("app", "main"+ops.PromoteBackupSuffix); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a refused promote must mint no safety fork, GetRef = %v", err)
	}

	out := call(t, dir, "promote", "app@exp", "--onto", "main", "--force")
	if !strings.HasPrefix(out, "promoted app@exp -> app@main at txid ") {
		t.Fatalf("forced promote output = %q", out)
	}
	if _, _, err := w.Store.GetRef("app", "main"+ops.PromoteBackupSuffix); err != nil {
		t.Fatalf("forced promote must leave the safety fork: %v", err)
	}
}

// secQAMCPSubprocess starts `go run . -store <store> mcp <extra...>` the way a
// client would (the same pattern as TestMCPSubprocessHandshakeAndCall) and
// returns a send function that writes one JSON-RPC line and decodes the
// one response line, plus a stop function.
func secQAMCPSubprocess(t *testing.T, storeDir string, extra ...string) (send func(string) map[string]any, stop func()) {
	t.Helper()
	args := append([]string{"run", ".", "-store", storeDir, "mcp"}, extra...)
	cmd := exec.Command("go", args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	send = func(line string) map[string]any {
		t.Helper()
		if _, err := stdin.Write([]byte(line + "\n")); err != nil {
			t.Fatalf("write: %v (stderr: %s)", err, stderr.String())
		}
		if !sc.Scan() {
			t.Fatalf("no response (stderr: %s)", stderr.String())
		}
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("bad response %q: %v (stderr: %s)", sc.Text(), err, stderr.String())
		}
		return m
	}
	stop = func() {
		stdin.Close()
		cmd.Wait()
	}
	return send, stop
}

// TestMCPAllowForceFlagIsWired pins reference.md "#offshoot-mcp":
// "-allow-force (off by default): the force gate ... a call carrying
// force: true against a protected branch is refused before any mutation,
// with a tool-result error naming -allow-force", and "Turning -allow-force
// on restores the pre-gate behavior in full" — through the real binary,
// so the flag's plumbing in cmd/offshoot (not just mcp.SetAllowForce) is
// what is exercised. Mutation verified against: main.go cmdMcp's
// `ts.SetAllowForce(allowForce)` call deleted (the flagged server then
// refuses the destroy exactly like the default one).
func TestMCPAllowForceFlagIsWired(t *testing.T) {
	testutil.RequireSQLite3(t)
	storeDir := filepath.Join(t.TempDir(), "store")
	call(t, storeDir, "init")
	call(t, storeDir, "create", "app")
	w, err := ops.Open(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	const destroyMain = `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"offshoot_destroy","arguments":{"database":"app","branch":"main","force":true}}}`
	toolText := func(m map[string]any) (text string, isError bool) {
		t.Helper()
		if m["error"] != nil {
			t.Fatalf("tools/call transport error: %v", m["error"])
		}
		res := m["result"].(map[string]any)
		for _, c := range res["content"].([]any) {
			text += c.(map[string]any)["text"].(string)
		}
		isError, _ = res["isError"].(bool)
		return text, isError
	}

	// Default server: force against protected main is refused, naming the
	// flag, and main is untouched.
	send, stop := secQAMCPSubprocess(t, storeDir)
	text, isError := toolText(send(destroyMain))
	stop()
	if !isError {
		t.Fatalf("forced destroy of protected main without -allow-force must be a tool error, got %q", text)
	}
	if !strings.Contains(text, "offshoot mcp -allow-force") {
		t.Fatalf("refusal must name the operator flag: %q", text)
	}
	if _, _, err := w.Store.GetRef("app", "main"); err != nil {
		t.Fatalf("main must survive the refused destroy: %v", err)
	}

	// With -allow-force the same call is honored.
	send, stop = secQAMCPSubprocess(t, storeDir, "-allow-force")
	text, isError = toolText(send(destroyMain))
	stop()
	if isError {
		t.Fatalf("forced destroy with -allow-force must succeed, got tool error %q", text)
	}
	if _, _, err := w.Store.GetRef("app", "main"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("main must be gone after the honored forced destroy, GetRef = %v", err)
	}
}

// secQAFreeLoopbackAddr reserves and releases an OS-assigned loopback port so a
// serve started with it can be reached without the CLI reporting its port.
func secQAFreeLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// secQAServeHTTP starts `offshoot serve -http addr -socket sock <extra...>` in
// the background, waits for the daemon to come up, and returns a stop
// function that shuts it down through `session shutdown` and waits for
// serve to return nil.
func secQAServeHTTP(t *testing.T, storeDir, httpAddr string, extra ...string) (stop func()) {
	t.Helper()
	sockDir, err := os.MkdirTemp("", "offshoot-sec-qa-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "s.sock")
	args := append([]string{"-store", storeDir, "serve", "-http", httpAddr, "-socket", sock}, extra...)
	done := make(chan error, 1)
	go func() { done <- run(args) }()
	t.Cleanup(func() { run([]string{"-store", storeDir, "session", "shutdown", "-socket", sock}) })
	deadline := time.Now().Add(10 * time.Second)
	for !daemon.Running(sock) {
		select {
		case err := <-done:
			t.Fatalf("serve exited before coming up: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("daemon never came up")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return func() {
		if err := run([]string{"-store", storeDir, "session", "shutdown", "-socket", sock}); err != nil {
			t.Fatalf("session shutdown: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("serve returned: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("serve did not exit after shutdown")
		}
	}
}

// secQARPCStatus POSTs a status request to base+/rpc with the given bearer token
// and returns the HTTP status code.
func secQARPCStatus(t *testing.T, base, token string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/rpc", bytes.NewReader([]byte(`{"op":"status"}`)))
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
	return resp.StatusCode
}

// TestServeHonorsOffshootTokenEnv pins operations.md
// "#httpauth-threat-model": "OFFSHOOT_TOKEN=... offshoot serve ... is the
// equivalent for environments that inject secrets as variables. Precedence
// when more than one is set: -token, then -token-file, then OFFSHOOT_TOKEN
// ... All three are subject to the same 16-character minimum." Three
// cases: a short env token is a startup error naming the 16-character
// floor; a valid env token alone lets a loopback serve start and is the
// token that authenticates; with -token-file as well, the file's token
// authenticates and the env token gets 401. Mutation verified against:
// main.go cmdServe's `if token == "" { token = os.Getenv("OFFSHOOT_TOKEN") }`
// removed (short env then starts fine; valid env then gets 401 because an
// auto-generated token is in effect).
func TestServeHonorsOffshootTokenEnv(t *testing.T) {
	testutil.RequireSQLite3(t)
	storeDir := filepath.Join(t.TempDir(), "s")
	call(t, storeDir, "init")
	const envToken = "env-token-value-0123456789abcdef"
	const fileToken = "file-token-value-0123456789abcdef"
	const otherToken = "other-token-value-0123456789abcde"

	t.Run("short env token is refused", func(t *testing.T) {
		t.Setenv("OFFSHOOT_TOKEN", "short")
		// A short socket path (unix socket paths are capped ~104 bytes on
		// macOS; t.TempDir() nests under the subtest's full name), so the
		// outcome is the token refusal whether the 16-character check runs
		// before or after the socket is bound.
		sockDir, err := os.MkdirTemp("", "offshoot-sec-qa-*")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(sockDir) })
		sock := filepath.Join(sockDir, "s.sock")
		// serve runs in the background: if the short token were accepted,
		// serve would come up and block forever, so a daemon appearing on
		// the socket is itself the failure (shut it down, then fail)
		// rather than a hung test.
		done := make(chan error, 1)
		go func() { done <- run([]string{"-store", storeDir, "serve", "-http", "127.0.0.1:0", "-socket", sock}) }()
		deadline := time.Now().Add(10 * time.Second)
		for {
			select {
			case err := <-done:
				if err == nil || !strings.Contains(err.Error(), "at least 16 characters") {
					t.Fatalf("serve with a 5-char OFFSHOOT_TOKEN: got %v, want the 16-character refusal", err)
				}
				return
			default:
			}
			if daemon.Running(sock) {
				run([]string{"-store", storeDir, "session", "shutdown", "-socket", sock})
				<-done
				t.Fatal("serve with a 5-char OFFSHOOT_TOKEN started a daemon; want the 16-character refusal")
			}
			if time.Now().After(deadline) {
				t.Fatal("serve neither refused the short OFFSHOOT_TOKEN nor came up within 10s")
			}
			time.Sleep(20 * time.Millisecond)
		}
	})

	t.Run("env token authenticates", func(t *testing.T) {
		t.Setenv("OFFSHOOT_TOKEN", envToken)
		addr := secQAFreeLoopbackAddr(t)
		stop := secQAServeHTTP(t, storeDir, addr)
		defer stop()
		base := "http://" + addr
		if got := secQARPCStatus(t, base, envToken); got != http.StatusOK {
			t.Fatalf("POST /rpc with the OFFSHOOT_TOKEN value: status = %d, want 200", got)
		}
		if got := secQARPCStatus(t, base, otherToken); got != http.StatusUnauthorized {
			t.Fatalf("POST /rpc with another token: status = %d, want 401", got)
		}
	})

	t.Run("token-file overrides env", func(t *testing.T) {
		t.Setenv("OFFSHOOT_TOKEN", envToken)
		tf := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(tf, []byte(fileToken+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		addr := secQAFreeLoopbackAddr(t)
		stop := secQAServeHTTP(t, storeDir, addr, "-token-file", tf)
		defer stop()
		base := "http://" + addr
		if got := secQARPCStatus(t, base, fileToken); got != http.StatusOK {
			t.Fatalf("POST /rpc with the -token-file token: status = %d, want 200", got)
		}
		if got := secQARPCStatus(t, base, envToken); got != http.StatusUnauthorized {
			t.Fatalf("POST /rpc with the overridden OFFSHOOT_TOKEN value: status = %d, want 401", got)
		}
	})
}
