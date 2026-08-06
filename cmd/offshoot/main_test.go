package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/daemon"
)

// call runs the CLI's run() with -store pointing at dir, capturing stdout.
func call(t *testing.T, dir string, args ...string) string {
	t.Helper()
	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	err := run(append([]string{"-store", dir}, args...))
	w.Close()
	os.Stdout = old
	buf := make([]byte, 1<<16)
	n, _ := r.Read(buf)
	if err != nil {
		t.Fatalf("offshoot %v: %v", args, err)
	}
	return string(buf[:n])
}

// TestCreateFromMissingFileArgErrors guards against a silent fallthrough:
// `create <db> --from` with the file argument omitted (len(rest)==2) used to
// fall through to the plain-create branch, silently creating an empty
// database instead of reporting the malformed command. Only exact arities
// (plain create: 1 arg; import: 3 args with rest[1]=="--from") are accepted.
func TestCreateFromMissingFileArgErrors(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "create", "app", "--from"}); err == nil {
		t.Fatal("create <db> --from with a missing file arg must error, not silently create an empty db")
	}
	if _, err := os.Stat(filepath.Join(dir, "refs", "app", "main")); err == nil {
		t.Fatal("no ref should have been created for the malformed command")
	}
}

// TestForkRejectsNonPositiveTTL pins the CLI-level half of the fix for fork
// silently treating a non-positive --ttl as no-TTL: `fork --ttl -1h` used to
// parse fine (time.ParseDuration accepts negative durations) and then fall
// through Fork's `if ttl > 0` check, forking a TTL-less branch while the
// caller believed it asked for one. fork also has no "none" sentinel the
// way touch does (a brand-new branch has no existing TTL to explicitly
// clear), so "--ttl none" must be refused too, naming the fix: omit --ttl
// for no TTL.
func TestForkRejectsNonPositiveTTL(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "create", "app"}); err != nil {
		t.Fatal(err)
	}
	for _, ttl := range []string{"-1h", "0s", "none"} {
		if err := run([]string{"-store", dir, "fork", "app", "kid-" + ttl, "--ttl", ttl}); err == nil {
			t.Fatalf("fork --ttl %q must be refused", ttl)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "refs", "app", "kid--1h")); err == nil {
		t.Fatal("a refused fork must not create the branch")
	}
	// Omitting --ttl entirely still means no TTL, unaffected by the guard.
	if err := run([]string{"-store", dir, "fork", "app", "kid-none"}); err != nil {
		t.Fatalf("fork with no --ttl flag must still succeed: %v", err)
	}
}

// TestVersion pins the `offshoot version` output shape: it must not require
// an initialized store (it's handled before ops.Open, alongside "init"),
// and it must report the build-time version var, the Go runtime version,
// and GOOS/GOARCH, in that order.
func TestVersion(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	out := call(t, dir, "version")
	want := fmt.Sprintf("offshoot %s %s %s/%s\n", version, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	if out != want {
		t.Fatalf("offshoot version = %q, want %q", out, want)
	}
	// No store was ever initialized at dir; version must not have created one.
	if _, err := os.Stat(dir); err == nil {
		t.Fatal("offshoot version must not create a store")
	}
}

func TestVersionRejectsArgs(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "version", "extra"}); err == nil {
		t.Fatal("offshoot version with extra args must be rejected")
	}
}

func TestQuickstartTranscript(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	store := filepath.Join(t.TempDir(), "s")
	call(t, store, "init")
	call(t, store, "create", "app")
	path := strings.TrimSpace(call(t, store, "checkout", "app"))

	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE users (name); INSERT INTO users VALUES ('ada');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	call(t, store, "checkpoint", "app", "v1")
	call(t, store, "fork", "app", "attempt-1")

	apath := strings.TrimSpace(call(t, store, "checkout", "app@attempt-1"))
	exec.Command("sqlite3", apath, "DELETE FROM users;").Run() // destructive attempt
	call(t, store, "checkpoint", "app@attempt-1", "oops")
	call(t, store, "rollback", "app@attempt-1", "--to", "fork")

	got, _ := exec.Command("sqlite3",
		strings.TrimSpace(call(t, store, "path", "app@attempt-1")), "SELECT name FROM users;").Output()
	if string(got) != "ada\n" {
		t.Fatalf("rollback lost data: %q", got)
	}

	// Winner path: modify attempt, promote onto main.
	exec.Command("sqlite3", apath, "INSERT INTO users VALUES ('grace');").Run()
	call(t, store, "checkpoint", "app@attempt-1", "winner")
	call(t, store, "promote", "app@attempt-1", "--onto", "main", "--force")
	mgot, _ := exec.Command("sqlite3",
		strings.TrimSpace(call(t, store, "path", "app")), "SELECT count(*) FROM users;").Output()
	if string(mgot) != "2\n" {
		t.Fatalf("promoted main: %q", mgot)
	}

	call(t, store, "destroy", "app@attempt-1")
	call(t, store, "gc", "--grace", "0s")

	status := call(t, store, "status")
	if !strings.Contains(status, "app@main") || strings.Contains(status, "attempt-1") {
		t.Fatalf("status:\n%s", status)
	}
}

// TestBareTrailingSocketFlagIsRejected guards a parsing gap: socketOverride
// used to only consume "-socket PATH" when a value followed it, silently
// leaving a bare trailing "-socket" in the remaining args otherwise. `serve`
// happened to reject that leftover anyway (it errors on any nonempty rest),
// but `session` has no such check — a trailing "-socket" there fell through
// to target(), which just treated it as an ordinary positional argument (or
// an unknown subcommand, depending on position) instead of reporting the
// malformed flag. Both paths must now reject it explicitly and identically.
func TestBareTrailingSocketFlagIsRejected(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "create", "app"}); err != nil {
		t.Fatal(err)
	}

	if err := run([]string{"-store", dir, "serve", "-socket"}); err == nil {
		t.Fatal("serve -socket with no PATH must error")
	}
	if err := run([]string{"-store", dir, "session", "open", "app", "-socket"}); err == nil {
		t.Fatal("session open app -socket with no PATH must error, not silently ignore -socket")
	}
	if err := run([]string{"-store", dir, "session", "-socket"}); err == nil {
		t.Fatal("session -socket with no PATH and no subcommand must error")
	}
}

// TestServeNegativeFlushEveryIsRejected pins the fix: a negative
// -flush-every must fail closed as a usage error, not silently disable
// auto-flush. 0 stays the explicit "disable" sentinel; a negative duration
// is (almost certainly) a mistake and must not be quietly aliased to it —
// run() must return before ever starting to serve, so this needs no daemon
// lifecycle at all (mirrors TestBareTrailingSocketFlagIsRejected's shape).
func TestServeNegativeFlushEveryIsRejected(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "create", "app"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "serve", "-flush-every", "-5s"}); err == nil {
		t.Fatal("serve -flush-every -5s must be rejected, not silently disable auto-flush")
	}
}

// TestSessionHonorsServeSocketOverride guards the fix for `serve -socket`
// and `session` disagreeing on where the socket lives: `serve -socket PATH`
// used to be unreachable by `session` subcommands because they only ever
// computed DefaultSocketPath(spec)/OFFSHOOT_SOCKET, never looking at a
// `-socket` flag of their own. This drives the CLI exactly as a user would:
// start a daemon with an explicit -socket, confirm session commands without
// -socket cannot reach it (they'd otherwise silently hit some other
// daemon's default socket and mask this bug), then confirm the matching
// -socket makes them agree.
func TestSessionHonorsServeSocketOverride(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	store := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", store, "init"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", store, "create", "app"}); err != nil {
		t.Fatal(err)
	}

	// Unix socket paths are capped around 104-108 bytes on several
	// platforms (notably macOS); keep this one short and independent of
	// t.TempDir(), which nests under the test's full name.
	sockDir, err := os.MkdirTemp("", "offshoot-cli-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "custom.sock")

	serveDone := make(chan error, 1)
	go func() { serveDone <- run([]string{"-store", store, "serve", "-socket", sock}) }()
	// Best-effort safety net: if the test fails before reaching its own
	// shutdown call below, this stops the daemon goroutine from outliving
	// the test. If the test already shut the daemon down, this fails
	// harmlessly (no listener) and is ignored.
	t.Cleanup(func() {
		run([]string{"-store", store, "session", "shutdown", "-socket", sock})
	})

	deadline := time.Now().Add(5 * time.Second)
	for !daemon.Running(sock) {
		if time.Now().After(deadline) {
			t.Fatal("daemon never came up on the -socket override")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Without a matching -socket, session commands derive the default
	// (store-hash) socket path, which is not where this daemon is
	// listening: they must fail, not silently succeed against nothing.
	if err := run([]string{"-store", store, "session", "status"}); err == nil {
		t.Fatal("session status without -socket must not reach a daemon started with -socket override")
	}

	out := call(t, store, "session", "open", "app", "-socket", sock)
	checkout := strings.TrimSpace(out)
	if checkout == "" {
		t.Fatalf("session open -socket printed no checkout path, got %q", out)
	}
	if _, err := os.Stat(checkout); err != nil {
		t.Fatalf("checkout path from session open -socket does not exist: %v", err)
	}

	if err := run([]string{"-store", store, "session", "close", "app", "-socket", sock}); err != nil {
		t.Fatalf("session close -socket: %v", err)
	}
	if err := run([]string{"-store", store, "session", "shutdown", "-socket", sock}); err != nil {
		t.Fatalf("session shutdown -socket: %v", err)
	}

	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("serve -socket returned: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve -socket did not exit after session shutdown -socket")
	}
}
