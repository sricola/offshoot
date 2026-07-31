package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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
