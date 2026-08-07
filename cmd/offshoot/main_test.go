package main

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/daemon"
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

// TestServeNonPositiveSnapshotEveryIsRejected pins -snapshot-every's own
// usage-error rule (Milestone 4 Task 6a): unlike -flush-every, there is no
// "0 means disabled" sentinel for the snapshot cadence — every flush must
// still eventually snapshot, so 0 and negative values are both rejected
// rather than silently aliased to "unlimited" or the default. run() must
// return before ever starting to serve, so this needs no daemon lifecycle
// (mirrors TestServeNegativeFlushEveryIsRejected's shape).
func TestServeNonPositiveSnapshotEveryIsRejected(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "create", "app"}); err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"0", "-1", "-4"} {
		if err := run([]string{"-store", dir, "serve", "-snapshot-every", v}); err == nil {
			t.Fatalf("serve -snapshot-every %s must be rejected", v)
		}
	}
	if err := run([]string{"-store", dir, "serve", "-snapshot-every", "not-a-number"}); err == nil {
		t.Fatal("serve -snapshot-every not-a-number must be rejected")
	}
}

// TestParseByteSize pins -ro-cache-budget's value grammar (Milestone 4 Task
// 5): bare integers are bytes (the contract), a trailing power-of-1024
// K/M/G/T(B) suffix is a convenience multiplier, "" (flag omitted) is 0
// (unlimited), and a negative or garbage value is rejected outright rather
// than silently coerced to 0 or ignored.
func TestParseByteSize(t *testing.T) {
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{"", 0, false},
		{"0", 0, false},
		{"1", 1, false},
		{"1024", 1024, false},
		{"500", 500, false},
		{"1K", 1024, false},
		{"1KB", 1024, false},
		{"1kb", 1024, false},
		{"1M", 1 << 20, false},
		{"500MB", 500 * (1 << 20), false},
		{"2G", 2 << 30, false},
		{"1GB", 1 << 30, false},
		{"1T", 1 << 40, false},
		{"1TB", 1 << 40, false},
		{"500B", 500, false},
		{"-1", 0, true},
		{"-1MB", 0, true},
		{"abc", 0, true},
		{"MB", 0, true},
		{"1.5MB", 0, true},
		{"100GB", 100 * (1 << 30), false},
		// 8388608T = 2^23 * 2^40 = 2^63, one past math.MaxInt64: on int64
		// this overflows to a negative product, which pre-fix was silently
		// treated as 0/"unlimited" — the opposite of what a huge-but-finite
		// budget request meant. Must be a clear error, not a wrapped value.
		{"8388608T", 0, true},
		{"9223372036854775807", math.MaxInt64, false},
	}
	for _, c := range cases {
		got, err := parseByteSize(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseByteSize(%q) = %d, <nil>, want an error", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseByteSize(%q) unexpected error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseByteSize(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

// TestServeNegativeROCacheBudgetIsRejected mirrors
// TestServeNegativeFlushEveryIsRejected's shape for the new flag: a
// negative -ro-cache-budget is refused at startup, before any socket/daemon
// is touched, rather than silently treated as 0 (unlimited) or something
// else surprising.
func TestServeNegativeROCacheBudgetIsRejected(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "create", "app"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "serve", "-ro-cache-budget", "-5"}); err == nil {
		t.Fatal("serve -ro-cache-budget -5 must be rejected, not silently treated as unlimited")
	}
}

// TestStatusShowsROCacheUsageAndBudget exercises `offshoot status`'s
// ro-cache summary line end to end through the CLI: an empty store reports
// zero usage with "unlimited" when -ro-cache-budget is omitted, and echoes
// back a given -ro-cache-budget (including its size-suffix parsing) when
// one is passed.
func TestStatusShowsROCacheUsageAndBudget(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "create", "app"}); err != nil {
		t.Fatal(err)
	}

	out := call(t, dir, "status")
	if !strings.Contains(out, "ro-cache: 0 entries, 0 bytes used (budget: unlimited)") {
		t.Fatalf("status without -ro-cache-budget:\n%s", out)
	}

	out = call(t, dir, "status", "-ro-cache-budget", "1MB")
	if !strings.Contains(out, fmt.Sprintf("budget %d bytes", int64(1<<20))) {
		t.Fatalf("status -ro-cache-budget 1MB:\n%s", out)
	}
}

// TestServeTokenWithoutHTTPIsRejected and
// TestServeAllowNonLoopbackWithoutHTTPIsRejected pin Milestone 4 Task 3's
// review-fold item: -token/-http-allow-non-loopback only mean anything
// alongside -http, so giving either one without it must fail as a startup
// error (almost certainly a typo'd or dropped -http ADDR) rather than
// silently doing nothing — the surprising alternative would be an operator
// noticing only by ABSENCE (no HTTP listener, no auth) instead of an
// explicit error. Both run() before ever touching the socket/daemon, same
// shape as TestServeNegativeFlushEveryIsRejected.
func TestServeTokenWithoutHTTPIsRejected(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	err := run([]string{"-store", dir, "serve", "-token", "some-token-value-0123456789"})
	if err == nil {
		t.Fatal("serve -token without -http must be rejected, not silently ignored")
	}
	if !strings.Contains(err.Error(), "-token given without -http") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestServeAllowNonLoopbackWithoutHTTPIsRejected(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	err := run([]string{"-store", dir, "serve", "-http-allow-non-loopback"})
	if err == nil {
		t.Fatal("serve -http-allow-non-loopback without -http must be rejected, not silently ignored")
	}
	if !strings.Contains(err.Error(), "-http-allow-non-loopback given without -http") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestServeHTTPNonLoopbackStartupErrorsAreDistinct exercises PM Amendment
// 10's two-distinct-errors requirement through the CLI end to end (not
// just daemon.ValidateHTTPBind directly, which internal/daemon's own tests
// already cover) — missing -http-allow-non-loopback vs. (ack given but) no
// explicit token must fail with different messages, both before run() ever
// creates a socket/daemon.
func TestServeHTTPNonLoopbackStartupErrorsAreDistinct(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	errNoAck := run([]string{"-store", dir, "serve", "-http", "0.0.0.0:0"})
	if errNoAck == nil {
		t.Fatal("serve -http on a non-loopback address without -http-allow-non-loopback must be rejected")
	}
	errNoToken := run([]string{"-store", dir, "serve", "-http", "0.0.0.0:0", "-http-allow-non-loopback"})
	if errNoToken == nil {
		t.Fatal("serve -http-allow-non-loopback without an explicit token must be rejected")
	}
	if errNoAck.Error() == errNoToken.Error() {
		t.Fatalf("the two non-loopback startup errors must be distinct, both were: %q", errNoAck.Error())
	}
}

// TestServeShortTokenIsRejected exercises the minHTTPTokenLen guard through
// the CLI: an explicit -token under 16 characters must be a startup error,
// not something only internal/daemon.StartHTTP's own tests catch.
func TestServeShortTokenIsRejected(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	err := run([]string{"-store", dir, "serve", "-http", "127.0.0.1:0", "-token", "short"})
	if err == nil {
		t.Fatal("serve -http with a short -token must be rejected")
	}
	if !strings.Contains(err.Error(), "at least") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestMCPDefaultTTLNegativeIsRejected mirrors
// TestServeNegativeFlushEveryIsRejected for mcp's -default-ttl: a negative
// duration is (almost certainly) a mistake and must fail closed as a usage
// error rather than being silently aliased to "disabled" (that's what 0 and
// "none" are for — see TestMCPDefaultTTLDisableSpellings). run()'s
// -default-ttl validation happens before mcp.NewOffshootTools/srv.Serve are
// ever reached, so — like the flush-every case — this needs no daemon
// lifecycle, and (unlike a valid `mcp` invocation) it can't block on
// os.Stdin either.
func TestMCPDefaultTTLNegativeIsRejected(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "mcp", "-default-ttl", "-1h"}); err == nil {
		t.Fatal("mcp -default-ttl -1h must be rejected, not silently disable the default")
	}
}

// TestMCPDefaultTTLUnparseableIsRejected pins that an unparseable
// -default-ttl comes back as a clear usage error rather than reaching
// mcp.NewOffshootTools with a zero-value duration it never asked for.
func TestMCPDefaultTTLUnparseableIsRejected(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	err := run([]string{"-store", dir, "mcp", "-default-ttl", "bananas"})
	if err == nil {
		t.Fatal("mcp -default-ttl bananas must be rejected")
	}
	if !strings.Contains(err.Error(), "-default-ttl") {
		t.Fatalf("error should name the offending flag: %v", err)
	}
}

// TestMCPDefaultTTLDisableSpellings and TestMCPDefaultTTLDefaultsTo24h
// exercise parseDefaultTTLFlag directly rather than driving `mcp` through
// run(): a valid -default-ttl (unlike the rejected cases above) falls
// through to srv.Serve(context.Background()), which blocks reading
// os.Stdin until EOF or cancellation — not a daemon lifecycle exactly, but
// still not something a unit test should spin up just to observe a flag's
// parsed value. parseDefaultTTLFlag is the actual code the "mcp" case
// calls, so this covers the real behavior, not a proxy for it.

// TestMCPDefaultTTLDisableSpellings pins that "0" and "none" both parse to
// the same disabled (zero) TTL, matching parseTTLFlag's touch/CLI
// convention reused here.
func TestMCPDefaultTTLDisableSpellings(t *testing.T) {
	for _, spelling := range []string{"0", "0s", "none"} {
		got, rest, err := parseDefaultTTLFlag([]string{"-default-ttl", spelling})
		if err != nil {
			t.Fatalf("-default-ttl %q: %v", spelling, err)
		}
		if got != 0 {
			t.Errorf("-default-ttl %q = %v, want 0 (disabled)", spelling, got)
		}
		if len(rest) != 0 {
			t.Errorf("-default-ttl %q: leftover args = %v, want none consumed", spelling, rest)
		}
	}
}

// TestMCPDefaultTTLDefaultsTo24h pins the documented default (README,
// docs/reference.md, the `offshoot mcp` usage line): omitting -default-ttl
// entirely applies 24h, not 0/disabled.
func TestMCPDefaultTTLDefaultsTo24h(t *testing.T) {
	got, rest, err := parseDefaultTTLFlag(nil)
	if err != nil {
		t.Fatalf("parseDefaultTTLFlag(nil): %v", err)
	}
	if got != 24*time.Hour {
		t.Errorf("default TTL with no -default-ttl flag = %v, want 24h", got)
	}
	if len(rest) != 0 {
		t.Errorf("leftover args = %v, want none", rest)
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
