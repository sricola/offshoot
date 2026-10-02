package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// callErr runs the CLI's run() with -store pointing at dir and returns
// stdout plus the error, for tests that assert on failures and messages.
func callErr(t *testing.T, dir string, args ...string) (string, error) {
	t.Helper()
	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	err := run(append([]string{"-store", dir}, args...))
	w.Close()
	os.Stdout = old
	buf := make([]byte, 1<<16)
	n, _ := r.Read(buf)
	return string(buf[:n]), err
}

// TestHelpNeverOpensTheStore: `offshoot --help`, `-h`, `help`, `help <cmd>`
// and `<cmd> --help` print usage to stdout and exit 0 in a directory with
// no store at all. Before this, `--help` was taken as a command name and
// failed with "store: not found", and after `init` a `create --help`
// created a database called "--help" while `gc --help` ran GC.
func TestHelpNeverOpensTheStore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "no-store-here")
	for _, args := range [][]string{
		{"--help"}, {"-h"}, {"help"}, {"help", "fork"}, {"fork", "--help"}, {"gc", "-h"}, {"create", "--help"},
	} {
		out, err := callErr(t, dir, args...)
		if err != nil {
			t.Fatalf("offshoot %v: %v", args, err)
		}
		if !strings.Contains(out, "offshoot ") {
			t.Fatalf("offshoot %v printed no usage: %q", args, out)
		}
		if _, statErr := os.Stat(dir); statErr == nil {
			t.Fatalf("offshoot %v created a store", args)
		}
	}
	out, _ := callErr(t, dir, "help", "fork")
	if strings.Contains(out, "offshoot serve") {
		t.Fatalf("help fork printed the whole usage instead of fork's entry")
	}
	if !strings.Contains(out, "offshoot fork") {
		t.Fatalf("help fork did not print fork's entry: %q", out)
	}
}

// TestVersionFlagAliases: --version behaves as `version`.
func TestVersionFlagAliases(t *testing.T) {
	out, err := callErr(t, filepath.Join(t.TempDir(), "none"), "--version")
	if err != nil || !strings.HasPrefix(out, "offshoot ") {
		t.Fatalf("--version: out=%q err=%v", out, err)
	}
}

// TestHelpOnAnInitializedStoreCreatesNothing: with a real store, `create
// --help` must not create a database and `gc --help` must not run GC.
func TestHelpOnAnInitializedStoreCreatesNothing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	call(t, dir, "init")
	if _, err := callErr(t, dir, "create", "--help"); err != nil {
		t.Fatal(err)
	}
	if out := call(t, dir, "status"); strings.Contains(out, "--help") {
		t.Fatalf("create --help created a database: %q", out)
	}
}

// TestNamesCannotStartWithADash: a name beginning with "-" is refused
// everywhere, so no flag typo becomes a database or branch.
func TestNamesCannotStartWithADash(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	call(t, dir, "init")
	if _, err := callErr(t, dir, "create", "-x"); err == nil || !strings.Contains(err.Error(), "must not start with") {
		t.Fatalf("create -x: want a leading-dash refusal, got %v", err)
	}
}

// TestErrorsNameWhatIsMissingOrDuplicate: the raw store sentinels ("store:
// not found", "compare-and-swap conflict: key exists") never reach the
// user; the message says which database, branch or checkpoint, and for a
// duplicate, that it already exists.
func TestErrorsNameWhatIsMissingOrDuplicate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	call(t, dir, "init")
	call(t, dir, "create", "app")
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"init"}, "already initialized"},
		{[]string{"create", "app"}, `database "app" already exists`},
		{[]string{"checkout", "nosuch"}, `no database "nosuch"`},
		{[]string{"checkout", "app@nosuch"}, "no branch app@nosuch"},
		{[]string{"rollback", "app", "--to", "v9"}, `no checkpoint "v9" on app@main (its checkpoints: init;`},
		{[]string{"fork", "app", "x", "--ttl", "banana"}, `--ttl "banana": not a duration`},
		{[]string{"checkout", "app@"}, `invalid target "app@"`},
		{[]string{"bogus"}, `unknown command "bogus"`},
	}
	for _, c := range cases {
		_, err := callErr(t, dir, c.args...)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("offshoot %v: got %v, want it to contain %q", c.args, err, c.want)
		}
	}
	call(t, dir, "fork", "app", "exp")
	if _, err := callErr(t, dir, "fork", "app", "exp"); err == nil || !strings.Contains(err.Error(), "branch app@exp already exists") {
		t.Fatalf("fork onto an existing branch: got %v", err)
	}
	_, err := callErr(t, filepath.Join(t.TempDir(), "absent"), "status")
	if err == nil || !strings.Contains(err.Error(), "no store at") || strings.Contains(err.Error(), "store: not found") {
		t.Fatalf("status without a store: got %v", err)
	}
}

// TestFlagsAcceptBothDashForms: -store/--store, --ttl/-ttl and --x=value
// all parse, so a user who guesses the other convention is not told their
// flag is a database name.
func TestFlagsAcceptBothDashForms(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"--store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"--store=" + dir, "create", "app"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "fork", "app", "a", "--ttl=2h"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "fork", "app", "b", "-ttl", "2h"}); err != nil {
		t.Fatal(err)
	}
	out := call(t, dir, "status")
	if !strings.Contains(out, "app@a ") || !strings.Contains(out, "app@b ") {
		t.Fatalf("forks with alternate flag spellings missing from status: %q", out)
	}
}

// TestCreateAndDestroyReportWhatTheyDid: like git, a successful create or
// destroy says so, instead of leaving the user to run status to find out.
func TestCreateAndDestroyReportWhatTheyDid(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	call(t, dir, "init")
	if out := call(t, dir, "create", "app"); !strings.Contains(out, "created app") {
		t.Fatalf("create output %q", out)
	}
	call(t, dir, "fork", "app", "x")
	if out := call(t, dir, "destroy", "app@x"); !strings.Contains(out, "destroyed app@x") {
		t.Fatalf("destroy output %q", out)
	}
}
