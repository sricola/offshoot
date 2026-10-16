package testutil

import "os/exec"

// SQLite3CLI builds a sqlite3 CLI command with a 5s busy timeout preloaded
// via -cmd ".timeout 5000". Test writes against a checkout with a live
// session attached need it: the session's capture engine takes short
// write-side locks of its own (startup checkpoint TRUNCATE, takeover
// checkpoint RESTART), and a CLI writer landing in one fails instantly with
// "database is locked (5)" instead of waiting it out. A dot-command rather
// than a leading PRAGMA statement so callers parsing stdout see only their
// own query's rows. args are passed through as sqlite3 would take them
// (options, then the database path, then SQL).
func SQLite3CLI(args ...string) *exec.Cmd {
	return exec.Command("sqlite3", append([]string{"-cmd", ".timeout 5000"}, args...)...)
}
