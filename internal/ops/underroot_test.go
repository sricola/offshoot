package ops

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/store"
)

// mustPanic runs f and reports whether it panicked with underRoot's
// invariant message.
func mustPanic(t *testing.T, label string, f func() string) {
	t.Helper()
	defer func() {
		t.Helper()
		r := recover()
		if r == nil {
			t.Errorf("%s: expected a containment panic, got none", label)
			return
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "store.ValidateName") {
			t.Errorf("%s: panic %v does not name the store.ValidateName invariant", label, r)
		}
	}()
	got := f()
	t.Errorf("%s: returned %q instead of panicking", label, got)
}

// TestUnderRootAcceptsEveryValidNameShape: every builder routed through
// underRoot returns exactly the filepath.Join it used to, for every shape
// store.ValidateName admits, and under absolute, relative, "." and "/"
// roots.
func TestUnderRootAcceptsEveryValidNameShape(t *testing.T) {
	names := []string{
		"a", "main", "z9", "0", "_", ".a", "a.", "a.b.c", "my-db_1.v2",
		"x--", "__", strings.Repeat("a", 64),
	}
	for _, n := range names {
		if err := store.ValidateName(n); err != nil {
			t.Fatalf("test name %q is not a valid name: %v", n, err)
		}
	}
	id := strings.Repeat("ab", 32) // a hex SHA-256, byChainPath's id shape
	roots := []string{
		filepath.Join(t.TempDir(), "store"),
		filepath.Join(t.TempDir(), "store") + string(filepath.Separator),
		"store",
		filepath.Join("rel", "store"),
		".",
		"",
		string(filepath.Separator),
	}
	for _, root := range roots {
		w := &Workspace{Root: root}
		for _, db := range names {
			for _, br := range names {
				if got, want := w.CheckoutPath(db, br), filepath.Join(root, "checkouts", db, br+".db"); got != want {
					t.Fatalf("root %q: CheckoutPath(%q,%q)=%q, want %q", root, db, br, got, want)
				}
				if got, want := w.CheckoutAtPath(db, br, db), filepath.Join(root, "checkouts-ro", db, br+"@"+db+".db"); got != want {
					t.Fatalf("root %q: CheckoutAtPath(%q,%q,%q)=%q, want %q", root, db, br, db, got, want)
				}
			}
			if got, want := w.byChainPath(db, id), filepath.Join(root, "checkouts-ro", db, byChainDir, id+".db"); got != want {
				t.Fatalf("root %q: byChainPath(%q)=%q, want %q", root, db, got, want)
			}
		}
	}
}

// TestUnderRootRejectsEscapingInput: an unvalidated name carrying a
// separator and ".." (which store.ValidateName would have refused) cannot
// produce a path outside the root; the builders panic instead.
func TestUnderRootRejectsEscapingInput(t *testing.T) {
	up := ".." + string(filepath.Separator)
	escapes := []struct{ db, branch string }{
		{"..", up + "escape"},
		{"app", up + up + up + "etc" + string(filepath.Separator) + "passwd"},
		{up + up + "x", "main"},
		{"app" + string(filepath.Separator) + up + up + up + "y", "main"},
	}
	for _, root := range []string{filepath.Join(t.TempDir(), "store"), "store", ".", ""} {
		w := &Workspace{Root: root}
		for _, e := range escapes {
			if err := store.ValidateName(e.db); err == nil {
				if err := store.ValidateName(e.branch); err == nil {
					t.Fatalf("crafted input %q/%q unexpectedly passes store.ValidateName", e.db, e.branch)
				}
			}
			mustPanic(t, "CheckoutPath "+root, func() string { return w.CheckoutPath(e.db, e.branch) })
			mustPanic(t, "CheckoutAtPath "+root, func() string { return w.CheckoutAtPath(e.db, e.branch, "c") })
			mustPanic(t, "byChainPath "+root, func() string { return w.byChainPath(e.db+string(filepath.Separator)+e.branch, "id") })
		}
	}
	// The root itself is not "under" it: nothing may be built at w.Root.
	w := &Workspace{Root: filepath.Join(t.TempDir(), "store")}
	mustPanic(t, "underRoot root itself", func() string { return w.underRoot("x", "..") })
}
