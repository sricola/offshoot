package store

import (
	"path/filepath"
	"strings"
	"testing"
)

// FuzzValidateName: ValidateName never panics, and every name it accepts is
// safe in every place a name becomes a key or a path — it round-trips
// through RefKey and ListRefs' split, the local backend accepts the key and
// keeps it under its root, and it is one plain directory entry when joined
// onto a checkout directory. Names reserved for internal directories
// ("~by-chain", the ops by-chain clone cache) are never accepted.
func FuzzValidateName(f *testing.F) {
	for _, s := range []string{
		"main", "db1", "feature-x", "a_b.c", "x", strings.Repeat("a", maxNameLen),
		"", ".", "..", "a..b", "a/b", "/abs", "UPPER", "~by-chain", "ünï", "a\x00b",
		strings.Repeat("a", maxNameLen+1), "a b", "refs/../x",
	} {
		f.Add([]byte(s))
	}
	if ValidateName("~by-chain") == nil {
		f.Fatal(`ValidateName accepts "~by-chain", the by-chain cache directory`)
	}
	root := f.TempDir()
	l := &Local{root: root}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		name := string(data)
		if ValidateName(name) != nil {
			return
		}
		if name == "" || len(name) > maxNameLen {
			t.Fatalf("accepted name of length %d", len(name))
		}
		if name == "." || name == ".." || strings.Contains(name, "..") || strings.ContainsAny(name, "/\\~\x00") {
			t.Fatalf("accepted unsafe name %q", name)
		}
		for i := 0; i < len(name); i++ {
			c := name[i]
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
				t.Fatalf("accepted name %q with byte %#x outside [a-z0-9-_.]", name, c)
			}
		}

		key := RefKey(name, name)
		parts := strings.Split(key, "/")
		if len(parts) != 3 || parts[0] != "refs" || parts[1] != name || parts[2] != name {
			t.Fatalf("RefKey(%q, %q) = %q does not split back into (db, branch)", name, name, key)
		}
		p, err := l.path(key)
		if err != nil {
			t.Fatalf("local backend rejects the ref key of an accepted name %q: %v", name, err)
		}
		if rel, err := filepath.Rel(root, p); err != nil || rel != filepath.FromSlash(key) {
			t.Fatalf("ref key %q resolves to %q, outside or reshaped under root %q", key, p, root)
		}
		dir := filepath.Join(root, "checkouts")
		if joined := filepath.Join(dir, name); filepath.Dir(joined) != dir || filepath.Base(joined) != name {
			t.Fatalf("name %q is not a single path element under %q (joined %q)", name, dir, joined)
		}
	})
}
