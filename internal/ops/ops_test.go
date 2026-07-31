package ops

import (
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/offshoot-db/offshoot/internal/store"
)

func newWS(t *testing.T) *Workspace {
	t.Helper()
	w, err := Init(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestInitAndOpen(t *testing.T) {
	root := filepath.Join(t.TempDir(), "s")
	if _, err := Open(root); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("open before init: want ErrNotFound, got %v", err)
	}
	if _, err := Init(root); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root); err != nil {
		t.Fatal(err)
	}
}

func TestCreateAndCheckout(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if err := w.Create("app"); err == nil {
		t.Fatal("duplicate create must fail")
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("checkout not writable: %v", err)
	}
	r, _, err := w.Store.GetRef("app", "main")
	if err != nil || !r.Protected || r.Checkpoints["init"] != 1 || r.HeadTXID != 1 {
		t.Fatalf("ref: %+v err=%v", r, err)
	}
}

func TestCreateFromImportsWithoutTouchingSource(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	src := filepath.Join(t.TempDir(), "legacy.db")
	if out, err := exec.Command("sqlite3", src,
		"CREATE TABLE t (v); INSERT INTO t VALUES (42);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	before, _ := os.ReadFile(src)

	w := newWS(t)
	if err := w.CreateFrom("legacy", src); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(src)
	if string(before) != string(after) {
		t.Fatal("import mutated the source file")
	}
	path, err := w.Checkout("legacy", "main")
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("sqlite3", path, "SELECT v FROM t;").Output()
	if err != nil || string(out) != "42\n" {
		t.Fatalf("imported content: %q err=%v", out, err)
	}
}

func TestParseTarget(t *testing.T) {
	db, br, err := ParseTarget("app")
	if err != nil || db != "app" || br != "main" {
		t.Fatalf("%s %s %v", db, br, err)
	}
	db, br, err = ParseTarget("app@x-1")
	if err != nil || db != "app" || br != "x-1" {
		t.Fatalf("%s %s %v", db, br, err)
	}
	if _, _, err := ParseTarget("a@b@c"); err == nil {
		t.Fatal("want error")
	}
	if _, _, err := ParseTarget("Bad@x"); err == nil {
		t.Fatal("want name validation error")
	}
}
