package dbfile_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	modulePath = "github.com/sricola/offshoot"
	sqlPkg     = "database/sql"
	driverPkg  = "github.com/mattn/go-sqlite3"
	dbfilePkg  = modulePath + "/internal/dbfile"
)

type siteClass string

const (
	pinned      siteClass = "pinned"
	notCheckout siteClass = "not-checkout"
)

// siteKey names an enclosing declaration: dir is the package directory
// relative to the module root, fn is "Func" or "Recv.Method". A function
// literal belongs to the declaration it sits in.
type siteKey struct{ dir, fn string }

// sqliteSites is the reviewed inventory of every in-process SQLite open in
// non-test code (the fd-budget design's "In-process SQLite opens,
// classified"). Test code is outside it, which is why tests that evict
// scope their eviction to their own directory (dbfile.EvictUnder).
var sqliteSites = map[siteKey]siteClass{
	{"internal/capture", "Engine.Run"}:       pinned,
	{"internal/ops", "quiesce"}:              pinned,
	{"internal/ops", "vacuumImportSource"}:   pinned,
	{"cmd/branchbench", "runner.stepSQL"}:    pinned,
	{"cmd/branchbench", "sumOrderLines"}:     pinned,
	{"cmd/branchbench", "writeSeedRows"}:     pinned,
	{"internal/ops", "Workspace.Create"}:     notCheckout, // a temp file it created
	{"internal/ops", "Workspace.CreateFrom"}: notCheckout, // its own VACUUM INTO copy
	{"internal/ops", "TableRowCounts"}:       notCheckout, // mode=ro&immutable=1
	{"internal/ops", "DiffSummary"}:          notCheckout, // mode=ro&immutable=1
}

// attachSites may open a second database through SQL (ATTACH), which no Go
// call shows. DiffSummary attaches its right side mode=ro&immutable=1.
var attachSites = map[siteKey]bool{{"internal/ops", "DiffSummary"}: true}

const classificationRule = `Every in-process SQLite open must be classified in internal/dbfile/sites_test.go:
  pinned: it can open a file dbfile may cache (any checkout, or any path a client names).
    Call release, ino, err := dbfile.Hold(path) BEFORE sql.Open; defer release() before sql.Open
    too, so it runs after the database's Close on every return path, and call it nowhere else;
    open with dbfile.NoCreateDSN (or a mode=ro URI) so a path
    removed after the Hold is not created empty; take the first connection with db.Conn; then
    require dbfile.Verify(path, ino). Closing any descriptor on an inode drops every POSIX lock
    this process holds there, and dbfile closes cached descriptors whenever nothing pins their
    inode.
  not-checkout: it only opens a file dbfile never caches (a temp file this function created),
    or opens with mode=ro&immutable=1, under which SQLite takes no locks.
Running sqlite3 or sqldiff through exec.Command needs no entry (replay's .dump, diff's sqldiff,
the torture binary's writers): POSIX locks are per process, so another process can neither
hold nor drop this one's.`

var attachRE = regexp.MustCompile(`(?i)\battach\s+(database\s+)?[?'":@$]`)

// siteUse records what one declaration does. releaseDefers are `defer r()`
// statements and releaseCalls every other use of r, where r is bound to the
// release a dbfile.Hold in the same declaration returned; unboundHolds are
// Holds whose release is not bound to a name at all.
type siteUse struct {
	opens, holds, verifies, attaches          []token.Pos
	releaseDefers, releaseCalls, unboundHolds []token.Pos
}

type scanResult struct {
	fset  *token.FileSet
	sites map[siteKey]*siteUse
	bad   []string
}

func (r *scanResult) use(k siteKey) *siteUse {
	if r.sites[k] == nil {
		r.sites[k] = &siteUse{}
	}
	return r.sites[k]
}

// scan parses every non-test .go file of the module rooted at root, skipping
// hidden directories, testdata, vendor, node_modules and nested modules.
// Build tags are ignored on purpose: a tagged file's open counts too.
func scan(root string) (*scanResult, error) {
	r := &scanResult{fset: token.NewFileSet(), sites: map[siteKey]*siteUse{}}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == root {
				return nil
			}
			name := d.Name()
			if strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor" || name == "node_modules" {
				return filepath.SkipDir
			}
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
				return filepath.SkipDir // another module (site/gen)
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		return r.file(filepath.ToSlash(rel), path)
	})
	return r, err
}

func (r *scanResult) file(dir, path string) error {
	f, err := parser.ParseFile(r.fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		return err
	}
	names := map[string]string{}
	for _, imp := range f.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		local := p[strings.LastIndex(p, "/")+1:]
		if p == driverPkg {
			local = "sqlite3"
		}
		if imp.Name != nil {
			switch imp.Name.Name {
			case "_":
				continue
			case ".":
				if p == sqlPkg || p == driverPkg || p == dbfilePkg {
					r.bad = append(r.bad, fmt.Sprintf("%s: dot import of %s hides its calls from this guardrail",
						r.fset.Position(imp.Pos()), p))
				}
				continue
			default:
				local = imp.Name.Name
			}
		}
		names[local] = p
	}
	isHold := func(e ast.Expr) bool {
		sel, ok := e.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		x, ok := sel.X.(*ast.Ident)
		return ok && names[x.Name] == dbfilePkg && sel.Sel.Name == "Hold"
	}
	for _, decl := range f.Decls {
		k := siteKey{dir, declName(decl)}
		// The names this declaration binds a Hold's release to, so the walk
		// below can tell how the release is used: the defer order is what
		// keeps the pin in place until the connection is closed.
		releases := map[string]bool{}
		binders := map[*ast.Ident]bool{}
		bound := map[token.Pos]bool{}
		ast.Inspect(decl, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok || len(as.Rhs) != 1 {
				return true
			}
			call, ok := as.Rhs[0].(*ast.CallExpr)
			if !ok || !isHold(call.Fun) {
				return true
			}
			if id, ok := as.Lhs[0].(*ast.Ident); ok && id.Name != "_" {
				releases[id.Name] = true
				binders[id] = true
				bound[call.Fun.Pos()] = true
			}
			return true
		})
		ast.Inspect(decl, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.DeferStmt:
				if id, ok := n.Call.Fun.(*ast.Ident); ok && releases[id.Name] && len(n.Call.Args) == 0 {
					r.use(k).releaseDefers = append(r.use(k).releaseDefers, n.Pos())
					return false
				}
			case *ast.Ident:
				if releases[n.Name] && !binders[n] {
					r.use(k).releaseCalls = append(r.use(k).releaseCalls, n.Pos())
				}
			case *ast.SelectorExpr:
				x, ok := n.X.(*ast.Ident)
				if !ok {
					return true
				}
				switch p := names[x.Name]; {
				case p == sqlPkg && (n.Sel.Name == "Open" || n.Sel.Name == "OpenDB"):
					r.use(k).opens = append(r.use(k).opens, n.Pos())
				case p == sqlPkg && n.Sel.Name == "Register":
					r.bad = append(r.bad, fmt.Sprintf("%s: sql.Register in %s %s: a driver registration bypasses this inventory\n%s",
						r.fset.Position(n.Pos()), k.dir, k.fn, classificationRule))
				case p == driverPkg:
					r.bad = append(r.bad, fmt.Sprintf("%s: go-sqlite3's %s in %s %s: a connector or driver use bypasses this inventory\n%s",
						r.fset.Position(n.Pos()), n.Sel.Name, k.dir, k.fn, classificationRule))
				case p == dbfilePkg && n.Sel.Name == "Hold":
					r.use(k).holds = append(r.use(k).holds, n.Pos())
					if !bound[n.Pos()] {
						r.use(k).unboundHolds = append(r.use(k).unboundHolds, n.Pos())
					}
				case p == dbfilePkg && n.Sel.Name == "Verify":
					r.use(k).verifies = append(r.use(k).verifies, n.Pos())
				}
			case *ast.BasicLit:
				if n.Kind == token.STRING && attachRE.MatchString(n.Value) {
					r.use(k).attaches = append(r.use(k).attaches, n.Pos())
				}
			}
			return true
		})
	}
	return nil
}

func declName(d ast.Decl) string {
	fd, ok := d.(*ast.FuncDecl)
	if !ok {
		return "<package scope>"
	}
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	t := fd.Recv.List[0].Type
	if s, ok := t.(*ast.StarExpr); ok {
		t = s.X
	}
	switch g := t.(type) {
	case *ast.IndexExpr:
		t = g.X
	case *ast.IndexListExpr:
		t = g.X
	}
	if id, ok := t.(*ast.Ident); ok {
		return id.Name + "." + fd.Name.Name
	}
	return fd.Name.Name
}

func minPos(ps []token.Pos) token.Pos {
	m := ps[0]
	for _, p := range ps[1:] {
		if p < m {
			m = p
		}
	}
	return m
}

func check(r *scanResult, want map[siteKey]siteClass, attach map[siteKey]bool) []string {
	problems := append([]string(nil), r.bad...)
	for k, u := range r.sites {
		if len(u.attaches) > 0 && !attach[k] {
			problems = append(problems, fmt.Sprintf("%s: SQL ATTACH in %s %s opens another database file with no sql.Open in sight; classify it in attachSites\n%s",
				r.fset.Position(u.attaches[0]), k.dir, k.fn, classificationRule))
		}
		if len(u.opens) == 0 {
			continue
		}
		class, ok := want[k]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s: unclassified SQLite open in %s %s\n%s",
				r.fset.Position(u.opens[0]), k.dir, k.fn, classificationRule))
			continue
		}
		if class != pinned {
			continue
		}
		first := minPos(u.opens)
		if len(u.holds) == 0 || minPos(u.holds) > first {
			problems = append(problems, fmt.Sprintf("%s: %s %s is classified pinned but does not call dbfile.Hold before its first sql.Open\n%s",
				r.fset.Position(first), k.dir, k.fn, classificationRule))
		}
		verified := false
		for _, v := range u.verifies {
			verified = verified || v > first
		}
		if !verified {
			problems = append(problems, fmt.Sprintf("%s: %s %s is classified pinned but never calls dbfile.Verify after opening\n%s",
				r.fset.Position(first), k.dir, k.fn, classificationRule))
		}
		// A defer registered before sql.Open runs after every defer
		// registered later, which includes any deferred Close of what that
		// open returned. A release run any other way can drop the pin while
		// the connection still holds its locks.
		if len(u.unboundHolds) > 0 {
			problems = append(problems, fmt.Sprintf("%s: %s %s is classified pinned but discards its dbfile.Hold's release\n%s",
				r.fset.Position(u.unboundHolds[0]), k.dir, k.fn, classificationRule))
		}
		deferred := false
		for _, d := range u.releaseDefers {
			deferred = deferred || d < first
		}
		if !deferred {
			problems = append(problems, fmt.Sprintf("%s: %s %s is classified pinned but does not defer its dbfile.Hold's release before its first sql.Open\n%s",
				r.fset.Position(first), k.dir, k.fn, classificationRule))
		}
		if len(u.releaseCalls) > 0 {
			problems = append(problems, fmt.Sprintf("%s: %s %s is classified pinned but uses its dbfile.Hold's release other than in that defer\n%s",
				r.fset.Position(u.releaseCalls[0]), k.dir, k.fn, classificationRule))
		}
	}
	for k := range want {
		if u := r.sites[k]; u == nil || len(u.opens) == 0 {
			problems = append(problems, fmt.Sprintf("sqliteSites lists %s %s, which opens no SQLite database: remove the entry", k.dir, k.fn))
		}
	}
	for k := range attach {
		if u := r.sites[k]; u == nil || len(u.attaches) == 0 {
			problems = append(problems, fmt.Sprintf("attachSites lists %s %s, which has no ATTACH: remove the entry", k.dir, k.fn))
		}
	}
	sort.Strings(problems)
	return problems
}

var moduleLine = regexp.MustCompile(`(?m)^module ` + regexp.QuoteMeta(modulePath) + `\s*$`)

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if b, err := os.ReadFile(filepath.Join(dir, "go.mod")); err == nil && moduleLine.Match(b) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no go.mod for %s above the test's directory", modulePath)
		}
		dir = parent
	}
}

func TestEveryInProcessSQLiteOpenIsClassified(t *testing.T) {
	r, err := scan(moduleRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range check(r, sqliteSites, attachSites) {
		t.Error(p)
	}
}

// TestGuardrailFlagsUnclassifiedOpens runs the scanner over a synthetic
// module, so the guardrail is proven to fail, not just to pass.
func TestGuardrailFlagsUnclassifiedOpens(t *testing.T) {
	root := t.TempDir()
	write := func(rel, src string) {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module "+modulePath+"\n")
	write("pkg/a.go", `package a

import (
	"database/sql"
	sq "github.com/mattn/go-sqlite3"
	"github.com/sricola/offshoot/internal/dbfile"
)

func leak(p string) { sql.Open("sqlite3", p) }

func lateHold(p string) {
	sql.Open("sqlite3", p)
	r, _, _ := dbfile.Hold(p)
	r()
}

func attach(db *sql.DB) { db.Exec("ATTACH DATABASE ? AS x", "f") }

var _ = sq.SQLiteDriver{}

func good(p string) {
	release, ino, err := dbfile.Hold(p)
	if err != nil {
		return
	}
	defer release()
	db, _ := sql.Open("sqlite3", p)
	defer db.Close()
	dbfile.Verify(p, ino)
}

func earlyRelease(p string) {
	r, ino, _ := dbfile.Hold(p)
	r()
	db, _ := sql.Open("sqlite3", p)
	defer db.Close()
	dbfile.Verify(p, ino)
}

func lateDefer(p string) {
	r, ino, _ := dbfile.Hold(p)
	db, _ := sql.Open("sqlite3", p)
	defer db.Close()
	defer r()
	dbfile.Verify(p, ino)
}

func discarded(p string) {
	_, ino, _ := dbfile.Hold(p)
	db, _ := sql.Open("sqlite3", p)
	defer db.Close()
	dbfile.Verify(p, ino)
}
`)
	r, err := scan(root)
	if err != nil {
		t.Fatal(err)
	}
	classes := map[siteKey]siteClass{}
	for _, fn := range []string{"lateHold", "good", "earlyRelease", "lateDefer", "discarded"} {
		classes[siteKey{"pkg", fn}] = pinned
	}
	got := strings.Join(check(r, classes, nil), "\n---\n")
	for _, want := range []string{
		"unclassified SQLite open in pkg leak",
		"pkg lateHold is classified pinned but does not call dbfile.Hold before its first sql.Open",
		"pkg lateHold is classified pinned but never calls dbfile.Verify",
		"SQL ATTACH in pkg attach",
		"go-sqlite3's SQLiteDriver",
		"pkg earlyRelease is classified pinned but does not defer its dbfile.Hold's release before its first sql.Open",
		"pkg earlyRelease is classified pinned but uses its dbfile.Hold's release other than in that defer",
		"pkg lateDefer is classified pinned but does not defer its dbfile.Hold's release before its first sql.Open",
		"pkg discarded is classified pinned but discards its dbfile.Hold's release",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("guardrail output lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "pkg good ") {
		t.Errorf("guardrail flags a correctly held open:\n%s", got)
	}
}
