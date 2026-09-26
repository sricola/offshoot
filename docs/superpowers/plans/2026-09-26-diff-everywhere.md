# Diff Everywhere — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make "what changed between attempt A and attempt B" answerable by an agent or a harness without a terminal: a content-aware, sqldiff-free per-table summary (added/removed/changed rows, schema changes) reachable from the CLI, the daemon (unix socket and HTTP), both SDKs, and MCP, plus a byte-capped, per-table full SQL diff where `sqldiff` is installed.

**Architecture:** `internal/ops/diff.go` grows a content-aware `DiffSummary` (one read-only SQLite connection, the other side `ATTACH`ed read-only, counts via key anti-joins and `EXCEPT`) and takes over the `sqldiff` invocation from the CLI (`Sqldiff` with a byte cap and a `table` filter, `ErrSqldiffMissing` sentinel). One shared `FormatDiffSummary` renders the table for CLI and MCP. The daemon gets a `diff` op that returns JSON (never a path, so it is HTTP-safe); both SDKs get `diff()`; MCP gets a read-only `offshoot_diff`. Docs re-capture their sample outputs from real runs.

**Tech Stack:** Go 1.26, `database/sql` + `mattn/go-sqlite3` (already linked; the driver opens with `SQLITE_OPEN_URI`, so `ATTACH 'file:...?mode=ro&immutable=1'` works), optional external `sqldiff`. Python 3.10+ SDK, TypeScript SDK (node:test).

**Spec:** `reports/offshoot next features roadmap.md`, Tier 1 item "Weeks 3-4: `diff` everywhere, with a changed-tables summary". Product decisions (PM + staff-engineer, market-grounded): (1) a row-count-only summary misleads an agent comparing attempts (equal counts, different values report "same"), so the summary must be content-aware and must not need `sqldiff`; (2) the per-table drill-down (`table`) is what an agent needs after a summary, and full SQL over the wire must be byte-capped with an explicit `truncated` flag; (3) the summary's table-level page mapping idea from the report is replaced by SQL counts over the materialized sides — O(size) either way, and SQL is correct across arbitrary lineages; (4) MCP diff stays within one database (cross-db stays a CLI/daemon/SDK capability).

## Global Constraints

- Go directive `1.26.0`; gofmt clean; `go vet ./...` clean; `go test ./... -count=1` green.
- Commit trailers: `Co-Authored-By: Claude <committing model> <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B`.
- Package publication is deferred indefinitely: no PyPI/npm install text anywhere.
- Existing MCP tools' names, argument names, descriptions, and prose results must not change; adding a tool is fine (this plan adds `offshoot_diff`, the ninth).
- Every diff path materializes read-only via `ops.Workspace.MaterializeForDiff` (never a live checkout, never a lease) and always closes both sides.
- Existing public names stay: `ops.DiffSummary`, `ops.TableDiff` (fields are added, none removed), `ops.TableRowCounts`, `ops.MaterializeForDiff`, `ops.DiffSide`.
- Wire shapes are snake_case JSON. The daemon `diff` op never returns a filesystem path.
- Every doc sample output labelled "real output" is re-captured from a real run in this plan, never hand-edited.

---

### Task 1: Content-aware `DiffSummary` and shared formatter (ops)

**Files:**
- Modify: `internal/ops/diff.go` (TableDiff, DiffSummary; add `DiffReport`, `FormatDiffSummary`)
- Test: `internal/ops/diff_test.go`

**Interfaces:**
- Produces:
  ```go
  type TableDiff struct {
      Table       string `json:"table"`
      LeftExists  bool   `json:"left_exists"`
      RightExists bool   `json:"right_exists"`
      Left        int    `json:"left_rows"`
      Right       int    `json:"right_rows"`
      // Comparable is true when the table exists on both sides with the
      // same column names in the same order; Added/Removed/Changed are
      // meaningful only then.
      Comparable    bool   `json:"comparable"`
      Added         int    `json:"added"`
      Removed       int    `json:"removed"`
      Changed       int    `json:"changed"`
      SchemaChanged bool   `json:"schema_changed"`
      // Status: "added" | "removed" | "same" | "changed"
      Status string `json:"status"`
  }
  func (d TableDiff) Delta() int
  type DiffTotals struct{ Same, Changed, Added, Removed int } // json: same, changed, added, removed
  type DiffReport struct {
      Tables []TableDiff `json:"tables"`
      Totals DiffTotals  `json:"totals"`
  }
  func DiffSummary(leftPath, rightPath string) ([]TableDiff, error)   // unchanged signature, content-aware now
  func DiffReportOf(tables []TableDiff) DiffReport                     // computes Totals
  func FormatDiffSummary(w io.Writer, rep DiffReport, leftLabel, rightLabel string) error
  ```
  `FormatDiffSummary` prints (tabwriter) the header `TABLE\t<leftLabel>\t<rightLabel>\tADDED\tREMOVED\tCHANGED\tSTATUS`, one row per table (`-` for a side where the table does not exist; `-` in the three count columns when not Comparable; `+N`/`-N`/`~N` style is NOT used, plain integers), then the totals line `N tables: A same, B changed, C added, D removed` (same wording as today).

- [ ] **Step 1: Write the failing tests**

Append to `internal/ops/diff_test.go` (imports already include `os/exec`, `path/filepath`, `testing`, `testutil`; add `bytes` and `strings` if missing):

```go
// TestDiffSummaryIsContentAware pins the reason the summary exists for
// agents: two attempts with identical row counts but different VALUES are
// not "same". With a declared primary key, a row whose key is on both sides
// but whose content differs is Changed; a key only on the left is Removed;
// only on the right is Added. Row counts stay reported alongside.
func TestDiffSummaryIsContentAware(t *testing.T) {
	testutil.RequireSQLite3(t)
	left := filepath.Join(t.TempDir(), "left.db")
	right := filepath.Join(t.TempDir(), "right.db")
	if out, err := exec.Command("sqlite3", left,
		"CREATE TABLE results (id INTEGER PRIMARY KEY, passed INT);"+
			"INSERT INTO results VALUES (1,1),(2,1),(3,1);",
	).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if out, err := exec.Command("sqlite3", right,
		"CREATE TABLE results (id INTEGER PRIMARY KEY, passed INT);"+
			"INSERT INTO results VALUES (1,1),(2,0),(4,1);", // 2 changed, 3 removed, 4 added
	).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	got, err := DiffSummary(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d tables, want 1: %+v", len(got), got)
	}
	r := got[0]
	if !r.Comparable || r.Left != 3 || r.Right != 3 {
		t.Fatalf("results = %+v, want comparable with 3/3 rows", r)
	}
	if r.Added != 1 || r.Removed != 1 || r.Changed != 1 || r.Status != "changed" {
		t.Fatalf("results = %+v, want added=1 removed=1 changed=1 status=changed", r)
	}
	if r.SchemaChanged {
		t.Fatalf("results schema must not be flagged changed: %+v", r)
	}
}

// TestDiffSummaryRowidTablesUseRowidAsKey: a table with no declared primary
// key is keyed by rowid (both sides descend from one seed, so rowids line
// up), and an in-place UPDATE shows as Changed, not Removed+Added.
func TestDiffSummaryRowidTablesUseRowidAsKey(t *testing.T) {
	testutil.RequireSQLite3(t)
	left := filepath.Join(t.TempDir(), "left.db")
	right := filepath.Join(t.TempDir(), "right.db")
	exec.Command("sqlite3", left, "CREATE TABLE t (v); INSERT INTO t VALUES ('a'),('b');").Run()
	exec.Command("sqlite3", right, "CREATE TABLE t (v); INSERT INTO t VALUES ('a'),('B');").Run()
	got, err := DiffSummary(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Changed != 1 || got[0].Added != 0 || got[0].Removed != 0 {
		t.Fatalf("t = %+v, want changed=1 only", got[0])
	}
}

// TestDiffSummarySchemaMismatchIsReportedNotCompared: when the column list
// differs, row-level counts are impossible to define, so the table is
// reported with row counts, Comparable=false, SchemaChanged=true, and
// Status "changed" — never a SQL error from a malformed EXCEPT.
func TestDiffSummarySchemaMismatchIsReportedNotCompared(t *testing.T) {
	testutil.RequireSQLite3(t)
	left := filepath.Join(t.TempDir(), "left.db")
	right := filepath.Join(t.TempDir(), "right.db")
	exec.Command("sqlite3", left, "CREATE TABLE t (a); INSERT INTO t VALUES (1);").Run()
	exec.Command("sqlite3", right, "CREATE TABLE t (a, b); INSERT INTO t VALUES (1, 2);").Run()
	got, err := DiffSummary(left, right)
	if err != nil {
		t.Fatal(err)
	}
	d := got[0]
	if d.Comparable || !d.SchemaChanged || d.Status != "changed" || d.Left != 1 || d.Right != 1 {
		t.Fatalf("t = %+v, want incomparable schema-changed with 1/1 rows", d)
	}
}

// TestDiffSummaryIdenticalSidesAreSame: same content, same schema → same.
func TestDiffSummaryIdenticalSidesAreSame(t *testing.T) {
	testutil.RequireSQLite3(t)
	left := filepath.Join(t.TempDir(), "left.db")
	right := filepath.Join(t.TempDir(), "right.db")
	for _, p := range []string{left, right} {
		exec.Command("sqlite3", p, "CREATE TABLE t (id INTEGER PRIMARY KEY, v); INSERT INTO t VALUES (1,'x');").Run()
	}
	got, err := DiffSummary(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Status != "same" || got[0].Added+got[0].Removed+got[0].Changed != 0 {
		t.Fatalf("t = %+v, want same", got[0])
	}
}

// TestDiffSummaryOpensBothSidesReadOnlyEvenOn0444Files: the attached right
// side must be opened read-only too (CheckoutAt/Export produce 0444 files).
func TestDiffSummaryOpensBothSidesReadOnlyEvenOn0444Files(t *testing.T) {
	testutil.RequireSQLite3(t)
	left := filepath.Join(t.TempDir(), "left.db")
	right := filepath.Join(t.TempDir(), "right.db")
	for _, p := range []string{left, right} {
		exec.Command("sqlite3", p, "CREATE TABLE t (id INTEGER PRIMARY KEY, v); INSERT INTO t VALUES (1,'x');").Run()
		if err := os.Chmod(p, 0o444); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := DiffSummary(left, right); err != nil {
		t.Fatalf("read-only sides must be diffable: %v", err)
	}
}

// TestFormatDiffSummaryRendersCountsAndTotals pins the shared renderer the
// CLI and MCP both use.
func TestFormatDiffSummaryRendersCountsAndTotals(t *testing.T) {
	rep := DiffReportOf([]TableDiff{
		{Table: "results", LeftExists: true, RightExists: true, Left: 3, Right: 3, Comparable: true, Added: 1, Removed: 1, Changed: 1, Status: "changed"},
		{Table: "scratch", LeftExists: true, Left: 2, Status: "removed"},
		{Table: "t", LeftExists: true, RightExists: true, Left: 1, Right: 1, Comparable: true, Status: "same"},
	})
	if rep.Totals != (DiffTotals{Same: 1, Changed: 1, Added: 0, Removed: 1}) {
		t.Fatalf("totals = %+v", rep.Totals)
	}
	var b bytes.Buffer
	if err := FormatDiffSummary(&b, rep, "L", "R"); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"TABLE", "ADDED", "REMOVED", "CHANGED", "STATUS", "results", "changed", "scratch", "removed", "3 tables: 1 same, 1 changed, 0 added, 1 removed"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q:\n%s", want, out)
		}
	}
}
```

Also update the existing `TestDiffSummaryReportsAddedRemovedAndChangedTables`: its `users` table has no PK and rows (1,2,3) vs (1,2,3,4,5); add assertions `users.Comparable && users.Added == 2 && users.Removed == 0 && users.Changed == 0 && users.Status == "changed"`, `gone.Status == "removed"`, `nt.Status == "added"`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/ops -run 'TestDiffSummary|TestFormatDiffSummary' -count=1`
Expected: compile failure (`Comparable`, `DiffReportOf`, `FormatDiffSummary` undefined).

- [ ] **Step 3: Implement**

In `internal/ops/diff.go`:

1. Replace the `TableDiff` struct with the Interfaces version (keep `Delta()`).
2. Add `DiffTotals`, `DiffReport`, `DiffReportOf`:

```go
// DiffTotals counts tables by Status.
type DiffTotals struct {
	Same    int `json:"same"`
	Changed int `json:"changed"`
	Added   int `json:"added"`
	Removed int `json:"removed"`
}

// DiffReport is DiffSummary's result plus its totals — the shape every
// surface (CLI, daemon, SDKs, MCP) returns.
type DiffReport struct {
	Tables []TableDiff `json:"tables"`
	Totals DiffTotals  `json:"totals"`
}

// DiffReportOf wraps a table list with its totals.
func DiffReportOf(tables []TableDiff) DiffReport {
	rep := DiffReport{Tables: tables}
	for _, d := range tables {
		switch d.Status {
		case "same":
			rep.Totals.Same++
		case "changed":
			rep.Totals.Changed++
		case "added":
			rep.Totals.Added++
		case "removed":
			rep.Totals.Removed++
		}
	}
	return rep
}
```

3. Rewrite `DiffSummary` to be content-aware. Keep `TableRowCounts` as is (still used by tests and docs). New helpers:

```go
// roDSN is the read-only, immutable URI both sides are opened with — see
// TableRowCounts's doc comment for why mode=ro and immutable=1 are real
// SQLite-enforced guarantees, not conventions.
func roDSN(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return "file:" + abs + "?mode=ro&immutable=1", nil
}

// tableInfo is one side's view of a table: its column names in order, the
// declared primary-key columns in pk order (empty for a rowid table), and
// its CREATE statement with whitespace collapsed (for SchemaChanged).
type tableInfo struct {
	cols   []string
	pk     []string
	schema string
}

func readTableInfo(db *sql.DB, schemaName, table string) (tableInfo, error) {
	var ti tableInfo
	rows, err := db.Query("PRAGMA " + quoteIdent(schemaName) + ".table_info(" + quoteIdent(table) + ")")
	if err != nil {
		return ti, err
	}
	defer rows.Close()
	type pkcol struct{ name string; pos int }
	var pks []pkcol
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return ti, err
		}
		ti.cols = append(ti.cols, name)
		if pk > 0 {
			pks = append(pks, pkcol{name, pk})
		}
	}
	if err := rows.Err(); err != nil {
		return ti, err
	}
	sort.Slice(pks, func(i, j int) bool { return pks[i].pos < pks[j].pos })
	for _, p := range pks {
		ti.pk = append(ti.pk, p.name)
	}
	var sqlText sql.NullString
	err = db.QueryRow("SELECT sql FROM "+quoteIdent(schemaName)+".sqlite_master WHERE type='table' AND name = ?", table).Scan(&sqlText)
	if err != nil {
		return ti, err
	}
	ti.schema = strings.Join(strings.Fields(sqlText.String), " ")
	return ti, nil
}

func listTables(db *sql.DB, schemaName string) ([]string, error) {
	rows, err := db.Query(`SELECT name FROM ` + quoteIdent(schemaName) + `.sqlite_master ` +
		`WHERE type = 'table' AND name NOT LIKE 'sqlite\_%' ESCAPE '\' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func countQ(db *sql.DB, q string) (int, error) {
	var n int
	err := db.QueryRow(q).Scan(&n)
	return n, err
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
```

`DiffSummary` body:

```go
func DiffSummary(leftPath, rightPath string) ([]TableDiff, error) {
	ldsn, err := roDSN(leftPath)
	if err != nil {
		return nil, fmt.Errorf("ops: diff summary: left: %w", err)
	}
	rdsn, err := roDSN(rightPath)
	if err != nil {
		return nil, fmt.Errorf("ops: diff summary: right: %w", err)
	}
	db, err := sql.Open("sqlite3", ldsn)
	if err != nil {
		return nil, fmt.Errorf("ops: diff summary: open left: %w", err)
	}
	defer db.Close()
	// One connection: ATTACH is per-connection, and database/sql would
	// otherwise hand later queries a connection without the attachment.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("ATTACH DATABASE ? AS r", rdsn); err != nil {
		return nil, fmt.Errorf("ops: diff summary: attach right: %w", err)
	}

	lt, err := listTables(db, "main")
	if err != nil {
		return nil, fmt.Errorf("ops: diff summary: left tables: %w", err)
	}
	rt, err := listTables(db, "r")
	if err != nil {
		return nil, fmt.Errorf("ops: diff summary: right tables: %w", err)
	}
	names := map[string]struct{}{}
	inL, inR := map[string]bool{}, map[string]bool{}
	for _, t := range lt {
		names[t] = struct{}{}
		inL[t] = true
	}
	for _, t := range rt {
		names[t] = struct{}{}
		inR[t] = true
	}
	sorted := make([]string, 0, len(names))
	for t := range names {
		sorted = append(sorted, t)
	}
	sort.Strings(sorted)

	out := make([]TableDiff, 0, len(sorted))
	for _, t := range sorted {
		d := TableDiff{Table: t, LeftExists: inL[t], RightExists: inR[t]}
		qt := quoteIdent(t)
		if d.LeftExists {
			if d.Left, err = countQ(db, "SELECT count(*) FROM main."+qt); err != nil {
				return nil, fmt.Errorf("ops: diff summary: counting left %s: %w", t, err)
			}
		}
		if d.RightExists {
			if d.Right, err = countQ(db, "SELECT count(*) FROM r."+qt); err != nil {
				return nil, fmt.Errorf("ops: diff summary: counting right %s: %w", t, err)
			}
		}
		switch {
		case !d.LeftExists:
			d.Status = "added"
		case !d.RightExists:
			d.Status = "removed"
		default:
			li, err := readTableInfo(db, "main", t)
			if err != nil {
				return nil, fmt.Errorf("ops: diff summary: schema of left %s: %w", t, err)
			}
			ri, err := readTableInfo(db, "r", t)
			if err != nil {
				return nil, fmt.Errorf("ops: diff summary: schema of right %s: %w", t, err)
			}
			d.SchemaChanged = li.schema != ri.schema
			if !equalStrings(li.cols, ri.cols) {
				d.Status = "changed" // columns differ: rows are not comparable
				break
			}
			d.Comparable = true
			// Key: declared PK columns, else rowid. Row identity for
			// EXCEPT includes the key so a row moving to a new key counts
			// as removed+added, consistent with the anti-joins.
			key := li.pk
			selectList := "*"
			if len(key) == 0 {
				key = []string{"rowid"}
				selectList = "rowid, *"
			}
			var conds []string
			for _, k := range key {
				conds = append(conds, "x."+quoteIdent(k)+" IS l."+quoteIdent(k))
			}
			where := strings.Join(conds, " AND ")
			if d.Removed, err = countQ(db, "SELECT count(*) FROM main."+qt+" AS l WHERE NOT EXISTS (SELECT 1 FROM r."+qt+" AS x WHERE "+where+")"); err != nil {
				return nil, fmt.Errorf("ops: diff summary: removed rows in %s: %w", t, err)
			}
			if d.Added, err = countQ(db, "SELECT count(*) FROM r."+qt+" AS l WHERE NOT EXISTS (SELECT 1 FROM main."+qt+" AS x WHERE "+where+")"); err != nil {
				return nil, fmt.Errorf("ops: diff summary: added rows in %s: %w", t, err)
			}
			lNotR, err := countQ(db, "SELECT count(*) FROM (SELECT "+selectList+" FROM main."+qt+" EXCEPT SELECT "+selectList+" FROM r."+qt+")")
			if err != nil {
				return nil, fmt.Errorf("ops: diff summary: changed rows in %s: %w", t, err)
			}
			d.Changed = lNotR - d.Removed
			if d.Changed < 0 {
				d.Changed = 0
			}
			if d.Added+d.Removed+d.Changed > 0 || d.SchemaChanged {
				d.Status = "changed"
			} else {
				d.Status = "same"
			}
		}
		out = append(out, d)
	}
	return out, nil
}
```

Note on `rowid, *` with `EXCEPT`: both sides have identical column lists, so the projected column counts match. For `WITHOUT ROWID` tables the PK is always declared, so the `rowid` path is never taken for them.

4. Add `FormatDiffSummary` (moved from the CLI's `printDiffSummary`, extended):

```go
// FormatDiffSummary renders a DiffReport as an aligned table plus the
// totals line. The two count columns are headered with the caller's own
// labels (the raw target strings) so the table is self-describing away
// from the "left: ... right: ..." line the CLI prints above it. Shared by
// the CLI and offshoot_diff.
func FormatDiffSummary(w io.Writer, rep DiffReport, leftLabel, rightLabel string) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "TABLE\t%s\t%s\tADDED\tREMOVED\tCHANGED\tSTATUS\n", leftLabel, rightLabel)
	for _, d := range rep.Tables {
		l, r := "-", "-"
		if d.LeftExists {
			l = fmt.Sprintf("%d", d.Left)
		}
		if d.RightExists {
			r = fmt.Sprintf("%d", d.Right)
		}
		a, rm, c := "-", "-", "-"
		if d.Comparable {
			a, rm, c = fmt.Sprintf("%d", d.Added), fmt.Sprintf("%d", d.Removed), fmt.Sprintf("%d", d.Changed)
		}
		status := d.Status
		if d.SchemaChanged {
			status += " (schema)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", d.Table, l, r, a, rm, c, status)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "%d tables: %d same, %d changed, %d added, %d removed\n",
		len(rep.Tables), rep.Totals.Same, rep.Totals.Changed, rep.Totals.Added, rep.Totals.Removed)
	return err
}
```

Add imports `io`, `text/tabwriter`.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/ops -run 'TestDiff|TestFormatDiff|TestTableRowCounts|TestMaterializeForDiff' -count=1`
Expected: PASS. Then `go build ./...` — the CLI still compiles (it uses `d.Delta()`, `LeftExists`, etc.; Task 2 replaces its printer).

- [ ] **Step 5: Commit**

```bash
git add internal/ops/diff.go internal/ops/diff_test.go
git commit -m "ops: content-aware diff summary (added/removed/changed rows, schema) without sqldiff

Two attempts with equal row counts but different values used to report
\"same\" — the exact answer that misleads an agent choosing which fork to
promote. DiffSummary now opens the left side read-only, ATTACHes the
right side read-only, and counts removed/added rows by key (declared PK,
else rowid) and changed rows via EXCEPT, per table, plus a SchemaChanged
flag; tables whose column lists differ are reported, not compared.
FormatDiffSummary is the one renderer the CLI and MCP share.

Co-Authored-By: Claude <model> <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B"
```

---

### Task 2: `ops.Sqldiff` (capped, per-table) and the CLI on the shared pieces

**Files:**
- Modify: `internal/ops/diff.go` (add `ErrSqldiffMissing`, `Sqldiff`, `SqldiffCapped`)
- Modify: `cmd/offshoot/diff.go` (use `ops.FormatDiffSummary`, `ops.Sqldiff`; add `--table`)
- Modify: `cmd/offshoot/main.go:654-660` (usage, `--table` flag)
- Test: `internal/ops/diff_test.go`, `cmd/offshoot/diff_test.go`

**Interfaces:**
- Produces:
  ```go
  var ErrSqldiffMissing = errors.New("sqldiff not found on PATH")
  // Sqldiff streams `sqldiff [--table T] left right` to w. Returns
  // ErrSqldiffMissing (wrapped) when the binary is absent.
  func Sqldiff(leftPath, rightPath, table string, w io.Writer) error
  // SqldiffCapped runs Sqldiff into a buffer capped at maxBytes (<=0 means
  // DefaultSqldiffMaxBytes); truncated reports whether output was cut.
  const DefaultSqldiffMaxBytes = 1 << 20
  func SqldiffCapped(leftPath, rightPath, table string, maxBytes int) (text string, truncated bool, err error)
  ```
  CLI: `offshoot diff <left> <right> [--summary] [--table T]`; `--table` applies to both modes (summary restricted to that table; sqldiff `--table`).

- [ ] **Step 1: Write the failing tests**

Append to `internal/ops/diff_test.go`:

```go
// TestSqldiffCappedTruncatesAndFlags: with sqldiff present, a cap smaller
// than the output yields exactly maxBytes bytes and truncated=true; a
// generous cap yields the whole output and truncated=false. Skips when
// sqldiff is not installed (CI installs sqlite3-tools).
func TestSqldiffCappedTruncatesAndFlags(t *testing.T) {
	testutil.RequireSQLite3(t)
	if _, err := exec.LookPath("sqldiff"); err != nil {
		t.Skip("sqldiff not on PATH")
	}
	left := filepath.Join(t.TempDir(), "left.db")
	right := filepath.Join(t.TempDir(), "right.db")
	exec.Command("sqlite3", left, "CREATE TABLE t (id INTEGER PRIMARY KEY, v);").Run()
	exec.Command("sqlite3", right, "CREATE TABLE t (id INTEGER PRIMARY KEY, v); INSERT INTO t VALUES (1,'aaaaaaaaaa'),(2,'bbbbbbbbbb'),(3,'cccccccccc');").Run()
	full, trunc, err := SqldiffCapped(left, right, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if trunc || !strings.Contains(full, "INSERT INTO t") {
		t.Fatalf("full=%q truncated=%v", full, trunc)
	}
	part, trunc, err := SqldiffCapped(left, right, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	if !trunc || len(part) != 20 || !strings.HasPrefix(full, part) {
		t.Fatalf("part=%q (len %d) truncated=%v", part, len(part), trunc)
	}
	only, _, err := SqldiffCapped(left, right, "t", 0)
	if err != nil {
		t.Fatal(err)
	}
	if only != full {
		t.Fatalf("--table t must equal the full diff for a one-table db:\n%s\n---\n%s", only, full)
	}
}

// TestSqldiffMissingIsASentinel: callers (CLI hint, daemon error) branch on
// errors.Is(err, ErrSqldiffMissing).
func TestSqldiffMissingIsASentinel(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	_, _, err := SqldiffCapped("/nonexistent/a.db", "/nonexistent/b.db", "", 0)
	if !errors.Is(err, ErrSqldiffMissing) {
		t.Fatalf("err = %v, want ErrSqldiffMissing", err)
	}
}
```

(add `errors` to the test imports.)

In `cmd/offshoot/diff_test.go`, extend `TestDiffSummaryCLIReportsExactRowCounts` to assert the new columns exist (`ADDED`, `REMOVED`, `CHANGED` in the output) and add:

```go
// TestDiffCLITableFlagRestrictsSummary: --table narrows the summary to one
// table (and is passed to sqldiff in default mode, covered by the ops test).
func TestDiffCLITableFlagRestrictsSummary(t *testing.T) {
	// Build a store with two tables on main, checkpoint v1, then diff
	// main@v1 against main@v1 with --table so only that table is listed.
	// Reuse this file's existing store/seed helpers (see the tests above
	// for newCLIWorkspace / seed patterns) and assert:
	//   out contains "only" and does not contain "other"
	//   out ends with "1 tables: 1 same, 0 changed, 0 added, 0 removed"
}
```

Fill the body using the same helpers the neighboring tests use (read `cmd/offshoot/diff_test.go:60-125` first and copy the seeding pattern; do not invent new helpers).

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/ops -run 'TestSqldiff' -count=1 && go test ./cmd/offshoot -run 'TestDiffCLITableFlag|TestDiffSummaryCLIReportsExactRowCounts' -count=1`
Expected: compile failure / assertion failure.

- [ ] **Step 3: Implement**

In `internal/ops/diff.go`:

```go
// ErrSqldiffMissing is returned (wrapped) by Sqldiff when the external
// sqldiff binary is not on PATH. sqldiff ships separately from the sqlite3
// CLI; callers turn this into a per-surface hint (the CLI names the
// package, the daemon and MCP name --summary/the summary as the
// sqldiff-free alternative).
var ErrSqldiffMissing = errors.New("sqldiff not found on PATH")

// DefaultSqldiffMaxBytes bounds a full SQL diff carried over the wire.
const DefaultSqldiffMaxBytes = 1 << 20

// Sqldiff runs `sqldiff [--table T] leftPath rightPath` and streams its
// stdout to w. stderr is captured into the returned error.
func Sqldiff(leftPath, rightPath, table string, w io.Writer) error {
	if _, err := exec.LookPath("sqldiff"); err != nil {
		return fmt.Errorf("ops: diff: %w", ErrSqldiffMissing)
	}
	args := []string{}
	if table != "" {
		args = append(args, "--table", table)
	}
	args = append(args, leftPath, rightPath)
	cmd := exec.Command("sqldiff", args...)
	var stderr bytes.Buffer
	cmd.Stdout = w
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(err, errSqldiffCapReached) {
			return nil
		}
		return fmt.Errorf("ops: diff: sqldiff: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

var errSqldiffCapReached = errors.New("sqldiff output cap reached")

// cappedWriter stops accepting bytes after max, reporting truncation.
type cappedWriter struct {
	buf       bytes.Buffer
	max       int
	truncated bool
}

func (c *cappedWriter) Write(p []byte) (int, error) {
	room := c.max - c.buf.Len()
	if room <= 0 {
		c.truncated = true
		return 0, errSqldiffCapReached
	}
	if len(p) > room {
		c.buf.Write(p[:room])
		c.truncated = true
		return len(p), errSqldiffCapReached
	}
	return c.buf.Write(p)
}

// SqldiffCapped is Sqldiff into a buffer of at most maxBytes (<= 0 means
// DefaultSqldiffMaxBytes). truncated reports whether output was cut; the
// returned text is always a prefix of the full diff.
func SqldiffCapped(leftPath, rightPath, table string, maxBytes int) (string, bool, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultSqldiffMaxBytes
	}
	cw := &cappedWriter{max: maxBytes}
	err := Sqldiff(leftPath, rightPath, table, cw)
	if err != nil && !cw.truncated {
		return "", false, err
	}
	return cw.buf.String(), cw.truncated, nil
}
```

Note: when the cap is hit, `cmd.Run` returns an `*exec.ExitError` or a write error; sqldiff gets SIGPIPE-like EPIPE only on a real pipe, but here stdout is an `io.Writer` so `exec` copies through a goroutine and the write error surfaces as `cmd.Run`'s error. Handle both: treat any error as success when `cw.truncated` is already true (that is what `SqldiffCapped` does); inside `Sqldiff` the `errors.Is(err, errSqldiffCapReached)` check covers the direct case. Add imports `bytes`, `errors`, `os/exec`.

In `cmd/offshoot/diff.go`:
- `runDiff(w, out, leftTarget, rightTarget string, summary bool, table string)`.
- Summary mode: `rows, err := ops.DiffSummary(left.Path, right.Path)`; if `table != ""`, filter `rows` to that table (error `no table %q on either side` if none); then `ops.FormatDiffSummary(out, ops.DiffReportOf(rows), leftTarget, rightTarget)`. Delete `printDiffSummary`.
- Default mode: `err := ops.Sqldiff(left.Path, right.Path, table, out)`; if `errors.Is(err, ops.ErrSqldiffMissing)` return `sqldiffNotFoundError()` (unchanged hint text, but change its last paragraph's "table-level row-count comparison" to "a content-aware per-table summary (added/removed/changed rows)").
- `main.go`: parse `--table T` with the existing `extractFlag` helper (see the `touch` case for its use), usage string `offshoot diff <db>[@branch[@checkpoint]] <db>[@branch[@checkpoint]] [--summary] [--table T]`, and the help listing line near `main.go:49-53` updated to say the summary is content-aware.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/ops ./cmd/offshoot -count=1`
Expected: PASS (the sqldiff-present tests skip if sqldiff is absent locally; say so in the report).

- [ ] **Step 5: Commit**

```bash
git add internal/ops/diff.go internal/ops/diff_test.go cmd/offshoot/diff.go cmd/offshoot/diff_test.go cmd/offshoot/main.go
git commit -m "diff: ops-owned sqldiff with a byte cap and --table; CLI on the shared renderer

Co-Authored-By: Claude <model> <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B"
```

---

### Task 3: Daemon `diff` op (HTTP-safe JSON)

**Files:**
- Modify: `internal/daemon/protocol.go` (Request fields, Response.Diff, DiffResult)
- Modify: `internal/daemon/server.go` (dispatch case + `opDiff`)
- Test: `internal/daemon/lifecycle_test.go`

**Interfaces:**
- Produces: Request fields
  ```go
  // Left/Right (diff only) are db[@branch[@checkpoint]] targets, the same
  // form export takes; Table (diff only) restricts both modes to one table;
  // Full (diff only) also runs sqldiff and returns its SQL capped at
  // MaxBytes (0 = ops.DefaultSqldiffMaxBytes, hard ceiling 8 MiB).
  Left     string `json:"left,omitempty"`
  Right    string `json:"right,omitempty"`
  Table    string `json:"table,omitempty"`
  Full     bool   `json:"full,omitempty"`
  MaxBytes int    `json:"max_bytes,omitempty"`
  ```
  Response field `Diff *DiffResult \`json:"diff,omitempty"\`` with
  ```go
  type DiffResult struct {
      Left      string          `json:"left"`
      Right     string          `json:"right"`
      Tables    []ops.TableDiff `json:"tables"`
      Totals    ops.DiffTotals  `json:"totals"`
      Full      string          `json:"full,omitempty"`
      Truncated bool            `json:"truncated,omitempty"`
  }
  ```
  Errors: bad target; missing branch/checkpoint; `Full` with sqldiff absent → `daemon: diff: sqldiff not found on the daemon host; omit full for the summary`; `MaxBytes > 8<<20` → error.

- [ ] **Step 1: Write the failing test**

Append to `internal/daemon/lifecycle_test.go`:

```go
// TestDiffOpReturnsContentAwareSummaryOverTheWire: the daemon diff op
// materializes both sides read-only, returns JSON (never a path), and its
// summary is content-aware — an UPDATE with unchanged row counts shows as
// changed=1. full=true with sqldiff present returns capped SQL; without
// sqldiff it is a clear error, not a silent empty string.
func TestDiffOpReturnsContentAwareSummaryOverTheWire(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	sqliteExec(t, open.Checkout, "CREATE TABLE results (id INTEGER PRIMARY KEY, passed INT); INSERT INTO results VALUES (1,1),(2,1),(3,1);")
	if r := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main", Name: "v1"}); !r.OK {
		t.Fatalf("flush v1 = %+v", r)
	}
	sqliteExec(t, open.Checkout, "UPDATE results SET passed=0 WHERE id=2;")
	if r := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main", Name: "v2"}); !r.OK {
		t.Fatalf("flush v2 = %+v", r)
	}
	if r := call(t, sock, Request{Op: "close", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("close = %+v", r)
	}
	_ = w

	r := call(t, sock, Request{Op: "diff", Left: "app@main@v1", Right: "app@main@v2"})
	if !r.OK || r.Diff == nil {
		t.Fatalf("diff = %+v", r)
	}
	if r.Diff.Left != "app@main@v1" || r.Diff.Right != "app@main@v2" || len(r.Diff.Tables) != 1 {
		t.Fatalf("diff result = %+v", r.Diff)
	}
	res := r.Diff.Tables[0]
	if res.Table != "results" || res.Left != 3 || res.Right != 3 || res.Changed != 1 || res.Status != "changed" {
		t.Fatalf("results = %+v, want 3/3 rows, changed=1", res)
	}
	if r.Diff.Totals.Changed != 1 || r.Diff.Full != "" {
		t.Fatalf("totals/full = %+v", r.Diff)
	}

	// table filter
	r = call(t, sock, Request{Op: "diff", Left: "app@main@v1", Right: "app@main@v2", Table: "nope"})
	if r.OK {
		t.Fatalf("unknown table must be an error, got %+v", r)
	}

	// full: depends on sqldiff availability on this host
	r = call(t, sock, Request{Op: "diff", Left: "app@main@v1", Right: "app@main@v2", Full: true, MaxBytes: 12})
	if _, err := exec.LookPath("sqldiff"); err != nil {
		if r.OK || !strings.Contains(r.Error, "sqldiff") {
			t.Fatalf("without sqldiff, full must be a clear error: %+v", r)
		}
	} else {
		if !r.OK || !r.Diff.Truncated || len(r.Diff.Full) != 12 {
			t.Fatalf("full capped at 12 bytes: %+v", r.Diff)
		}
	}

	// bad shapes
	if r := call(t, sock, Request{Op: "diff", Left: "a@b@c@d", Right: "app"}); r.OK {
		t.Fatal("malformed target must be an error")
	}
	if r := call(t, sock, Request{Op: "diff", Left: "app", Right: "app", MaxBytes: 9 << 20}); r.OK {
		t.Fatal("max_bytes above the ceiling must be an error")
	}
}
```

Add `os/exec` to imports if missing.

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/daemon -run TestDiffOp -count=1`
Expected: compile failure (`Request.Left` undefined).

- [ ] **Step 3: Implement**

`protocol.go`: add the Request fields (doc comments as in Interfaces; also extend the `Op` list comment with `"diff"`), the `DiffResult` type, and `Diff *DiffResult` on Response. Import `ops`.

`server.go`: add `case "diff": return s.opDiff(req)` and:

```go
// opDiff is the daemon's read-only branch diff: both sides materialize
// through ops.MaterializeForDiff (a checkpoint via the ro-cache, a head via
// a private fresh export) and the answer is JSON — never a filesystem path,
// which is why this op is allowed on the HTTP surface while export is not.
// The summary is content-aware and needs no sqldiff; Full additionally
// shells out to sqldiff on the daemon host, capped at MaxBytes.
func (s *Server) opDiff(req Request) Response {
	const maxBytesCeiling = 8 << 20
	if req.Left == "" || req.Right == "" {
		return errResp(fmt.Errorf("daemon: diff needs left and right targets (db[@branch[@checkpoint]])"))
	}
	if req.MaxBytes < 0 || req.MaxBytes > maxBytesCeiling {
		return errResp(fmt.Errorf("daemon: diff max_bytes must be 0..%d", maxBytesCeiling))
	}
	ldb, lbr, lcp, err := ops.ParseExportTarget(req.Left)
	if err != nil {
		return errResp(err)
	}
	rdb, rbr, rcp, err := ops.ParseExportTarget(req.Right)
	if err != nil {
		return errResp(err)
	}
	left, err := s.ws.MaterializeForDiff(ldb, lbr, lcp)
	if err != nil {
		return errResp(fmt.Errorf("daemon: diff: materializing %s: %w", req.Left, err))
	}
	defer left.Close()
	right, err := s.ws.MaterializeForDiff(rdb, rbr, rcp)
	if err != nil {
		return errResp(fmt.Errorf("daemon: diff: materializing %s: %w", req.Right, err))
	}
	defer right.Close()

	tables, err := ops.DiffSummary(left.Path, right.Path)
	if err != nil {
		return errResp(err)
	}
	if req.Table != "" {
		var only []ops.TableDiff
		for _, d := range tables {
			if d.Table == req.Table {
				only = append(only, d)
			}
		}
		if len(only) == 0 {
			return errResp(fmt.Errorf("daemon: diff: no table %q on either side", req.Table))
		}
		tables = only
	}
	rep := ops.DiffReportOf(tables)
	res := &DiffResult{Left: req.Left, Right: req.Right, Tables: rep.Tables, Totals: rep.Totals}
	if req.Full {
		text, truncated, err := ops.SqldiffCapped(left.Path, right.Path, req.Table, req.MaxBytes)
		if err != nil {
			if errors.Is(err, ops.ErrSqldiffMissing) {
				return errResp(fmt.Errorf("daemon: diff: sqldiff not found on the daemon host; omit full for the sqldiff-free summary"))
			}
			return errResp(err)
		}
		res.Full, res.Truncated = text, truncated
	}
	return Response{OK: true, Diff: res}
}
```

Ensure `errors` is imported in server.go. Do NOT add `diff` to `httpForbiddenOps`.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/daemon -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/daemon/protocol.go internal/daemon/server.go internal/daemon/lifecycle_test.go
git commit -m "daemon: diff op — content-aware summary as JSON, optional capped sqldiff

Co-Authored-By: Claude <model> <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B"
```

---

### Task 4: SDK `diff()` in Python and TypeScript

**Files:**
- Modify: `sdk/python/offshoot/client.py` (dataclasses `TableDiff`, `DiffResult`; `Client.diff`)
- Modify: `sdk/python/tests/test_client.py`
- Modify: `sdk/typescript/src/client.ts` (interfaces `TableDiff`, `DiffTotals`, `DiffResult`, `DiffOptions`; `Client.diff`)
- Modify: `sdk/typescript/test/client.test.ts`

**Interfaces:**
- Python: `def diff(self, left: str, right: str, *, table: str | None = None, full: bool = False, max_bytes: int | None = None) -> DiffResult` where
  ```python
  @dataclass
  class TableDiff:
      table: str
      left_exists: bool
      right_exists: bool
      left_rows: int
      right_rows: int
      comparable: bool
      added: int
      removed: int
      changed: int
      schema_changed: bool
      status: str

  @dataclass
  class DiffResult:
      left: str
      right: str
      tables: list[TableDiff]
      totals: dict[str, int]   # same/changed/added/removed
      full: str = ""
      truncated: bool = False
  ```
  Built from `resp["diff"]` with `.get` defaults so an older daemon field-set still decodes.
- TypeScript: `interface TableDiff { table; left_exists; right_exists; left_rows; right_rows; comparable; added; removed; changed; schema_changed; status }`, `interface DiffTotals { same; changed; added; removed }`, `interface DiffResult { left; right; tables: TableDiff[]; totals: DiffTotals; full?: string; truncated?: boolean }`, `interface DiffOptions { table?: string; full?: boolean; maxBytes?: number }`, `async diff(left: string, right: string, opts: DiffOptions = {}): Promise<DiffResult>` sending `{op:"diff", left, right, table: opts.table ?? "", full: opts.full ?? false, max_bytes: opts.maxBytes ?? 0}`.

- [ ] **Step 1: Write the failing tests**

Python (`sdk/python/tests/test_client.py`, inside `TestClient`, next to the export tests; mirror their setup):

```python
    def test_diff_is_content_aware_and_needs_no_sqldiff(self):
        with offshoot.connect(self.d.sock) as c:
            c.create("diffdb")
            s = c.open("diffdb")
            db = sqlite3.connect(s.path)
            db.execute("CREATE TABLE results (id INTEGER PRIMARY KEY, passed INT)")
            db.executemany("INSERT INTO results VALUES (?, ?)", [(1, 1), (2, 1), (3, 1)])
            db.commit()
            s.flush("v1")
            db.execute("UPDATE results SET passed=0 WHERE id=2")
            db.commit()
            s.flush("v2")
            db.close()
            s.close()

            res = c.diff("diffdb@main@v1", "diffdb@main@v2")
            self.assertEqual((res.left, res.right), ("diffdb@main@v1", "diffdb@main@v2"))
            self.assertEqual(len(res.tables), 1)
            t = res.tables[0]
            self.assertEqual((t.table, t.left_rows, t.right_rows, t.changed, t.status),
                             ("results", 3, 3, 1, "changed"))
            self.assertEqual(res.totals["changed"], 1)
            self.assertEqual(res.full, "")
            self.assertFalse(res.truncated)
            with self.assertRaises(offshoot.OffshootError):
                c.diff("diffdb@main@v1", "diffdb@main@v2", table="nope")
```

TypeScript (`sdk/typescript/test/client.test.ts`, after the export tests, same fixture pattern):

```ts
test("diff: content-aware summary over the wire, no sqldiff", async (t: TestContext) => {
  if (!canRun) {
    t.skip("go and/or sqlite3 not on PATH");
    return;
  }
  const c = await connect(fixture!.sock);
  try {
    await c.create("diff-app");
    const s = await c.open("diff-app");
    sqlite3(s.path, "CREATE TABLE results (id INTEGER PRIMARY KEY, passed INT); INSERT INTO results VALUES (1,1),(2,1),(3,1);");
    await s.flush("v1");
    sqlite3(s.path, "UPDATE results SET passed=0 WHERE id=2;");
    await s.flush("v2");
    await s.close();

    const res = await c.diff("diff-app@main@v1", "diff-app@main@v2");
    assert.equal(res.left, "diff-app@main@v1");
    assert.equal(res.tables.length, 1);
    assert.equal(res.tables[0].table, "results");
    assert.equal(res.tables[0].changed, 1);
    assert.equal(res.tables[0].status, "changed");
    assert.equal(res.totals.changed, 1);
    assert.equal(res.full ?? "", "");
    await assert.rejects(c.diff("diff-app@main@v1", "diff-app@main@v2", { table: "nope" }));
  } finally {
    await c.close();
  }
});
```

- [ ] **Step 2: Run to verify they fail**

Run: `make test-python-sdk PYTHON=/opt/homebrew/bin/python3.14` (or any 3.10+; see Makefile's `check-python-version`) and `make test-ts-sdk`.
Expected: the new tests fail (`Client` has no `diff`).

- [ ] **Step 3: Implement**

Python: add the two dataclasses beside `Branch` (docstrings: mirror `internal/daemon/protocol.go`'s `DiffResult`/`ops.TableDiff`), and:

```python
    def diff(self, left: str, right: str, *, table: str | None = None,
             full: bool = False, max_bytes: int | None = None) -> DiffResult:
        """Compare two targets (db[@branch[@checkpoint]], same form as export)
        read-only and return a content-aware per-table summary: rows added,
        removed, and changed (by primary key, or rowid when none is declared)
        plus schema changes, with no sqldiff needed. table restricts the
        answer to one table. full=True also returns sqldiff's SQL (requires
        sqldiff on the daemon host), capped at max_bytes (daemon default
        1 MiB) with truncated=True when cut. Never touches a live checkout
        or takes a lease; a head-side target reads the last durable state.
        """
        resp = self._call("diff", left=left, right=right, table=table or "",
                          full=full, max_bytes=max_bytes or 0)
        d = resp.get("diff") or {}
        tables = [TableDiff(
            table=t.get("table", ""), left_exists=t.get("left_exists", False),
            right_exists=t.get("right_exists", False), left_rows=t.get("left_rows", 0),
            right_rows=t.get("right_rows", 0), comparable=t.get("comparable", False),
            added=t.get("added", 0), removed=t.get("removed", 0), changed=t.get("changed", 0),
            schema_changed=t.get("schema_changed", False), status=t.get("status", ""),
        ) for t in d.get("tables", [])]
        return DiffResult(left=d.get("left", left), right=d.get("right", right), tables=tables,
                          totals=dict(d.get("totals", {})), full=d.get("full", ""),
                          truncated=bool(d.get("truncated", False)))
```

Note `_call` drops falsy fields, which is fine (`""`, `False`, `0` are the daemon defaults).

TypeScript: add the interfaces next to `ExportOptions`, and the method next to `export`, with a doc comment matching the Python docstring. Export the new types from the package index if `src/index.ts` re-exports types explicitly (check `grep -n export sdk/typescript/src/index.ts`).

- [ ] **Step 4: Run tests**

Run: `make test-python-sdk PYTHON=<3.10+ path>` and `make test-ts-sdk`.
Expected: all pass (Python suite count goes up by 1; TS by 1).

- [ ] **Step 5: Commit**

```bash
git add sdk/python/offshoot/client.py sdk/python/tests/test_client.py sdk/typescript/src/client.ts sdk/typescript/test/client.test.ts
git commit -m "sdk: diff() in Python and TypeScript

Co-Authored-By: Claude <model> <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B"
```

---

### Task 5: MCP `offshoot_diff` (ninth tool, read-only)

**Files:**
- Modify: `internal/mcp/tools.go` (Tools(), Call switch, `diffArgs`, handler)
- Modify: `internal/mcp/tools_test.go` (`toolArgStructs`, annotations want-map, `< 8` → `< 9`)
- Test: `internal/mcp/tools_test.go`

**Interfaces:**
- Produces: tool `offshoot_diff` with args
  ```go
  type diffArgs struct {
      Database string `json:"database"`
      Left     string `json:"left"`     // branch or branch@checkpoint
      Right    string `json:"right"`    // branch or branch@checkpoint
      Table    string `json:"table"`
      Full     bool   `json:"full"`
      MaxBytes int    `json:"max_bytes"`
  }
  ```
  Schema: `reqStr("database"), reqStr("left"), reqStr("right"), optStr("table"), optBool("full"), optInt("max_bytes")` — add `optInt(name) prop { jsonType: "integer" }`. Annotation `annotate("Compare two branches or checkpoints", true, false, true)`. Description: "Compare two branches (or checkpoints, `branch@checkpoint`) of one database and report, per table, rows added, removed, and changed plus schema changes — without sqldiff. Call this to decide which attempt to promote, to check what a migration changed against a checkpoint, or to compare an attempt with a golden checkpoint. `table` narrows to one table. `full` also returns the SQL statements that turn left into right (needs sqldiff on the host), capped at `max_bytes` (default 32768, at most 262144) with `truncated` set when cut; prefer the summary first and `full` with `table` for a drill-down. Read-only: never touches a live checkout, never takes a lease; a head-side branch reads its last durable (flushed/checkpointed) state."
  Result text: `left: <db@left> right: <db@right>` line, then `ops.FormatDiffSummary` output, then (if Full) a blank line, `-- sqldiff (truncated at N bytes)` or `-- sqldiff`, then the SQL. structuredContent: `{"database","left","right","tables":[...TableDiff as JSON...],"totals":{...},"full":..., "truncated":...}` (omit `full`/`truncated` keys when not requested).
  MCP defaults: `max_bytes` 0 → 32768; > 262144 → tool error. Cross-database is not offered on MCP (one `database` arg); `left`/`right` are parsed by splitting on `@` into at most two parts, each validated with `validateNames`.

- [ ] **Step 1: Write the failing test**

Append to `internal/mcp/tools_test.go`:

```go
// TestDiffToolComparesAttemptsContentAware: the ninth tool answers "what
// changed between attempt A and B" for an agent, without sqldiff, and its
// structuredContent carries the per-table counts a harness needs.
func TestDiffToolComparesAttemptsContentAware(t *testing.T) {
	ts, w := newTools(t)
	p, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	sqlite := func(path, q string) {
		t.Helper()
		if out, err := exec.Command("sqlite3", path, q).CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
	}
	sqlite(p, "CREATE TABLE results (id INTEGER PRIMARY KEY, passed INT); INSERT INTO results VALUES (1,1),(2,1),(3,1);")
	if _, err := w.Checkpoint("app", "main", "seed", nil); err != nil {
		t.Fatal(err)
	}
	for _, b := range []string{"attempt-1", "attempt-2"} {
		if _, err := w.Fork("app", "main", b, "seed", 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	p2, _ := w.Checkout("app", "attempt-2")
	sqlite(p2, "UPDATE results SET passed=0 WHERE id=2; INSERT INTO results VALUES (4,1);")
	if _, err := w.Checkpoint("app", "attempt-2", "done", nil); err != nil {
		t.Fatal(err)
	}

	r := call(t, ts, "offshoot_diff", map[string]any{"database": "app", "left": "attempt-1@fork", "right": "attempt-2@done"})
	if r.IsError {
		t.Fatalf("diff: %s", text(r))
	}
	if !strings.Contains(text(r), "left:  app@attempt-1@fork right: app@attempt-2@done") || !strings.Contains(text(r), "1 tables: 0 same, 1 changed, 0 added, 0 removed") {
		t.Fatalf("diff text:\n%s", text(r))
	}
	sc := r.StructuredContent.(map[string]any)
	tables := sc["tables"].([]map[string]any)
	if len(tables) != 1 || tables[0]["changed"] != 1 || tables[0]["added"] != 1 || tables[0]["status"] != "changed" {
		t.Fatalf("structured tables = %v", tables)
	}
	if _, has := sc["full"]; has {
		t.Fatalf("full must be omitted when not requested: %v", sc)
	}
	// head-side left (no checkpoint) is allowed: attempt-1's head equals its fork point.
	if r := call(t, ts, "offshoot_diff", map[string]any{"database": "app", "left": "attempt-1", "right": "attempt-2@done"}); r.IsError {
		t.Fatalf("head-side diff: %s", text(r))
	}
	// bad shapes are tool errors
	for _, bad := range []map[string]any{
		{"database": "app", "left": "a@b@c", "right": "main"},
		{"database": "app", "left": "../x", "right": "main"},
		{"database": "app", "left": "main", "right": "main", "max_bytes": 1 << 20},
		{"database": "app", "left": "main", "right": "main", "table": "nope"},
	} {
		if r := call(t, ts, "offshoot_diff", bad); !r.IsError {
			t.Fatalf("args %v must be a tool error", bad)
		}
	}
	// full: sqldiff-dependent
	r = call(t, ts, "offshoot_diff", map[string]any{"database": "app", "left": "attempt-1@fork", "right": "attempt-2@done", "full": true, "table": "results"})
	if _, err := exec.LookPath("sqldiff"); err != nil {
		if !r.IsError || !strings.Contains(text(r), "sqldiff") {
			t.Fatalf("without sqldiff, full must say so: %s", text(r))
		}
	} else {
		if r.IsError || !strings.Contains(text(r), "UPDATE results") {
			t.Fatalf("full diff: %s", text(r))
		}
		if _, has := r.StructuredContent.(map[string]any)["full"]; !has {
			t.Fatal("full must be present in structuredContent when requested")
		}
	}
}
```

Note: `structuredContent`'s `tables` must be built as `[]map[string]any` (not `[]ops.TableDiff`) so the in-process assertion above works; the JSON shape is identical. Add `os/exec` to the test imports if missing. Add `"offshoot_diff": diffArgs{}` to `toolArgStructs`, `"offshoot_diff": {true, false, true}` to the annotations want-map, and bump `TestToolsAdvertiseSchemas`'s `< 8` to `< 9`.

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/mcp -run 'TestDiffTool|TestToolSchemaTypes' -count=1`
Expected: compile failure (`diffArgs` undefined).

- [ ] **Step 3: Implement**

`optInt`:
```go
// optInt builds an optional integer property (e.g. `max_bytes`).
func optInt(name string) prop { return prop{name: name, jsonType: "integer"} }
```

Tool entry (after `offshoot_touch`), `Call` case, and handler:

```go
type diffArgs struct {
	Database string `json:"database"`
	Left     string `json:"left"`
	Right    string `json:"right"`
	Table    string `json:"table"`
	Full     bool   `json:"full"`
	MaxBytes int    `json:"max_bytes"`
}

const (
	diffDefaultMaxBytes = 32 << 10
	diffMaxBytesCeiling = 256 << 10
)

// splitSide parses "branch" or "branch@checkpoint" (never a database: MCP
// diff is scoped to one database) and validates both names.
func splitSide(arg, side string) (branch, checkpoint string, r ToolResult, bad bool) {
	parts := strings.Split(arg, "@")
	switch len(parts) {
	case 1:
		branch = parts[0]
	case 2:
		branch, checkpoint = parts[0], parts[1]
	default:
		return "", "", ErrorResult("%s must be branch or branch@checkpoint, got %q", side, arg), true
	}
	if branch == "" {
		return "", "", ErrorResult("%s needs a branch name", side), true
	}
	named := []namedArg{namedArg(side+" branch", branch)}
	if checkpoint != "" {
		named = append(named, namedArg(side+" checkpoint", checkpoint))
	}
	if r, bad := validateNames(named...); bad {
		return "", "", r, true
	}
	return branch, checkpoint, ToolResult{}, false
}

// diff is read-only: both sides materialize through MaterializeForDiff (the
// ro-cache for a checkpoint, a private fresh export for a head) and are
// closed before returning. Summary needs no sqldiff; full shells out to it
// with a byte cap sized for a model's context, not a file.
func (t *OffshootTools) diff(args json.RawMessage) (ToolResult, error) {
	var a diffArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return ErrorResult("invalid arguments: %v", err), nil
	}
	if a.Database == "" || a.Left == "" || a.Right == "" {
		return ErrorResult("database, left, and right are required"), nil
	}
	if r, bad := validateNames(namedArg("database", a.Database)); bad {
		return r, nil
	}
	lbr, lcp, r, bad := splitSide(a.Left, "left")
	if bad {
		return r, nil
	}
	rbr, rcp, r, bad := splitSide(a.Right, "right")
	if bad {
		return r, nil
	}
	if a.MaxBytes < 0 || a.MaxBytes > diffMaxBytesCeiling {
		return ErrorResult("max_bytes must be 0..%d", diffMaxBytesCeiling), nil
	}
	maxBytes := a.MaxBytes
	if maxBytes == 0 {
		maxBytes = diffDefaultMaxBytes
	}
	left, err := t.ws.MaterializeForDiff(a.Database, lbr, lcp)
	if err != nil {
		return ErrorResult("left: %v", err), nil
	}
	defer left.Close()
	right, err := t.ws.MaterializeForDiff(a.Database, rbr, rcp)
	if err != nil {
		return ErrorResult("right: %v", err), nil
	}
	defer right.Close()

	tables, err := ops.DiffSummary(left.Path, right.Path)
	if err != nil {
		return ErrorResult("%v", err), nil
	}
	if a.Table != "" {
		var only []ops.TableDiff
		for _, d := range tables {
			if d.Table == a.Table {
				only = append(only, d)
			}
		}
		if len(only) == 0 {
			return ErrorResult("no table %q on either side", a.Table), nil
		}
		tables = only
	}
	rep := ops.DiffReportOf(tables)
	leftLabel := a.Database + "@" + a.Left
	rightLabel := a.Database + "@" + a.Right
	var b strings.Builder
	fmt.Fprintf(&b, "left:  %s right: %s\n", leftLabel, rightLabel)
	if err := ops.FormatDiffSummary(&b, rep, leftLabel, rightLabel); err != nil {
		return ErrorResult("%v", err), nil
	}
	rows := make([]map[string]any, 0, len(rep.Tables))
	for _, d := range rep.Tables {
		rows = append(rows, map[string]any{
			"table": d.Table, "left_exists": d.LeftExists, "right_exists": d.RightExists,
			"left_rows": d.Left, "right_rows": d.Right, "comparable": d.Comparable,
			"added": d.Added, "removed": d.Removed, "changed": d.Changed,
			"schema_changed": d.SchemaChanged, "status": d.Status,
		})
	}
	sc := map[string]any{
		"database": a.Database, "left": a.Left, "right": a.Right, "tables": rows,
		"totals": map[string]any{"same": rep.Totals.Same, "changed": rep.Totals.Changed, "added": rep.Totals.Added, "removed": rep.Totals.Removed},
	}
	if a.Full {
		text, truncated, err := ops.SqldiffCapped(left.Path, right.Path, a.Table, maxBytes)
		if err != nil {
			if errors.Is(err, ops.ErrSqldiffMissing) {
				return ErrorResult("full diff needs the sqldiff binary on this host (it is not installed); the summary above needs nothing — call again without full"), nil
			}
			return ErrorResult("%v", err), nil
		}
		if truncated {
			fmt.Fprintf(&b, "\n-- sqldiff (truncated at %d bytes; narrow with table or raise max_bytes)\n", maxBytes)
		} else {
			b.WriteString("\n-- sqldiff\n")
		}
		b.WriteString(text)
		sc["full"], sc["truncated"] = text, truncated
	}
	return StructuredResult(sc, "%s", b.String()), nil
}
```

Add imports `errors`, `strings` if missing. Update the `Tools()` doc comment to "nine lifecycle tools".

- [ ] **Step 4: Run tests**

Run: `go test ./internal/mcp -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/mcp/tools.go internal/mcp/tools_test.go
git commit -m "mcp: add offshoot_diff — compare attempts per table, read-only, no sqldiff needed

Co-Authored-By: Claude <model> <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B"
```

---

### Task 6: Docs re-captured from real runs

**Files:**
- Modify: `docs/diff.md`, `docs/reference.md` (CLI entry ~191-230; daemon op section; parity table row for diff), `docs/eval-harness.md` (the `--summary` "misses the regression" passage ~396-440), `docs/agents.md` (nine tools; table row), `plugin/skills/offshoot/SKILL.md` (a "compare before you promote" step), `docs/status.md` (diff row), `docs/demo/mcp-walkthrough.md` (re-capture `tools/list` only), `README.md` ("eight tools" → nine; verb table `diff` cell), `CHANGELOG.md` (Unreleased)
- Also: `sdk/python/README.md` and `sdk/typescript/README.md` if they list client methods (grep `export(`; add `diff()` beside it).

- [ ] **Step 1: Re-capture every sample output**

Build the binary once (`go build -o /tmp/offshoot-doc ./cmd/offshoot`) and reproduce, in a temp store, the exact scenarios each doc describes, pasting the real output:
1. `docs/diff.md`'s `--summary` sample (evals attempt-1/attempt-2 with results 40→46 and scratch dropped): re-seed per the doc's own description and capture the new seven-column table.
2. `docs/eval-harness.md`'s passed-vs-failed sample: the summary now reports `results` as `changed` with `CHANGED 1` — rewrite the passage: the summary catches the flipped row; the default `sqldiff` mode still tells you which row. Keep the `scratch` table example.
3. `docs/demo/mcp-walkthrough.md`: re-capture the `tools/list` block with the brief-standard command (initialize + tools/list over stdio) so `offshoot_diff` and its annotations appear; keep every other block untouched (their responses did not change).

- [ ] **Step 2: Write the reference and other docs**

- `docs/reference.md` CLI entry: new usage with `--table`; "content-aware summary: rows added/removed/changed by primary key (rowid when none), schema changes flagged; no sqldiff"; the `sqldiff` paragraph unchanged. Daemon ops: add a `diff` entry (`left`, `right`, `table`, `full`, `max_bytes` request fields; `diff` response object; allowed over HTTP because it returns JSON, not a path). Parity table: `diff` row becomes yes / yes / yes with the MCP note "one database; `left`/`right` are `branch[@checkpoint]`".
- `docs/agents.md`: nine tools; table row `| \`offshoot_diff\` | \`database\`, \`left\`, \`right\`, \`table?\`, \`full?\`, \`max_bytes?\` | Per-table rows added/removed/changed (and schema changes) between two branches or checkpoints — decide which attempt to promote; \`full\` adds capped sqldiff SQL |`; annotations paragraph: diff is read-only.
- `plugin/skills/offshoot/SKILL.md`: add step "Compare before you promote: `offshoot_diff {database, left: 'attempt-2@done', right: 'main'}` (or against a golden checkpoint) — read the per-table added/removed/changed counts; use `table` + `full` to see the exact rows" and mention nine tools.
- `docs/status.md`: update the diff row: content-aware, daemon op + SDK `diff()` + MCP `offshoot_diff`, tests `TestDiffSummaryIsContentAware`, `TestDiffOpReturnsContentAwareSummaryOverTheWire`, `TestDiffToolComparesAttemptsContentAware`, both SDK suites; version "v0.2.11 (unreleased)"; remove the "CLI-only, no daemon op, no SDK parity" wording wherever it appears (grep `CLI-only` in docs and ROADMAP's Milestone 3 diff bullet — update that bullet's parenthetical too).
- `README.md`: nine tools; verb table `diff` cell mentions "content-aware summary, or sqldiff".
- `CHANGELOG.md` Unreleased: Added bullets for the content-aware summary, `--table`, daemon `diff` op, SDK `diff()`, MCP `offshoot_diff`; Changed: `--summary` output gained ADDED/REMOVED/CHANGED columns.

- [ ] **Step 3: Verify**

Run: `go test ./internal/mcp ./internal/ops ./cmd/offshoot -count=1 && make check-plugin && grep -rn -i 'eight tools\|eight lifecycle\|the eight\|CLI-only per\|row-counts-only\|row-count-only' README.md docs internal/mcp plugin | grep -v superpowers`
Expected: tests pass; grep prints nothing (fix any hit).

- [ ] **Step 4: Commit**

```bash
git add README.md docs plugin CHANGELOG.md ROADMAP.md sdk/python/README.md sdk/typescript/README.md
git commit -m "docs: diff everywhere — content-aware summary, daemon/SDK/MCP reach, samples re-captured

Co-Authored-By: Claude <model> <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B"
```

---

## Self-review notes

- Spec coverage: content-aware summary (T1), no-sqldiff path everywhere (T1, T3, T5), per-table drill-down and capped full SQL (T2, T3, T5), daemon op HTTP-safe (T3), SDK parity (T4), MCP tool with read-only annotation and structuredContent (T5), docs re-captured (T6). Not done, by decision: the LTX page-number table mapping (replaced by SQL counts) and cross-database diff over MCP.
- Type consistency: `ops.TableDiff` JSON tags match the SDK field names and the MCP `rows` map keys; `DiffTotals` keys `same/changed/added/removed` everywhere; `SqldiffCapped(left, right, table, maxBytes) (string, bool, error)` used identically by daemon and MCP.
- Known risk for the implementer: `ATTACH DATABASE ? AS r` with a `file:` URI requires the connection to have been opened with URI handling; the driver's `_sqlite3_open_v2` adds `SQLITE_OPEN_URI` (verified in the module source), and `TestDiffSummaryOpensBothSidesReadOnlyEvenOn0444Files` proves it. If it fails, fall back to `ATTACH ? AS r` with the plain absolute path plus `PRAGMA r.query_only = 1` and say so in the report.
