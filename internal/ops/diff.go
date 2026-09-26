package ops

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
)

// DiffSide is one materialized, read-only side of a branch diff: a plain
// SQLite file path, plus whatever cleanup owning that materialization
// requires. Close is always safe to call, including on a zero DiffSide
// (e.g. after a failed MaterializeForDiff) and more than once.
type DiffSide struct {
	Path string

	cleanup func() error
}

// Close releases anything MaterializeForDiff allocated exclusively for this
// call. For a checkpoint-named side (backed by the checkouts-ro cache via
// CheckoutAt) this is a deliberate no-op: that cache is meant to persist and
// be reused by a later diff (or checkout --at) for the same
// db@branch@checkpoint, exactly like every other CheckoutAt caller gets for
// free. Only a head side's private temp file/directory (see
// MaterializeForDiff's doc comment) is ever actually removed here.
func (s DiffSide) Close() error {
	if s.cleanup == nil {
		return nil
	}
	return s.cleanup()
}

// MaterializeForDiff read-only-materializes db@branch[@checkpoint] for
// Diff/DiffSummary (Milestone 3 Task 6), picking between the two
// materialization primitives Task 2 already built based on one question:
// does this side name a specific checkpoint, or does it want the branch's
// current head?
//
//   - checkpoint != "": CheckoutAt(db, branch, checkpoint, force=false) — the
//     read-only cache. A checkpoint's content is immutable once created, so
//     this is a legitimate, idempotent cache hit on repeat diffs against the
//     same checkpoint, exactly as CheckoutAt's own doc comment describes.
//   - checkpoint == "" (head): CheckoutAt has no head concept (it "requires a
//     checkpoint name" — see its doc comment) and deliberately so: a
//     head-keyed entry in checkouts-ro CANNOT be idempotently cached the way
//     a checkpoint-keyed one can, because head moves. Caching it would mean
//     either (a) re-validating against the store on every call anyway
//     (defeating the point of a cache) or (b) silently serving a stale head
//     after a write the caller expected to see reflected — exactly the
//     "no stale ro-cache served" property this task's own test plan calls
//     out. Rather than teach CheckoutAt a force-by-default-for-head special
//     case that would asymmetrically weaken its documented cache-hit
//     contract for every OTHER caller, a head side is exported fresh, every
//     time, to a private temp file this call alone owns: Export already
//     reads the branch's current durable head with zero caching of its own,
//     which is exactly the "always re-materialize, no staleness possible"
//     semantics a moving target needs. The temp file is not registered
//     anywhere else in the store or the checkouts-ro tree — it is this
//     DiffSide's alone, and Close removes it (directory and all).
//
// The temp directory is created directly (os.MkdirTemp, not under
// Workspace.Root) since nothing about it needs to live inside the store: Export
// itself is already atomic (temp-in-destination's-own-directory + rename,
// see Export's doc comment), so the directory just needs to exist and be
// removable, not share a filesystem with anything else.
func (w *Workspace) MaterializeForDiff(db, branch, checkpoint string) (DiffSide, error) {
	if checkpoint != "" {
		path, err := w.CheckoutAt(db, branch, checkpoint, false)
		if err != nil {
			return DiffSide{}, err
		}
		return DiffSide{Path: path}, nil
	}

	dir, err := os.MkdirTemp("", "offshoot-diff-")
	if err != nil {
		return DiffSide{}, fmt.Errorf("ops: diff: materializing %s@%s head: %w", db, branch, err)
	}
	path := filepath.Join(dir, "head.db")
	if err := w.Export(db, branch, "", path, false); err != nil {
		os.RemoveAll(dir)
		return DiffSide{}, err
	}
	return DiffSide{
		Path:    path,
		cleanup: func() error { return os.RemoveAll(dir) },
	}, nil
}

// quoteIdent double-quotes a SQLite identifier, doubling any embedded
// double-quote — the standard SQL escaping for a quoted identifier. Used
// only for table names read back from sqlite_master itself (never
// caller-supplied), but applied anyway: a table name is technically
// unconstrained (anything quoted at CREATE TABLE time survives), so this is
// cheap insurance against a pathological name breaking the generated
// `SELECT count(*) FROM <name>` rather than a defense against an untrusted
// caller.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// TableRowCounts opens path READ-ONLY — `file:<abs>?mode=ro&immutable=1`,
// verified against a 0444 file (exactly what CheckoutAt/Export produce): the
// mattn/go-sqlite3 driver always requests SQLITE_OPEN_READWRITE|
// SQLITE_OPEN_CREATE from SQLite itself, but a `mode=ro` URI parameter is
// STRICTER than those flags, and SQLite only rejects a mode parameter that
// is LESS restrictive than the flags argument — so `mode=ro` layers a real,
// SQLite-enforced read-only guarantee on top, and `immutable=1` additionally
// tells SQLite the file (and its absence of a `-wal`/`-journal` sibling)
// won't change out from under this connection, skipping the locking and
// change-detection machinery entirely. Neither flag is a mere convention:
// an attempted write through this connection fails at the SQLite layer, not
// just by caller discipline.
//
// Returns a row count (`SELECT count(*) FROM <table>`) for every ordinary
// table in sqlite_master (type='table', excluding SQLite's own internal
// `sqlite_%` tables — sqlite_sequence and friends aren't user schema and
// have no place in a row-count diff).
func TableRowCounts(path string) (map[string]int, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("ops: diff: %s: %w", path, err)
	}
	dsn := "file:" + abs + "?mode=ro&immutable=1"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("ops: diff: open %s: %w", path, err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT name FROM sqlite_master ` +
		`WHERE type = 'table' AND name NOT LIKE 'sqlite\_%' ESCAPE '\' ` +
		`ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("ops: diff: listing tables in %s: %w", path, err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, fmt.Errorf("ops: diff: listing tables in %s: %w", path, err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ops: diff: listing tables in %s: %w", path, err)
	}
	rows.Close()

	counts := make(map[string]int, len(tables))
	for _, t := range tables {
		var n int
		q := "SELECT count(*) FROM " + quoteIdent(t)
		if err := db.QueryRow(q).Scan(&n); err != nil {
			return nil, fmt.Errorf("ops: diff: counting rows in %s.%s: %w", path, t, err)
		}
		counts[t] = n
	}
	return counts, nil
}

// TableDiff is one row of a --summary table-level diff between two
// materialized sides: a table name (from the union of both sides'
// sqlite_master) plus each side's row count, and whether the table exists
// there at all — a table present on only one side (Left/RightExists false
// on the other) is "added" or "removed" wholesale, not a row-count delta.
//
// Comparable is true when the table exists on both sides with the same
// column names in the same order; Added/Removed/Changed are meaningful
// only then — a schema mismatch makes row-level identity undefined, so the
// table is reported (with row counts) but not compared.
type TableDiff struct {
	Table       string `json:"table"`
	LeftExists  bool   `json:"left_exists"`
	RightExists bool   `json:"right_exists"`
	Left        int    `json:"left_rows"`
	Right       int    `json:"right_rows"`

	Comparable    bool `json:"comparable"`
	Added         int  `json:"added"`
	Removed       int  `json:"removed"`
	Changed       int  `json:"changed"`
	SchemaChanged bool `json:"schema_changed"`

	// Status is "added" | "removed" | "same" | "changed".
	Status string `json:"status"`
}

// Delta is Right's row count minus Left's — only meaningful when both
// LeftExists and RightExists are true; callers comparing a table that
// exists on only one side should branch on that instead (see
// cmd/offshoot's --summary printer).
func (d TableDiff) Delta() int { return d.Right - d.Left }

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
	type pkcol struct {
		name string
		pos  int
	}
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

// DiffSummary computes the --summary table-level diff between two
// materialized SQLite files: leftPath and rightPath may be two entirely
// different databases (cross-db diff — legitimate for eval comparisons, see
// Milestone 3 Task 6) or two checkpoints/heads of the same one; DiffSummary
// itself doesn't know or care which. Returns one TableDiff per table in the
// union of both sides' schemas, sorted by table name for a stable,
// diffable-itself output.
//
// Beyond row counts, DiffSummary is content-aware: for a table present on
// both sides with matching column lists, it counts rows added/removed/
// changed by key (the table's declared primary key, else rowid) — needed
// because two attempts can have equal row counts with different values,
// and that must never report "same". A table whose column list differs
// between sides is reported (with row counts) but not compared, since row
// identity has no defined meaning across a schema change.
//
// Both sides are opened through ONE connection: the left path is opened
// read-only/immutable (see TableRowCounts's doc comment for why those URI
// flags are real, SQLite-enforced guarantees), and the right path is
// ATTACHed to that same connection under the read-only/immutable URI too —
// ATTACH DATABASE is per-connection, so database/sql's pooling is pinned to
// a single connection (SetMaxOpenConns(1)) to guarantee every query in this
// call sees the attachment.
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
