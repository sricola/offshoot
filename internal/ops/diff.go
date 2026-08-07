package ops

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
type TableDiff struct {
	Table       string
	LeftExists  bool
	RightExists bool
	Left        int // meaningful only when LeftExists
	Right       int // meaningful only when RightExists
}

// Delta is Right's row count minus Left's — only meaningful when both
// LeftExists and RightExists are true; callers comparing a table that
// exists on only one side should branch on that instead (see
// cmd/offshoot's --summary printer).
func (d TableDiff) Delta() int { return d.Right - d.Left }

// DiffSummary computes the --summary table-level row-count diff between two
// materialized SQLite files: leftPath and rightPath may be two entirely
// different databases (cross-db diff — legitimate for eval comparisons, see
// Milestone 3 Task 6) or two checkpoints/heads of the same one; DiffSummary
// itself doesn't know or care which. Returns one TableDiff per table in the
// union of both sides' schemas, sorted by table name for a stable,
// diffable-itself output.
func DiffSummary(leftPath, rightPath string) ([]TableDiff, error) {
	left, err := TableRowCounts(leftPath)
	if err != nil {
		return nil, fmt.Errorf("ops: diff summary: left: %w", err)
	}
	right, err := TableRowCounts(rightPath)
	if err != nil {
		return nil, fmt.Errorf("ops: diff summary: right: %w", err)
	}

	names := make(map[string]struct{}, len(left)+len(right))
	for t := range left {
		names[t] = struct{}{}
	}
	for t := range right {
		names[t] = struct{}{}
	}
	sorted := make([]string, 0, len(names))
	for t := range names {
		sorted = append(sorted, t)
	}
	sort.Strings(sorted)

	out := make([]TableDiff, 0, len(sorted))
	for _, t := range sorted {
		l, lok := left[t]
		r, rok := right[t]
		out = append(out, TableDiff{Table: t, LeftExists: lok, RightExists: rok, Left: l, Right: r})
	}
	return out, nil
}
