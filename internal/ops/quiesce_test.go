package ops

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

// TestQuiesceWithPassiveReportsAReaderAsBusy: PASSIVE never sets the busy
// column — when a reader blocks the backfill it returns OK with fewer
// frames checkpointed than the log holds — so quiesceWith reads the
// counts. Without that, warnIfUncheckpointed fell through to checkoutState
// on a half-folded file and told the user to "run offshoot checkout".
func TestQuiesceWithPassiveReportsAReaderAsBusy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL; CREATE TABLE t (v); INSERT INTO t VALUES (1);"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	// A reader pinned on the empty WAL, like the capture engine's read
	// transaction; then a commit it keeps from being backfilled.
	reader, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.ExecContext(context.Background(), "BEGIN; SELECT count(*) FROM t;"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (2);"); err != nil {
		t.Fatal(err)
	}

	if err := quiesceWith(path, "PASSIVE"); !errors.Is(err, errQuiesceBusy) {
		t.Fatalf("PASSIVE against a reader holding the WAL: err = %v, want errQuiesceBusy", err)
	}
	if _, err := reader.ExecContext(context.Background(), "COMMIT"); err != nil {
		t.Fatal(err)
	}
	if err := quiesceWith(path, "PASSIVE"); err != nil {
		t.Fatalf("PASSIVE with no reader: %v", err)
	}
}
