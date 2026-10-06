package ops_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sricola/offshoot/internal/ops"
)

// epochBenchCounts are the at-rest checkpoint counts the epoch benchmarks
// seed. Seeding 1000 checkpoints takes most of a minute, and runs twice (Go
// calls a benchmark once with b.N=1 first), so only checkpoints=1 runs
// under -short (`make bench`); run the rest on their own, as
// docs/benchmarks.md does.
var epochBenchCounts = []int{1, 100, 1000}

// seedAtRestCheckpoints creates app@main in a fresh local store and takes n
// sequential at-rest checkpoints on it, each after a one-row insert. It
// returns the workspace, the store's root and an open connection on the
// checkout (one connection, none idle, so the checkpoints' quiesce never
// finds it open), which the caller closes.
func seedAtRestCheckpoints(b *testing.B, n int) (*ops.Workspace, string, *sql.DB) {
	b.Helper()
	if testing.Short() && n > 1 {
		b.Skip("skipped under -short: seeding takes up to a minute; see docs/benchmarks.md")
	}
	root := filepath.Join(b.TempDir(), "store")
	w, err := ops.Init(root)
	if err != nil {
		b.Fatal(err)
	}
	if err := w.Create("app"); err != nil {
		b.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		b.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(200))"); err != nil {
			b.Fatal(err)
		}
		if _, err := w.CheckpointWith("app", "main", fmt.Sprintf("c%d", i), nil, ops.CheckpointOptions{}); err != nil {
			b.Fatal(err)
		}
	}
	return w, root, db
}

// BenchmarkChainAfterCheckpoints measures resolving a branch's head chain
// (store.Store.Chain, which checkout, materialize and a fork below head
// each run, as does every descendant resolving through the lineage as a
// base) after n sequential at-rest checkpoints on the branch. Each at-rest
// checkpoint writes under the epoch its lease acquire minted, so on a
// local store it leaves one more data/<lineage>/<epoch>/ directory, and
// Local.List walks them all: the cost grows with the checkpoints a lineage
// has taken until a compact (or a rollback or promote) starts a fresh one.
// An S3 listing of the lineage is flat and does not grow with the epoch
// count. epochDirs reports the directories the lineage has.
//
//	go test ./internal/ops -run '^$' -bench ChainAfterCheckpoints -benchtime=50x
func BenchmarkChainAfterCheckpoints(b *testing.B) {
	for _, n := range epochBenchCounts {
		b.Run(fmt.Sprintf("checkpoints=%d", n), func(b *testing.B) {
			w, root, db := seedAtRestCheckpoints(b, n)
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
			ref, _, err := w.Store.GetRef("app", "main")
			if err != nil {
				b.Fatal(err)
			}
			dirs, err := os.ReadDir(filepath.Join(root, "data", ref.Lineage))
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := w.Store.Chain(ref.Lineage, ref.HeadTXID); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(dirs)), "epochDirs")
		})
	}
}

// BenchmarkSegmentCheckpointAfterCheckpoints measures one more at-rest
// checkpoint of a one-row insert after n sequential at-rest checkpoints on
// the branch: the checkpoint a branch takes over and over. It reuses the
// chain the previous checkpoint recorded in the checkout's sidecar instead
// of listing the lineage, so it should not grow with the lineage's epoch
// directories the way BenchmarkChainAfterCheckpoints does. Only the
// checkpoint is timed, not the insert; segments reports the fraction of
// timed checkpoints that wrote a segment (the rest reached the snapshot
// bound, or the filesystem cannot clone a shadow).
//
//	go test ./internal/ops -run '^$' -bench SegmentCheckpointAfterCheckpoints -benchtime=50x
func BenchmarkSegmentCheckpointAfterCheckpoints(b *testing.B) {
	for _, n := range epochBenchCounts {
		b.Run(fmt.Sprintf("checkpoints=%d", n), func(b *testing.B) {
			w, _, db := seedAtRestCheckpoints(b, n)
			segments := 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(200))"); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				res, err := w.CheckpointWith("app", "main", fmt.Sprintf("bench%d", i), nil, ops.CheckpointOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if res.Kind == "segment" {
					segments++
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(segments)/float64(b.N), "segments")
		})
	}
}
