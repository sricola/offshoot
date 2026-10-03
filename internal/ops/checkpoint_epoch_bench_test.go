package ops_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sricola/offshoot/internal/ops"
)

// BenchmarkChainAfterCheckpoints measures resolving a branch's head chain
// (store.Store.Chain, which a segment checkpoint, checkout, fork and
// materialize each run, as does every descendant resolving through the
// lineage as a base) after n sequential at-rest checkpoints on the branch.
// Each at-rest checkpoint writes under the epoch its lease acquire minted,
// so on a local store it leaves one more data/<lineage>/<epoch>/ directory,
// and Local.List walks them all: the cost grows with the checkpoints a
// lineage has taken until a compact (or a rollback or promote) starts a
// fresh one. An S3 listing of the lineage is flat and does not grow with
// the epoch count. epochDirs reports the directories the lineage has.
//
// Seeding 1000 checkpoints takes most of a minute, and runs twice (Go calls
// a benchmark once with b.N=1 first), so only checkpoints=1 runs under
// -short (`make bench`); run the rest on their own, as docs/benchmarks.md
// does:
//
//	go test ./internal/ops -run '^$' -bench ChainAfterCheckpoints -benchtime=50x
func BenchmarkChainAfterCheckpoints(b *testing.B) {
	for _, n := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("checkpoints=%d", n), func(b *testing.B) {
			if testing.Short() && n > 1 {
				b.Skip("skipped under -short: seeding takes up to a minute; see docs/benchmarks.md")
			}
			root := b.TempDir()
			w, err := ops.Init(filepath.Join(root, "store"))
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
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
			ref, _, err := w.Store.GetRef("app", "main")
			if err != nil {
				b.Fatal(err)
			}
			dirs, err := os.ReadDir(filepath.Join(root, "store", "data", ref.Lineage))
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
