package ops_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/store"
)

// epochBenchCounts are the at-rest checkpoint counts the epoch benchmarks
// seed. Seeding 1000 checkpoints takes most of a minute, and runs twice (Go
// calls a benchmark once with b.N=1 first), so only checkpoints=1 runs
// under -short (`make bench`); run the rest on their own, as
// docs/benchmarks.md does.
var epochBenchCounts = []int{1, 100, 1000}

// seedAtRestCheckpoints creates app@main in a fresh local store and takes n
// sequential at-rest checkpoints on it, each after a one-row insert, each
// under its own name ("c0".."c(n-1)"), so the ref's Checkpoints map grows
// to n entries. It returns the workspace, the store's root and an open
// connection on the checkout (one connection, none idle, so the
// checkpoints' quiesce never finds it open), which the caller closes.
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
// base, whose own checkpoints list only until its recorded chain serves
// them, from its second checkpoint after a checkout to its own snapshot)
// after n sequential at-rest checkpoints on the branch. Each at-rest
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
//	go test ./internal/ops -run '^$' -bench SegmentCheckpointAfterCheckpoints -benchtime=20x
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

// BenchmarkSharedChildCheckpointAfterParentCheckpoints measures a shared
// child's second at-rest segment checkpoint (of a one-row insert) after its
// parent took n sequential at-rest checkpoints. Each iteration forks a fresh
// child at main's head, checks it out and takes the child's first
// checkpoint, all untimed, and then times the child's second checkpoint.
// The fork shares main's lineage as its base, so the child's chain spans
// two lineages: main's (n+1 epoch directories on a local store) and the
// child's own. The first checkpoint records that chain in the child
// checkout's sidecar; the second takes it from there when the recorded
// chain is accepted for a shared child lineage, and otherwise lists main's
// epoch directories and the child's, as every checkpoint of a shared child
// did before the recorded chain served one, until the child wrote its own
// snapshot. A fresh child per iteration keeps every timed checkpoint ahead
// of that snapshot: one child checkpointing over and over writes its own
// snapshot once its chain reaches the bound (Workspace.SnapshotEvery,
// else ForkShareMaxDepth; at n=1000 main's head chain is 9 deep, so at the
// child's 8th checkpoint), after which both paths read only the child's
// lineage. segments reports the fraction of timed
// checkpoints that wrote a segment. Only checkpoints=1 runs under -short.
//
//	go test ./internal/ops -run '^$' -bench SharedChildCheckpointAfterParentCheckpoints -benchtime=50x
func BenchmarkSharedChildCheckpointAfterParentCheckpoints(b *testing.B) {
	for _, n := range epochBenchCounts {
		b.Run(fmt.Sprintf("checkpoints=%d", n), func(b *testing.B) {
			w, _, mainDB := seedAtRestCheckpoints(b, n)
			if err := mainDB.Close(); err != nil {
				b.Fatal(err)
			}
			segments := 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				child := fmt.Sprintf("child%d", i)
				if _, err := w.Fork("app", "main", child, "", 0, nil); err != nil {
					b.Fatal(err)
				}
				path, err := w.Checkout("app", child)
				if err != nil {
					b.Fatal(err)
				}
				db, err := sql.Open("sqlite3", path)
				if err != nil {
					b.Fatal(err)
				}
				db.SetMaxOpenConns(1)
				db.SetMaxIdleConns(0)
				if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(200))"); err != nil {
					b.Fatal(err)
				}
				if _, err := w.CheckpointWith("app", child, "first", nil, ops.CheckpointOptions{}); err != nil {
					b.Fatal(err)
				}
				if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(200))"); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				res, err := w.CheckpointWith("app", child, "second", nil, ops.CheckpointOptions{})
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if res.Kind == "segment" {
					segments++
				}
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			b.StopTimer()
			b.ReportMetric(float64(segments)/float64(b.N), "segments")
		})
	}
}

// BenchmarkSegmentCheckpointRefGrowth isolates whether
// BenchmarkSegmentCheckpointAfterCheckpoints's small residual climb across
// n (25.5 -> 30.3 ms, 1 -> 1,000 checkpoints, 2026-10-06) comes from the
// ref's own Checkpoints map growing by one entry per checkpoint — read,
// json-decoded, mutated and written back twice per at-rest checkpoint (the
// lease acquire, then the head write) — or from something else n at-rest
// checkpoints also does regardless of the map's size (one more epoch
// directory each, one more 200-byte row in the checkout each).
//
// Both variants seed n at-rest checkpoints exactly as
// BenchmarkSegmentCheckpointAfterCheckpoints does (seedAtRestCheckpoints:
// distinct names, the ref grows to n entries, n epoch directories
// accumulate, the checkout grows by n rows) — identical setup. The "grown"
// variant then times one more checkpoint per b.N exactly as that
// benchmark does, letting the ref keep growing by one entry per iteration.
// The "trimmed" variant force-trims the ref's Checkpoints map to empty via
// store.Store.PutRef, directly and outside the timer, immediately before
// each timed checkpoint — CheckpointWith refuses a name already in the
// map, so emptying it first is what makes reusing one name legal — so the
// ref a trimmed checkpoint reads and writes never holds more than the one
// entry it itself adds, no matter how large n was. Trimming the map does
// not touch the lineage, the head identity or the sidecar the chain cache
// keys off, so it cannot change which path (cache or list) a checkpoint's
// own chain resolve takes; it changes only the ref's size.
//
//	go test ./internal/ops -run '^$' -bench SegmentCheckpointRefGrowth -benchtime=20x
func BenchmarkSegmentCheckpointRefGrowth(b *testing.B) {
	for _, n := range []int{1, 1000} {
		for _, trimmed := range []bool{false, true} {
			variant := "grown"
			if trimmed {
				variant = "trimmed"
			}
			b.Run(fmt.Sprintf("checkpoints=%d/%s", n, variant), func(b *testing.B) {
				w, _, db := seedAtRestCheckpoints(b, n)
				ref, _, err := w.Store.GetRef("app", "main")
				if err != nil {
					b.Fatal(err)
				}
				seededEntries := len(ref.Checkpoints)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(200))"); err != nil {
						b.Fatal(err)
					}
					cpName := fmt.Sprintf("bench%d", i)
					if trimmed {
						cpName = "bench"
						r, etag, err := w.Store.GetRef("app", "main")
						if err != nil {
							b.Fatal(err)
						}
						r.Checkpoints = map[string]store.Checkpoint{}
						if _, err := w.Store.PutRef("app", "main", r, etag); err != nil {
							b.Fatal(err)
						}
					}
					b.StartTimer()
					if _, err := w.CheckpointWith("app", "main", cpName, nil, ops.CheckpointOptions{}); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(seededEntries), "seededRefEntries")
			})
		}
	}
}
