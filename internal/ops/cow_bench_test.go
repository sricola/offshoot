// Copy-on-write fork benchmarks (the v0.2.x share path), companion to
// fork_bench_test.go (whose numbers describe the MATERIALIZE path — see
// docs/benchmarks.md's version note). External ops_test package for the
// same reason as the rest of this suite: the divergence benchmark needs
// session, which imports ops.
//
// Every benchmark here is named BenchmarkCoW* so `make bench-cow` can
// select exactly this file's suite (-bench 'CoW'), and so `make bench`
// (-bench . -short) still runs the cheap sizes as a regression tripwire
// while skipping the 1GB case.
//
// The added-bytes benchmarks account LOGICAL object-store bytes (the sum
// of stored object sizes, which is exactly what an S3 backend would bill),
// measured by walking the local store directory and excluding the
// checkouts/ tree (working copies are not store objects). Note that on
// APFS the local backend's materialize path clones (clonefile), so
// PHYSICAL disk usage of a materialized fork is lower than the logical
// number reported here — the logical number is the one that transfers to
// a real object store.
package ops_test

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/session"
)

// cowBenchSizes is the CoW sweep: small/medium/large. 12MB rather than a
// round 10 because mustSeed's chunking requires a multiple of
// benchSeedChunkMB (4). The 1GB entry is skipped under -short (make
// bench's default), mirroring fork_bench_test.go's 4GB convention.
var cowBenchSizes = []benchSize{
	{name: "12MB", id: "12mb", mb: 12},
	{name: "100MB", id: "100mb", mb: 100},
	{name: "1GB", id: "1gb", mb: 1024},
}

// cowRequireSQLite3 skips benchmarks that drive writes through the sqlite3
// CLI (the same tool cow_divergence_test.go's tests use) when it is not on
// PATH.
func cowRequireSQLite3(b *testing.B) {
	b.Helper()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		b.Skip("sqlite3 CLI not on PATH")
	}
}

// cowStoreUsage walks the LOCAL store directory and returns the total
// logical byte size and count of stored objects, excluding the checkouts/
// tree (working copies, not store objects) and transient .lock files
// (Local's CAS artifacts). Skips the benchmark when the workspace is not
// backed by a local directory store: byte accounting via directory walk
// only works there, and the object COUNTS are identical on S3 by
// construction (the same Put/CopyObject calls run either way).
func cowStoreUsage(b *testing.B, w *ops.Workspace) (bytes int64, objects int) {
	b.Helper()
	if strings.Contains(w.Spec, "://") {
		b.Skip("added-bytes accounting requires the local directory backend (object counts are identical on S3)")
	}
	err := filepath.WalkDir(w.Spec, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == filepath.Join(w.Spec, "checkouts") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".lock") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		bytes += info.Size()
		objects++
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	return bytes, objects
}

// mustShare asserts the fork that was just taken actually SHARED (Base
// pointer set) — guarding every CoW benchmark against silently measuring
// the materialize path instead (e.g. if a future change moves the
// fork-time floor).
func mustShare(b *testing.B, w *ops.Workspace, db, branch string) {
	b.Helper()
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		b.Fatal(err)
	}
	if ref.Base == nil {
		b.Fatalf("fork %s@%s did not share (Base nil) — this benchmark would measure the materialize path", db, branch)
	}
}

// BenchmarkCoWSharedFork measures ops.Fork's SHARE path latency vs database
// size, in both call shapes:
//
//   - at=head (at="", the default): includes Fork's pre-existing O(size)
//     uncheckpointed-changes check (warnIfUncheckpointed -> a full SHA-256
//     of the checkout file — Task 1 machinery, unchanged by CoW), so this
//     number GROWS with size even though the share itself does not.
//   - at=checkpoint (at="seed"): skips that check, isolating the share
//     path itself — two small object writes (base.json + ref), expected
//     near-constant across sizes.
//
// Both are published deliberately: at=head is what a bare `offshoot fork`
// pays today; at=checkpoint is the CoW mechanism's own cost.
func BenchmarkCoWSharedFork(b *testing.B) {
	for _, sz := range cowBenchSizes {
		b.Run("size="+sz.name, func(b *testing.B) {
			if sz.mb >= 1024 && testing.Short() {
				b.Skip("size=1GB skipped under -short (make bench's default); run `make bench-cow` for the full sweep")
			}
			w := newBenchWorkspace(b)
			const db = "app"
			mustSeed(b, w, db, sz.mb)
			dbBytes := checkoutSize(b, w, db, "main")
			for _, at := range []struct{ label, cp string }{
				{"head", ""},
				{"checkpoint", "seed"},
			} {
				b.Run("at="+at.label, func(b *testing.B) {
					b.SetBytes(dbBytes)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						branch := fmt.Sprintf("f-%s-%s-%d", sz.id, at.label, i)
						if _, err := w.Fork(db, "main", branch, at.cp, 0, nil); err != nil {
							b.Fatal(err)
						}
						b.StopTimer()
						if i == 0 {
							mustShare(b, w, db, branch)
						}
						if err := w.Destroy(db, branch, true); err != nil {
							b.Fatal(err)
						}
						b.StartTimer()
					}
				})
			}
		})
	}
}

// BenchmarkCoWSharedForkAddedBytes measures the object-store bytes a SHARED
// fork adds: b.N forks of a 100MB database, none destroyed, store usage
// diffed before/after and reported per fork. Run with -benchtime=1x, 10x,
// 100x for the N=1/10/100 table in docs/benchmarks.md. Expected: two tiny
// objects per fork (the child lineage's base.json + the branch ref),
// independent of database size.
func BenchmarkCoWSharedForkAddedBytes(b *testing.B) {
	w := newBenchWorkspace(b)
	const db = "app"
	mustSeed(b, w, db, 100)
	beforeBytes, beforeObjects := cowStoreUsage(b, w)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.Fork(db, "main", fmt.Sprintf("share-%d", i), "", 0, nil); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	mustShare(b, w, db, "share-0")
	afterBytes, afterObjects := cowStoreUsage(b, w)
	b.ReportMetric(float64(afterBytes-beforeBytes)/float64(b.N), "addedBytes/fork")
	b.ReportMetric(float64(afterObjects-beforeObjects)/float64(b.N), "addedObjects/fork")
}

// BenchmarkCoWMaterializedForkAddedBytes is BenchmarkCoWSharedForkAddedBytes's
// honest baseline: the same b.N forks of the same 100MB database, but forced
// down the MATERIALIZE path (the fork-time floor / pre-CoW behavior) via the
// test hook. Expected: ~one full database-sized snapshot object per fork.
func BenchmarkCoWMaterializedForkAddedBytes(b *testing.B) {
	w := newBenchWorkspace(b)
	const db = "app"
	mustSeed(b, w, db, 100)
	ops.SetForkMaterializeForTest(true)
	defer ops.SetForkMaterializeForTest(false)
	beforeBytes, beforeObjects := cowStoreUsage(b, w)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.Fork(db, "main", fmt.Sprintf("mat-%d", i), "", 0, nil); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	afterBytes, afterObjects := cowStoreUsage(b, w)
	b.ReportMetric(float64(afterBytes-beforeBytes)/float64(b.N), "addedBytes/fork")
	b.ReportMetric(float64(afterObjects-beforeObjects)/float64(b.N), "addedObjects/fork")
}

// BenchmarkCoWCpBaseline is the filesystem baseline both fork paths get
// compared against: a plain `cp` of the 100MB checkout file, the naive
// "copy the SQLite file" a test harness without offshoot would do. Its
// added bytes are by definition the full file size per copy.
func BenchmarkCoWCpBaseline(b *testing.B) {
	w := newBenchWorkspace(b)
	const db = "app"
	mustSeed(b, w, db, 100)
	src := w.CheckoutPath(db, "main")
	b.SetBytes(checkoutSize(b, w, db, "main"))
	dst := filepath.Join(b.TempDir(), "copy.db")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if out, err := exec.Command("cp", src, dst).CombinedOutput(); err != nil {
			b.Fatalf("cp: %v: %s", err, out)
		}
		b.StopTimer()
		if err := os.Remove(dst); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}

// BenchmarkCoWDivergenceAddedBytes measures what a shared fork's DIVERGENCE
// costs in object-store bytes: fork a 100MB database (share), then write k
// single-row transactions through a session (one Flush each, default
// snapshot cadence), and diff store usage across the whole loop. The
// checkout is pre-materialized before Open so the settling-flush
// suppression arms and the child's flushes are SEGMENTS over the shared
// seam (see cow_divergence_test.go's openAndWrite comment) — the honest
// steady-state, not a full settle snapshot on flush one.
//
//   - k=8: under the default SnapshotEvery=16 cadence, all 8 flushes stay
//     segments — expected O(changed pages) per transaction, not O(DB).
//   - k=20: crosses the cadence, so the divergence floor writes ONE full
//     self-snapshot mid-run — expected ~one database size amortized over 20
//     transactions. Published deliberately: per-transaction cost is only
//     O(changed pages) BETWEEN snapshot floors.
//
// Metrics are per-transaction bytes and the loop total; ns/op covers the
// whole fork+open+write+close loop and is not a per-write latency claim.
func BenchmarkCoWDivergenceAddedBytes(b *testing.B) {
	cowRequireSQLite3(b)
	for _, k := range []int{8, 20} {
		b.Run(fmt.Sprintf("k=%d", k), func(b *testing.B) {
			w := newBenchWorkspace(b)
			const db = "app"
			mustSeed(b, w, db, 100)
			beforeBytes, _ := cowStoreUsage(b, w)
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				branch := fmt.Sprintf("div-%d-%d", k, i)
				if _, err := w.Fork(db, "main", branch, "", 0, nil); err != nil {
					b.Fatal(err)
				}
				if _, err := w.Checkout(db, branch); err != nil {
					b.Fatal(err)
				}
				s, err := session.Open(ctx, session.Options{WS: w, DB: db, Branch: branch})
				if err != nil {
					b.Fatal(err)
				}
				for j := 0; j < k; j++ {
					stmt := fmt.Sprintf("INSERT INTO blobs (data) VALUES (x'%02x');", j)
					if out, err := exec.Command("sqlite3", s.CheckoutPath(), stmt).CombinedOutput(); err != nil {
						b.Fatalf("INSERT %d: %v: %s", j, err, out)
					}
					if _, err := s.Flush("", nil); err != nil {
						b.Fatal(err)
					}
				}
				if err := s.Close(); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if i == 0 {
					mustShare(b, w, db, branch)
				}
				b.StartTimer()
			}
			b.StopTimer()
			afterBytes, _ := cowStoreUsage(b, w)
			total := float64(afterBytes - beforeBytes)
			b.ReportMetric(total/float64(b.N), "addedBytes/child")
			b.ReportMetric(total/float64(b.N*k), "addedBytes/txn")
		})
	}
}

// BenchmarkCoWCheckoutAfterFork is the read-path sanity check for the
// bounded-replay claim: materializing a shared fork's checkout from scratch
// (resolving through its base pointer into the parent's chain) vs
// materializing the parent's own checkout from scratch (its own snapshot,
// no base hop). Each iteration deletes the checkout file and its .sum
// sidecar first (StopTimer'd) so Checkout's clean-skip fast path never
// fires and every timed call is a full materialization.
func BenchmarkCoWCheckoutAfterFork(b *testing.B) {
	w := newBenchWorkspace(b)
	const db = "app"
	mustSeed(b, w, db, 100)
	if _, err := w.Fork(db, "main", "shared", "", 0, nil); err != nil {
		b.Fatal(err)
	}
	mustShare(b, w, db, "shared")
	dbBytes := checkoutSize(b, w, db, "main")
	for _, branch := range []struct{ label, name string }{
		{"parent", "main"},
		{"shared-child", "shared"},
	} {
		b.Run("branch="+branch.label, func(b *testing.B) {
			b.SetBytes(dbBytes)
			path := w.CheckoutPath(db, branch.name)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				for _, p := range []string{path, path + ".sum", path + "-wal", path + "-shm"} {
					if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				if _, err := w.Checkout(db, branch.name); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
