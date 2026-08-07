// Package ops_test (external, not internal ops) so these benchmarks can
// import session (which imports ops) without a compile cycle — see
// gc_chain_test.go's identical rationale.
package ops_test

import (
	"context"
	crand "crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/offshoot-db/offshoot/internal/ops"
	"github.com/offshoot-db/offshoot/internal/session"
	"github.com/offshoot-db/offshoot/internal/store"
)

// benchSize is one entry in the size table every benchmark in this file
// shares, so a before/after comparison of Fork's slow path (today:
// ops.Workspace.copySnapshotToNewLineage materializes the source checkpoint
// and re-encodes a fresh snapshot for the child lineage — O(size)) against
// Task 6's fast path (applies only to single-snapshot chains) reads off the
// SAME benchmark names before and after that change lands. name is the
// b.Run label (matches the brief's "size=64MB" form); id is a
// store.ValidateName-safe (lowercase) form used for db/branch names, since
// "64MB" is not a legal branch name ([a-z0-9-_.] only).
type benchSize struct {
	name string
	id   string
	mb   int
}

// benchSizes is the default sweep `make bench` runs. 4GB is deliberately
// separate (see benchmarkHugeSize below): timeboxed to a single manual
// measurement per the PM's amendment, not part of the routine sweep every
// `make bench` invocation pays for.
var benchSizes = []benchSize{
	{name: "64MB", id: "64mb", mb: 64},
	{name: "512MB", id: "512mb", mb: 512},
}

// benchmarkHugeSize is the 4GB case: `size=4GB` under each Benchmark*
// function, skipped under -short (the default for `make bench` — see
// Makefile) and meant to be run explicitly, once, per docs/benchmarks.md's
// "one-off 4GB measurement" section.
var benchmarkHugeSize = benchSize{name: "4GB", id: "4gb", mb: 4096}

// benchSeedChunkMB is the blob size mustSeed inserts repeatedly to reach the
// target size; mb must divide evenly by this for every entry above.
const benchSeedChunkMB = 4

// newBenchWorkspace returns a Workspace backed by a fresh local store,
// unless OFFSHOOT_S3_TEST_BUCKET is set — in which case it targets that
// bucket instead, the same env-var convention TestS3RealProvider and `make
// bench-s3` (docker MinIO) use. Every call gets its own key prefix so
// concurrent/-count>1 runs never collide.
func newBenchWorkspace(b *testing.B) *ops.Workspace {
	b.Helper()
	if bucket := os.Getenv("OFFSHOOT_S3_TEST_BUCKET"); bucket != "" {
		b.Setenv("OFFSHOOT_CHECKOUTS", b.TempDir())
		buf := make([]byte, 6)
		if _, err := crand.Read(buf); err != nil {
			b.Fatal(err)
		}
		name := strings.ReplaceAll(strings.ReplaceAll(b.Name(), "/", "-"), "=", "-")
		spec := "s3://" + bucket + "/bench-" + name + "-" + hex.EncodeToString(buf)
		w, err := ops.Init(spec)
		if err != nil {
			b.Fatal(err)
		}
		return w
	}
	w, err := ops.Init(filepath.Join(b.TempDir(), "store"))
	if err != nil {
		b.Fatal(err)
	}
	return w
}

// mustSeed creates db@main, bulk-inserts mb megabytes of blob content in a
// single transaction, then checkpoints once — leaving a clean, current,
// on-disk checkout (sidecar fingerprint written) of exactly the shape a real
// agent's database would be in when Fork/Checkout/session.Open are called
// against it. Runs once per size, outside any benchmark's timed b.N loop.
func mustSeed(b *testing.B, w *ops.Workspace, db string, mb int) {
	b.Helper()
	if err := w.Create(db); err != nil {
		b.Fatal(err)
	}
	path, err := w.Checkout(db, "main")
	if err != nil {
		b.Fatal(err)
	}
	sdb, err := sql.Open("sqlite3", path)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := sdb.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF;"); err != nil {
		b.Fatal(err)
	}
	if _, err := sdb.Exec("CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)"); err != nil {
		b.Fatal(err)
	}
	// One chunk of pseudo-random bytes, reused for every row: content need
	// not be unique (nothing here is compressed — ltxio.EncodeSnapshot walks
	// raw SQLite pages), and generating megabytes of fresh randomness per
	// row would dominate seed time at the 4GB size for no benchmark value.
	chunk := make([]byte, benchSeedChunkMB<<20)
	mrand.New(mrand.NewSource(1)).Read(chunk) //nolint:staticcheck // deterministic seed content, not security-sensitive
	tx, err := sdb.Begin()
	if err != nil {
		b.Fatal(err)
	}
	stmt, err := tx.Prepare("INSERT INTO blobs (data) VALUES (?)")
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < mb/benchSeedChunkMB; n++ {
		if _, err := stmt.Exec(chunk); err != nil {
			b.Fatal(err)
		}
	}
	if err := stmt.Close(); err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	if err := sdb.Close(); err != nil {
		b.Fatal(err)
	}
	if _, err := w.Checkpoint(db, "main", "seed", nil); err != nil {
		b.Fatal(err)
	}
}

// checkoutSize is the on-disk byte size mustSeed produced — used for
// b.SetBytes so ns/op and MB/s both land in the reported line, per the
// brief. Measured from the actual file rather than assumed from mb: SQLite
// page/header overhead makes them close but not identical.
func checkoutSize(b *testing.B, w *ops.Workspace, db, branch string) int64 {
	b.Helper()
	fi, err := os.Stat(w.CheckoutPath(db, branch))
	if err != nil {
		b.Fatal(err)
	}
	return fi.Size()
}

// runForkAtHead is BenchmarkForkAtHead's body for one size: seed once
// (untimed), then fork b.N times against unique branch names, destroying
// each fork (StopTimer'd) before the next iteration so peak extra storage
// stays ~1x the seeded size instead of growing with b.N.
func runForkAtHead(b *testing.B, sz benchSize) {
	w := newBenchWorkspace(b)
	const db = "app"
	mustSeed(b, w, db, sz.mb)
	b.SetBytes(checkoutSize(b, w, db, "main"))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		branch := fmt.Sprintf("fork-%s-%d", sz.id, i)
		if _, err := w.Fork(db, "main", branch, "", 0, nil); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if err := w.Destroy(db, branch, true); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}

// BenchmarkForkAtHead measures ops.Workspace.Fork's current slow path:
// copySnapshotToNewLineage materializes the source checkpoint and re-encodes
// a fresh snapshot into the child's own lineage — a full copy, O(size). This
// is the baseline Task 6's fast path (single-snapshot chains only) will be
// compared against under the SAME subtest names.
func BenchmarkForkAtHead(b *testing.B) {
	for _, sz := range benchSizes {
		sz := sz
		b.Run("size="+sz.name, func(b *testing.B) { runForkAtHead(b, sz) })
	}
	b.Run("size="+benchmarkHugeSize.name, func(b *testing.B) {
		if testing.Short() {
			b.Skip("size=4GB skipped under -short (make bench's default); see docs/benchmarks.md for the one-off measurement")
		}
		runForkAtHead(b, benchmarkHugeSize)
	})
}

// BenchmarkCheckoutCleanSkip measures Task 1's win: a Checkout call against
// an already-clean, already-current checkout returns the existing file
// without re-materializing. mustSeed's Checkpoint call already leaves the
// checkout in exactly that state, and repeated Checkout calls never dirty
// it (the clean path performs no write), so every iteration exercises the
// same fast path. Honest framing (see docs/benchmarks.md): this is cheaper
// than a rebuild, not free — checkoutState still quiesces and hashes the
// whole file every call, so it remains O(size), just a smaller constant.
func BenchmarkCheckoutCleanSkip(b *testing.B) {
	for _, sz := range benchSizes {
		sz := sz
		b.Run("size="+sz.name, func(b *testing.B) {
			w := newBenchWorkspace(b)
			const db = "app"
			mustSeed(b, w, db, sz.mb)
			b.SetBytes(checkoutSize(b, w, db, "main"))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := w.Checkout(db, "main"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSessionOpen measures session.Open's latency: AcquireLease +
// Checkout (clean-skip, since mustSeed already checkpointed) + starting the
// capture engine and waiting for its resume-or-rebase verdict to settle.
// Each iteration uses a fresh session.Dir (no reuse), so Open never takes
// the synchronous rebaseline() path — the capture engine's own startup
// rebase runs asynchronously in the background and does not gate Open's
// return (see session.Open's doc comment).
//
// It also reports, once per size (not per-op — see the comment at the
// ReportMetric call), the size of the settling-flush snapshot: the ledgered
// controller decision from Task 2 that every daemon session's first
// auto-flush tick uploads a FULL snapshot (forceSnapshot), even for a
// read-only session. That cost is measured directly here by forcing exactly
// that snapshot and reading back its stored object size, rather than only
// asserted in prose.
func BenchmarkSessionOpen(b *testing.B) {
	for _, sz := range benchSizes {
		sz := sz
		b.Run("size="+sz.name, func(b *testing.B) {
			w := newBenchWorkspace(b)
			const db = "app"
			mustSeed(b, w, db, sz.mb)
			b.SetBytes(checkoutSize(b, w, db, "main"))
			b.ReportAllocs()
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s, err := session.Open(ctx, session.Options{WS: w, DB: db, Branch: "main"})
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if err := s.Close(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			b.StopTimer()

			s, err := session.Open(ctx, session.Options{WS: w, DB: db, Branch: "main"})
			if err != nil {
				b.Fatal(err)
			}
			if _, err := s.Flush("", nil); err != nil {
				b.Fatal(err)
			}
			ref, _, err := w.Store.GetRef(db, "main")
			if err != nil {
				b.Fatal(err)
			}
			obj, _, err := w.Store.B.Get(store.SnapshotKey(ref.Lineage, ref.HeadEpoch, ref.HeadTXID))
			if err != nil {
				b.Fatal(err)
			}
			if err := s.Close(); err != nil {
				b.Fatal(err)
			}
			// Not divided by b.N: this is a single measurement of the
			// settling-flush snapshot's stored size for this db size, not a
			// per-iteration cost. Reported anyway via ReportMetric so it
			// lands in the same benchstat-parseable output line as the rest
			// of this subtest's numbers.
			b.ReportMetric(float64(len(obj)), "settleSnapshotBytes")
		})
	}
}
