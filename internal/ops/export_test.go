package ops

// This file exists only in test binaries (export_test.go is never compiled
// into non-test builds). It exposes the internal fork fast-path test knobs
// (forkSlowPathForTest, forkFastPathHits — see copySnapshotToNewLineage's
// doc comment in ops.go) to the EXTERNAL ops_test package. ops_test must
// stay external (package ops_test, not ops) for files that import session
// — session imports ops, so an internal ops test file importing session
// would be a compile cycle (see gc_chain_test.go's identical rationale) —
// but Task 6's "fork past a segment still takes the slow path" test needs
// exactly that (session.Options.SnapshotEvery to build a multi-member
// chain). This is the standard Go pattern for exporting unexported test
// hooks across that boundary.

// SetForkSlowPathForTest forces (true) or releases (false)
// copySnapshotToNewLineage's slow materialize-and-re-encode path,
// regardless of whether the fast-path precondition holds. Callers should
// restore it (e.g. via t.Cleanup) since it is process-global test state.
func SetForkSlowPathForTest(v bool) { forkSlowPathForTest = v }

// ForkFastPathHits returns how many times the fast object-copy fork path
// has fired since process start.
func ForkFastPathHits() int { return int(forkFastPathHits.Load()) }
