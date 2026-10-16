package ops

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// Lifecycle conformance tests (QA audit 2026-10-13, report-lifecycle.md
// items 4-8 and 10, plus the matrix gaps R13/R14/R23/R29/R31/R54). Every
// test here pins one documented sentence and was verified against the
// production mutation named in its doc comment.

// rawRef returns the ref's stored bytes, for byte-identical comparisons.
func rawRef(t *testing.T, w *Workspace, db, branch string) []byte {
	t.Helper()
	data, _, err := w.Store.B.Get(store.RefKey(db, branch))
	if err != nil {
		t.Fatalf("raw ref %s@%s: %v", db, branch, err)
	}
	return data
}

// seedMainV1V2 creates app, checks main out, and leaves it with
// checkpoints v1 and v2 (head v2, txid 2) and an un-checkpointed row, so a
// checkpoint attempted next would have something to write.
func seedMainV1V2(t *testing.T, w *Workspace) string {
	t.Helper()
	testutil.RequireSQLite3(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path := mustCheckout(t, w, "app", "main")
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "INSERT INTO t VALUES (2);")
	if _, err := w.Checkpoint("app", "main", "v2", nil); err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "INSERT INTO t VALUES (3);")
	return path
}

// TestCheckpointAcquireRefusesAClaimedBranchBeforeWriting pins
// reference.md#offshoot-checkpoint Errors: "branch mid-destroy or mid-reap"
// is refused. The acquire reads the claim and refuses before it writes
// anything: the ref is byte-identical afterwards (no lease landed, no
// epoch bump) and no object appeared under data/.
//
// Mutation verified: deleting the Deleting/Reaping checks at the top of
// refuseIfHeld (ops.go) — the store's own guard then answers with
// "store: branch is being deleted/reaped", not the documented wording.
func TestCheckpointAcquireRefusesAClaimedBranchBeforeWriting(t *testing.T) {
	for _, tc := range []struct {
		name  string
		claim func(r *store.Ref)
		want  string
	}{
		{"deleting", func(r *store.Ref) {
			r.Deleting = true
			r.DeletingAt = time.Now().UTC().Format(time.RFC3339Nano)
		}, "ops: app@main is being destroyed; cannot checkpoint it"},
		{"reaping", func(r *store.Ref) { r.Reaping = true }, "ops: app@main is being reaped (its TTL expired); cannot checkpoint it"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWS(t)
			seedMainV1V2(t, w)
			ref, etag, err := w.Store.GetRef("app", "main")
			if err != nil {
				t.Fatal(err)
			}
			tc.claim(&ref)
			if _, err := w.Store.PutRef("app", "main", ref, etag); err != nil {
				t.Fatal(err)
			}
			before := refOf(t, w, "app", "main")
			beforeRaw := rawRef(t, w, "app", "main")
			beforeData := dataKeys(t, w)

			_, err = w.CheckpointWith("app", "main", "v3", nil, CheckpointOptions{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("checkpoint under a %s claim: err = %v, want containing %q", tc.name, err, tc.want)
			}
			after := refOf(t, w, "app", "main")
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("refused checkpoint changed the ref:\n before %+v\n after  %+v", before, after)
			}
			if !bytes.Equal(beforeRaw, rawRef(t, w, "app", "main")) {
				t.Fatal("refused checkpoint rewrote the ref's bytes")
			}
			if afterData := dataKeys(t, w); !reflect.DeepEqual(beforeData, afterData) {
				t.Fatalf("refused checkpoint wrote data objects:\n before %v\n after  %v", beforeData, afterData)
			}
		})
	}
}

// TestSetProtectedRefusesAClaimedBranch pins reference.md#offshoot-protect
// Errors: "the branch is currently being reaped or destroyed ('too late to
// change its protection')". Both directions (protect and unprotect) are
// refused under either claim and the flag is left exactly as it was.
//
// Mutation verified: deleting the `if ref.Reaping || ref.Deleting` refusal
// in protect.go SetProtected.
func TestSetProtectedRefusesAClaimedBranch(t *testing.T) {
	for _, tc := range []struct {
		name  string
		claim func(r *store.Ref)
	}{
		{"reaping", func(r *store.Ref) { r.Reaping = true }},
		{"deleting", func(r *store.Ref) {
			r.Deleting = true
			r.DeletingAt = time.Now().UTC().Format(time.RFC3339Nano)
		}},
	} {
		for _, startProtected := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/protected=%v", tc.name, startProtected), func(t *testing.T) {
				w := newWS(t)
				if err := w.Create("app"); err != nil {
					t.Fatal(err)
				}
				ref, etag, err := w.Store.GetRef("app", "main")
				if err != nil {
					t.Fatal(err)
				}
				ref.Protected = startProtected
				tc.claim(&ref)
				if _, err := w.Store.PutRef("app", "main", ref, etag); err != nil {
					t.Fatal(err)
				}
				before := rawRef(t, w, "app", "main")
				_, err = w.SetProtected("app", "main", !startProtected)
				const want = "ops: app@main is being removed; too late to change its protection"
				if err == nil || err.Error() != want {
					t.Fatalf("SetProtected(%v) under a %s claim: err = %v, want %q", !startProtected, tc.name, err, want)
				}
				if got := refOf(t, w, "app", "main").Protected; got != startProtected {
					t.Fatalf("Protected flipped to %v under a %s claim", got, tc.name)
				}
				if !bytes.Equal(before, rawRef(t, w, "app", "main")) {
					t.Fatal("refused SetProtected rewrote the ref")
				}
			})
		}
	}
}

// pastStamp is a TouchedAt value no Touch could ever reproduce, planted
// before each verb so any Touch is observable as a change.
const pastStamp = "2000-01-01T00:00:00Z"

// plantTouchedAt sets db@branch's TouchedAt to pastStamp and returns the
// ref it wrote.
func plantTouchedAt(t *testing.T, w *Workspace, db, branch string) store.Ref {
	t.Helper()
	ref, etag, err := w.Store.GetRef(db, branch)
	if err != nil {
		t.Fatal(err)
	}
	ref.TouchedAt = pastStamp
	if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
		t.Fatal(err)
	}
	return ref
}

// assertTouched fails unless db@branch's TouchedAt moved off pastStamp to
// a later, well-formed stamp.
func assertTouched(t *testing.T, w *Workspace, db, branch, verb string) {
	t.Helper()
	got := refOf(t, w, db, branch).TouchedAt
	if got == pastStamp {
		t.Fatalf("%s left %s@%s's TouchedAt at the planted %q; a repoint is a durable write and must stamp the TTL clock", verb, db, branch, pastStamp)
	}
	ts, err := time.Parse(time.RFC3339Nano, got)
	if err != nil {
		t.Fatalf("%s stamped an unparseable TouchedAt %q: %v", verb, got, err)
	}
	past, _ := time.Parse(time.RFC3339Nano, pastStamp)
	if !ts.After(past) {
		t.Fatalf("%s stamped TouchedAt %q, not after %q", verb, got, pastStamp)
	}
}

// TestRepointsStampTheTTLClock pins architecture.md "TTL semantics": TTL is
// measured from the last durable write, and a repoint (rollback, promote
// onto, compact) is one — each stamps the repointed branch's TouchedAt.
//
// Mutation verified, one verb at a time: removing `next.Touch(time.Now())`
// before the ref CAS in RollbackWith, PromoteWith and CompactWith (ops.go).
func TestRepointsStampTheTTLClock(t *testing.T) {
	t.Run("rollback", func(t *testing.T) {
		w := newWS(t)
		seedMainV1V2(t, w)
		plantTouchedAt(t, w, "app", "main")
		if _, err := w.RollbackWith("app", "main", "v1", RollbackOptions{NoBackup: true}); err != nil {
			t.Fatal(err)
		}
		assertTouched(t, w, "app", "main", "rollback")
	})
	t.Run("promote", func(t *testing.T) {
		w := newWS(t)
		seedMainV1V2(t, w)
		mustFork(t, w, "app", "main", "kid", "")
		// Create protects main; unprotect it (SetProtected never touches
		// the clock) so the promote needs no force.
		if _, err := w.SetProtected("app", "main", false); err != nil {
			t.Fatal(err)
		}
		plantTouchedAt(t, w, "app", "main")
		if _, err := w.PromoteWith("app", "kid", "main", PromoteOptions{NoBackup: true}); err != nil {
			t.Fatal(err)
		}
		assertTouched(t, w, "app", "main", "promote")
	})
	t.Run("compact", func(t *testing.T) {
		w := newWS(t)
		seedMainV1V2(t, w)
		mustFork(t, w, "app", "main", "kid", "")
		kid := plantTouchedAt(t, w, "app", "kid")
		if kid.Base == nil {
			t.Fatal("fork did not share; compact would be a no-op")
		}
		if _, err := w.Compact("app", "kid"); err != nil {
			t.Fatal(err)
		}
		after := refOf(t, w, "app", "kid")
		if after.Base != nil || after.Lineage == kid.Lineage {
			t.Fatalf("compact did not repoint kid: %+v", after)
		}
		assertTouched(t, w, "app", "kid", "compact")
	})
}

// seedPages creates db, checks main out, and fills it with rows of 3000
// bytes (one leaf page each) until the checkout is exactly pages pages
// long, then checkpoints it as "seed" with --snapshot, so the chain the
// next checkpoint extends is exactly {seed} whatever the checkout's own
// shadow allowed. Returns the checkout path.
func seedPages(t *testing.T, w *Workspace, db string, pages int) string {
	t.Helper()
	testutil.RequireSQLite3(t)
	if err := w.Create(db); err != nil {
		t.Fatal(err)
	}
	path := mustCheckout(t, w, db, "main")
	mustSQL(t, path, "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);")
	// Page 1 (sqlite_master) and the table's interior root make 2; every
	// 3000-byte row fills its own leaf page.
	mustSQL(t, path, fmt.Sprintf(
		"WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < %d) "+
			"INSERT INTO t (v) SELECT randomblob(3000) FROM n;", pages-2))
	if got := pageCountOf(t, path); got != pages {
		t.Fatalf("seeded %d pages, want exactly %d", got, pages)
	}
	if res := mustCheckpointWith(t, w, db, "main", "seed", CheckpointOptions{Snapshot: true}); res.Kind != "snapshot" {
		t.Fatalf("seed: kind %q, want snapshot", res.Kind)
	}
	return path
}

func pageCountOf(t *testing.T, path string) int {
	t.Helper()
	n, err := strconv.Atoi(strings.TrimSpace(querySQL(t, path, "PRAGMA page_count;")))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestSegmentFractionCheckSkippedUnderSixtyFourPages pins
// reference.md#offshoot-checkpoint "Segment or snapshot": "fewer than half
// the pages changed (a database under 64 pages always counts as under that
// bar)". Rewriting every row of a 63-page checkout still writes a segment;
// the same rewrite on a 64-page checkout crosses the bar and writes a
// snapshot. Both checkpoints are verified against the store's bytes.
//
// Mutations verified: in planSegment (checkpoint_delta.go) changing
// `commit >= minPagesForFractionCheck` to `>` (64 pages becomes a
// segment) and dropping the size guard entirely (63 pages becomes a
// snapshot).
func TestSegmentFractionCheckSkippedUnderSixtyFourPages(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	for _, tc := range []struct {
		pages int
		want  string
	}{
		{minPagesForFractionCheck - 1, "segment"},
		{minPagesForFractionCheck, "snapshot"},
	} {
		db := fmt.Sprintf("db%d", tc.pages)
		path := seedPages(t, w, db, tc.pages)
		mustSQL(t, path, "UPDATE t SET v = randomblob(3000);")
		if got := pageCountOf(t, path); got != tc.pages {
			t.Fatalf("%s: the rewrite changed the page count to %d, want %d", db, got, tc.pages)
		}
		res := mustCheckpointWith(t, w, db, "main", "rewrite", CheckpointOptions{})
		if res.Kind != tc.want {
			t.Fatalf("%d pages, every row rewritten: kind %q, want %s", tc.pages, res.Kind, tc.want)
		}
		switch tc.want {
		case "segment":
			assertSegmentHead(t, w, db, "main", "rewrite", path, res)
			if res.Pages < tc.pages-2 {
				t.Fatalf("segment carries %d pages; the rewrite touched at least the %d leaf pages", res.Pages, tc.pages-2)
			}
		case "snapshot":
			ref := refOf(t, w, db, "main")
			if _, _, err := w.Store.B.Get(store.SnapshotKey(ref.Lineage, ref.Epoch, res.TXID)); err != nil {
				t.Fatalf("%s: snapshot object: %v", db, err)
			}
		}
		if ref := refOf(t, w, db, "main"); ref.Checkpoints["rewrite"].Kind != tc.want {
			t.Fatalf("%s: ref records kind %q, want %s", db, ref.Checkpoints["rewrite"].Kind, tc.want)
		}
	}
}

// TestRollbackUnknownCheckpointMintsNoSafetyFork pins
// reference.md#offshoot-rollback Errors: "unknown checkpoint name" fails
// before anything is minted — no `<branch>-pre-rollback` exists afterwards,
// the branch's ref is byte-identical, and the error lists the branch's own
// checkpoints.
//
// Mutation verified: moving the safety-fork block above the `to` lookup in
// RollbackWith (ops.go).
func TestRollbackUnknownCheckpointMintsNoSafetyFork(t *testing.T) {
	w := newWS(t)
	seedMainV1V2(t, w)
	before := refOf(t, w, "app", "main")
	beforeRaw := rawRef(t, w, "app", "main")
	_, err := w.RollbackWith("app", "main", "nope", RollbackOptions{})
	if err == nil || !strings.Contains(err.Error(), `no checkpoint "nope" on app@main`) || !strings.Contains(err.Error(), "its checkpoints: init, v1, v2") {
		t.Fatalf("rollback to an unknown checkpoint: %v", err)
	}
	if _, _, err := w.Store.GetRef("app", "main"+RollbackBackupSuffix); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a failed rollback minted the safety fork: err = %v, want ErrNotFound", err)
	}
	if after := refOf(t, w, "app", "main"); !reflect.DeepEqual(before, after) {
		t.Fatalf("a failed rollback changed the ref:\n before %+v\n after  %+v", before, after)
	}
	if !bytes.Equal(beforeRaw, rawRef(t, w, "app", "main")) {
		t.Fatal("a failed rollback rewrote the ref's bytes")
	}
}

// TestSafetyForkUnderASessionLeaseSaysCloseThatSession pins
// reference.md#offshoot-rollback Errors: "the previous safety fork has a
// live lease (nothing is touched — close that session first, or
// --no-backup)". With a daemon session's holder on the previous safety
// fork the refusal names closing that session and --no-backup, never
// --force (which rollback has no lever for here) nor the checkpoint
// holder's "retry when it finishes"; main and the safety fork are left
// byte-identical.
//
// Mutation verified: in safetyFork (ops.go) changing the session advice
// "close that session, or pass --no-backup" to "pass --no-backup".
func TestSafetyForkUnderASessionLeaseSaysCloseThatSession(t *testing.T) {
	w := newWS(t)
	seedMainV1V2(t, w)
	res, err := w.RollbackWith("app", "main", "v1", RollbackOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Backup != "main"+RollbackBackupSuffix {
		t.Fatalf("Backup = %q, want %q", res.Backup, "main"+RollbackBackupSuffix)
	}
	holder := NewSessionHolder()
	if !IsSessionHolder(holder) {
		t.Fatalf("NewSessionHolder() = %q is not a session holder", holder)
	}
	if _, err := w.AcquireLease("app", res.Backup, holder, time.Minute); err != nil {
		t.Fatal(err)
	}
	mainBefore := rawRef(t, w, "app", "main")
	forkBefore := rawRef(t, w, "app", res.Backup)

	_, err = w.RollbackWith("app", "main", "v1", RollbackOptions{})
	if err == nil {
		t.Fatal("rollback over a safety fork a session holds succeeded")
	}
	msg := err.Error()
	for _, want := range []string{
		"ops: rollback: replacing the previous safety fork app@main" + RollbackBackupSuffix,
		`held by "` + holder + `"`,
		"ask the human (close that session, or pass --no-backup)",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("refusal lacks %q: %v", want, err)
		}
	}
	for _, bad := range []string{"--force", "retry when it finishes"} {
		if strings.Contains(msg, bad) {
			t.Fatalf("the refusal says %q: %v", bad, err)
		}
	}
	if !bytes.Equal(mainBefore, rawRef(t, w, "app", "main")) {
		t.Fatal("the refused rollback rewrote main's ref")
	}
	if !bytes.Equal(forkBefore, rawRef(t, w, "app", res.Backup)) {
		t.Fatal("the refused rollback rewrote the safety fork's ref (its lease must survive)")
	}
}

// TestSharedForkChildCarriesOnlyTheForkCheckpoint pins architecture.md
// "Core model": "Children do not inherit parent checkpoints — a child's
// checkpoint map starts fresh at its fork point (just the auto-created
// fork checkpoint)", on the DEFAULT shared path (no materialize hook): a
// parent with init, v1, v2 forks a child whose map is exactly {fork} at
// the parent's head txid, epoch 1, stamped.
//
// Mutation verified: copying src.Checkpoints into the child before
// SetCheckpoint("fork", ...) in forkWith (ops.go).
func TestSharedForkChildCarriesOnlyTheForkCheckpoint(t *testing.T) {
	w := newWS(t)
	seedMainV1V2(t, w)
	parent := refOf(t, w, "app", "main")
	if len(parent.Checkpoints) != 3 {
		t.Fatalf("parent has %d checkpoints, want 3 (init, v1, v2): %+v", len(parent.Checkpoints), parent.Checkpoints)
	}
	txid, err := w.Fork("app", "main", "kid", "", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if txid != parent.HeadTXID {
		t.Fatalf("fork at head returned txid %d, parent head is %d", txid, parent.HeadTXID)
	}
	kid := refOf(t, w, "app", "kid")
	if kid.Base == nil {
		t.Fatalf("default fork did not share: %+v", kid)
	}
	if len(kid.Checkpoints) != 1 {
		t.Fatalf("child carries %d checkpoints, want exactly 1: %+v", len(kid.Checkpoints), kid.Checkpoints)
	}
	cp, ok := kid.Checkpoints["fork"]
	if !ok {
		t.Fatalf("child's only checkpoint is not \"fork\": %+v", kid.Checkpoints)
	}
	if cp.TXID != parent.HeadTXID || cp.Epoch != 1 || cp.CreatedAt == "" {
		t.Fatalf("fork checkpoint = %+v, want txid %d, epoch 1, stamped", cp, parent.HeadTXID)
	}
}

// TestForkAtUnknownCheckpointAndTakenName pins reference.md#offshoot-fork
// Errors: "new-branch already exists; unknown --at checkpoint name". The
// unknown-checkpoint error names the checkpoint and lists the source
// branch's own checkpoints (sorted) with the fork-from-parent hint, and
// creates no branch; a fork onto an existing name says "already exists"
// and leaves that branch's ref byte-identical.
//
// Mutation verified: errNoCheckpoint (ops.go) dropping the checkpoint
// list ("its checkpoints: ...").
func TestForkAtUnknownCheckpointAndTakenName(t *testing.T) {
	w := newWS(t)
	seedMainV1V2(t, w)

	_, err := w.Fork("app", "main", "kid", "nope", 0, nil)
	if err == nil {
		t.Fatal("fork --at an unknown checkpoint succeeded")
	}
	const want = `ops: no checkpoint "nope" on app@main (its checkpoints: init, v1, v2; checkpoints belong to one branch — to start from a parent's checkpoint, fork it: offshoot fork app@<parent> <new> --at nope)`
	if err.Error() != want {
		t.Fatalf("fork --at unknown:\n got  %q\n want %q", err.Error(), want)
	}
	if _, _, err := w.Store.GetRef("app", "kid"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a refused fork created the branch: err = %v, want ErrNotFound", err)
	}

	mustFork(t, w, "app", "main", "kid", "v1")
	before := rawRef(t, w, "app", "kid")
	_, err = w.Fork("app", "main", "kid", "", 0, nil)
	if err == nil || !strings.Contains(err.Error(), "ops: branch app@kid already exists") {
		t.Fatalf("fork onto a taken name: %v", err)
	}
	if !bytes.Equal(before, rawRef(t, w, "app", "kid")) {
		t.Fatal("a refused fork rewrote the existing branch's ref")
	}
}
