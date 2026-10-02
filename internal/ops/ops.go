// Package ops implements offshoot's branch lifecycle operations over a
// store.Backend: create, checkout, checkpoint, fork (copy-on-write shared
// by default), rollback, promote, compact, destroy, leases, TTL/reap, and
// reachability GC. It backs both the CLI's at-rest mode (checkpoints
// against fixed checkout paths: a segment diffed against a reflinked shadow
// when it can, a full snapshot otherwise) and the daemon (which layers
// live sessions and segment flushes from internal/session on top).
package ops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/store"
)

type Workspace struct {
	Store *store.Store
	Root  string
	// Spec is the store spec this Workspace was opened with, retained so
	// callers/tests can reopen the same store.
	Spec string
	// SnapshotEvery is the snapshot cadence the fork-time floor bounds a
	// shared fork's resolved base chain against; 0 means use the default
	// (ForkShareMaxDepth). Set by the daemon from its configured session
	// SnapshotEvery so the fork floor and the divergence floor agree. An
	// at-rest CheckpointWith bounds the head's chain by it too: no segment
	// checkpoint grows the chain past this many members.
	SnapshotEvery int
}

// bestEffortDelete removes key from the backend, logging to stderr (never
// failing) on error. It exists for orphan cleanup after a lost ref CAS:
// every caller deletes keys in a freshly-minted lineage no rival can
// reference (or a CAS-confirmed orphan), so a failed delete is safe to
// leave behind — reachability GC reclaims it eventually — but should be
// LOUD rather than invisible, matching the janitor's logging convention.
func (w *Workspace) bestEffortDelete(key string) {
	if err := w.Store.B.Delete(key); err != nil {
		fmt.Fprintf(os.Stderr,
			"offshoot: best-effort cleanup of %s failed (reachability GC will reclaim it): %v\n",
			key, err)
	}
}

// Metadata caps (design spec § Metadata; Milestone 3 Global Constraints):
// branch-level lineage is the grain, not row-level provenance, so the map
// stays small. Enforced here, at the ops layer, so every caller — CLI,
// daemon (fork/flush ops), MCP tools (which currently always pass nil, see
// their own doc comments) — gets the identical check and identical wording,
// rather than each wire boundary re-implementing (or forgetting) it.
const (
	MaxMetaKeys     = 32
	MaxMetaKeyLen   = 64
	MaxMetaValueLen = 512
)

// ValidateMeta enforces the metadata caps on a fork/checkpoint meta map: at
// most MaxMetaKeys entries, each key at most MaxMetaKeyLen bytes, each value
// at most MaxMetaValueLen bytes. A nil or empty map always passes — "no
// metadata" is never a cap violation. Errors name the specific limit
// violated so a caller can fix its call without guessing which cap it hit.
func ValidateMeta(meta map[string]string) error {
	if len(meta) > MaxMetaKeys {
		return fmt.Errorf("ops: metadata has %d keys, exceeds the %d-key limit", len(meta), MaxMetaKeys)
	}
	for k, v := range meta {
		if len(k) > MaxMetaKeyLen {
			return fmt.Errorf("ops: metadata key %q is %d bytes, exceeds the %d-byte limit", k, len(k), MaxMetaKeyLen)
		}
		if len(v) > MaxMetaValueLen {
			return fmt.Errorf("ops: metadata value for key %q is %d bytes, exceeds the %d-byte limit", k, len(v), MaxMetaValueLen)
		}
	}
	return nil
}

// nowStamp is the RFC3339 UTC timestamp ops stamps onto every checkpoint it
// creates (Create's "init", Checkpoint, Fork's "fork", Promote's
// "promote"). A plain function (not a field/hook) — nothing in this
// package's tests has needed to fake the clock for this value, unlike
// store.Ref.Touch's now time.Time parameter, which callers already had a
// concrete time available for.
func nowStamp() string { return time.Now().UTC().Format(time.RFC3339) }

// Init creates a new store at spec and returns a workspace for it.
func Init(spec string) (*Workspace, error) {
	b, err := store.OpenBackend(context.Background(), spec)
	if err != nil {
		return nil, err
	}
	s := &store.Store{B: b}
	if err := s.InitManifest(); err != nil {
		if errors.Is(err, store.ErrCAS) {
			return nil, fmt.Errorf("store already initialized at %s", spec)
		}
		return nil, err
	}
	root, err := checkoutRoot(spec)
	if err != nil {
		return nil, err
	}
	return &Workspace{Store: s, Root: root, Spec: spec}, nil
}

// Open attaches to an existing store at spec.
func Open(spec string) (*Workspace, error) {
	b, err := store.OpenBackend(context.Background(), spec)
	if err != nil {
		return nil, err
	}
	s := &store.Store{B: b}
	if err := s.CheckManifest(); err != nil {
		return nil, err
	}
	root, err := checkoutRoot(spec)
	if err != nil {
		return nil, err
	}
	return &Workspace{Store: s, Root: root, Spec: spec}, nil
}

// checkoutRoot decides where materialized checkouts live. For a local store
// they sit inside the store directory (unchanged from local mode). For a
// remote store they go to OFFSHOOT_CHECKOUTS, or a per-store directory under
// the user cache dir — checkouts are real SQLite files and must be local.
func checkoutRoot(spec string) (string, error) {
	if !strings.Contains(spec, "://") {
		return spec, nil
	}
	if u, err := url.Parse(spec); err == nil && u.Scheme == "file" {
		return u.Path, nil
	}
	if dir := os.Getenv("OFFSHOOT_CHECKOUTS"); dir != "" {
		return dir, nil
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("ops: no checkout directory (set OFFSHOOT_CHECKOUTS): %w", err)
	}
	// Hash the RESOLVED store identity, not the raw spec string: two
	// sessions can use the identical spec string (e.g. "s3://bucket/prefix")
	// while OFFSHOOT_S3_ENDPOINT resolves it to different backends (MinIO
	// one session, real AWS the next). Hashing the raw spec would collide
	// both onto the same local checkout cache dir, silently discarding
	// un-checkpointed edits from whichever backend wrote there last.
	id, err := store.StoreIdentity(spec)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(id))
	return filepath.Join(cache, "offshoot", hex.EncodeToString(sum[:8])), nil
}

func ParseTarget(s string) (string, string, error) {
	parts := strings.Split(s, "@")
	db, branch := parts[0], "main"
	switch len(parts) {
	case 1:
	case 2:
		branch = parts[1]
	default:
		return "", "", fmt.Errorf("ops: invalid target %q (want db or db@branch)", s)
	}
	if db == "" || branch == "" {
		return "", "", fmt.Errorf("ops: invalid target %q (want db or db@branch)", s)
	}
	if err := store.ValidateName(db); err != nil {
		return "", "", err
	}
	if err := store.ValidateName(branch); err != nil {
		return "", "", err
	}
	return db, branch, nil
}

// underRoot joins elems under w.Root and asserts the result is still inside
// it. It is the one containment check every name-derived local path goes
// through (CheckoutPath, CheckoutAtPath, byChainPath; sidecar and shadow
// paths are suffixes of those). The returned string is exactly
// filepath.Join(w.Root, elems...): the check adds no normalization a
// caller could observe.
//
// Every element reaching here is a literal, a hex digest, or a name that
// has already passed store.ValidateName ([a-z0-9-_.], never "." or "..",
// never containing ".." or a separator), so the check cannot fail for any
// input the package admits. A failure therefore means a caller skipped
// validation: a programming error that would otherwise read, write or
// rename a file outside the workspace. Returning an error would invite a
// caller to log it and carry on with a path it should never have built,
// and none of the builders has an error return to carry one, so the
// impossible case panics, loudly and at the point of the bypass, naming
// the invariant that was broken.
//
// The check is strings.HasPrefix on the filepath.Clean-ed join, the form
// static analysis (CodeQL go/path-injection) recognizes as a containment
// barrier. A root of "." (or "") has no prefix to test, since Join drops
// it, so there the equivalent check is filepath.IsLocal.
func (w *Workspace) underRoot(elems ...string) string {
	root := filepath.Clean(w.Root)
	p := filepath.Clean(filepath.Join(append([]string{root}, elems...)...))
	if root == "." {
		if filepath.IsLocal(p) {
			return p
		}
	} else {
		prefix := root
		if !strings.HasSuffix(prefix, string(filepath.Separator)) {
			prefix += string(filepath.Separator)
		}
		if strings.HasPrefix(p, prefix) {
			return p
		}
	}
	panic(fmt.Sprintf("ops: path %q escapes workspace root %q: invariant violated: every element must be a name validated by store.ValidateName", p, w.Root))
}

// CheckoutPath is db@branch's writable checkout file. db and branch must
// already have passed store.ValidateName; underRoot enforces that the
// result stays under w.Root.
func (w *Workspace) CheckoutPath(db, branch string) string {
	return w.underRoot("checkouts", db, branch+".db")
}

// snapshotTo encodes dbPath (a quiesced SQLite file) as snapshot txid into a
// fresh lineage at epoch and returns the lineage id.
func (w *Workspace) snapshotTo(dbPath string, txid uint64) (string, error) {
	lineage := store.NewLineageID()
	var buf bytes.Buffer
	if _, err := ltxio.EncodeSnapshot(dbPath, txid, &buf); err != nil {
		return "", err
	}
	// Immutable data object: create-only put under a fresh lineage/epoch.
	if _, err := w.Store.B.PutIf(store.SnapshotKey(lineage, 1, txid), buf.Bytes(), ""); err != nil {
		return "", err
	}
	return lineage, nil
}

func (w *Workspace) Create(db string) error {
	if err := store.ValidateName(db); err != nil {
		return err
	}
	// Build an empty SQLite DB in a temp dir, snapshot it as TXID 1.
	tmp := filepath.Join(os.TempDir(), "offshoot-create-"+store.NewLineageID()+".db")
	defer func() { os.Remove(tmp); os.Remove(tmp + "-wal"); os.Remove(tmp + "-shm") }()
	conn, err := sql.Open("sqlite3", tmp)
	if err != nil {
		return err
	}
	if _, err := conn.Exec("PRAGMA journal_mode=WAL; PRAGMA user_version=0; PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		conn.Close()
		return err
	}
	conn.Close()
	return w.createFromQuiesced(db, tmp)
}

func (w *Workspace) createFromQuiesced(db, quiescedPath string) error {
	lineage, err := w.snapshotTo(quiescedPath, 1)
	if err != nil {
		return err
	}
	ref := store.Ref{
		Lineage: lineage, Epoch: 1, HeadTXID: 1,
		Protected: true, // main is protected by default (spec § Security posture)
	}
	ref.SetCheckpoint("init", store.Checkpoint{TXID: 1, Epoch: 1, CreatedAt: nowStamp(), Kind: "snapshot"})
	if _, err := w.Store.PutRef(db, "main", ref, ""); err != nil {
		// Freshly-minted lineage no rival can reference: safe to delete the
		// orphaned snapshot (mirrors Fork's cleanup on the same failure).
		w.bestEffortDelete(store.SnapshotKey(lineage, 1, 1))
		if errors.Is(err, store.ErrCAS) {
			return fmt.Errorf("ops: database %q already exists (offshoot status lists databases)", db)
		}
		return fmt.Errorf("ops: create %s: %w", db, err)
	}
	return nil
}

// CreateFrom imports an existing SQLite file. The source is never modified:
// the file (plus -wal/-shm if present) is copied to a temp dir, the COPY is
// checkpointed to quiesce it, and the copy is snapshotted.
func (w *Workspace) CreateFrom(db, srcPath string) error {
	if err := store.ValidateName(db); err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "offshoot-import-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	cp := filepath.Join(dir, "import.db")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := copyFile(srcPath+suffix, cp+suffix); err != nil {
			if suffix != "" && os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("ops: import %s: %w", srcPath+suffix, err)
		}
	}
	conn, err := sql.Open("sqlite3", cp)
	if err != nil {
		return fmt.Errorf("ops: import %s: %w", srcPath, err)
	}
	if _, err := conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		conn.Close()
		return fmt.Errorf("ops: import %s: not a usable SQLite database: %w", srcPath, err)
	}
	conn.Close()
	return w.createFromQuiesced(db, cp)
}

// Checkout materializes db@branch's head snapshot to its fixed path. If a
// checkout already lives at that path, it must be quiesced first (busy ->
// clean failure, like Checkpoint): re-materializing renames over the file,
// which would delete a live writer's WAL out from under it. If the existing
// checkout has un-checkpointed local edits, Checkout proceeds (head always
// wins) but warns first, since those edits are about to be discarded.
func (w *Workspace) Checkout(db, branch string) (string, error) {
	res, err := w.CheckoutProven(db, branch)
	if err != nil {
		return "", err
	}
	return res.Path, nil
}

// CheckoutProven is Checkout plus the clean-cache proof described on
// CheckoutResult.Clean/PostApplyChecksum. Exported (rather than folding
// those into Checkout's own signature) so Checkout's three other call sites
// (cmd/offshoot, internal/mcp, internal/daemon) — none of which need the
// proof — are unaffected.
func (w *Workspace) CheckoutProven(db, branch string) (CheckoutResult, error) {
	if err := store.ValidateName(db); err != nil {
		return CheckoutResult{}, err
	}
	if err := store.ValidateName(branch); err != nil {
		return CheckoutResult{}, err
	}
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		return CheckoutResult{}, err
	}
	path := w.CheckoutPath(db, branch)
	if _, err := os.Stat(path); err == nil {
		if err := quiesce(path); err != nil {
			return CheckoutResult{}, err
		}
		// checkoutState compares the sidecar's recorded (lineage, epoch,
		// txid) against ref.Lineage/ref.HeadEpoch/ref.HeadTXID — the CURRENT
		// head, just fetched above — so "clean" here already means
		// "byte-correct for the head right now", not merely "matches
		// whatever it was last verified against". Epoch is load-bearing in
		// that comparison, not redundant with txid: Chain resolution can
		// resolve the same txid to different bytes under different epochs
		// (a fenced writer's orphan vs. the live object), so lineage+txid
		// alone would not prove identity. A clean, current checkout needs no
		// re-materialization: return it as-is rather than paying the
		// temp+rename cost (and, via materializeAt->dbfile, stranding
		// another descriptor) to rebuild bytes that are already correct.
		state, postApplyChecksum := checkoutState(path, ref)
		switch state {
		case "clean":
			return CheckoutResult{Path: path, Clean: true, Ref: ref, PostApplyChecksum: postApplyChecksum}, nil
		case "modified":
			fmt.Fprintf(os.Stderr, "offshoot: warning: overwriting un-checkpointed changes in %s@%s checkout\n", db, branch)
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return CheckoutResult{}, err
	}
	// Stale or missing: resolve the chain first, so an identical (or
	// prefix-sharing) file already in the by-chain cache is cloned instead
	// of decoded — see materializeFromChain. Whichever way the bytes
	// arrive, the sidecar is stamped with THIS branch's identity; only the
	// content fingerprint (hash, checksum, chain_id) is shared.
	members, err := w.resolveChain(ref, headCheckpoint(ref), path)
	if err != nil {
		return CheckoutResult{}, err
	}
	placed, err := w.materializeFromChain(db, ref.Lineage, members, path, 0o600)
	if err != nil {
		return CheckoutResult{}, err
	}
	if observeCheckoutSource != nil {
		observeCheckoutSource(placed.kind)
	}
	if placed.kind != "clone" { // a populate may have added an entry
		w.pruneByChain(db, DefaultByChainMaxEntries)
	}
	if placed.hash == "" {
		// No-cache fallback: hash the file here, sandwiched between two
		// fingerprints exactly as writeSum does, so a write racing the hash
		// is never recorded as a fingerprint paired with the old hash.
		if err := writeSum(path, ref.Lineage, ref.HeadEpoch, ref.HeadTXID, placed.checksum, placed.chainID); err != nil {
			return CheckoutResult{}, err
		}
	} else if err := StampSum(path, placed.hash, ref.Lineage, ref.HeadEpoch, ref.HeadTXID, placed.checksum, placed.chainID); err != nil {
		return CheckoutResult{}, err
	}
	refreshShadow(path)
	return CheckoutResult{Path: path, Clean: false, Ref: ref}, nil
}

// materializeAt writes the state identified by cp into dst, and returns its
// checksum and chainID (see materializeChainAt). It is a thin wrapper over
// materializeChainAt (see materialize.go), which resolves the full
// snapshot+segment chain rather than assuming cp's txid is itself a
// snapshot; Export picks that up unchanged. CheckoutProven, CheckoutAt and
// the Rollback/Promote/Compact refreshes (refreshFromChain) resolve the
// chain themselves and go through materializeFromChain (chainid.go)
// instead, to consult the by-chain cache.
func (w *Workspace) materializeAt(ref store.Ref, cp store.Checkpoint, dst string) (postApply uint64, chainID string, err error) {
	return w.materializeChainAt(ref, cp, dst)
}

// headCheckpoint is the ref's current head as a Checkpoint.
func headCheckpoint(ref store.Ref) store.Checkpoint {
	return store.Checkpoint{TXID: ref.HeadTXID, Epoch: ref.HeadEpoch}
}

func copyFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(to)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// checkpointAfterQuiesceForTest, when non-nil, runs in CheckpointWith after
// it has read the ref and quiesced the checkout, before it plans and encodes
// — where a rival checkpoint can commit and leave this call a late loser.
// Test-only; process-global, restore via t.Cleanup (as
// compactBeforeCASForTest).
var checkpointAfterQuiesceForTest func()

// CheckpointOptions tunes CheckpointWith.
type CheckpointOptions struct {
	// Snapshot forces a full snapshot even when a segment would do.
	Snapshot bool
}

// CheckpointResult is what CheckpointWith wrote.
type CheckpointResult struct {
	TXID uint64
	// Kind is "snapshot" or "segment".
	Kind string
	// Pages is the number of pages a segment carries (0 for a snapshot).
	Pages int
	// Bytes is the size of the object uploaded, segment or snapshot.
	Bytes int64
}

// Checkpoint is CheckpointWith with default options, returning only the
// new txid.
func (w *Workspace) Checkpoint(db, branch, name string, meta map[string]string) (uint64, error) {
	res, err := w.CheckpointWith(db, branch, name, meta, CheckpointOptions{})
	return res.TXID, err
}

// CheckpointWith records the current checkout state as a named checkpoint.
// Plan-2 (CLI/at-rest) semantics: requires the checkout to be quiescible
// (busy timeout 3s, then clean failure). It writes a segment of only the
// pages changed since the head when planSegment allows (a shadow of the
// head is kept next to the checkout; see its doc comment for the rule),
// and a full snapshot otherwise or when opts.Snapshot is set. Either way
// the object goes up with the same create-only put and ref CAS, and the
// sidecar and shadow are refreshed after the CAS.
//
// NOT SAFE against a live in-process session's checkout: it raw-opens (and
// closes) the checkout path to encode it, and that close drops every
// SQLite lock this process holds on it — the POSIX (process, inode)
// lock-drop hazard, see internal/dbfile. Today only the CLI (cmd/offshoot)
// and MCP (internal/mcp) reach this, both of which are separate processes
// from the daemon that runs sessions, so no in-process session can be
// holding that checkout. The daemon conspicuously has no checkpoint op; if
// one is ever added it MUST NOT call this directly — route the snapshot
// through the session's own engine, or through dbfile.
//
// meta (nil = none) is a small string->string map describing this specific
// checkpoint (e.g. eval run id, git SHA, agent id), capped by ValidateMeta
// and stored on the checkpoint's own store.Checkpoint.Meta — not on the
// branch's Ref.Meta, which Fork's meta param sets instead. Rejected (before
// any store I/O) if it exceeds the caps.
func (w *Workspace) CheckpointWith(db, branch, name string, meta map[string]string, opts CheckpointOptions) (CheckpointResult, error) {
	start := time.Now()
	if err := store.ValidateName(db); err != nil {
		return CheckpointResult{}, err
	}
	if err := store.ValidateName(branch); err != nil {
		return CheckpointResult{}, err
	}
	if err := store.ValidateName(name); err != nil {
		return CheckpointResult{}, err
	}
	if err := ValidateMeta(meta); err != nil {
		return CheckpointResult{}, err
	}
	ref, etag, err := w.Store.GetRef(db, branch)
	if err != nil {
		return CheckpointResult{}, err
	}
	if _, exists := ref.Checkpoints[name]; exists {
		return CheckpointResult{}, fmt.Errorf("ops: checkpoint %q already exists on %s@%s", name, db, branch)
	}
	path := w.CheckoutPath(db, branch)
	if _, err := os.Stat(path); err != nil {
		return CheckpointResult{}, fmt.Errorf("ops: no checkout for %s@%s (run 'offshoot checkout %s@%s' first)", db, branch, db, branch)
	}
	if err := quiesce(path); err != nil {
		return CheckpointResult{}, err
	}
	// The checkout's fingerprint right after quiesce, before the encode:
	// stampCheckpoint compares it with the file it stamps, so a write
	// landing between the encode and the stamp cannot get our checksum
	// attributed to bytes we never encoded.
	fpEncode, fpEncodeErr := stampFingerprint(path)
	if checkpointAfterQuiesceForTest != nil {
		checkpointAfterQuiesceForTest()
	}
	txid := ref.HeadTXID + 1
	res := CheckpointResult{TXID: txid, Kind: "snapshot"}
	var buf bytes.Buffer
	var key string
	var checksum uint64
	// A segment is written only when no snapshot already sits at txid. Only
	// a failed earlier attempt (object uploaded, ref write never landed, or
	// a crash between them) can have left one there, since no ref names
	// that key; left beside a segment it would anchor the head (the chain
	// resolver anchors on the newest snapshot at or below the target), so
	// the head would resolve to that attempt's older content. Checking
	// before the write rather than deleting after the CAS means no failure
	// or crash can leave the two side by side: a snapshot here takes the
	// create-only put's overwrite path below instead. An error from the
	// probe also answers "write a snapshot", which is always correct.
	//
	// The head's chain is resolved here, once, and only when a segment is
	// locally possible (segmentShadow); planSegment uses this resolution
	// and never re-resolves. A failed resolve leaves members nil, which
	// planSegment answers with a snapshot.
	var members []store.ChainMember
	if _, _, ok := segmentShadow(path, ref, opts); ok {
		members, _ = w.Store.Chain(ref.Lineage, ref.HeadTXID)
	}
	if d, ok := w.planSegment(path, ref, members, opts); ok && !w.snapshotMayExist(ref, txid) {
		if err := ltxio.EncodeSegment(d.pageSize, d.commit, txid, txid, d.pre, d.post, d.pages, &buf); err != nil {
			return CheckpointResult{}, err
		}
		key, checksum = store.SegmentKey(ref.Lineage, ref.Epoch, txid, txid), d.post
		res.Kind, res.Pages = "segment", len(d.pages)
	} else {
		if checksum, err = ltxio.EncodeSnapshot(path, txid, &buf); err != nil {
			return CheckpointResult{}, err
		}
		key = store.SnapshotKey(ref.Lineage, ref.Epoch, txid)
	}
	res.Bytes = int64(buf.Len())
	ownEtag, err := w.Store.B.PutIf(key, buf.Bytes(), "")
	if err != nil {
		if !errors.Is(err, store.ErrCAS) {
			return CheckpointResult{}, err
		}
		// An object already lives at this deterministic key. HeadTXID only
		// ever advances via a successful ref write, and nothing is written
		// under a txid beyond HeadTXID+1 until that happens, so nothing can
		// legitimately reference this key yet: it can only be (a) an orphan
		// left by a crashed prior Checkpoint attempt (object uploaded, ref
		// write never landed), or (b) a rival Checkpoint call racing on this
		// same branch right now, which computed the identical HeadTXID+1 and
		// simply lost the write here. Overwriting is benign under either
		// cause: both writers quiesced and encoded the SAME checkout file at
		// the SAME pre-checkpoint state, so whichever encoding ends up
		// stored is content-equivalent, and only one of the two racers can
		// possibly win the PutRef CAS below to ever reference this key by
		// name — there is no rival ref left dangling by the overwrite. Safe
		// to overwrite unconditionally and proceed. The key names the kind
		// (snapshot or segment), so a racer that chose the other kind wrote
		// a different key; both then race the same CAS below. That loser's
		// object is NOT an orphan like any other when it is a snapshot
		// beside a winning segment: the chain resolver anchors the head on
		// the newest snapshot at or below it, so the head resolves to the
		// loser's content. A live loser deletes it (the ErrCAS path below);
		// a dead one, or one that has not got there yet, is caught by the
		// winner's post-CAS probe (snapshotMayExist after PutRef).
		//
		// "Content-equivalent" holds only when nothing wrote to the checkout
		// between the two encodes. When something did, whichever racer
		// wins the CAS may find the other's content under its key; the
		// post-CAS verifyOwnObject below detects exactly that. Put returns
		// no etag, so ownEtag stays "" here and the check compares content.
		if err := w.Store.B.Put(key, buf.Bytes()); err != nil {
			return CheckpointResult{}, err
		}
	}
	ref.HeadTXID = txid
	ref.HeadEpoch = ref.Epoch
	ref.SetCheckpoint(name, store.Checkpoint{TXID: txid, Epoch: ref.Epoch, CreatedAt: nowStamp(), Meta: meta, Kind: res.Kind})
	ref.Touch(time.Now())
	if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
		// Decide whether to clean up the object after PutRef failure.
		// Unlike Fork/Rollback/Promote (which delete keys in freshly-minted
		// lineages no rival can reference), we must gate cleanup on the error
		// type: under concurrent Checkpoint calls on the same branch, every
		// racer computes the same deterministic txid (HeadTXID+1) and key.
		//
		// On ErrCAS (lost the CAS race): serialization via PutIf means the
		// winner's ref is already visible, so the object is confirmed an
		// orphan left by a crashed prior Checkpoint — safe to delete so a
		// retry's create-only put doesn't wedge behind our orphan.
		//
		// On non-CAS errors (lock timeout, I/O error): deletion is UNSAFE.
		// A concurrent checkpointer may already be mid-write, and we can't
		// tell from here whether they've landed their ref yet. Deleting would
		// rip the object out from under them, leaving a checkpoint recorded
		// in their ref with no backing object — silent corruption for them.
		// Leave the object alone; worst case it's a harmless orphan for a
		// later GC pass.
		//
		// On ErrCAS with a winner on this lineage whose head covers txid, our
		// key is the winner's own when the winner wrote the same kind (the
		// key is deterministic per kind), and must stay. When the winner
		// wrote the OTHER kind at txid, our object is an orphan beside it,
		// and a harmful one: a loser's snapshot beside a winner's segment
		// becomes the head's anchor (the chain resolver anchors on the
		// newest snapshot at or below the head), so if the checkout changed
		// between the two encodes every later segment's pre-apply checksum
		// fails. The winner's ref records its kind on the checkpoint entry
		// at txid (store.Checkpoint.Kind); we delete our object only when
		// that entry exists with a known kind different from ours. An
		// unknown kind, or no entry at txid, keeps it: probing for the
		// other key's existence instead cannot tell the winner's object
		// from an orphan a crashed earlier attempt left at the same txid.
		if errors.Is(err, store.ErrCAS) {
			if cur, _, gerr := w.Store.GetRef(db, branch); gerr == nil {
				if cur.Lineage != ref.Lineage || cur.HeadTXID < txid {
					w.bestEffortDelete(key)
				} else if k := checkpointKindAt(cur, txid); k != "" && k != res.Kind {
					w.bestEffortDelete(key)
				}
			}
			return CheckpointResult{}, fmt.Errorf("ops: ref update lost a race (retry): %w", err)
		}
		return CheckpointResult{}, fmt.Errorf("ops: ref update for checkpoint %q on %s@%s: %w", name, db, branch, err)
	}
	// The ref CAS is the point of no return: only now does the checkout truly
	// equal committed state (lineage unchanged, head advanced to txid).
	// Writing the sidecar here, after the CAS, means an interrupt between the
	// encode above and this point leaves the OLD sidecar in place — which
	// still correctly describes the checkout's actual (pre-checkpoint)
	// identity, rather than claiming a commit that never landed. The same
	// goes for the shadow: it is re-cloned only after the new stamp, which
	// records no shadow until refreshShadow has one in place.
	//
	// What the store now resolves the head to may not be what we encoded:
	//
	//   - a racing same-kind checkpoint can have replaced the object at our
	//     key with its own content (verifyOwnObject);
	//   - a racing snapshot checkpoint can have left its snapshot at txid
	//     beside our segment, and the chain resolver anchors the head on
	//     that snapshot, not on our segment. Its loser-side cleanup does
	//     not help when the loser died first or has not reached it yet, so
	//     a segment winner probes for one (one List) and, when present,
	//     takes the head's checksum from it.
	//
	// Either way the overwrite is counted, and stampCheckpoint then stamps
	// the checksum only when the live checkout provably holds the content
	// the store resolves the head to; otherwise the stamp records no
	// checksum and a hash no file can match, so the checkout reads
	// "modified", the shadow is dropped, and the next checkpoint writes a
	// snapshot of whatever the checkout holds. The same happens when the
	// checkout changed between our encode and this stamp.
	headSum, headKnown := w.verifyOwnObject(key, ownEtag, checksum)
	if res.Kind == "segment" && w.snapshotMayExist(ref, txid) {
		headSum, headKnown = w.snapshotBesideSegment(store.SnapshotKey(ref.Lineage, ref.Epoch, txid), headSum, headKnown)
	}
	if (!headKnown || headSum != checksum) && ObserveCheckpointOverwrite != nil {
		ObserveCheckpointOverwrite()
	}
	trusted, err := stampCheckpoint(path, ref.Lineage, ref.HeadEpoch, txid, headSum, headKnown, checksum, fpEncode, fpEncodeErr == nil)
	if err != nil {
		return CheckpointResult{}, fmt.Errorf("ops: checkpoint %q committed (txid %d), but the checkout fingerprint could not be refreshed: %w", name, txid, err)
	}
	if trusted {
		refreshShadow(path)
	} else {
		dropShadow(path)
	}
	if ObserveCheckpoint != nil {
		ObserveCheckpoint(time.Since(start))
	}
	return res, nil
}

// verifyOwnObject returns the post-apply checksum of the object at key,
// just committed by a winning ref CAS, and whether it is known. A Head
// whose etag equals ownEtag (the etag our create-only put returned)
// answers checksum with one request. Otherwise — a rival's overwrite, or
// our own unconditional overwrite, which returns no etag — the object is
// fetched and its trailer read: two encodes of the same checkout state
// differ in bytes (an LTX header carries its encode time) but not in
// checksum, so only a real content change returns a different one. An
// object that cannot be read or decoded after an etag mismatch is logged
// and answers unknown: distrusting costs one snapshot, trusting a wrong
// checksum costs a session's correctness. A failing Head never fails the
// checkpoint: it is logged and answers checksum, as before this check
// existed.
func (w *Workspace) verifyOwnObject(key, ownEtag string, checksum uint64) (uint64, bool) {
	etag, _, err := w.Store.Head(key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "offshoot: warning: could not verify checkpoint object %s after committing it (trusting it): %v\n", key, err)
		return checksum, true
	}
	if ownEtag != "" && etag == ownEtag {
		return checksum, true
	}
	data, _, err := w.Store.B.Get(key)
	return objectChecksum(key, data, err)
}

// snapshotBesideSegment is the head's checksum when a segment checkpoint
// has just won its CAS and snapshotMayExist reported a snapshot at the
// same txid (snapKey): that snapshot anchors the head, so its trailer is
// the head's content. A snapshot that turns out not to exist (the probe
// answers "maybe" when its List fails) leaves the segment's own (sum,
// known).
func (w *Workspace) snapshotBesideSegment(snapKey string, sum uint64, known bool) (uint64, bool) {
	data, _, err := w.Store.B.Get(snapKey)
	if errors.Is(err, store.ErrNotFound) {
		return sum, known
	}
	return objectChecksum(snapKey, data, err)
}

// objectChecksum is the trailer post-apply checksum of data, the LTX
// object fetched from key (getErr is that fetch's error), or unknown —
// logged, since the caller then counts it as an overwrite it could not
// verify — when it could not be fetched or decoded.
func objectChecksum(key string, data []byte, getErr error) (uint64, bool) {
	if getErr != nil {
		fmt.Fprintf(os.Stderr, "offshoot: warning: could not verify checkpoint object %s after committing it (distrusting it): %v\n", key, getErr)
		return 0, false
	}
	sum, err := ltxio.TrailerPostApplyChecksum(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "offshoot: warning: could not decode checkpoint object %s after committing it (distrusting it): %v\n", key, err)
		return 0, false
	}
	return sum, true
}

// snapshotMayExist reports whether a snapshot object may already sit at
// txid in ref's lineage and writer epoch: true when one does, and when the
// probe (one List of that exact key) fails, since the caller's safe answer
// to "maybe" is to write a snapshot. See CheckpointWith.
func (w *Workspace) snapshotMayExist(ref store.Ref, txid uint64) bool {
	exists, err := w.Store.ObjectExists(store.SnapshotKey(ref.Lineage, ref.Epoch, txid))
	return err != nil || exists
}

// checkpointKindAt is the recorded Kind of ref's checkpoint at txid, or ""
// when none is recorded there or its writer did not record a kind.
func checkpointKindAt(ref store.Ref, txid uint64) string {
	for _, c := range ref.Checkpoints {
		if c.TXID == txid && c.Kind != "" {
			return c.Kind
		}
	}
	return ""
}

// errQuiesceBusy is quiesce's error specifically for wal_checkpoint(TRUNCATE)
// reporting busy != 0 — a live connection (reader or writer) is preventing a
// full checkpoint right now. Distinct from every other quiesce failure
// (sql.Open failure, a path that isn't a valid SQLite file at all): a caller
// that needs to tell "someone is actively using this file right this
// instant" apart from "this file can't be quiesced for some other reason"
// checks errors.Is against this sentinel — see ops.BranchStateAt, which
// treats a busy checkout as itself evidence of un-checkpointed activity
// ("dirty") rather than an absence of evidence ("idle"). The wrapped message
// text is unchanged from before this sentinel existed, so every existing
// caller that only logs/propagates quiesce's error (Checkpoint,
// CheckoutProven, Rollback's refresh, Promote's refresh,
// warnIfUncheckpointed) sees byte-identical output.
var errQuiesceBusy = errors.New("ops: database is busy (live writer or reader); close connections and retry")

// quiesceBusyTimeoutMS is the SQLite busy timeout (milliseconds) quiesce
// opens with. Deliberately DIFFERENT from the capture engine's 5000ms
// (internal/capture/engine.go's captureBusyTimeoutMS): quiesce fails
// cleanly on a busy database (errQuiesceBusy) and its callers treat that as
// an answer, so it gives up sooner rather than stalling the caller.
const quiesceBusyTimeoutMS = 3000

// quiesce checkpoints the WAL fully, failing cleanly on a busy database (see
// errQuiesceBusy).
func quiesce(path string) error {
	conn, err := sql.Open("sqlite3", fmt.Sprintf("%s?_busy_timeout=%d", path, quiesceBusyTimeoutMS))
	if err != nil {
		return err
	}
	defer conn.Close()
	var busy, logN, ckptN int
	if err := conn.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logN, &ckptN); err != nil {
		return fmt.Errorf("ops: checkpoint: %w", err)
	}
	if busy != 0 {
		return errQuiesceBusy
	}
	return nil
}

// warnIfUncheckpointed checks db@branch's checkout (if one exists) against
// ref and, if it diverges, prints a warning to os.Stderr explaining what the
// caller (Fork) is about to do about it: proceed from ref.HeadTXID either
// way. It never fails the caller's operation: any error along the way is
// treated as "nothing to warn about". ref is the same ref the caller already
// fetched to decide the fork point, so this reuses it rather than issuing a
// second (potentially inconsistent) GetRef.
func (w *Workspace) warnIfUncheckpointed(db, branch string, ref store.Ref, action string) {
	path := w.CheckoutPath(db, branch)
	if _, err := os.Stat(path); err != nil {
		return
	}
	if err := quiesce(path); err != nil {
		fmt.Fprintf(os.Stderr, "offshoot: warning: checkout of %s@%s is busy; %s (txid %d)\n", db, branch, action, ref.HeadTXID)
		return
	}
	state, _ := checkoutState(path, ref)
	switch state {
	case "modified":
		fmt.Fprintf(os.Stderr, "offshoot: warning: checkout of %s@%s has un-checkpointed changes; %s (txid %d) — run 'offshoot checkpoint' first to keep them\n", db, branch, action, ref.HeadTXID)
	case "stale":
		fmt.Fprintf(os.Stderr, "offshoot: warning: checkout of %s@%s is stale (branch was repointed since it was materialized); %s (txid %d) — run 'offshoot checkout' to refresh\n", db, branch, action, ref.HeadTXID)
	}
}

// errNoCheckpoint is the error for a checkpoint name that db@branch does
// not carry. Checkpoints belong to one branch and are not inherited by
// forks (a fork carries only "fork" at its fork point until it makes its
// own), which is the single most common surprise behind this error, so the
// message lists the branch's own checkpoints and names the way to start a
// branch from a parent's checkpoint.
func errNoCheckpoint(db, branch, name string, ref store.Ref) error {
	names := make([]string, 0, len(ref.Checkpoints))
	for n := range ref.Checkpoints {
		names = append(names, n)
	}
	sort.Strings(names)
	have := "none"
	if len(names) > 0 {
		have = strings.Join(names, ", ")
	}
	return fmt.Errorf("ops: no checkpoint %q on %s@%s (its checkpoints: %s; checkpoints belong to one branch — to start from a parent's checkpoint, fork it: offshoot fork %s@<parent> <new> --at %s)",
		name, db, branch, have, db, name)
}

// copySnapshotIntoLineage materializes src's lineage at cp — resolving its
// full snapshot+segment chain, not assuming cp.TXID is itself a snapshot
// object; e.g. forking or rolling back at HEAD after segment writes lands on
// a txid only reachable by applying segments past src's last snapshot — into
// a scratch file, then re-encodes that as a single fresh snapshot in
// lineage at epoch 1 via a create-only put, and returns the key it was
// written under. Destinations are always freshly-minted lineages, and a
// fresh lineage always starts at epoch 1: this is the primitive behind fork,
// rollback, and promote, every branch repoint gets a fresh lineage,
// preserving one-writer-per-lineage. Re-encoding as a single snapshot
// (rather than copying whatever mix of objects src's chain resolved to) is
// also what keeps the destination lineage storage-independent from src.
func (w *Workspace) copySnapshotIntoLineage(src store.Ref, cp store.Checkpoint, lineage string) (string, error) {
	members, err := w.Store.Chain(src.Lineage, cp.TXID)
	if err != nil {
		return "", fmt.Errorf("ops: resolving chain for lineage %s to txid %d: %w", src.Lineage, cp.TXID, err)
	}
	return w.copySnapshotIntoLineageFromChain(src.Lineage, members, cp, lineage)
}

// copySnapshotIntoLineageFromChain is copySnapshotIntoLineage's guts, taking
// an already-resolved chain instead of re-resolving it via store.Chain — see
// materializeMembersAt's doc comment for why that split exists and who
// benefits from it (Task 6's fast-path fork attempt falling back to the
// slow path for the SAME checkpoint it already resolved a chain for).
func (w *Workspace) copySnapshotIntoLineageFromChain(srcLineage string, members []store.ChainMember, cp store.Checkpoint, dstLineage string) (string, error) {
	tmp := filepath.Join(os.TempDir(), "offshoot-copy-"+store.NewLineageID()+".db")
	defer os.Remove(tmp)
	if _, err := w.materializeMembersAt(srcLineage, members, tmp); err != nil {
		return "", fmt.Errorf("ops: materializing lineage %s at txid %d for copy: %w", srcLineage, cp.TXID, err)
	}
	var buf bytes.Buffer
	if _, err := ltxio.EncodeSnapshot(tmp, cp.TXID, &buf); err != nil {
		return "", err
	}
	key := store.SnapshotKey(dstLineage, 1, cp.TXID)
	if _, err := w.Store.B.PutIf(key, buf.Bytes(), ""); err != nil {
		return "", err
	}
	return key, nil
}

// forkSlowPathForTest, when true, forces copySnapshotToNewLineage to take
// the slow materialize-and-re-encode path unconditionally, even when the
// fast-path precondition holds. Test-only: never set outside a test, and
// tests that set it must restore it (e.g. via t.Cleanup) since it is
// process-global. It exists so the ops equivalence tests can produce a
// "definitely slow path" child from the exact same source as a fast-path
// child, to compare their outputs — see ops_test.go's fork fast-path tests.
// The external ops_test package (which needs to import session without an
// import cycle) reaches this via the exported test-only wrapper in
// export_test.go. Since the copy-on-write shared fork path landed, this knob
// also forces Fork itself onto the MATERIALIZE branch (a "definitely slow
// path" child must materialize at all before it can re-encode), so setting
// it alone yields a fully-slow fork.
var forkSlowPathForTest bool

// forkMaterializeForTest, when true, forces Fork to take the MATERIALIZE
// branch (copySnapshotToNewLineage) even when the shared base-pointer path
// would apply, WITHOUT suppressing the fast object-copy inside it (that
// suppression is forkSlowPathForTest's job; setting that knob also implies
// materializing — see its doc comment). Test-only,
// process-global, restore via t.Cleanup, same rules as forkSlowPathForTest.
// It exists because the copy-on-write shared path made a plain Fork of a
// short chain share instead of materialize, so the materialize-path
// equivalence tests (fast copy vs slow re-encode) need a way to reach the
// materialize machinery at all from Fork.
var forkMaterializeForTest bool

// ForkShareMaxDepth is the DEFAULT fork-time snapshot-floor bound, used
// when Workspace.SnapshotEvery is unset (0): a shared fork whose
// FULLY-resolved base chain (transitive ancestors included) already
// reaches the bound materializes a fresh floor snapshot instead of
// sharing, so no fork spine's resolved chain exceeds it. Mirrors
// session.DefaultSnapshotEvery — the bound that keeps materialization
// bounded (ops must not import internal/session, hence the local
// constant). A configured cadence overrides it via Workspace.SnapshotEvery
// (the daemon sets that from its session SnapshotEvery, keeping the fork
// floor and the divergence floor in agreement).
const ForkShareMaxDepth = 16

// forkFastPathHits counts how many times copySnapshotToNewLineage has taken
// the fast object-copy path (single-snapshot chain, backend CopyObject
// succeeded) since process start. Test-only instrumentation — nothing in
// non-test code reads it — so tests can assert the fast path did or didn't
// fire without depending on backend-specific side effects (e.g. whether a
// given machine's filesystem actually supports reflink). Atomic because
// Fork itself has no lock preventing concurrent callers (see
// TestConcurrentForksFromSameParent), and this counter must not race with
// itself just because it is test instrumentation.
var forkFastPathHits atomic.Int64

// ObserveFork, when non-nil, is invoked by Fork immediately before it
// returns successfully (never on error — a failed fork has no meaningful
// "how long did this take", and the metric it feeds,
// offshoot_fork_duration_seconds, is defined as successful-fork latency)
// with the wall-clock duration of the whole call and whether it took the
// fast (single-snapshot backend-level object copy — see
// copySnapshotToNewLineage's doc comment) or slow (materialize + re-encode)
// path. It is also this package's source for offshoot_fork_total{path};
// unlike forkFastPathHits (test-only, package-internal, never reset), this
// hook is the real production signal.
//
// shared reports the fork's STORAGE MODE — the copy-on-write feature's
// headline number: true for a shared fork (a base pointer into the parent's
// chain, zero data objects copied), false for a materialized one (a full
// copy in the child's own lineage — the fork-time snapshot floor, or the
// test hooks that force it). fast is orthogonal and only meaningful when
// shared is false: it names the materialize path's copy strategy (a shared
// fork copies nothing, so it always reports fast=false). The daemon feeds
// shared into offshoot_fork_mode_total{mode="shared"|"materialized"}.
//
// Injection shape: a package-level, nil-checked func var — the same pattern
// this file already uses for its own test hooks (forkSlowPathForTest,
// FlushEncodeHook/FlushUploadHook over in flush.go) — rather than an
// Observer interface with Fork/Checkpoint methods. Two independent,
// stateless, single-call callbacks with no shared lifecycle between them
// don't benefit from being bundled behind one interface; doing so would
// only force this package to name and depend on a type whose one real
// implementation lives in internal/metrics, for no gain over two funcs.
// ops must not import internal/metrics (the M4 plan's explicit constraint,
// so a later swap of the metrics backend never touches this package) — the
// daemon assigns both hooks once, at server construction, closing over its
// own *metrics.Registry-backed counters/histograms so ops never needs to
// know metrics exists.
var ObserveFork func(dur time.Duration, fast, shared bool)

// ObserveCheckpoint, when non-nil, is invoked by Checkpoint immediately
// before it returns successfully (never on error, matching ObserveFork's
// same reasoning) with the call's wall-clock duration. See ObserveFork's
// doc comment for the injection-shape rationale, which applies identically
// here.
//
// In today's architecture this hook only ever fires from a process that
// calls ops.Workspace.Checkpoint directly — the CLI and MCP tool processes
// (see Checkpoint's own doc comment: it is "NOT SAFE against a live
// in-process session's checkout", so the daemon has no "checkpoint" op and
// never calls this method itself; a live session's checkpoint is a NAMED
// Flush instead, observed separately via offshoot_flush_total/
// offshoot_flush_duration_seconds — see internal/session's OnTransition
// hook). So although the daemon registers offshoot_checkpoint_duration_seconds
// and assigns this hook at startup (per this task's brief), that histogram
// reads as all-zero on a daemon that never gains a direct checkpoint op —
// a known, documented gap, not a bug; see this task's report.
var ObserveCheckpoint func(dur time.Duration)

// ObserveCheckpointOverwrite, when non-nil, is invoked by CheckpointWith
// each time its post-CAS check finds that the store may not resolve the
// head to the content it encoded: the object it committed was replaced, at
// its shared key, by a racing same-kind checkpoint's different content, or
// could not be verified after an etag mismatch (see verifyOwnObject); or a
// racing snapshot with different content (or one that could not be read)
// sits beside its winning segment and anchors the head (see
// snapshotBesideSegment). The checkpoint still succeeds; the daemon
// feeds this into offshoot_checkpoint_overwrite_detected_total. Same
// injection shape as ObserveFork.
var ObserveCheckpointOverwrite func()

// ObserveRollback and ObservePromote, when non-nil, are invoked once the
// verb's ref CAS has landed (the repoint happened, whatever the checkout
// refresh does next) with the new lineage's storage mode: shared (a base
// pointer into the old history) or not (a self-contained copy). The daemon
// feeds them into offshoot_rollback_total{mode} / offshoot_promote_total
// {mode}; same injection shape as ObserveFork.
var (
	ObserveRollback func(shared bool)
	ObservePromote  func(shared bool)
)

// copySnapshotToNewLineage copies the snapshot identified by cp (in src's
// lineage) into a brand-new lineage (epoch 1) and returns the lineage id,
// plus whether it took the fast path (see below) — the latter is
// ObserveFork's "fast bool" straight from its one caller that reports it,
// Fork; the other callers (Rollback and Promote on their materialize path,
// Compact) discard it.
//
// Fast path (Task 6a): when src's chain at cp resolves to EXACTLY ONE
// member and that member is itself a snapshot, the child's seed is a
// byte-identical backend-level copy of the source snapshot object rather
// than a materialize-then-re-encode. This is sound because an LTX object
// names no lineage anywhere in its own bytes — only the KEY it is stored
// under does (see store.SnapshotKey/store.SegmentKey and
// github.com/superfly/ltx's Header, which carries page size/commit/TXIDs/
// checksums/timestamp/NodeID, nothing lineage-identifying) — so the same
// object is valid content at a different key in a different lineage.
// tryFastForkCopy verifies the result the same way every other path trusts
// its output: the child's chain must resolve after the copy; the LTX
// decoder's own checksum verification happens at first materialization, as
// everywhere else.
//
// Any chain longer than one member (a checkpoint reached via segments past
// the lineage's last snapshot — the daemon's flush cadence) does not meet
// the precondition and falls through to the slow path unchanged. A backend
// that cannot perform CopyObject at all for this particular object (store.
// ErrCopyUnsupported) also falls through to the slow path — every backend
// offshoot ships (local since Task 6a, S3 since Task 6b) supports
// CopyObject in general, including S3 objects over its 5GB single-request
// limit (via multipart UploadPartCopy), so the sentinel now only fires for
// a source over S3's actual 5TiB per-object ceiling — see
// store.S3.CopyObject's doc comment.
func (w *Workspace) copySnapshotToNewLineage(src store.Ref, cp store.Checkpoint) (string, bool, error) {
	// Resolved once, up front, and threaded through both the fast-path
	// attempt and the slow-path fallback — see materializeMembersAt's doc
	// comment for why sharing this one store.Chain call (rather than each
	// path resolving its own) matters on a remote backend.
	members, err := w.Store.Chain(src.Lineage, cp.TXID)
	if err != nil {
		return "", false, fmt.Errorf("ops: resolving chain for lineage %s to txid %d: %w", src.Lineage, cp.TXID, err)
	}
	return w.copySnapshotToNewLineageFromChain(src, cp, members)
}

// copySnapshotToNewLineageFromChain is copySnapshotToNewLineage's guts,
// taking src's already-resolved chain at cp instead of re-resolving it.
// newLineageAt's materialize branch (Fork, Rollback, Promote) calls this
// directly with the chain it already resolved for the floor decision (the
// floor trips exactly when the chain is long, i.e. when re-resolving is most
// expensive — perf audit M1); Compact, which holds no prior resolution, goes
// through the wrapper above.
func (w *Workspace) copySnapshotToNewLineageFromChain(src store.Ref, cp store.Checkpoint, members []store.ChainMember) (string, bool, error) {
	lineage := store.NewLineageID()
	if !forkSlowPathForTest {
		ok, err := w.tryFastForkCopy(members, cp, lineage)
		if err != nil {
			return "", false, err
		}
		if ok {
			return lineage, true, nil
		}
	}
	if _, err := w.copySnapshotIntoLineageFromChain(src.Lineage, members, cp, lineage); err != nil {
		return "", false, err
	}
	return lineage, false, nil
}

// tryFastForkCopy attempts the fast object-copy fork path into lineage,
// given src's already-resolved chain at cp (see copySnapshotToNewLineage's
// doc comment for the precondition and why it is sound). ok is false, err is
// nil when the precondition doesn't hold or the backend doesn't support
// CopyObject — both are "fall back to the slow path", not errors. A non-nil
// err means something actually went wrong (a CopyObject failure that isn't
// the unsupported sentinel, or the copy landed but the child chain didn't
// resolve) and must propagate rather than being swallowed into a silent
// fallback — the slow path would very likely fail the same way, and masking
// a real error as "just take the slow path" would hide it behind a
// redundant, slower failure instead of reporting it.
func (w *Workspace) tryFastForkCopy(members []store.ChainMember, cp store.Checkpoint, lineage string) (ok bool, err error) {
	if len(members) != 1 || !members[0].Snapshot {
		return false, nil
	}
	srcKey := members[0].Key
	dstKey := store.SnapshotKey(lineage, 1, cp.TXID)
	if err := w.Store.B.CopyObject(dstKey, srcKey); err != nil {
		if errors.Is(err, store.ErrCopyUnsupported) {
			return false, nil
		}
		return false, fmt.Errorf("ops: fast-path fork copy %s -> %s: %w", srcKey, dstKey, err)
	}
	// Verify the copy the same way the slow path's output is trusted: the
	// child chain must resolve. Checksum verification itself happens at
	// first materialization, same as every other path (Checkout, Rollback,
	// Promote) already relies on.
	if _, err := w.Store.Chain(lineage, cp.TXID); err != nil {
		return false, fmt.Errorf("ops: fast-path fork copy %s -> %s: child chain did not resolve: %w", srcKey, dstKey, err)
	}
	forkFastPathHits.Add(1)
	return true, nil
}

// Fork creates newBranch from db@srcBranch at head or a named checkpoint.
// ttl > 0 sets the child's TTL (never the parent's — creating a child does
// not extend the parent's activity clock either); ttl == 0 means no TTL —
// that is the API's one way to say "no TTL" (Fork has no separate "none"
// sentinel the way Touch does, since a brand-new branch has no existing TTL
// to preserve vs. clear). ttl < 0 is refused outright rather than silently
// treated as ttl == 0: a caller that passed a negative duration made a
// mistake, and swallowing it would mean the child forks with no TTL while
// the caller believes it asked for one. Callers taking TTL as a wire string
// (opFork, the CLI's --ttl) are expected to reject a non-positive value
// before ever reaching here — see their own docs — so this is a second,
// defense-in-depth check, not the primary one a caller should rely on for a
// good error message.
//
// meta (nil = none) is a small string->string map describing the new
// branch's lineage (e.g. eval run id, git SHA, agent id), capped by
// ValidateMeta and stored on the child ref's Meta field — branch-level
// lineage is the grain, not per-checkpoint (see Checkpoint's own meta param
// for that). Rejected (before any store I/O) if it exceeds the caps.
func (w *Workspace) Fork(db, srcBranch, newBranch, at string, ttl time.Duration, meta map[string]string) (uint64, error) {
	start := time.Now()
	if ttl < 0 {
		return 0, fmt.Errorf("ops: fork ttl must be zero (no TTL) or positive, got %s", ttl)
	}
	if err := store.ValidateName(newBranch); err != nil {
		return 0, err
	}
	if err := ValidateMeta(meta); err != nil {
		return 0, err
	}
	src, _, err := w.Store.GetRef(db, srcBranch)
	if err != nil {
		return 0, err
	}
	cp := headCheckpoint(src)
	if at != "" {
		c, ok := src.Checkpoints[at]
		if !ok {
			return 0, errNoCheckpoint(db, srcBranch, at, src)
		}
		cp = c
	} else {
		w.warnIfUncheckpointed(db, srcBranch, src, "forking last committed state")
	}
	txid := cp.TXID
	// Fork-time snapshot-floor decision on the fork point's fully-resolved
	// chain: SHARE below the bound, MATERIALIZE at it — see newLineageAt.
	baseMembers, err := w.Store.Chain(src.Lineage, cp.TXID)
	if err != nil {
		return 0, fmt.Errorf("ops: resolving chain for lineage %s to txid %d: %w", src.Lineage, cp.TXID, err)
	}
	materialize := forkMaterializeForTest || forkSlowPathForTest
	childLineage, base, fast, err := w.newLineageAt(src, cp, baseMembers, materialize, fmt.Sprintf("fork %s@%s", db, newBranch))
	if err != nil {
		return 0, err
	}
	child := store.Ref{
		Lineage: childLineage, Epoch: 1, HeadTXID: txid, HeadEpoch: 1,
		Parent: fmt.Sprintf("%s@%s@%d", db, srcBranch, txid),
		Meta:   meta,
		Base:   base,
	}
	if ttl > 0 {
		child.TTL = ttl.String()
	}
	child.Touch(time.Now())
	child.SetCheckpoint("fork", store.Checkpoint{TXID: txid, Epoch: 1, CreatedAt: nowStamp()})
	if _, err := w.Store.PutRef(db, newBranch, child, ""); err != nil {
		// Branch already exists (or lost a race): remove the orphan — the
		// base object on the shared path (there is no snapshot to delete),
		// the snapshot on the materialize path.
		if base != nil {
			w.bestEffortDelete(store.BaseKey(childLineage))
		} else {
			w.bestEffortDelete(store.SnapshotKey(childLineage, 1, txid))
		}
		if errors.Is(err, store.ErrCAS) {
			return 0, fmt.Errorf("ops: branch %s@%s already exists (offshoot status lists branches)", db, newBranch)
		}
		return 0, fmt.Errorf("ops: fork %s@%s: %w", db, newBranch, err)
	}
	if ObserveFork != nil {
		// A shared fork reports fast=false: the fast/slow split was defined
		// for the materialize path's copy strategy, and a shared fork copies
		// nothing. Its storage mode travels in the dedicated shared bool
		// instead (base != nil is exactly the SHARE branch above) — see
		// ObserveFork's doc comment.
		ObserveFork(time.Since(start), fast, base != nil)
	}
	return txid, nil
}

// shareBound is the fork-time snapshot floor: the configured session
// cadence when set, so a daemon running SnapshotEvery below the default
// can't mint shared lineages whose resolved chains exceed its own cadence,
// else ForkShareMaxDepth.
func (w *Workspace) shareBound() int {
	if w.SnapshotEvery > 0 {
		return w.SnapshotEvery
	}
	return ForkShareMaxDepth
}

// newLineageAt mints the fresh lineage a new or repointed branch starts on
// at cp of src, given src's already-resolved chain at cp (members). It is
// the share-versus-materialize decision Fork, Rollback and Promote all take:
//
//   - SHARE (below the floor, materialize false): no snapshot/segment
//     objects at all — the lineage is born as a base pointer {L, cp.TXID}
//     into src's already-durable chain, returned as base, where L is
//     src.Lineage with every pass-through hop at cp.TXID skipped
//     (store.CollapseBase): same resolved content, flat spine. Reads at
//     or below cp.TXID resolve purely in src (Chain's target <= base.TXID
//     branch); the new lineage's own segments concatenate on top.
//   - MATERIALIZE (members at the floor, or materialize set): cp is copied
//     into the lineage as one self-contained snapshot, exactly the pre-CoW
//     path; base is nil and fast reports the copy strategy.
//
// The floor is on the FULLY-resolved chain (Chain follows base pointers
// transitively): sharing adds zero members of its own, so sharing below the
// bound can never push a resolved chain past it, and materializing at the
// bound resets the spine's depth to one. what names the caller in errors
// ("fork db@branch"). On a lost ref CAS afterwards the caller removes the
// orphan: BaseKey(lineage) when base != nil, else the copied snapshot.
func (w *Workspace) newLineageAt(src store.Ref, cp store.Checkpoint, members []store.ChainMember, materialize bool, what string) (lineage string, base *store.BasePointer, fast bool, err error) {
	if materialize || len(members) >= w.shareBound() {
		lineage, fast, err = w.copySnapshotToNewLineageFromChain(src, cp, members)
		return lineage, nil, fast, err
	}
	// Name the nearest lineage that owns cp.TXID, skipping hops that only
	// pass through at it, so repeated shares of one old txid (the rollback
	// loop) never stack a pass-through lineage per call.
	baseLineage, err := w.Store.CollapseBase(src.Lineage, cp.TXID)
	if err != nil {
		return "", nil, false, fmt.Errorf("ops: %s: resolving base for lineage %s at txid %d: %w", what, src.Lineage, cp.TXID, err)
	}
	lineage = store.NewLineageID()
	// A base pointer must never land in a store an old (layout v1) binary
	// could still open — its lineage-granular GC would sweep the shared
	// ancestor out from under the new lineage. Bump the manifest first.
	if err := w.Store.EnsureLayoutV2(); err != nil {
		return "", nil, false, fmt.Errorf("ops: %s: %w", what, err)
	}
	bp := store.BasePointer{Lineage: baseLineage, TXID: cp.TXID}
	// The durable per-lineage base object is the resolution source of truth
	// (it outlives the ref if src's branch is destroyed); Ref.Base is only
	// its reporting mirror.
	if err := w.Store.WriteLineageBase(lineage, bp); err != nil {
		return "", nil, false, fmt.Errorf("ops: %s: %w", what, err)
	}
	return lineage, &bp, false, nil
}

// RollbackBackupSuffix names rollback's safety fork of the branch: before
// the repoint lands, the branch's current head is kept as
// <branch>-pre-rollback (a shared fork — two metadata objects, no data
// copy), mirroring PromoteBackupSuffix. One rolling safety fork per branch:
// the next rollback of the same branch replaces it.
const RollbackBackupSuffix = "-pre-rollback"

// RollbackBackupMetaKey marks a branch as rollback's own safety fork; its
// value is the branch it was taken from. Rollback only ever replaces a
// branch at the safety-fork name when it carries this marker — a user's
// branch that merely shares the name is never destroyed.
const RollbackBackupMetaKey = "offshoot.pre-rollback"

// RollbackOptions tunes RollbackWith. The zero value is a plain rollback
// with the safety fork on at DefaultPromoteBackupTTL (reused as rollback's
// default too — there is no separate DefaultRollbackBackupTTL).
type RollbackOptions struct {
	// NoBackup skips the <branch>-pre-rollback safety fork entirely.
	NoBackup bool
	// BackupTTL is the safety fork's TTL; <= 0 means DefaultPromoteBackupTTL.
	// A safety fork always carries a TTL.
	BackupTTL time.Duration
	// Materialize copies the checkpoint into a self-contained lineage
	// instead of pointing at it through a base pointer (see RollbackWith).
	Materialize bool
}

// RollbackResult reports a rollback: the refreshed checkout path, when one
// was minted the safety fork's branch name (empty under NoBackup), and
// whether the new lineage shares the old one's history through a base
// pointer (false: a self-contained copy).
type RollbackResult struct {
	Path   string
	Backup string
	Shared bool
	// BackupIsTarget reports that the branch's head already sat at the
	// target checkpoint, so the safety fork in Backup holds the same
	// committed state the rollback landed on: the rollback discarded only
	// un-checkpointed edits in the checkout, and "undo" has nothing to
	// restore. The CLI words its output accordingly.
	BackupIsTarget bool
}

// Rollback is RollbackWith with defaults: safety fork on, default TTL. Kept
// for the CLI/daemon/MCP call sites and tests that predate RollbackOptions.
func (w *Workspace) Rollback(db, branch, to string) (string, error) {
	res, err := w.RollbackWith(db, branch, to, RollbackOptions{})
	return res.Path, err
}

// RollbackWith repoints db@branch at a NEW lineage seeded from checkpoint
// `to` and re-materializes the fixed checkout path, returning the checkout
// path and, unless NoBackup, the branch's previous head kept first as a
// TTL'd safety fork (see RollbackOptions and RollbackBackupSuffix).
// Checkpoints at or before `to` are kept; later ones are dropped.
//
// Below the snapshot floor (see newLineageAt) the new lineage is a base
// pointer {old lineage, to's txid}: nothing is copied, and every kept
// checkpoint resolves through the pointer (its txid is at or below the
// base txid), so it keeps its recorded epoch and kind. GC's base-spine
// marking pins exactly those objects; the abandoned future above `to` is
// reclaimable once the safety fork is gone. At the floor, or with
// opts.Materialize, `to` and EVERY other kept checkpoint are copied into the
// new self-contained lineage at epoch 1 so they survive the old lineage
// being reaped.
//
// The ref CAS is the point of no return: once it lands, the branch has
// repointed. The checkout refresh that follows (busy probe, materialize,
// fingerprint) is best-effort — a failure there is reported as a partial
// success (repointed, checkout stale) rather than losing that state in a
// plain error.
//
// The busy probe itself is a point-in-time check, not a lock: a connection
// opened between the probe and the materialize rename still holds a stale
// file descriptor. Acceptable for the single-operator local CLI; daemon
// mode (Plan 3) will own the data path and close this gap.
func (w *Workspace) RollbackWith(db, branch, to string, opts RollbackOptions) (RollbackResult, error) {
	if err := store.ValidateName(db); err != nil {
		return RollbackResult{}, err
	}
	if err := store.ValidateName(branch); err != nil {
		return RollbackResult{}, err
	}
	if err := store.ValidateName(to); err != nil {
		return RollbackResult{}, err
	}
	ref, etag, err := w.Store.GetRef(db, branch)
	if err != nil {
		return RollbackResult{}, err
	}
	cp, ok := ref.Checkpoints[to]
	if !ok {
		return RollbackResult{}, errNoCheckpoint(db, branch, to, ref)
	}
	txid := cp.TXID
	backupIsTarget := ref.HeadTXID == txid

	// The safety fork comes after the checkpoint lookup (a bad `to` fails
	// before anything is minted) and before the new lineage is minted, so the branch's current head is durably reachable from its
	// own safety-fork name before the repoint abandons it. safetyFork reads
	// branch's ref itself (Fork never writes the source ref), so `ref`/`etag`
	// captured above stay valid across it — exactly the invariant
	// promoteBackup documents for PromoteWith's tgtEtag.
	var backup string
	if !opts.NoBackup {
		backup, err = w.safetyFork(db, branch, RollbackBackupSuffix, RollbackBackupMetaKey, "rollback", opts.BackupTTL)
		if err != nil {
			return RollbackResult{}, err
		}
	}

	members, err := w.Store.Chain(ref.Lineage, txid)
	if err != nil {
		return RollbackResult{}, fmt.Errorf("ops: resolving chain for lineage %s to txid %d: %w", ref.Lineage, txid, err)
	}
	lineage, base, _, err := w.newLineageAt(ref, cp, members, opts.Materialize, fmt.Sprintf("rollback %s@%s", db, branch))
	if err != nil {
		return RollbackResult{}, err
	}
	kept := map[string]store.Checkpoint{}
	for name, c := range ref.Checkpoints {
		if c.TXID <= txid {
			kept[name] = c
		}
	}
	var cleanup func()
	if base != nil {
		// Shared: every kept checkpoint resolves through the base pointer
		// as recorded, so kept stands unchanged.
		cleanup = func() { w.bestEffortDelete(store.BaseKey(lineage)) }
	} else {
		copiedKeys := []string{store.SnapshotKey(lineage, 1, txid)}
		cleanup = func() {
			for _, k := range copiedKeys {
				w.bestEffortDelete(k)
			}
		}
		// `to`'s own snapshot is already copied above. Copy every OTHER
		// kept checkpoint's snapshot into the new lineage too, so it
		// survives the old lineage being orphaned and later reaped by GC.
		// Every copy lands at epoch 1 in the new lineage — a fresh lineage
		// always starts there — so rewrite each kept checkpoint to epoch 1
		// to match where its object now actually is. Abort and clean up
		// anything already copied before touching the ref if any copy fails.
		done := map[uint64]bool{txid: true}
		for name, c := range kept {
			if !done[c.TXID] {
				done[c.TXID] = true
				key, err := w.copySnapshotIntoLineage(ref, c, lineage)
				if err != nil {
					cleanup()
					return RollbackResult{}, fmt.Errorf("ops: rollback: copying checkpoint snapshot for txid %d: %w", c.TXID, err)
				}
				copiedKeys = append(copiedKeys, key)
			}
			// A location update, not a new checkpoint: CreatedAt/Meta
			// survive unchanged. Kind is not carried over: whatever it was
			// in the old lineage, the copy is a snapshot.
			kept[name] = store.Checkpoint{TXID: c.TXID, Epoch: 1, CreatedAt: c.CreatedAt, Meta: c.Meta, Kind: "snapshot"}
		}
	}

	next := ref
	// Base mirrors the new lineage's base.json exactly: set on the shared
	// path, nil on the copy path — carrying a formerly-shared branch's Base
	// forward would break the invariant that Ref.Base != nil iff
	// base.json(Ref.Lineage) exists.
	next.Lineage, next.Epoch, next.HeadTXID, next.HeadEpoch, next.Checkpoints, next.Base = lineage, 1, txid, 1, kept, base
	// A repoint is itself a revocation: the old holder is already fenced (its
	// epoch no longer matches), but carrying its lease forward would leave a
	// fresh acquirer refused ErrLeaseHeld by a holder that can never renew —
	// stuck until the stale TTL lapses. Clear the lease so the branch is
	// immediately acquirable post-repoint.
	next.LeaseHolder, next.LeaseExpiry = "", ""
	next.Touch(time.Now())
	if _, err := w.Store.PutRef(db, branch, next, etag); err != nil {
		cleanup()
		return RollbackResult{}, fmt.Errorf("ops: rollback lost a race (retry): %w", err)
	}

	// The branch has repointed. Everything below is a best-effort refresh of
	// the local checkout; any failure here must not read as if the rollback
	// itself failed.
	path := w.CheckoutPath(db, branch)
	refresh := func() error {
		if _, err := os.Stat(path); err == nil {
			if err := quiesce(path); err != nil {
				return err
			}
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		checksum, chain, err := w.refreshFromChain(db, next, path)
		if err != nil {
			return err
		}
		// The checkout now equals committed state: refresh the fingerprint
		// (identity too, since this repointed to a new lineage) so a later
		// Fork sees it as clean rather than stale.
		if err := writeSum(path, next.Lineage, next.HeadEpoch, txid, checksum, chain); err != nil {
			return err
		}
		refreshShadow(path)
		return nil
	}
	if ObserveRollback != nil {
		ObserveRollback(base != nil)
	}
	if err := refresh(); err != nil {
		return RollbackResult{}, fmt.Errorf("ops: branch repointed to checkpoint %q (txid %d), but the checkout could not be refreshed (run 'offshoot checkout' to re-materialize): %w", to, txid, err)
	}
	return RollbackResult{Path: path, Backup: backup, Shared: base != nil, BackupIsTarget: backupIsTarget}, nil
}

// Promote repoints db@target at a NEW lineage seeded from db@source's head
// (promote-as-fork, spec § Promote). Requires --force for protected targets
// (force param). Source branch survives unchanged. Target's old lineage is
// orphaned. Target's checkout (if any) is re-materialized after a busy
// probe. Target's checkpoint map is reset to {"promote": txid}.
//
// The busy probe is a point-in-time check, not a lock: a connection opened
// between the probe and the materialize rename still holds a stale file
// descriptor. Acceptable for the single-operator local CLI; daemon mode
// (Plan 3) will own the data path and close this gap.
// PromoteBackupSuffix names promote's safety fork of the target: before the
// repoint lands, the target's current head is kept as <target>-pre-promote
// (a shared fork — two metadata objects, no data copy) so the one verb whose
// inverse the user otherwise had to build by hand ("fork main first, then
// promote") builds it itself. One rolling safety fork per target: the next
// promote onto the same target replaces it, which is what keeps both the
// branch namespace and the old lineage's pinned storage bounded.
const PromoteBackupSuffix = "-pre-promote"

// PromoteBackupMetaKey marks a branch as promote's own safety fork; its
// value is the target branch it was taken from. Promote only ever replaces
// a branch at the safety-fork name when it carries this marker — a user's
// branch that merely shares the name is never destroyed.
const PromoteBackupMetaKey = "offshoot.pre-promote"

// DefaultPromoteBackupTTL bounds how long a safety fork (and the abandoned
// lineage its base pointer keeps alive) survives when the caller sets no
// BackupTTL. It matches the MCP fork default: an undo window, not an
// archive — fork explicitly to keep old state indefinitely.
const DefaultPromoteBackupTTL = 24 * time.Hour

// PromoteOptions tunes PromoteWith. The zero value is a plain promote with
// the safety fork on at DefaultPromoteBackupTTL.
type PromoteOptions struct {
	// Force overrides the protected-target refusal.
	Force bool
	// NoBackup skips the <target>-pre-promote safety fork entirely.
	NoBackup bool
	// BackupTTL is the safety fork's TTL; <= 0 means DefaultPromoteBackupTTL.
	// A safety fork always carries a TTL.
	BackupTTL time.Duration
	// Materialize copies the source head into a self-contained lineage
	// instead of pointing at it through a base pointer (see PromoteWith).
	Materialize bool
}

// PromoteResult reports a promote: the promoted txid and, when one was
// minted, the safety fork's branch name (empty under NoBackup, and when the
// source itself IS the target's safety fork — the undo path — since minting
// one would mean replacing the very branch being promoted).
type PromoteResult struct {
	TXID   uint64
	Backup string
	// Shared reports that target's new lineage points at source's history
	// through a base pointer (false: a self-contained copy).
	Shared bool
}

// Promote is PromoteWith with only Force settable: safety fork on, default
// TTL. Kept for the CLI/daemon/MCP call sites and tests that predate
// PromoteOptions.
func (w *Workspace) Promote(db, source, target string, force bool) (uint64, error) {
	res, err := w.PromoteWith(db, source, target, PromoteOptions{Force: force})
	return res.TXID, err
}

// safetyFork mints (or replaces) branch's safety fork <branch><suffix>. It
// is the shared helper behind both Promote's <target>-pre-promote and
// Rollback's <branch>-pre-rollback: verb names the caller in every error
// (so "not a promote safety fork" / "not a rollback safety fork" reads
// naturally) and metaKey is that caller's own marker key. It replaces a
// branch at the safety-fork name only when it carries meta[metaKey]==branch
// — a user's branch that merely shares the name is never destroyed, and the
// replace refuses instead. An unforced Destroy of the previous safety fork
// is deliberate: a live lease on it (someone is working in it) refuses
// rather than pulling the branch out from under them. It never touches
// branch's own ref — a caller's etag from an earlier GetRef(db, branch)
// stays valid across it, since Fork only ever writes the NEW branch's ref.
func (w *Workspace) safetyFork(db, branch, suffix, metaKey, verb string, ttl time.Duration) (string, error) {
	name := branch + suffix
	if err := store.ValidateName(name); err != nil {
		return "", fmt.Errorf("ops: %s: cannot name the safety fork of %s@%s (pass --no-backup to skip it): %w", verb, db, branch, err)
	}
	if ttl <= 0 {
		ttl = DefaultPromoteBackupTTL
	}
	existing, _, err := w.Store.GetRef(db, name)
	switch {
	case err == nil:
		if existing.Meta[metaKey] != branch {
			return "", fmt.Errorf("ops: %s: %s@%s already exists and is not a %s safety fork of %s@%s; destroy or rename it, or pass --no-backup", verb, db, name, verb, db, branch)
		}
		// Ours from an earlier call: replace it. The unforced Destroy above
		// most commonly fails on a live lease (someone has a session open on
		// the previous fork); its error text says "use --force" for a CLI
		// caller, but neither promote nor rollback exposes a --force lever
		// for THIS replacement (force only ever overrides the branch's OWN
		// protected/lease check, never the safety fork's), so that phrase
		// would misdirect a caller here — replace it before wrapping.
		if err := w.Destroy(db, name, false); err != nil {
			msg := strings.Replace(err.Error(), "use --force", "ask the human", 1)
			return "", fmt.Errorf("ops: %s: replacing the previous safety fork %s@%s: %s (close that session, or pass --no-backup)",
				verb, db, name, msg)
		}
	case errors.Is(err, store.ErrNotFound):
	default:
		return "", err
	}
	if _, err := w.Fork(db, branch, name, "", ttl, map[string]string{metaKey: branch}); err != nil {
		return "", fmt.Errorf("ops: %s: safety fork of %s@%s: %w", verb, db, branch, err)
	}
	return name, nil
}

// promoteBackup mints (or replaces) the target's safety fork. It runs after
// the protected check and before the repoint, and never touches the
// target's own ref — the caller's tgtEtag stays valid across it.
func (w *Workspace) promoteBackup(db, target string, ttl time.Duration) (string, error) {
	return w.safetyFork(db, target, PromoteBackupSuffix, PromoteBackupMetaKey, "promote", ttl)
}

// PromoteWith repoints target at a new lineage seeded from source's head,
// keeping target's previous head as a safety fork first unless opted out —
// see PromoteOptions and PromoteBackupSuffix. Below the snapshot floor the
// new lineage is a base pointer {source's lineage, head txid} and nothing is
// copied (source's base.json-reachable history outlives source's ref); at
// the floor, or with opts.Materialize, the head is copied into a
// self-contained lineage — see newLineageAt.
func (w *Workspace) PromoteWith(db, source, target string, opts PromoteOptions) (PromoteResult, error) {
	force := opts.Force
	if source == target {
		return PromoteResult{}, fmt.Errorf("ops: cannot promote a branch onto itself")
	}
	if err := store.ValidateName(db); err != nil {
		return PromoteResult{}, err
	}
	if err := store.ValidateName(source); err != nil {
		return PromoteResult{}, err
	}
	if err := store.ValidateName(target); err != nil {
		return PromoteResult{}, err
	}
	src, _, err := w.Store.GetRef(db, source)
	if err != nil {
		return PromoteResult{}, err
	}
	w.warnIfUncheckpointed(db, source, src, "promoting last committed state")
	tgt, tgtEtag, err := w.Store.GetRef(db, target)
	if err != nil {
		return PromoteResult{}, err
	}
	if tgt.Protected && !force {
		return PromoteResult{}, fmt.Errorf("ops: %s@%s is protected; use --force", db, target)
	}
	// The safety fork comes after every refusal above (an unforced promote
	// onto a protected target mints nothing) and before the repoint, so the
	// target's current head is durably reachable from its own branch name
	// before anything abandons it.
	var backup string
	if !opts.NoBackup && source != target+PromoteBackupSuffix {
		backup, err = w.promoteBackup(db, target, opts.BackupTTL)
		if err != nil {
			return PromoteResult{}, err
		}
	}
	cp := headCheckpoint(src)
	txid := cp.TXID
	members, err := w.Store.Chain(src.Lineage, txid)
	if err != nil {
		return PromoteResult{}, fmt.Errorf("ops: resolving chain for lineage %s to txid %d: %w", src.Lineage, txid, err)
	}
	lineage, base, _, err := w.newLineageAt(src, cp, members, opts.Materialize, fmt.Sprintf("promote %s@%s", db, target))
	if err != nil {
		return PromoteResult{}, err
	}
	result := PromoteResult{TXID: txid, Backup: backup, Shared: base != nil}
	next := tgt
	// Base mirrors the new lineage's base.json, exactly as in Rollback's
	// repoint (see the comment there for the invariant).
	next.Lineage, next.Epoch, next.HeadTXID, next.HeadEpoch, next.Base = lineage, 1, txid, 1, base
	next.Checkpoints = nil
	next.SetCheckpoint("promote", store.Checkpoint{TXID: txid, Epoch: 1, CreatedAt: nowStamp()})
	next.Parent = fmt.Sprintf("%s@%s@%d", db, source, txid)
	// A repoint is itself a revocation: the old holder is already fenced (its
	// epoch no longer matches), but carrying its lease forward would leave a
	// fresh acquirer refused ErrLeaseHeld by a holder that can never renew —
	// stuck until the stale TTL lapses. Clear the lease so the branch is
	// immediately acquirable post-repoint.
	next.LeaseHolder, next.LeaseExpiry = "", ""
	next.Touch(time.Now())
	if _, err := w.Store.PutRef(db, target, next, tgtEtag); err != nil {
		if base != nil {
			w.bestEffortDelete(store.BaseKey(lineage))
		} else {
			w.bestEffortDelete(store.SnapshotKey(lineage, 1, txid))
		}
		return PromoteResult{}, fmt.Errorf("ops: promote lost a race (retry): %w", err)
	}
	if ObservePromote != nil {
		ObservePromote(base != nil)
	}
	// Refresh the target checkout if one exists and is quiescible.
	path := w.CheckoutPath(db, target)
	if _, err := os.Stat(path); err == nil {
		if err := quiesce(path); err != nil {
			return result, fmt.Errorf("ops: promoted, but checkout %s is in use and was NOT refreshed: %w", path, err)
		}
		checksum, chain, err := w.refreshFromChain(db, next, path)
		if err != nil {
			return result, fmt.Errorf("ops: promoted, but checkout %s could not be refreshed: %w", path, err)
		}
		// The checkout now equals committed state: refresh the fingerprint
		// (identity too, since this repointed to a new lineage) so a later
		// Fork sees it as clean rather than stale.
		if err := writeSum(path, next.Lineage, next.HeadEpoch, txid, checksum, chain); err != nil {
			return result, fmt.Errorf("ops: promoted, but checkout %s could not be refreshed: %w", path, err)
		}
		refreshShadow(path)
	}
	return result, nil
}

// compactBeforeCASForTest, when non-nil, runs between Compact's snapshot
// copy and its ref CAS — the window a concurrent ref write (a flush, a
// touch) races. Test-only; process-global, restore via t.Cleanup, same
// rules as forkSlowPathForTest.
var compactBeforeCASForTest func()

// Compact turns a shared fork back into a self-contained lineage — the
// manual cord-cutter. The branch's full base-following chain at head is
// re-encoded as ONE snapshot in a fresh lineage (copySnapshotToNewLineage,
// exactly Promote's materialize step) and the ref is CAS-repointed at it
// with Base cleared. The old lineage — including its base.json — is then
// unreferenced by this branch, so once nothing else reads through it,
// reachability GC reclaims it (and, transitively, whatever of the
// ancestor's storage only this branch was keeping alive). No explicit
// old-object deletion happens here; GC owns reclaim.
//
// A branch whose lineage has an empty DURABLE base spine
// (store.BaseSpine — the base.json chain, the resolution source of truth,
// not the ref's reporting mirror) is already self-contained: compact is a
// NO-OP returning the current head txid — the postcondition already
// holds, and erroring would make scripted "compact everything" loops fail
// on exactly the branches that need nothing done.
//
// Checkpoints are PRESERVED, Rollback-style: every existing checkpoint
// qualifies (every c.TXID <= head, by construction — compact never drops
// history), so each one's snapshot is copied into the new self-contained
// lineage and rewritten to epoch 1 (CreatedAt/Meta preserved) exactly as
// Rollback does for its kept map, and a "compact" checkpoint at the head
// txid is ADDED alongside them (not a replacement) — old checkpoints
// anchored on the shared ancestor would not otherwise resolve once the
// old lineage is later reclaimed by GC. Cost: one snapshot copy per
// distinct checkpoint txid (checkpoints sharing a txid share a copy, as
// Rollback's `done` set does for its head).
//
// The ref CAS is the point of no return, exactly as in Promote: a CAS
// loss (a concurrent flush advanced the head) deletes the orphan snapshot
// and returns a retry error — no internal retry loop, which would orphan
// a lineage per attempt. The checkout refresh that follows is best-effort
// and reports partial success on failure, same as Promote.
func (w *Workspace) Compact(db, branch string) (uint64, error) {
	if err := store.ValidateName(db); err != nil {
		return 0, err
	}
	if err := store.ValidateName(branch); err != nil {
		return 0, err
	}
	ref, etag, err := w.Store.GetRef(db, branch)
	if err != nil {
		return 0, err
	}
	// The no-op decision consults the DURABLE base spine (base.json chain),
	// not the ref's Base mirror: compact repoints the branch at a brand-new
	// lineage, so it must be authoritative even if some code path left a
	// stale mirror behind — a stale non-nil mirror on a genuinely
	// self-contained branch must not trigger a needless materialize (and,
	// were checkpoints ever reset instead of preserved, a needless wipe).
	// An empty spine means resolution never leaves this lineage: already
	// self-contained, nothing to cut.
	spine, err := w.Store.BaseSpine(ref.Lineage)
	if err != nil {
		return 0, fmt.Errorf("ops: compact %s@%s: resolving base spine: %w", db, branch, err)
	}
	if len(spine) == 0 {
		// Already self-contained: no-op, see the doc comment.
		return ref.HeadTXID, nil
	}
	cp := headCheckpoint(ref)
	txid := cp.TXID
	lineage, _, err := w.copySnapshotToNewLineage(ref, cp)
	if err != nil {
		return 0, err
	}
	copiedKeys := []string{store.SnapshotKey(lineage, 1, txid)}
	cleanup := func() {
		for _, k := range copiedKeys {
			w.bestEffortDelete(k)
		}
	}

	// Every existing checkpoint qualifies (every c.TXID <= head, by
	// construction — compact never drops history), exactly as Rollback's
	// kept map does for checkpoints at or before its target. Copy each
	// one's snapshot into the new lineage (the head's copy above is reused
	// for any checkpoint that already sits at head — the `done` set mirrors
	// Rollback's) and rewrite it to epoch 1 (where the copy now actually
	// lives) while preserving CreatedAt/Meta, a location update rather than
	// a new checkpoint.
	kept := map[string]store.Checkpoint{}
	for name, c := range ref.Checkpoints {
		kept[name] = c
	}
	done := map[uint64]bool{txid: true}
	for name, c := range kept {
		if !done[c.TXID] {
			done[c.TXID] = true
			key, err := w.copySnapshotIntoLineage(ref, c, lineage)
			if err != nil {
				cleanup()
				return 0, fmt.Errorf("ops: compact: copying checkpoint snapshot for txid %d: %w", c.TXID, err)
			}
			copiedKeys = append(copiedKeys, key)
		}
		kept[name] = store.Checkpoint{TXID: c.TXID, Epoch: 1, CreatedAt: c.CreatedAt, Meta: c.Meta, Kind: "snapshot"}
	}
	kept["compact"] = store.Checkpoint{TXID: txid, Epoch: 1, CreatedAt: nowStamp(), Kind: "snapshot"}

	next := ref
	next.Lineage, next.Epoch, next.HeadTXID, next.HeadEpoch = lineage, 1, txid, 1
	next.Base = nil
	next.Checkpoints = kept
	// A repoint is itself a revocation — same reasoning as Promote: clear
	// the lease so the branch is immediately acquirable post-repoint.
	next.LeaseHolder, next.LeaseExpiry = "", ""
	next.Touch(time.Now())
	if compactBeforeCASForTest != nil {
		compactBeforeCASForTest()
	}
	if _, err := w.Store.PutRef(db, branch, next, etag); err != nil {
		cleanup()
		return 0, fmt.Errorf("ops: compact lost a race (retry): %w", err)
	}
	// Refresh the checkout if one exists and is quiescible.
	path := w.CheckoutPath(db, branch)
	if _, err := os.Stat(path); err == nil {
		if err := quiesce(path); err != nil {
			return txid, fmt.Errorf("ops: compacted, but checkout %s is in use and was NOT refreshed: %w", path, err)
		}
		checksum, chain, err := w.refreshFromChain(db, next, path)
		if err != nil {
			return txid, fmt.Errorf("ops: compacted, but checkout %s could not be refreshed: %w", path, err)
		}
		// The checkout now equals committed state: refresh the fingerprint
		// (identity too, since this repointed to a new lineage) so a later
		// Fork sees it as clean rather than stale.
		if err := writeSum(path, next.Lineage, next.HeadEpoch, txid, checksum, chain); err != nil {
			return txid, fmt.Errorf("ops: compacted, but checkout %s could not be refreshed: %w", path, err)
		}
		refreshShadow(path)
	}
	return txid, nil
}
