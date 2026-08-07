// Package ops implements offshoot's branch lifecycle operations over a
// store.Backend: create, checkout, checkpoint, fork, rollback, promote,
// destroy, and GC. Plan-2 scope is CLI/at-rest mode: full-snapshot
// checkpoints, fixed checkout paths, no daemon.
package ops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
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
	"github.com/offshoot-db/offshoot/internal/dbfile"
	"github.com/offshoot-db/offshoot/internal/ltxio"
	"github.com/offshoot-db/offshoot/internal/store"
)

type Workspace struct {
	Store *store.Store
	Root  string
	Spec  string
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
	if err := store.ValidateName(db); err != nil {
		return "", "", err
	}
	if err := store.ValidateName(branch); err != nil {
		return "", "", err
	}
	return db, branch, nil
}

func (w *Workspace) CheckoutPath(db, branch string) string {
	return filepath.Join(w.Root, "checkouts", db, branch+".db")
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
	ref.SetCheckpoint("init", store.Checkpoint{TXID: 1, Epoch: 1, CreatedAt: nowStamp()})
	if _, err := w.Store.PutRef(db, "main", ref, ""); err != nil {
		// Freshly-minted lineage no rival can reference: safe to delete the
		// orphaned snapshot (mirrors Fork's cleanup on the same failure).
		w.Store.B.Delete(store.SnapshotKey(lineage, 1, 1))
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
			return err
		}
	}
	conn, err := sql.Open("sqlite3", cp)
	if err != nil {
		return err
	}
	if _, err := conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		conn.Close()
		return err
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

// CheckoutResult is CheckoutProven's return value: the materialized path,
// plus the proof session.Open's settling-flush suppression needs — see
// Clean and PostApplyChecksum.
type CheckoutResult struct {
	Path string
	// Clean reports whether this checkout was ALREADY byte-identical to
	// ref's CURRENT head when this call ran (the sidecar "clean" fast path
	// below) rather than freshly (re)materialized. When true, Ref is the
	// exact head identity (lineage, HeadEpoch, HeadTXID) this checkout is
	// now proven to equal.
	//
	// The proof is the sidecar's SHA-256 content hash matching the file's
	// actual current bytes (checkoutState's "clean" verdict) AND the
	// sidecar's recorded identity matching a GetRef read of the CURRENT
	// ref, both already computed below at no extra cost — Clean adds no
	// store read of its own.
	//
	// See Session.Open / Session.rebaseline's doc comment for how the
	// session package consumes this to decide whether its startup settling
	// flush can be safely skipped.
	Clean bool
	Ref   store.Ref
	// PostApplyChecksum is the LTX postApplyChecksum this checkout's
	// content is known to embody — sumRecord.PostApplyChecksum, read
	// straight out of the sidecar checkoutState already parsed to decide
	// Clean, at no extra cost (no store read, no re-hash). Valid (non-zero;
	// 0 means "absent", matching every other zero-means-absent LTX checksum
	// convention in this codebase — see EncodeSegment's own preApplyChecksum/
	// postApplyChecksum == 0 checks) only when Clean is true AND the
	// sidecar that proved it was itself stamped by code new enough to
	// record one (see writeSum/StampSum's postApplyChecksum parameter); an
	// older-format sidecar, or one written before this field existed, has
	// PostApplyChecksum == 0 here even when Clean is true.
	//
	// Trusting a checksum read from a LOCAL file, rather than fetched fresh
	// from the store on every Open, is sound under exactly the same guard
	// Clean itself already required, not a new hazard: checkoutState only
	// ever reports "clean" after confirming the sidecar's recorded
	// (lineage, epoch, txid) identity still matches the CURRENT ref (see
	// checkoutState's doc comment) — a sidecar whose branch has since moved
	// (rollback/promote, or any other repoint) already falls to "stale",
	// never "clean", before this field is ever consulted. So whatever
	// checksum a "clean" sidecar carries necessarily describes the SAME
	// (lineage, epoch, txid) that identity check just re-verified as
	// current — no more, no less.
	//
	// See Session.Open / Session.rebaseline's doc comment for how the
	// settling-flush suppression uses this to avoid a store round trip
	// (and, worse, a full-object DOWNLOAD when the head is a snapshot) on
	// every single Open.
	PostApplyChecksum uint64
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
	checksum, err := w.materializeAt(ref, headCheckpoint(ref), path)
	if err != nil {
		return CheckoutResult{}, err
	}
	if err := writeSum(path, ref.Lineage, ref.HeadEpoch, ref.HeadTXID, checksum); err != nil {
		return CheckoutResult{}, err
	}
	return CheckoutResult{Path: path, Clean: false, Ref: ref}, nil
}

// StampSum writes path's .sum sidecar directly from a hash the caller
// already obtained independently, skipping writeSum's own read-and-hash of
// path entirely — see sumRecord's doc comment for the on-disk shape.
// postApplyChecksum is optional (0 = absent, matching sumRecord's own
// zero-means-absent convention — see its doc comment); Session.
// commitSidecarRefresh, this function's one caller, always has one (its
// last successful flush's checksumAtEncode, tracked as s.flushChecksum).
//
// Exported for Session.Close's clean-close sidecar refresh (M2 follow-up):
// Session.commitSidecarRefresh has the capture engine's own post-shutdown
// MainHash fingerprint (capture.State — a SHA-256 of the checkout's main
// file, computed by the engine itself immediately after its shutdown
// verified everything captured was cleanly folded in, see that type's doc
// comment) and reuses that number directly rather than re-deriving one.
// That is a deliberate choice, not just an optimization: an independent
// hash pass here would need its own fresh SQLite connection on path, and
// this checkout may still have the capture engine's own connection state
// nearby in the same process — see commitSidecarRefresh's doc comment for
// why trusting the engine's already-verified fingerprint, rather than
// re-deriving one, is what keeps this call site off that hazard (and, as a
// second-order benefit, off the risk of folding in content the engine's own
// shutdown verification specifically refused to vouch for).
func StampSum(path, hash, lineage string, epoch, txid, postApplyChecksum uint64) error {
	data, err := json.Marshal(sumRecord{
		Hash: hash, Lineage: lineage, Epoch: epoch, TXID: txid, PostApplyChecksum: postApplyChecksum,
	})
	if err != nil {
		return err
	}
	return os.WriteFile(path+".sum", data, 0o644)
}

// sumRecord is the on-disk shape of a checkout's .sum sidecar: a content hash
// plus the ref identity (lineage + epoch + txid) the checkout embodied at the
// moment the sidecar was written. Recording identity (not just a bare hash)
// is what lets checkoutState tell apart a checkout with local edits from one
// whose branch ref moved out from under it without a refresh.
//
// Epoch is part of that identity, not decoration: Chain resolution
// (store.keepHighestEpoch) exists precisely because a fenced-out writer's
// orphaned object can share a TXID with the live one under a different
// epoch, so lineage+txid alone does not uniquely identify committed content
// — epoch does. Without comparing it here, checkoutState's "clean" verdict
// would rest on an unenforced cross-module assumption that HeadEpoch only
// ever changes in lockstep with Lineage/TXID; a future change to that
// invariant would make the fast path in Checkout serve stale bytes as
// current.
//
// PostApplyChecksum is the LTX rolling checksum (see ltxio.ChecksumDatabase)
// the checkout's content embodies at (Lineage, Epoch, TXID) above — omitted
// (zero value) from the JSON on disk when absent, via `omitempty`, so a
// pre-this-field sidecar decodes with it simply at 0, and callers already
// treat 0 as "no checksum available" (see CheckoutResult.PostApplyChecksum).
// Recording this is what lets Session.Open's settling-flush suppression
// read a trustworthy checksum straight from this local file instead of
// fetching (and, when the head is a snapshot, fully downloading) the head
// object from the store on every single Open — see CheckoutResult's doc
// comment for exactly why trusting it is safe under the SAME identity guard
// this whole sidecar mechanism already enforces, not a new one.
type sumRecord struct {
	Hash              string `json:"hash"`
	Lineage           string `json:"lineage"`
	Epoch             uint64 `json:"epoch"`
	TXID              uint64 `json:"txid"`
	PostApplyChecksum uint64 `json:"post_apply_checksum,omitempty"`
}

// writeSum computes the hex SHA-256 of the file at path and writes it, along
// with the (lineage, epoch, txid) ref identity the checkout currently
// embodies and its LTX postApplyChecksum (0 if the caller doesn't have one
// handy — see sumRecord's doc comment), to path + ".sum". This is the
// checkout fingerprint: it records what the checkout file looked like, and
// which branch state it was, at the moment it was last known to equal a
// committed state (fresh materialize, a successful checkpoint encode, or a
// post-repoint refresh). Callers pass the ref's HeadEpoch (the epoch the
// checkout's current head was written under), not the ref's own
// (writer-generation) Epoch — see sumRecord's doc comment.
func writeSum(path string, lineage string, epoch, txid, postApplyChecksum uint64) error {
	sum, err := fileSum(path)
	if err != nil {
		return err
	}
	data, err := json.Marshal(sumRecord{
		Hash: sum, Lineage: lineage, Epoch: epoch, TXID: txid, PostApplyChecksum: postApplyChecksum,
	})
	if err != nil {
		return err
	}
	return os.WriteFile(path+".sum", data, 0o644)
}

// checkoutState reports how the checkout at path relates to ref, and — only
// when the verdict is "clean" — the sidecar's own recorded
// PostApplyChecksum (0 for every other verdict, and for a "clean" verdict
// against an older-format sidecar that never recorded one; see
// CheckoutResult.PostApplyChecksum for how callers are expected to treat a
// zero value):
//
//   - "clean": the sidecar's recorded identity (lineage, epoch, txid)
//     matches ref, and the file's content still matches the recorded hash.
//   - "modified": the sidecar's recorded identity matches ref, but the
//     file's content has changed since it was last fingerprinted — local,
//     un-checkpointed edits.
//   - "stale": the sidecar's recorded identity no longer matches ref — the
//     branch was repointed (rollback/promote with a skipped refresh) since
//     this checkout was last materialized or checkpointed. A sidecar
//     written before Epoch was tracked (or any other record whose Epoch
//     doesn't decode to ref.HeadEpoch) falls here too: zero-value Epoch
//     never matches a real ref's HeadEpoch (always >= 1), so an old-format
//     sidecar reads as stale rather than being trusted as clean.
//   - "unknown": no sidecar, or one that isn't a valid current-format
//     record (including legacy bare-hash sidecars predating this fix, and
//     corrupt files). Provenance can't be determined, so callers should
//     stay silent rather than warn spuriously.
func checkoutState(path string, ref store.Ref) (string, uint64) {
	raw, err := os.ReadFile(path + ".sum")
	if err != nil {
		return "unknown", 0
	}
	var rec sumRecord
	if err := json.Unmarshal(raw, &rec); err != nil || rec.Hash == "" {
		return "unknown", 0
	}
	if rec.Lineage != ref.Lineage || rec.Epoch != ref.HeadEpoch || rec.TXID != ref.HeadTXID {
		return "stale", 0
	}
	got, err := fileSum(path)
	if err != nil {
		return "unknown", 0
	}
	if got != rec.Hash {
		return "modified", 0
	}
	return "clean", rec.PostApplyChecksum
}

// fileSum is the SHA-256 of a checkout file's bytes, used by checkoutState
// to tell "clean" from "modified".
//
// It reads through internal/dbfile rather than os.Open/defer Close, and that
// is load-bearing rather than stylistic. path here is a live checkout, and a
// session's capture engine may hold SQLite connections on it in this same
// process. POSIX advisory locks are keyed by (process, inode), so an ordinary
// open/close of this file would drop every lock this process holds on it —
// silently, with no error, and without SQLite noticing or re-acquiring —
// leaving that engine running unlocked until a foreign writer's close-time
// checkpoint folds and unlinks the WAL out from under it. See dbfile's
// package comment for the full mechanism.
//
// It is tempting to argue this is unreachable because the only caller path
// (warnIfUncheckpointed) runs quiesce first, and quiesce fails busy against
// an engine that holds its read lock — verified empirically. That argument
// is a race, not an invariant: the engine releases and re-takes that lock
// around every takeover() and rebase(), so a quiesce landing in one of those
// windows succeeds and this function then runs against an engine that has
// since re-locked. Do not reintroduce a bare os.Open here on the strength of
// the quiesce guard.
func fileSum(path string) (string, error) {
	r, err := dbfile.Reader(path)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// materializeAt writes the state identified by cp into dst, and returns its
// checksum (see materializeChainAt). It is a thin wrapper over
// materializeChainAt (see materialize.go), which resolves the full
// snapshot+segment chain rather than assuming cp's txid is itself a
// snapshot: every caller here (Checkout, Rollback's refresh, Promote's
// refresh, copySnapshotIntoLineage's read side) picks that up unchanged.
func (w *Workspace) materializeAt(ref store.Ref, cp store.Checkpoint, dst string) (uint64, error) {
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

// Checkpoint snapshots the current checkout state as a named checkpoint.
// Plan-2 (CLI/at-rest) semantics: full-snapshot encode; requires the
// checkout to be quiescible (busy timeout 3s, then clean failure).
//
// NOT SAFE against a live in-process session's checkout: it calls
// ltxio.EncodeSnapshot, which raw-opens (and closes) the checkout path, and
// that close drops every SQLite lock this process holds on it — the POSIX
// (process, inode) lock-drop hazard, see internal/dbfile. Today only the CLI
// (cmd/offshoot) and MCP (internal/mcp) reach this, both of which are
// separate processes from the daemon that runs sessions, so no in-process
// session can be holding that checkout. The daemon conspicuously has no
// checkpoint op; if one is ever added it MUST NOT call this directly —
// route the snapshot through the session's own engine, or through dbfile.
//
// meta (nil = none) is a small string->string map describing this specific
// checkpoint (e.g. eval run id, git SHA, agent id), capped by ValidateMeta
// and stored on the checkpoint's own store.Checkpoint.Meta — not on the
// branch's Ref.Meta, which Fork's meta param sets instead. Rejected (before
// any store I/O) if it exceeds the caps.
func (w *Workspace) Checkpoint(db, branch, name string, meta map[string]string) (uint64, error) {
	if err := store.ValidateName(db); err != nil {
		return 0, err
	}
	if err := store.ValidateName(branch); err != nil {
		return 0, err
	}
	if err := store.ValidateName(name); err != nil {
		return 0, err
	}
	if err := ValidateMeta(meta); err != nil {
		return 0, err
	}
	ref, etag, err := w.Store.GetRef(db, branch)
	if err != nil {
		return 0, err
	}
	if _, exists := ref.Checkpoints[name]; exists {
		return 0, fmt.Errorf("ops: checkpoint %q already exists on %s@%s", name, db, branch)
	}
	path := w.CheckoutPath(db, branch)
	if _, err := os.Stat(path); err != nil {
		return 0, fmt.Errorf("ops: no checkout for %s@%s (run checkout first): %w", db, branch, err)
	}
	if err := quiesce(path); err != nil {
		return 0, err
	}
	txid := ref.HeadTXID + 1
	var buf bytes.Buffer
	checksum, err := ltxio.EncodeSnapshot(path, txid, &buf)
	if err != nil {
		return 0, err
	}
	snapKey := store.SnapshotKey(ref.Lineage, ref.Epoch, txid)
	if _, err := w.Store.B.PutIf(snapKey, buf.Bytes(), ""); err != nil {
		if !errors.Is(err, store.ErrCAS) {
			return 0, err
		}
		// An object already lives at this deterministic key. HeadTXID only
		// ever advances via a successful ref write, and nothing is written
		// under a txid beyond HeadTXID+1 until that happens, so nothing can
		// legitimately reference this key yet: it can only be (a) an orphan
		// left by a crashed prior Checkpoint attempt (snapshot uploaded, ref
		// write never landed), or (b) a rival Checkpoint call racing on this
		// same branch right now, which computed the identical HeadTXID+1 and
		// simply lost the write here. Overwriting is benign under either
		// cause: both writers quiesced and encoded the SAME checkout file at
		// the SAME pre-checkpoint state, so whichever encoding ends up
		// stored is content-equivalent, and only one of the two racers can
		// possibly win the PutRef CAS below to ever reference this key by
		// name — there is no rival ref left dangling by the overwrite. Safe
		// to overwrite unconditionally and proceed.
		if err := w.Store.B.Put(snapKey, buf.Bytes()); err != nil {
			return 0, err
		}
	}
	ref.HeadTXID = txid
	ref.HeadEpoch = ref.Epoch
	ref.SetCheckpoint(name, store.Checkpoint{TXID: txid, Epoch: ref.Epoch, CreatedAt: nowStamp(), Meta: meta})
	ref.Touch(time.Now())
	if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
		// Decide whether to clean up the snapshot after PutRef failure.
		// Unlike Fork/Rollback/Promote (which delete keys in freshly-minted
		// lineages no rival can reference), we must gate cleanup on the error
		// type: under concurrent Checkpoint calls on the same branch, every
		// racer computes the same deterministic txid (HeadTXID+1) and snapKey.
		//
		// On ErrCAS (lost the CAS race): serialization via PutIf means the
		// winner's ref is already visible, so the snapshot is confirmed an
		// orphan left by a crashed prior Checkpoint — safe to delete so a
		// retry's create-only put doesn't wedge behind our orphan.
		//
		// On non-CAS errors (lock timeout, I/O error): deletion is UNSAFE.
		// A concurrent checkpointer may already be mid-write, and we can't
		// tell from here whether they've landed their ref yet. Deleting would
		// rip the snapshot out from under them, leaving a checkpoint recorded
		// in their ref with no backing object — silent corruption for them.
		// Leave the object alone; worst case it's a harmless orphan for a
		// later GC pass.
		if errors.Is(err, store.ErrCAS) {
			if cur, _, gerr := w.Store.GetRef(db, branch); gerr == nil && (cur.Lineage != ref.Lineage || cur.HeadTXID < txid) {
				w.Store.B.Delete(snapKey)
			}
			return 0, fmt.Errorf("ops: ref update lost a race (retry): %w", err)
		}
		return 0, fmt.Errorf("ops: ref update for checkpoint %q on %s@%s: %w", name, db, branch, err)
	}
	// The ref CAS is the point of no return: only now does the checkout truly
	// equal committed state (lineage unchanged, head advanced to txid).
	// Writing the sidecar here, after the CAS, means an interrupt between the
	// encode above and this point leaves the OLD sidecar in place — which
	// still correctly describes the checkout's actual (pre-checkpoint)
	// identity, rather than claiming a commit that never landed.
	if err := writeSum(path, ref.Lineage, ref.HeadEpoch, txid, checksum); err != nil {
		return 0, fmt.Errorf("ops: checkpoint %q committed (txid %d), but the checkout fingerprint could not be refreshed: %w", name, txid, err)
	}
	return txid, nil
}

// quiesce checkpoints the WAL fully, failing cleanly on a busy database.
func quiesce(path string) error {
	conn, err := sql.Open("sqlite3", path+"?_busy_timeout=3000")
	if err != nil {
		return err
	}
	defer conn.Close()
	var busy, logN, ckptN int
	if err := conn.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logN, &ckptN); err != nil {
		return fmt.Errorf("ops: checkpoint: %w", err)
	}
	if busy != 0 {
		return fmt.Errorf("ops: database is busy (live writer or reader); close connections and retry")
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
func (w *Workspace) warnIfUncheckpointed(db, branch string, ref store.Ref) {
	path := w.CheckoutPath(db, branch)
	if _, err := os.Stat(path); err != nil {
		return
	}
	if err := quiesce(path); err != nil {
		fmt.Fprintf(os.Stderr, "offshoot: warning: checkout of %s@%s is busy; forking last committed state (txid %d)\n", db, branch, ref.HeadTXID)
		return
	}
	state, _ := checkoutState(path, ref)
	switch state {
	case "modified":
		fmt.Fprintf(os.Stderr, "offshoot: warning: checkout of %s@%s has un-checkpointed changes; forking last committed state (txid %d)\n", db, branch, ref.HeadTXID)
	case "stale":
		fmt.Fprintf(os.Stderr, "offshoot: warning: checkout of %s@%s is stale (branch was repointed since it was materialized); forking branch head (txid %d) — run 'offshoot checkout' to refresh\n", db, branch, ref.HeadTXID)
	}
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
// export_test.go.
var forkSlowPathForTest bool

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

// copySnapshotToNewLineage copies the snapshot identified by cp (in src's
// lineage) into a brand-new lineage (epoch 1) and returns the lineage id.
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
// CopyObject in general, but S3's is gated to objects at or under its 5GB
// single-request CopyObject limit, so the sentinel still fires for anything
// larger — see store.S3.CopyObject's doc comment.
func (w *Workspace) copySnapshotToNewLineage(src store.Ref, cp store.Checkpoint) (string, error) {
	lineage := store.NewLineageID()
	// Resolved once, up front, and threaded through both the fast-path
	// attempt and the slow-path fallback — see materializeMembersAt's doc
	// comment for why sharing this one store.Chain call (rather than each
	// path resolving its own) matters on a remote backend.
	members, err := w.Store.Chain(src.Lineage, cp.TXID)
	if err != nil {
		return "", fmt.Errorf("ops: resolving chain for lineage %s to txid %d: %w", src.Lineage, cp.TXID, err)
	}
	if !forkSlowPathForTest {
		ok, err := w.tryFastForkCopy(members, cp, lineage)
		if err != nil {
			return "", err
		}
		if ok {
			return lineage, nil
		}
	}
	if _, err := w.copySnapshotIntoLineageFromChain(src.Lineage, members, cp, lineage); err != nil {
		return "", err
	}
	return lineage, nil
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
			return 0, fmt.Errorf("ops: no checkpoint %q on %s@%s", at, db, srcBranch)
		}
		cp = c
	} else {
		w.warnIfUncheckpointed(db, srcBranch, src)
	}
	txid := cp.TXID
	// Materialized fork point: copy the source snapshot into the child's own
	// lineage so the child never references parent storage.
	childLineage, err := w.copySnapshotToNewLineage(src, cp)
	if err != nil {
		return 0, err
	}
	child := store.Ref{
		Lineage: childLineage, Epoch: 1, HeadTXID: txid, HeadEpoch: 1,
		Parent: fmt.Sprintf("%s@%s@%d", db, srcBranch, txid),
		Meta:   meta,
	}
	if ttl > 0 {
		child.TTL = ttl.String()
	}
	child.Touch(time.Now())
	child.SetCheckpoint("fork", store.Checkpoint{TXID: txid, Epoch: 1, CreatedAt: nowStamp()})
	if _, err := w.Store.PutRef(db, newBranch, child, ""); err != nil {
		// Branch already exists (or lost a race): remove the orphan snapshot.
		w.Store.B.Delete(store.SnapshotKey(childLineage, 1, txid))
		return 0, fmt.Errorf("ops: fork %s@%s: %w", db, newBranch, err)
	}
	return txid, nil
}

// Rollback repoints db@branch at a NEW lineage seeded from checkpoint `to`
// and re-materializes the fixed checkout path, returning the checkout path.
// The old lineage is orphaned (collected later by GC). Checkpoints at or
// before `to` are kept, and EVERY kept checkpoint's snapshot is copied into
// the new lineage (not just `to`'s) so a later rollback or fork to an
// earlier kept checkpoint still finds its snapshot object once the old
// lineage is gone — otherwise it fails "not found" once GC reaps the old
// lineage, since the ref itself no longer references it after this repoint.
// Later checkpoints are dropped.
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
func (w *Workspace) Rollback(db, branch, to string) (string, error) {
	if err := store.ValidateName(db); err != nil {
		return "", err
	}
	if err := store.ValidateName(branch); err != nil {
		return "", err
	}
	if err := store.ValidateName(to); err != nil {
		return "", err
	}
	ref, etag, err := w.Store.GetRef(db, branch)
	if err != nil {
		return "", err
	}
	cp, ok := ref.Checkpoints[to]
	if !ok {
		return "", fmt.Errorf("ops: no checkpoint %q on %s@%s", to, db, branch)
	}
	txid := cp.TXID
	lineage, err := w.copySnapshotToNewLineage(ref, cp)
	if err != nil {
		return "", err
	}
	copiedKeys := []string{store.SnapshotKey(lineage, 1, txid)}
	cleanup := func() {
		for _, k := range copiedKeys {
			w.Store.B.Delete(k)
		}
	}

	kept := map[string]store.Checkpoint{}
	for name, c := range ref.Checkpoints {
		if c.TXID <= txid {
			kept[name] = c
		}
	}

	// `to`'s own snapshot is already copied above. Copy every OTHER kept
	// checkpoint's snapshot into the new lineage too, so it survives the old
	// lineage being orphaned and later reaped by GC. Every copy (including
	// `to`'s, above) lands at epoch 1 in the new lineage — a fresh lineage
	// always starts there — so once copied, kept's checkpoints no longer
	// live at whatever epoch they were recorded under in the old lineage;
	// rewrite each one to epoch 1 to match where its object now actually
	// is. Abort and clean up anything already copied before touching the
	// ref if any copy fails.
	done := map[uint64]bool{txid: true}
	for name, c := range kept {
		if !done[c.TXID] {
			done[c.TXID] = true
			key, err := w.copySnapshotIntoLineage(ref, c, lineage)
			if err != nil {
				cleanup()
				return "", fmt.Errorf("ops: rollback: copying checkpoint snapshot for txid %d: %w", c.TXID, err)
			}
			copiedKeys = append(copiedKeys, key)
		}
		// Rewrite epoch to 1 (where the copy now actually lives) while
		// preserving CreatedAt/Meta — this is a location update, not a new
		// checkpoint, so its recorded creation time and metadata must
		// survive the rewrite unchanged.
		kept[name] = store.Checkpoint{TXID: c.TXID, Epoch: 1, CreatedAt: c.CreatedAt, Meta: c.Meta}
	}

	next := ref
	next.Lineage, next.Epoch, next.HeadTXID, next.HeadEpoch, next.Checkpoints = lineage, 1, txid, 1, kept
	// A repoint is itself a revocation: the old holder is already fenced (its
	// epoch no longer matches), but carrying its lease forward would leave a
	// fresh acquirer refused ErrLeaseHeld by a holder that can never renew —
	// stuck until the stale TTL lapses. Clear the lease so the branch is
	// immediately acquirable post-repoint.
	next.LeaseHolder, next.LeaseExpiry = "", ""
	next.Touch(time.Now())
	if _, err := w.Store.PutRef(db, branch, next, etag); err != nil {
		cleanup()
		return "", fmt.Errorf("ops: rollback lost a race (retry): %w", err)
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
		checksum, err := w.materializeAt(next, headCheckpoint(next), path)
		if err != nil {
			return err
		}
		// The checkout now equals committed state: refresh the fingerprint
		// (identity too, since this repointed to a new lineage) so a later
		// Fork sees it as clean rather than stale.
		return writeSum(path, next.Lineage, next.HeadEpoch, txid, checksum)
	}
	if err := refresh(); err != nil {
		return "", fmt.Errorf("ops: branch repointed to checkpoint %q (txid %d), but the checkout could not be refreshed (run 'offshoot checkout' to re-materialize): %w", to, txid, err)
	}
	return path, nil
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
func (w *Workspace) Promote(db, source, target string, force bool) (uint64, error) {
	if source == target {
		return 0, fmt.Errorf("ops: cannot promote a branch onto itself")
	}
	if err := store.ValidateName(db); err != nil {
		return 0, err
	}
	if err := store.ValidateName(source); err != nil {
		return 0, err
	}
	if err := store.ValidateName(target); err != nil {
		return 0, err
	}
	src, _, err := w.Store.GetRef(db, source)
	if err != nil {
		return 0, err
	}
	w.warnIfUncheckpointed(db, source, src)
	tgt, tgtEtag, err := w.Store.GetRef(db, target)
	if err != nil {
		return 0, err
	}
	if tgt.Protected && !force {
		return 0, fmt.Errorf("ops: %s@%s is protected; use --force", db, target)
	}
	cp := headCheckpoint(src)
	txid := cp.TXID
	lineage, err := w.copySnapshotToNewLineage(src, cp)
	if err != nil {
		return 0, err
	}
	next := tgt
	next.Lineage, next.Epoch, next.HeadTXID, next.HeadEpoch = lineage, 1, txid, 1
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
		w.Store.B.Delete(store.SnapshotKey(lineage, 1, txid))
		return 0, fmt.Errorf("ops: promote lost a race (retry): %w", err)
	}
	// Refresh the target checkout if one exists and is quiescible.
	path := w.CheckoutPath(db, target)
	if _, err := os.Stat(path); err == nil {
		if err := quiesce(path); err != nil {
			return txid, fmt.Errorf("ops: promoted, but checkout %s is in use and was NOT refreshed: %w", path, err)
		}
		checksum, err := w.materializeAt(next, headCheckpoint(next), path)
		if err != nil {
			return txid, fmt.Errorf("ops: promoted, but checkout %s could not be refreshed: %w", path, err)
		}
		// The checkout now equals committed state: refresh the fingerprint
		// (identity too, since this repointed to a new lineage) so a later
		// Fork sees it as clean rather than stale.
		if err := writeSum(path, next.Lineage, next.HeadEpoch, txid, checksum); err != nil {
			return txid, fmt.Errorf("ops: promoted, but checkout %s could not be refreshed: %w", path, err)
		}
	}
	return txid, nil
}

type BranchStatus struct {
	DB, Branch  string
	HeadTXID    uint64
	Checkpoints []string
	Protected   bool
	Parent      string
	CheckedOut  bool
	// TTL is the branch's TTL verbatim from the ref ("" if none).
	TTL string
	// TTLRemaining is how long until the branch is eligible for reaping,
	// measured from the later of TouchedAt and LeaseExpiry ("" if TTL is
	// unset; "expired" once past the deadline).
	TTLRemaining string
}

// ReapDeadline computes when ref's TTL expires, measured from the later of
// its activity clock (TouchedAt) and its lease expiry — either kind of
// activity defers reaping. ok is false when ref has no TTL, its TTL is
// non-positive (a negative or zero duration string is not a real TTL — fail
// closed rather than compute a deadline already in the past), or its TTL or
// timestamps fail to parse.
func ReapDeadline(ref store.Ref) (time.Time, bool) {
	if ref.TTL == "" {
		return time.Time{}, false
	}
	d, err := time.ParseDuration(ref.TTL)
	if err != nil || d <= 0 {
		return time.Time{}, false
	}
	base, ok := parseRefTime(ref.TouchedAt)
	if !ok {
		return time.Time{}, false
	}
	if lease, ok := parseRefTime(ref.LeaseExpiry); ok && lease.After(base) {
		base = lease
	}
	return base.Add(d), true
}

// FormatTTLRemaining renders ref's time-to-reap as of now, for display:
// "" if ref has no (real) TTL, "expired" once past ReapDeadline, otherwise
// the remaining time.Duration's String(). Shared by Status and the daemon's
// "branches" op so the two report identical numbers computed the same way,
// rather than each formatting ReapDeadline's result independently.
func FormatTTLRemaining(ref store.Ref, now time.Time) string {
	deadline, ok := ReapDeadline(ref)
	if !ok {
		return ""
	}
	if now.After(deadline) {
		return "expired"
	}
	return deadline.Sub(now).String()
}

// parseRefTime parses an RFC3339Nano ref timestamp field (TouchedAt or
// LeaseExpiry), which is empty when unset.
func parseRefTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func (w *Workspace) Status() ([]BranchStatus, error) {
	refs, err := w.Store.ListRefs()
	if err != nil {
		return nil, err
	}
	var dbs []string
	for db := range refs {
		dbs = append(dbs, db)
	}
	sort.Strings(dbs)
	now := time.Now()
	var out []BranchStatus
	for _, db := range dbs {
		for _, br := range refs[db] {
			r, _, err := w.Store.GetRef(db, br)
			if err != nil {
				return nil, err
			}
			var cps []string
			for name := range r.Checkpoints {
				cps = append(cps, name)
			}
			sort.Strings(cps)
			_, coErr := os.Stat(w.CheckoutPath(db, br))
			out = append(out, BranchStatus{
				DB: db, Branch: br, HeadTXID: r.HeadTXID, Checkpoints: cps,
				Protected: r.Protected, Parent: r.Parent, CheckedOut: coErr == nil,
				TTL: r.TTL, TTLRemaining: FormatTTLRemaining(r, now),
			})
		}
	}
	return out, nil
}
