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
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/offshoot-db/offshoot/internal/ltxio"
	"github.com/offshoot-db/offshoot/internal/store"
)

type Workspace struct {
	Store *store.Store
	Root  string
	Spec  string
}

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
	if err := ltxio.EncodeSnapshot(dbPath, txid, &buf); err != nil {
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
	ref.SetCheckpoint("init", 1, 1)
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
	if err := store.ValidateName(db); err != nil {
		return "", err
	}
	if err := store.ValidateName(branch); err != nil {
		return "", err
	}
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		return "", err
	}
	path := w.CheckoutPath(db, branch)
	if _, err := os.Stat(path); err == nil {
		if err := quiesce(path); err != nil {
			return "", err
		}
		if checkoutState(path, ref) == "modified" {
			fmt.Fprintf(os.Stderr, "offshoot: warning: overwriting un-checkpointed changes in %s@%s checkout\n", db, branch)
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	if err := w.materializeAt(ref, headCheckpoint(ref), path); err != nil {
		return "", err
	}
	if err := writeSum(path, ref.Lineage, ref.HeadTXID); err != nil {
		return "", err
	}
	return path, nil
}

// sumRecord is the on-disk shape of a checkout's .sum sidecar: a content hash
// plus the ref identity (lineage + txid) the checkout embodied at the moment
// the sidecar was written. Recording identity (not just a bare hash) is what
// lets checkoutState tell apart a checkout with local edits from one whose
// branch ref moved out from under it without a refresh.
type sumRecord struct {
	Hash    string `json:"hash"`
	Lineage string `json:"lineage"`
	TXID    uint64 `json:"txid"`
}

// writeSum computes the hex SHA-256 of the file at path and writes it, along
// with the (lineage, txid) ref identity the checkout currently embodies, to
// path + ".sum". This is the checkout fingerprint: it records what the
// checkout file looked like, and which branch state it was, at the moment it
// was last known to equal a committed state (fresh materialize, a successful
// checkpoint encode, or a post-repoint refresh).
func writeSum(path string, lineage string, txid uint64) error {
	sum, err := fileSum(path)
	if err != nil {
		return err
	}
	data, err := json.Marshal(sumRecord{Hash: sum, Lineage: lineage, TXID: txid})
	if err != nil {
		return err
	}
	return os.WriteFile(path+".sum", data, 0o644)
}

// checkoutState reports how the checkout at path relates to ref:
//
//   - "clean": the sidecar's recorded identity (lineage, txid) matches ref,
//     and the file's content still matches the recorded hash.
//   - "modified": the sidecar's recorded identity matches ref, but the
//     file's content has changed since it was last fingerprinted — local,
//     un-checkpointed edits.
//   - "stale": the sidecar's recorded identity no longer matches ref — the
//     branch was repointed (rollback/promote with a skipped refresh) since
//     this checkout was last materialized or checkpointed.
//   - "unknown": no sidecar, or one that isn't a valid current-format
//     record (including legacy bare-hash sidecars predating this fix, and
//     corrupt files). Provenance can't be determined, so callers should
//     stay silent rather than warn spuriously.
func checkoutState(path string, ref store.Ref) string {
	raw, err := os.ReadFile(path + ".sum")
	if err != nil {
		return "unknown"
	}
	var rec sumRecord
	if err := json.Unmarshal(raw, &rec); err != nil || rec.Hash == "" {
		return "unknown"
	}
	if rec.Lineage != ref.Lineage || rec.TXID != ref.HeadTXID {
		return "stale"
	}
	got, err := fileSum(path)
	if err != nil {
		return "unknown"
	}
	if got != rec.Hash {
		return "modified"
	}
	return "clean"
}

func fileSum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// materializeAt writes the state identified by cp into dst. It is a thin
// wrapper over materializeChainAt (see materialize.go), which resolves the
// full snapshot+segment chain rather than assuming cp's txid is itself a
// snapshot: every caller here (Checkout, Rollback's refresh, Promote's
// refresh, copySnapshotIntoLineage's read side) picks that up unchanged.
func (w *Workspace) materializeAt(ref store.Ref, cp store.Checkpoint, dst string) error {
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
func (w *Workspace) Checkpoint(db, branch, name string) (uint64, error) {
	if err := store.ValidateName(db); err != nil {
		return 0, err
	}
	if err := store.ValidateName(branch); err != nil {
		return 0, err
	}
	if err := store.ValidateName(name); err != nil {
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
	if err := ltxio.EncodeSnapshot(path, txid, &buf); err != nil {
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
	ref.SetCheckpoint(name, txid, ref.Epoch)
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
	if err := writeSum(path, ref.Lineage, txid); err != nil {
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
	switch checkoutState(path, ref) {
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
	tmp := filepath.Join(os.TempDir(), "offshoot-copy-"+store.NewLineageID()+".db")
	defer os.Remove(tmp)
	if err := w.materializeChainAt(src, cp, tmp); err != nil {
		return "", fmt.Errorf("ops: materializing lineage %s at txid %d for copy: %w", src.Lineage, cp.TXID, err)
	}
	var buf bytes.Buffer
	if err := ltxio.EncodeSnapshot(tmp, cp.TXID, &buf); err != nil {
		return "", err
	}
	key := store.SnapshotKey(lineage, 1, cp.TXID)
	if _, err := w.Store.B.PutIf(key, buf.Bytes(), ""); err != nil {
		return "", err
	}
	return key, nil
}

// copySnapshotToNewLineage copies the snapshot identified by cp (in src's
// lineage) into a brand-new lineage (epoch 1) and returns the lineage id.
func (w *Workspace) copySnapshotToNewLineage(src store.Ref, cp store.Checkpoint) (string, error) {
	lineage := store.NewLineageID()
	if _, err := w.copySnapshotIntoLineage(src, cp, lineage); err != nil {
		return "", err
	}
	return lineage, nil
}

// Fork creates newBranch from db@srcBranch at head or a named checkpoint.
// ttl > 0 sets the child's TTL (never the parent's — creating a child does
// not extend the parent's activity clock either); ttl == 0 means no TTL.
func (w *Workspace) Fork(db, srcBranch, newBranch, at string, ttl time.Duration) (uint64, error) {
	if err := store.ValidateName(newBranch); err != nil {
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
	}
	if ttl > 0 {
		child.TTL = ttl.String()
	}
	child.Touch(time.Now())
	child.SetCheckpoint("fork", txid, 1)
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
		kept[name] = store.Checkpoint{TXID: c.TXID, Epoch: 1}
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
		if err := w.materializeAt(next, headCheckpoint(next), path); err != nil {
			return err
		}
		// The checkout now equals committed state: refresh the fingerprint
		// (identity too, since this repointed to a new lineage) so a later
		// Fork sees it as clean rather than stale.
		return writeSum(path, next.Lineage, txid)
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
	next.SetCheckpoint("promote", txid, 1)
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
		if err := w.materializeAt(next, headCheckpoint(next), path); err != nil {
			return txid, fmt.Errorf("ops: promoted, but checkout %s could not be refreshed: %w", path, err)
		}
		// The checkout now equals committed state: refresh the fingerprint
		// (identity too, since this repointed to a new lineage) so a later
		// Fork sees it as clean rather than stale.
		if err := writeSum(path, next.Lineage, txid); err != nil {
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
