// Package ops implements offshoot's branch lifecycle operations over a
// store.Backend: create, checkout, checkpoint, fork, rollback, promote,
// destroy, and GC. Plan-2 scope is CLI/at-rest mode: full-snapshot
// checkpoints, fixed checkout paths, no daemon.
package ops

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/offshoot-db/offshoot/internal/ltxio"
	"github.com/offshoot-db/offshoot/internal/store"
)

type Workspace struct {
	Store *store.Store
	Root  string
}

func Init(root string) (*Workspace, error) {
	b, err := store.NewLocal(root)
	if err != nil {
		return nil, err
	}
	s := &store.Store{B: b}
	if err := s.InitManifest(); err != nil {
		return nil, err
	}
	return &Workspace{Store: s, Root: root}, nil
}

func Open(root string) (*Workspace, error) {
	b, err := store.NewLocal(root)
	if err != nil {
		return nil, err
	}
	s := &store.Store{B: b}
	if err := s.CheckManifest(); err != nil {
		return nil, err
	}
	return &Workspace{Store: s, Root: root}, nil
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
		Schema: 1, Lineage: lineage, Epoch: 1, HeadTXID: 1,
		Checkpoints: map[string]uint64{"init": 1},
		Protected:   true, // main is protected by default (spec § Security posture)
	}
	if _, err := w.Store.PutRef(db, "main", ref, ""); err != nil {
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

// Checkout materializes db@branch's head snapshot to its fixed path.
func (w *Workspace) Checkout(db, branch string) (string, error) {
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		return "", err
	}
	path := w.CheckoutPath(db, branch)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	if err := w.materialize(ref, ref.HeadTXID, path); err != nil {
		return "", err
	}
	if err := writeSum(path); err != nil {
		return "", err
	}
	return path, nil
}

// writeSum computes the hex SHA-256 of the file at path and writes it to
// path + ".sum". This is the checkout fingerprint: it records what the
// checkout file looked like at the moment it was last known to equal a
// committed state (fresh materialize, or a successful checkpoint encode).
func writeSum(path string) error {
	sum, err := fileSum(path)
	if err != nil {
		return err
	}
	return os.WriteFile(path+".sum", []byte(sum), 0o644)
}

// sumMatches reports whether the file at path still matches its recorded
// .sum fingerprint. A missing .sum file means unknown provenance (e.g. a
// checkout materialized before this fingerprint existed, or a manually
// dropped-in file) and is treated as a match so callers don't warn
// spuriously.
func sumMatches(path string) (bool, error) {
	want, err := os.ReadFile(path + ".sum")
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	got, err := fileSum(path)
	if err != nil {
		return false, err
	}
	return string(want) == got, nil
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

func (w *Workspace) materialize(ref store.Ref, txid uint64, dst string) error {
	data, _, err := w.Store.B.Get(store.SnapshotKey(ref.Lineage, ref.Epoch, txid))
	if err != nil {
		return fmt.Errorf("ops: snapshot txid %d not found in lineage %s: %w", txid, ref.Lineage, err)
	}
	if _, err := ltxio.Materialize(bytes.NewReader(data), dst); err != nil {
		return err
	}
	return nil
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
	// The checkout now equals the state being committed (quiesce merged the
	// WAL, encode captured it): refresh the fingerprint so a later Fork
	// doesn't see stale writes as un-checkpointed.
	if err := writeSum(path); err != nil {
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
		// legitimately reference this key yet: it can only be an orphan left
		// by a crashed prior Checkpoint attempt (snapshot uploaded, ref write
		// never landed). Safe to overwrite unconditionally and proceed.
		if err := w.Store.B.Put(snapKey, buf.Bytes()); err != nil {
			return 0, err
		}
	}
	ref.HeadTXID = txid
	if ref.Checkpoints == nil {
		ref.Checkpoints = map[string]uint64{}
	}
	ref.Checkpoints[name] = txid
	if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
		// Roll back the snapshot upload so a retry's create-only put at this
		// same deterministic key doesn't get wedged behind this attempt's
		// orphan (mirrors Fork's cleanup-on-ref-failure pattern).
		w.Store.B.Delete(snapKey)
		if errors.Is(err, store.ErrCAS) {
			return 0, fmt.Errorf("ops: ref update lost a race (retry): %w", err)
		}
		return 0, fmt.Errorf("ops: ref update for checkpoint %q on %s@%s: %w", name, db, branch, err)
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

// warnIfUncheckpointed checks db@branch's checkout (if one exists) for
// un-checkpointed changes and, if any are found, prints a warning to
// os.Stderr noting that the caller (Fork) is proceeding from the last
// committed state at headTXID. It never fails the caller's operation: any
// error along the way is treated as "nothing to warn about".
func (w *Workspace) warnIfUncheckpointed(db, branch string, headTXID uint64) {
	path := w.CheckoutPath(db, branch)
	if _, err := os.Stat(path); err != nil {
		return
	}
	if err := quiesce(path); err != nil {
		fmt.Fprintf(os.Stderr, "offshoot: warning: checkout of %s@%s is busy; forking last committed state (txid %d)\n", db, branch, headTXID)
		return
	}
	match, err := sumMatches(path)
	if err != nil || match {
		return
	}
	fmt.Fprintf(os.Stderr, "offshoot: warning: checkout of %s@%s has un-checkpointed changes; forking last committed state (txid %d)\n", db, branch, headTXID)
}

// copySnapshotToNewLineage copies the snapshot at (src.Lineage, src.Epoch,
// txid) into a brand-new lineage (epoch 1) and returns the lineage id.
// This is the primitive behind fork, rollback, and promote: every branch
// repoint gets a fresh lineage, preserving one-writer-per-lineage.
func (w *Workspace) copySnapshotToNewLineage(src store.Ref, txid uint64) (string, error) {
	data, _, err := w.Store.B.Get(store.SnapshotKey(src.Lineage, src.Epoch, txid))
	if err != nil {
		return "", fmt.Errorf("ops: snapshot txid %d in lineage %s: %w", txid, src.Lineage, err)
	}
	lineage := store.NewLineageID()
	if _, err := w.Store.B.PutIf(store.SnapshotKey(lineage, 1, txid), data, ""); err != nil {
		return "", err
	}
	return lineage, nil
}

// Fork creates newBranch from db@srcBranch at head or a named checkpoint.
func (w *Workspace) Fork(db, srcBranch, newBranch, at string) (uint64, error) {
	if err := store.ValidateName(newBranch); err != nil {
		return 0, err
	}
	src, _, err := w.Store.GetRef(db, srcBranch)
	if err != nil {
		return 0, err
	}
	txid := src.HeadTXID
	if at != "" {
		t, ok := src.Checkpoints[at]
		if !ok {
			return 0, fmt.Errorf("ops: no checkpoint %q on %s@%s", at, db, srcBranch)
		}
		txid = t
	} else {
		w.warnIfUncheckpointed(db, srcBranch, txid)
	}
	// Materialized fork point: copy the source snapshot into the child's own
	// lineage so the child never references parent storage.
	childLineage, err := w.copySnapshotToNewLineage(src, txid)
	if err != nil {
		return 0, fmt.Errorf("ops: source snapshot txid %d: %w", txid, err)
	}
	child := store.Ref{
		Schema: 1, Lineage: childLineage, Epoch: 1, HeadTXID: txid,
		Checkpoints: map[string]uint64{"fork": txid},
		Parent:      fmt.Sprintf("%s@%s@%d", db, srcBranch, txid),
	}
	if _, err := w.Store.PutRef(db, newBranch, child, ""); err != nil {
		// Branch already exists (or lost a race): remove the orphan snapshot.
		w.Store.B.Delete(store.SnapshotKey(childLineage, 1, txid))
		return 0, fmt.Errorf("ops: fork %s@%s: %w", db, newBranch, err)
	}
	return txid, nil
}

// Rollback repoints db@branch at a NEW lineage seeded from checkpoint `to`,
// re-materializes the fixed checkout path (after a lock probe — fails
// cleanly if the checkout is held open), and returns the checkout path.
// The old lineage is orphaned (collected later by GC). Checkpoints at or
// before `to` are kept; later ones are dropped.
func (w *Workspace) Rollback(db, branch, to string) (string, error) {
	ref, etag, err := w.Store.GetRef(db, branch)
	if err != nil {
		return "", err
	}
	txid, ok := ref.Checkpoints[to]
	if !ok {
		return "", fmt.Errorf("ops: no checkpoint %q on %s@%s", to, db, branch)
	}
	path := w.CheckoutPath(db, branch)
	if _, err := os.Stat(path); err == nil {
		if err := quiesce(path); err != nil {
			return "", fmt.Errorf("ops: checkout in use; close connections before rollback: %w", err)
		}
	}
	lineage, err := w.copySnapshotToNewLineage(ref, txid)
	if err != nil {
		return "", err
	}
	kept := map[string]uint64{}
	for name, t := range ref.Checkpoints {
		if t <= txid {
			kept[name] = t
		}
	}
	next := ref
	next.Lineage, next.Epoch, next.HeadTXID, next.Checkpoints = lineage, 1, txid, kept
	if _, err := w.Store.PutRef(db, branch, next, etag); err != nil {
		w.Store.B.Delete(store.SnapshotKey(lineage, 1, txid))
		return "", fmt.Errorf("ops: rollback lost a race (retry): %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	if err := w.materialize(next, txid, path); err != nil {
		return "", err
	}
	// The checkout now equals committed state: refresh the fingerprint so a
	// later Fork doesn't spuriously warn about "un-checkpointed changes".
	if err := writeSum(path); err != nil {
		return "", err
	}
	return path, nil
}

// Promote repoints db@target at a NEW lineage seeded from db@source's head
// (promote-as-fork, spec § Promote). Requires --force for protected targets
// (force param). Source branch survives unchanged. Target's old lineage is
// orphaned. Target's checkout (if any) is re-materialized after a lock probe.
// Target's checkpoint map is reset to {"promote": txid}.
func (w *Workspace) Promote(db, source, target string, force bool) (uint64, error) {
	src, _, err := w.Store.GetRef(db, source)
	if err != nil {
		return 0, err
	}
	tgt, tgtEtag, err := w.Store.GetRef(db, target)
	if err != nil {
		return 0, err
	}
	if tgt.Protected && !force {
		return 0, fmt.Errorf("ops: %s@%s is protected; use --force", db, target)
	}
	txid := src.HeadTXID
	lineage, err := w.copySnapshotToNewLineage(src, txid)
	if err != nil {
		return 0, err
	}
	next := tgt
	next.Lineage, next.Epoch, next.HeadTXID = lineage, 1, txid
	next.Checkpoints = map[string]uint64{"promote": txid}
	next.Parent = fmt.Sprintf("%s@%s@%d", db, source, txid)
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
		if err := w.materialize(next, txid, path); err != nil {
			return txid, err
		}
		// The checkout now equals committed state: refresh the fingerprint so
		// a later Fork doesn't spuriously warn about "un-checkpointed changes".
		if err := writeSum(path); err != nil {
			return txid, err
		}
	}
	return txid, nil
}
