package ops

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/offshoot-db/offshoot/internal/store"
)

// Export materializes db@branch's state at checkpoint ("" = the branch's
// current head) to a plain SQLite file at dstPath — anywhere on the local
// filesystem, with ZERO ongoing relationship to the store: no `.sum`
// sidecar, no lease, nothing checkoutState or any other ops machinery will
// ever look at again. It is a one-shot copy-out, not a checkout.
//
// Locked semantics (Milestone 3 PM Amendment 6):
//
//   - Refuses to overwrite an existing dstPath unless force is true. This is
//     a stat-then-materialize check, not an atomic exclusive-create — like
//     every other point-in-time "is this busy/does this exist" probe in this
//     package (see Rollback/Promote's busy-probe doc comments), a rival
//     process creating the exact same dstPath between the check and the
//     rename below is a real, if narrow, race. Acceptable for the
//     single-operator CLI / same-host-same-user daemon trust model this
//     package already assumes elsewhere; not a promise of exclusivity under
//     a concurrent, adversarial writer to the same path.
//   - The write itself IS unconditionally atomic, independent of the check
//     above: materializeAt (via materializeChainAt -> ltxio.MaterializeChain)
//     always creates its scratch file IN dstPath's OWN DIRECTORY
//     (os.CreateTemp(filepath.Dir(dstPath), ...)) and os.Renames it over
//     dstPath only once every chain member (snapshot + segments) has been
//     fetched, decoded, and its checksum verified. So a failure partway
//     through — a fetch error, a checksum mismatch, a decode error — leaves
//     dstPath untouched (either absent, or the pre-existing file force
//     allowed this call to overwrite, still intact) and no partial file
//     anywhere: the temp file is removed on any error path (see
//     ltxio.MaterializeChain's own doc comment). The rename is necessarily
//     same-filesystem — same directory, not just same volume — so it can
//     never fall back to a cross-device copy that could itself fail
//     partway.
//
// Export discards the checksum materializeAt returns. Milestone 3 Task 3
// threaded a PostApplyChecksum through that machinery for the checkout
// `.sum` sidecar's own use (see CheckoutResult.PostApplyChecksum); Export
// writes no sidecar, so it has nothing to stamp with it.
func (w *Workspace) Export(db, branch, checkpoint, dstPath string, force bool) error {
	if err := store.ValidateName(db); err != nil {
		return err
	}
	if err := store.ValidateName(branch); err != nil {
		return err
	}
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		return err
	}
	cp := headCheckpoint(ref)
	if checkpoint != "" {
		c, ok := ref.Checkpoints[checkpoint]
		if !ok {
			return fmt.Errorf("ops: no checkpoint %q on %s@%s", checkpoint, db, branch)
		}
		cp = c
	}
	if !force {
		if _, err := os.Stat(dstPath); err == nil {
			return fmt.Errorf("ops: export destination %s already exists (use force to overwrite)", dstPath)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("ops: export destination %s: %w", dstPath, err)
		}
	}
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	if _, err := w.materializeAt(ref, cp, dstPath); err != nil {
		return fmt.Errorf("ops: export %s@%s: %w", db, branch, err)
	}
	return nil
}

// CheckoutAtPath is CheckoutAt's read-only cache path for db@branch's
// checkpoint: <Root>/checkouts-ro/<db>/<branch>@<checkpoint>.db — distinct
// from CheckoutPath's writable <Root>/checkouts/<db>/<branch>.db, both in
// directory (checkouts-ro vs checkouts) and in filename (it encodes the
// checkpoint too, since more than one can be cached per branch). Safe to
// build from db/branch/checkpoint alone: none of the three can contain '@'
// or '/' (store.ValidateName's charset), so this join is never ambiguous
// and never escapes the checkouts-ro tree.
func (w *Workspace) CheckoutAtPath(db, branch, checkpoint string) string {
	return filepath.Join(w.Root, "checkouts-ro", db, branch+"@"+checkpoint+".db")
}

// CheckoutAt materializes db@branch's state at checkpoint into its
// dedicated read-only cache file (CheckoutAtPath) — a SEPARATE path from
// CheckoutPath's writable checkout, never that path, and never touched by
// anything that reads or writes the writable checkout's inode chain.
//
// That separation is what makes this safe to call alongside an open
// daemon session on the SAME branch: CheckoutAt never opens, stats through,
// or renames over the writable checkout, so it cannot race a live capture
// engine's own file descriptor on that path the way a second Checkout or
// Rollback call would (see internal/dbfile's package comment for exactly
// what a stray open/close of a live checkout risks). The ro-cache path
// itself is never opened by dbfile.Reader or any capture engine either —
// nothing in this codebase treats it as a live database, only ever as a
// static file to read with an ordinary connection.
//
// No `.sum` sidecar is written and no lease is taken: this file has zero
// ongoing relationship to the store, exactly like Export's output (see its
// doc comment) — safe to `rm -rf` the whole checkouts-ro tree at any time;
// the next CheckoutAt call for anything under it simply rebuilds what it
// needs. The result is chmod 0444 after materializing, reinforcing in the
// filesystem itself what the name already promises: nothing should write
// through this path.
//
// Repeat calls for the SAME (db, branch, checkpoint) are cheap by design:
// if the cache file already exists, force is false returns it as-is with
// NO store access at all (not even a GetRef) — a checkpoint's content is
// immutable once created, so a cached file for it never needs
// revalidating. This is the same tradeoff Checkout's clean-and-current
// fast path and the daemon's settling-flush suppression already make
// elsewhere in this codebase (see docs/status.md's "Clean-and-current
// checkout served without chain validation" row): once proven correct by
// construction, re-verifying against the store on every call is pure
// waste. The one way this can go stale is if db@branch is destroyed and
// later recreated with a checkpoint of the SAME name but different
// content — CheckoutAtPath has no way to tell that apart from the
// original, and would keep serving the old cached bytes; force, or
// clearing the checkouts-ro directory, is the way out.
//
// force=true re-materializes unconditionally, re-reading the checkpoint
// from the store even if a cached file is already present — this is the
// one path that DOES touch the store on a call that could otherwise be a
// pure cache hit, and will surface a real error (e.g. "no checkpoint") if
// the checkpoint has since become unreachable, rather than silently
// serving the stale cache the force=false path would have returned.
func (w *Workspace) CheckoutAt(db, branch, checkpoint string, force bool) (string, error) {
	if err := store.ValidateName(db); err != nil {
		return "", err
	}
	if err := store.ValidateName(branch); err != nil {
		return "", err
	}
	if checkpoint == "" {
		return "", fmt.Errorf("ops: checkout-at requires a checkpoint name")
	}
	path := w.CheckoutAtPath(db, branch, checkpoint)
	if !force {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		return "", err
	}
	cp, ok := ref.Checkpoints[checkpoint]
	if !ok {
		return "", fmt.Errorf("ops: no checkpoint %q on %s@%s", checkpoint, db, branch)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	if _, err := w.materializeAt(ref, cp, path); err != nil {
		return "", fmt.Errorf("ops: checkout-at %s@%s@%s: %w", db, branch, checkpoint, err)
	}
	if err := os.Chmod(path, 0o444); err != nil {
		return "", fmt.Errorf("ops: checkout-at %s@%s@%s materialized but could not be marked read-only: %w", db, branch, checkpoint, err)
	}
	return path, nil
}

// ParseExportTarget parses export's <db>[@branch[@checkpoint]] target form:
// up to three '@'-separated components (branch defaults to "main",
// checkpoint defaults to "" meaning head, exactly like ParseTarget's own
// db/branch defaulting). Splitting on '@' is unambiguous because a branch
// or checkpoint name can never itself contain '@' (store.ValidateName's
// charset excludes it) — this mirrors ParseTarget's own reasoning, extended
// by one optional component.
func ParseExportTarget(s string) (db, branch, checkpoint string, err error) {
	parts := strings.Split(s, "@")
	db, branch = parts[0], "main"
	switch len(parts) {
	case 1:
	case 2:
		branch = parts[1]
	case 3:
		branch, checkpoint = parts[1], parts[2]
	default:
		return "", "", "", fmt.Errorf("ops: invalid export target %q (want db, db@branch, or db@branch@checkpoint)", s)
	}
	if err := store.ValidateName(db); err != nil {
		return "", "", "", err
	}
	if err := store.ValidateName(branch); err != nil {
		return "", "", "", err
	}
	if checkpoint != "" {
		if err := store.ValidateName(checkpoint); err != nil {
			return "", "", "", err
		}
	}
	return db, branch, checkpoint, nil
}
