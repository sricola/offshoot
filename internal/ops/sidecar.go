package ops

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"

	"github.com/sricola/offshoot/internal/dbfile"
	"github.com/sricola/offshoot/internal/store"
)

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
	rec, ok := readSidecar(path)
	if !ok {
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

// readSidecar reads and parses path's .sum sidecar (see sumRecord's doc
// comment) into rec, with ok=false if the file is absent or doesn't decode
// as a valid current-format record (including a legacy bare-hash sidecar or
// a corrupt file) — the same "nothing readable" bucket checkoutState's own
// "unknown" verdict already treats as unknown. Shared by checkoutState and
// ops.BranchStateAt (status.go) so the two can never disagree on what
// counts as a readable sidecar; BranchStateAt needs the raw record (not
// just checkoutState's collapsed stale/modified/clean verdict) to tell a
// lineage mismatch (its "detached" state) apart from a same-lineage
// epoch/txid mismatch (its "idle" — needs re-materialize, not orphaned) —
// see BranchStateAt's doc comment.
func readSidecar(path string) (sumRecord, bool) {
	raw, err := os.ReadFile(path + ".sum")
	if err != nil {
		return sumRecord{}, false
	}
	var rec sumRecord
	if err := json.Unmarshal(raw, &rec); err != nil || rec.Hash == "" {
		return sumRecord{}, false
	}
	return rec, true
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
