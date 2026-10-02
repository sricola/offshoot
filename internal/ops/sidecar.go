package ops

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/sricola/offshoot/internal/dbfile"
	"github.com/sricola/offshoot/internal/fsutil"
	"github.com/sricola/offshoot/internal/ltxio"
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

// fingerprintSafetyMargin is the minimum wall-clock gap checkoutState
// requires between a sidecar's recorded mtime and the moment it was stamped
// before it will trust a fingerprint match — git's "racily clean" rule
// (see the racy-git problem in git's own index code) applied to this
// sidecar.
//
// Without this margin, a write landing in the same mtime TICK as the stamp
// (materialize, checkpoint, or a hash-verified re-stamp all stamp
// immediately after reading the file they just proved clean) could leave
// the file's reported mtime IDENTICAL to what was just recorded even though
// the content changed again moments later, on any filesystem whose mtime
// resolution is coarser than the gap between the two writes: HFS+ (1s),
// many network filesystems (1s or worse), and even local filesystems under
// certain configurations. 1 second is what git itself uses for exactly this
// reason and comfortably covers every filesystem this codebase runs on.
//
// checkoutState only ever needs to wait out this margin once per stamp: a
// call that arrives too soon falls back to a full hash (as it always did
// before this field existed) and, on a match, re-stamps with a fresh
// StampedNS — so the very next call that arrives at least one margin later
// is fast again. See checkoutState's doc comment.
const fingerprintSafetyMargin = 1 * time.Second

// fingerprint is a checkout's fast-path evidence at some instant: its size,
// modification time (nanoseconds), SQLite's on-disk header change counter
// (bytes 24-27, big-endian), and whether the header's file-format
// read/write version bytes (18-19) currently read 2 (WAL mode). See
// stampFingerprint and sumRecord's doc comment for how each field is used.
type fingerprint struct {
	size          int64
	mtimeNS       int64
	changeCounter uint32
	walMode       bool
}

// stampFingerprint reads path's current fingerprint (see the fingerprint
// type's doc comment). A header shorter than 28 bytes — not a valid SQLite
// file yet, or one that predates its first committed transaction — is NOT
// an error: changeCounter and walMode simply read as their zero values,
// exactly as if the header held all-zero bytes there, and size/mtimeNS are
// still the real stat. A caller comparing this fingerprint against a
// recorded one for a REAL SQLite file will find size or mtime differ (a
// real database file is never this short), and falls back to a hash exactly
// like any other fingerprint mismatch.
//
// The header is read through internal/dbfile, not a bare os.Open, for the
// same reason fileSum is (see fileSum's doc comment): path may be a live
// checkout with SQLite connections held on it in this same process, and an
// ordinary open/close would silently drop this process's POSIX advisory
// locks on it.
func stampFingerprint(path string) (fingerprint, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return fingerprint{}, err
	}
	r, err := dbfile.Reader(path)
	if err != nil {
		return fingerprint{}, err
	}
	var header [28]byte
	n, err := r.ReadAt(header[:], 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return fingerprint{}, err
	}
	fp := fingerprint{size: fi.Size(), mtimeNS: fi.ModTime().UnixNano()}
	if n >= len(header) {
		fp.changeCounter = binary.BigEndian.Uint32(header[24:28])
		fp.walMode = header[18] == 2 && header[19] == 2
	}
	return fp, nil
}

// StampSum writes path's .sum sidecar directly from a hash the caller
// already obtained independently, skipping writeSum's own read-and-hash of
// path entirely — see sumRecord's doc comment for the on-disk shape.
// postApplyChecksum is optional (0 = absent, matching sumRecord's own
// zero-means-absent convention — see its doc comment); Session.
// commitSidecarRefresh instead uses StampSumHashOnly (see its doc comment
// for why). Inside this package, ops.go's post-materialize stamp and the
// by-chain cache (chainid.go) stamp through StampSum too.
//
// chainID is the chainID (see chainid.go) of the store chain the file's
// content is known to equal, or "" when the caller has none in hand (e.g. a
// session's close, whose content came from local writes, not a chain).
//
// StampSum also stats path itself and records its fingerprint (see the
// fingerprint type's doc comment): every StampSum caller stamps the SAME
// file whose bytes the hash argument describes, taken after that file
// reached its final path (a rename, where applicable, already happened) —
// see this function's own callers in ops.go and chainid.go. A caller with
// no file yet on disk is, by construction, not a StampSum caller.
//
// This single post-hash stat is why StampSum is not itself used by writeSum
// or checkoutState's own re-stamp: a stat taken here, after hash was
// already computed by some earlier, independent read, cannot prove hash
// still describes path's CURRENT bytes — a write could land in the gap
// between that read and this stat. Callers that compute the hash
// themselves via fileSum use stampSumWithFingerprint directly, sandwiching
// fileSum's read between two fingerprint reads and only trusting (and
// recording) the fingerprint when they agree — see writeSum and
// checkoutState.
func StampSum(path, hash, lineage string, epoch, txid, postApplyChecksum uint64, chainID string) error {
	statNS := time.Now().UnixNano()
	fp, err := stampFingerprint(path)
	if err != nil {
		return err
	}
	return stampSumWithFingerprint(path, hash, lineage, epoch, txid, postApplyChecksum, chainID, fp, statNS, true, false)
}

// StampSumHashOnly writes path's .sum sidecar from a hash and identity the
// caller already has, recording NO fingerprint at all (as if this were a
// pre-fingerprint sidecar) — the next checkoutState call against path
// always falls back to a full hash rather than trusting a fingerprint
// nothing actually verified against that specific hash. It then re-stamps
// with a real, sandwich-verified fingerprint on its own (see checkoutState's
// doc comment), so every call after that first one is fast again.
//
// Use this instead of StampSum whenever hash's provenance can't be
// correlated with a fresh stat of path taken here: Session.
// commitSidecarRefresh's hash is capture.State.MainHash, computed by the
// capture engine itself at its own verified-clean shutdown with no
// fingerprint recorded alongside it, so a stat taken independently, later,
// by this call cannot prove path still holds exactly the bytes MainHash was
// computed from. Re-hashing here to verify would defeat the entire point of
// trusting the engine's already-verified hash instead of paying for a
// second one — see StampSum's doc comment for the general hazard this
// avoids, and commitSidecarRefresh's own doc comment for why re-deriving a
// hash at that call site is specifically undesirable.
func StampSumHashOnly(path, hash, lineage string, epoch, txid, postApplyChecksum uint64, chainID string) error {
	return stampSumWithFingerprint(path, hash, lineage, epoch, txid, postApplyChecksum, chainID, fingerprint{}, 0, false, false)
}

// stampSumWithFingerprint writes path's .sum sidecar from a hash, identity,
// and a fingerprint the caller already captured (fp, meaningful only when
// ok) rather than one derived from a fresh stat of its own — the shared
// writer behind StampSum (single stat, ok always true), StampSumHashOnly
// (ok always false), and the sandwiched stamps in writeSum and
// checkoutState (ok reflects whether a fingerprint taken just before
// hashing still matched one taken just after). ok=false omits Size,
// ModTimeNS and ChangeCounter from the record (StampedNS along with them),
// exactly like a pre-fingerprint sidecar.
//
// stampedNS (meaningful only when ok) is recorded as StampedNS: the wall
// clock read just BEFORE fp's stat, never the time of this write. The
// racily-clean guard (fingerprintMatches) trusts a fingerprint only when
// its mtime is a margin older than StampedNS, and that guard is about the
// instant the evidence was observed: a sandwiched hash can take longer
// than the margin, and anchoring at write time would let an mtime written
// just after the before-stat, in the same coarse mtime tick, pass as
// settled.
//
// shadow is sumRecord.Shadow. Every stamp of new content passes false: the
// shadow (if any) was cloned from the PREVIOUS content, and only
// refreshShadow, after re-cloning, sets it again. checkoutState's
// hash-verified re-stamp is the one caller that passes the record's own
// value through, since it re-stamps the same identity and content.
func stampSumWithFingerprint(path, hash, lineage string, epoch, txid, postApplyChecksum uint64, chainID string, fp fingerprint, stampedNS int64, ok, shadow bool) error {
	rec := sumRecord{
		Hash: hash, Lineage: lineage, Epoch: epoch, TXID: txid,
		PostApplyChecksum: postApplyChecksum, ChainID: chainID, Shadow: shadow,
	}
	if ok {
		rec.Size, rec.ModTimeNS, rec.ChangeCounter = fp.size, fp.mtimeNS, fp.changeCounter
		rec.StampedNS = stampedNS
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return fsutil.ReplaceFileAtomic(path+".sum", data, 0o644)
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
//
// ChainID is the chainID (see chainid.go) of the resolved store chain the
// content was materialized from, omitted when unknown. It is additive: a
// sidecar without it decodes with ChainID "" and simply offers no by-chain
// fast path. It never replaces the (Lineage, Epoch, TXID) identity above,
// which stays the destination branch's own even when the bytes were cloned
// from another branch's identical chain.
//
// Size, ModTimeNS, ChangeCounter and StampedNS are the checkout's fingerprint
// (see the fingerprint type) at the moment it was stamped, plus the
// wall-clock instant (nanoseconds since the Unix epoch) that stamp was
// taken. checkoutState compares Size, ModTimeNS and (outside WAL mode)
// ChangeCounter against the LIVE file before ever falling back to a hash:
// when identity already matches (above), those fields also match, and
// ModTimeNS is safely older than StampedNS (by at least
// fingerprintSafetyMargin — the racily-clean guard; see its doc comment for
// why this is required, not optional), the checkout is proven clean in
// O(1) — no full-file read at all.
//
// ChangeCounter is NOT bumped on every committed write, contrary to an
// earlier draft of this comment: it is reliable evidence in
// rollback-journal mode, but SQLite's own WAL-mode commit path can leave it
// (and the rest of the main file's header) completely unchanged across
// several real, content-changing commits — verified empirically (three
// committed WAL writes, each followed by wal_checkpoint(TRUNCATE), left
// bytes 24-27 unchanged). checkoutState therefore never treats a matching
// ChangeCounter as evidence when the LIVE file's header reads as WAL mode
// (file-format version bytes 18-19 == 2): Size, ModTimeNS and the
// racily-clean guard alone carry that case.
//
// Shadow records that shadowPath(path) is a copy-on-write clone of this
// checkout taken right after this record was stamped, so it holds exactly
// the content at (Lineage, Epoch, TXID) — what an at-rest Checkpoint diffs
// against to write a segment (see planSegment). Additive: a sidecar
// without it decodes as false, which only means "no shadow; snapshot".
//
// All four fingerprint fields are additive and omitted (zero value) together on a
// pre-this-field sidecar; a real checked-out SQLite file always has a
// non-empty header and a real mtime, so an all-zero fingerprint can never
// spuriously match one, and checkoutState falls back to its full hash
// exactly as it always has. A fingerprint mismatch (including this all-zero
// old-format case) only ever costs an extra hash pass — it can never report
// "clean" for content that changed, because the hash comparison it falls
// back to is the same one this package always trusted.
type sumRecord struct {
	Hash              string `json:"hash"`
	Lineage           string `json:"lineage"`
	Epoch             uint64 `json:"epoch"`
	TXID              uint64 `json:"txid"`
	PostApplyChecksum uint64 `json:"post_apply_checksum,omitempty"`
	ChainID           string `json:"chain_id,omitempty"`
	Size              int64  `json:"size,omitempty"`
	ModTimeNS         int64  `json:"mtime_ns,omitempty"`
	ChangeCounter     uint32 `json:"change_counter,omitempty"`
	StampedNS         int64  `json:"stamped_ns,omitempty"`
	Shadow            bool   `json:"shadow,omitempty"`
}

// fingerprintMatches reports whether fp — a LIVE fingerprint, just read —
// is trustworthy evidence that path still holds rec's recorded content,
// without hashing:
//
//   - Size and ModTimeNS must both equal rec's.
//   - ChangeCounter must also equal rec's, UNLESS fp.walMode: SQLite's
//     WAL-mode commit path can leave the change counter unchanged across
//     real, content-changing commits (see sumRecord's doc comment), so it
//     must never be what makes this return true in WAL mode.
//   - rec.ModTimeNS must be strictly older than rec.StampedNS minus
//     fingerprintSafetyMargin — the racily-clean guard (see
//     fingerprintSafetyMargin's doc comment). A pre-fingerprint sidecar has
//     StampedNS == 0, so this is never satisfied by a real mtime (always a
//     large positive nanosecond timestamp) — the same zero-means-absent
//     fallback the other fields already rely on.
func fingerprintMatches(rec sumRecord, fp fingerprint) bool {
	if fp.size != rec.Size || fp.mtimeNS != rec.ModTimeNS {
		return false
	}
	if !fp.walMode && fp.changeCounter != rec.ChangeCounter {
		return false
	}
	return rec.ModTimeNS < rec.StampedNS-int64(fingerprintSafetyMargin)
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
// (writer-generation) Epoch — see sumRecord's doc comment. chainID is as
// StampSum's.
//
// The fingerprint is taken BEFORE fileSum's read and again AFTER: fileSum
// can take arbitrarily long on a large file, and path may have a capture
// engine holding connections on it in this same process (see fileSum's own
// doc comment) — a write landing anywhere in that window would otherwise
// get silently attributed to the hash computed from the OLD bytes, and
// every later checkoutState call would then trust that stale hash as
// "clean" without ever having hashed the new content. Only when the two
// fingerprints agree — nothing wrote to path while it was being hashed — is
// it safe to record one at all; on a mismatch, no fingerprint is recorded
// (the sidecar still gets the correct hash and identity), so the next
// checkoutState call simply hashes once more rather than trusting an
// unverified linkage.
func writeSum(path string, lineage string, epoch, txid, postApplyChecksum uint64, chainID string) error {
	sum, fp, beforeNS, ok, err := sandwichedSum(path)
	if err != nil {
		return err
	}
	return stampSumWithFingerprint(path, sum, lineage, epoch, txid, postApplyChecksum, chainID, fp, beforeNS, ok, false)
}

// untrustedHash is the Hash a checkpoint stamps when it cannot vouch that
// the checkout holds the content its committed head resolves to (see
// stampCheckpoint). It is not a hex SHA-256, so no file's fileSum ever
// equals it: checkoutState reads such a checkout "modified" against that
// head, never "clean", and warnIfUncheckpointed and CheckoutProven treat
// it as un-checkpointed changes.
func untrustedHash(txid uint64) string {
	return "untrusted:" + strconv.FormatUint(txid, 10)
}

// stampCheckpoint writes the sidecar for a checkpoint that just won its ref
// CAS at (lineage, epoch, txid), and reports whether it trusted the stamp
// (only then may the caller refresh the shadow).
//
// headSum is the post-apply checksum of the content the store resolves the
// head to (headKnown false when it could not be established), and encSum
// the checksum of the bytes this checkpoint encoded, whose fingerprint
// (taken right after quiesce, before the encode) is fpEncode (fpEncodeOK
// false when that stat failed). The live checkout is hashed between two
// fingerprints as in writeSum; its post-apply checksum is encSum when that
// fingerprint still equals fpEncode, and otherwise ltxio.ChecksumDatabase
// read under the same fingerprint.
//
// The stamp is trusted — the real hash, fingerprint and headSum — only
// when the live checkout's checksum is known and equals headSum: the store
// holds exactly what the checkout holds. Otherwise (a racer's different
// content resolves the head, the checkout changed between the encode and
// now, or either side is unknown) it records checksum 0 and untrustedHash
// with no fingerprint, so checkoutState reads "modified" and the next
// checkpoint writes a snapshot.
func stampCheckpoint(path, lineage string, epoch, txid, headSum uint64, headKnown bool, encSum uint64, fpEncode fingerprint, fpEncodeOK bool) (bool, error) {
	sum, fp, beforeNS, ok, err := sandwichedSum(path)
	if err != nil {
		return false, err
	}
	if headKnown && ok {
		liveSum, liveKnown := encSum, fpEncodeOK && fp == fpEncode
		if !liveKnown {
			if c, cerr := ltxio.ChecksumDatabase(path); cerr == nil {
				if fpAfter, ferr := stampFingerprint(path); ferr == nil && fpAfter == fp {
					liveSum, liveKnown = c, true
				}
			}
		}
		if liveKnown && liveSum == headSum {
			return true, stampSumWithFingerprint(path, sum, lineage, epoch, txid, headSum, "", fp, beforeNS, true, false)
		}
	}
	return false, StampSumHashOnly(path, untrustedHash(txid), lineage, epoch, txid, 0, "")
}

// sandwichedSum hashes path (fileSum) between two fingerprint reads. ok
// reports whether both reads succeeded and agree — nothing wrote to path
// while it was hashed — and only then is fp the fingerprint to record,
// with beforeNS (the wall clock read just before the first stat) as its
// StampedNS; see writeSum and stampSumWithFingerprint.
func sandwichedSum(path string) (sum string, fp fingerprint, beforeNS int64, ok bool, err error) {
	beforeNS = time.Now().UnixNano()
	fpBefore, errBefore := stampFingerprint(path)
	if sum, err = fileSum(path); err != nil {
		return "", fingerprint{}, 0, false, err
	}
	fpAfter, errAfter := stampFingerprint(path)
	if errBefore == nil && errAfter == nil && fpBefore == fpAfter {
		return sum, fpAfter, beforeNS, true, nil
	}
	return sum, fingerprint{}, 0, false, nil
}

// checkoutState reports how the checkout at path relates to ref, and — only
// when the verdict is "clean" — the sidecar's own recorded
// PostApplyChecksum (0 for every other verdict, and for a "clean" verdict
// against an older-format sidecar that never recorded one; see
// CheckoutResult.PostApplyChecksum for how callers are expected to treat a
// zero value):
//
//   - "clean": the sidecar's recorded identity (lineage, epoch, txid)
//     matches ref, and the file's content still matches the recorded hash
//     — proven either from a full hash (as always) or, when
//     fingerprintMatches finds the sidecar's recorded fingerprint
//     trustworthy against the LIVE file, without hashing at all (see
//     fingerprintMatches's doc comment for exactly what that requires,
//     including the racily-clean guard and the WAL-mode change-counter
//     exclusion).
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
//
// The fingerprint fast path never overrides the identity check above: it
// only ever runs once (lineage, epoch, txid) already matches ref. Nothing
// in fingerprintMatches can make an actually-modified file read as clean:
// any fingerprint mismatch (including every pre-fingerprint sidecar, whose
// four fields all decode to zero and so can never match a real file's
// fingerprint) falls back to the full hash exactly as this function always
// behaved.
//
// When that hash still matches, the sidecar is re-stamped so a LATER call
// can take the fast path — but only with a fingerprint taken just before
// this function's own fileSum call if one taken just after still agrees
// (the same sandwich writeSum uses, and for the same reason: fileSum's read
// is not instantaneous, and a write racing it must not get attributed to
// the hash just computed from the bytes before it). Best-effort either way
// — a failure, or a disagreeing sandwich, costs a future call a hash pass,
// never correctness, and never changes this call's own "clean" verdict.
//
// Correctness for both SQLite journal modes rests entirely on this
// function's callers (CheckoutProven, warnIfUncheckpointed), which already
// quiesce path (a full wal_checkpoint(TRUNCATE)) before ever calling this:
// a rollback-journal commit bumps the main file's header change counter (and
// mtime) directly, but a WAL-mode commit can leave the header (change
// counter included) untouched even once checkpointed — quiescing first is
// what makes the main file's SIZE and MTIME trustworthy in either mode
// (what fingerprintMatches actually leans on for WAL), exactly as it
// already had to be for fileSum's own read below.
func checkoutState(path string, ref store.Ref) (string, uint64) {
	rec, ok := readSidecar(path)
	if !ok {
		return "unknown", 0
	}
	if rec.Lineage != ref.Lineage || rec.Epoch != ref.HeadEpoch || rec.TXID != ref.HeadTXID {
		return "stale", 0
	}
	beforeNS := time.Now().UnixNano()
	fpBefore, fpErr := stampFingerprint(path)
	if fpErr == nil && fingerprintMatches(rec, fpBefore) {
		return "clean", rec.PostApplyChecksum
	}
	got, err := fileSum(path)
	if err != nil {
		return "unknown", 0
	}
	if got != rec.Hash {
		return "modified", 0
	}
	if fpAfter, err2 := stampFingerprint(path); fpErr == nil && err2 == nil && fpBefore == fpAfter {
		_ = stampSumWithFingerprint(path, got, ref.Lineage, ref.HeadEpoch, ref.HeadTXID, rec.PostApplyChecksum, rec.ChainID, fpAfter, beforeNS, true, rec.Shadow)
	} else {
		_ = stampSumWithFingerprint(path, got, ref.Lineage, ref.HeadEpoch, ref.HeadTXID, rec.PostApplyChecksum, rec.ChainID, fingerprint{}, 0, false, rec.Shadow)
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

// observeFileSum, when non-nil, is called at the top of every fileSum
// invocation — a test-only seam (mirrors observeCheckoutSource in
// chainid.go) letting tests count how many times the full O(size) hash path
// actually ran, rather than inferring it indirectly. nil in production.
var observeFileSum func()

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
	if observeFileSum != nil {
		observeFileSum()
	}
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

// shadowPath is where a writable checkout's shadow lives: a sibling file,
// never under checkouts-ro (see sumRecord.Shadow).
func shadowPath(checkoutPath string) string { return checkoutPath + ".shadow" }

// refreshShadow makes path's shadow a copy-on-write clone of path as it is
// now and records that in its sidecar. Call it right after stamping path's
// sidecar with the committed state path now holds (a checkpoint's CAS, a
// materialize, a repoint's refresh): the stamp wrote Shadow=false, so a
// failure anywhere below leaves a sidecar that already says "no shadow".
// The clone goes to a temp name and is renamed into place, never written
// over the old shadow. A filesystem that cannot clone (reflink.
// ErrUnsupported), or any other failure, drops the shadow instead: the
// next Checkpoint then writes a snapshot, exactly as before shadows
// existed. Best-effort by design, so it returns nothing.
func refreshShadow(path string) {
	shadow := shadowPath(path)
	tmp := shadow + ".tmp"
	os.Remove(tmp) // a leftover from an interrupted refresh is never live
	if err := cloneFile(tmp, path); err != nil {
		dropShadow(path)
		return
	}
	// The shadow holds the checkout's content, so it gets the checkout's
	// 0600 whatever mode the clone was created with (Linux FICLONE creates
	// its destination itself).
	if err := os.Chmod(tmp, 0o600); err != nil {
		os.Remove(tmp)
		dropShadow(path)
		return
	}
	if err := os.Rename(tmp, shadow); err != nil {
		os.Remove(tmp)
		dropShadow(path)
		return
	}
	if err := setSidecarShadow(path, true); err != nil {
		dropShadow(path)
	}
}

// RefreshShadow is refreshShadow for internal/session, whose clean Close
// re-stamps the checkout after its daemon session moved the head by
// segments, so the next at-rest Checkpoint can diff against that head.
func RefreshShadow(path string) { refreshShadow(path) }

// dropShadow records Shadow=false in path's sidecar (when it has one) and
// removes the shadow file, in that order, so no moment exists where the
// sidecar vouches for a shadow that is gone. Best-effort.
func dropShadow(path string) {
	_ = setSidecarShadow(path, false)
	os.Remove(shadowPath(path))
	os.Remove(shadowPath(path) + ".tmp")
}

// setSidecarShadow rewrites path's sidecar with Shadow=on and every other
// field as recorded. It never re-stamps the fingerprint: the checkout's
// bytes did not change, so the recorded fingerprint (or its absence) stays
// exactly as true as it was. A sidecar that is missing or unreadable is
// left alone when on is false (nothing vouches for a shadow) and is an
// error when on is true.
func setSidecarShadow(path string, on bool) error {
	rec, ok := readSidecar(path)
	if !ok {
		if on {
			return errors.New("ops: no readable sidecar to record a shadow in")
		}
		return nil
	}
	if rec.Shadow == on {
		return nil
	}
	rec.Shadow = on
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return fsutil.ReplaceFileAtomic(path+".sum", data, 0o644)
}
