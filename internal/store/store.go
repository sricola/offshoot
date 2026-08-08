package store

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	LayoutVersion = 2
	RefSchema     = 2
	manifestKey   = "offshoot.json"
	maxNameLen    = 128
)

type Manifest struct {
	LayoutVersion int    `json:"layout_version"`
	CreatedAt     string `json:"created_at"`
}

// Checkpoint locates a snapshot object: its transaction id and the epoch of
// the prefix it was written under. Epoch matters because acquiring or
// reclaiming a branch bumps the epoch, and objects stay where they were
// written.
type Checkpoint struct {
	TXID  uint64 `json:"txid"`
	Epoch uint64 `json:"epoch"`
	// CreatedAt is when this checkpoint was created, RFC3339 UTC, stamped by
	// every ops call site that creates one (Create's "init", Checkpoint,
	// Fork's "fork", Promote's "promote", and a named session flush).
	// Omitempty so a checkpoint written before this field existed (or a v1
	// ref's upgraded bare-number checkpoints, which have no creation time to
	// recover) decodes with it empty rather than a fabricated value.
	CreatedAt string `json:"created_at,omitempty"`
	// Meta is a small user-supplied string->string map describing this
	// specific checkpoint (e.g. eval run id, git SHA, agent id), capped at
	// the ops layer (ops.ValidateMeta: at most 32 keys, keys <= 64 bytes,
	// values <= 512 bytes — see the design spec's metadata cap). Omitempty;
	// nil/absent means no metadata was given.
	Meta map[string]string `json:"meta,omitempty"`
}

type Ref struct {
	Schema  int    `json:"schema"`
	Lineage string `json:"lineage"`
	// Epoch identifies a lineage's current writer generation. AcquireLease
	// bumps it on every fresh acquisition (and on reclaim of a dead lease —
	// see lease.go), fencing out whatever a superseded holder might still
	// write: chain resolution collapses same-range members down to the
	// highest epoch (keepHighestEpoch), so a fenced writer's stragglers
	// lose deterministically.
	Epoch       uint64                `json:"epoch"`
	HeadTXID    uint64                `json:"head_txid"`
	HeadEpoch   uint64                `json:"head_epoch"`
	Checkpoints map[string]Checkpoint `json:"checkpoints"`
	Parent      string                `json:"parent,omitempty"`
	Protected   bool                  `json:"protected"`
	// Lease fields are empty when no writer holds the branch.
	LeaseHolder string `json:"lease_holder,omitempty"`
	LeaseExpiry string `json:"lease_expiry,omitempty"` // RFC3339Nano UTC
	// TTL fields govern branch reaping (Plan 8). TTL is a Go duration string
	// ("2h"); empty means the branch is never reaped. TouchedAt is the
	// activity clock: Reaping (ops.Reap, Task 2) measures TTL from the later
	// of TouchedAt and LeaseExpiry, so any durable write or lease activity
	// defers expiry. Reaping is set by Reap's CAS claim (Task 2) and, while
	// true, refuses ops.Touch. All three are omitempty so a Plan-7 ref
	// decodes with them empty/false — no TTL means never reaped.
	TTL       string `json:"ttl,omitempty"`
	TouchedAt string `json:"touched_at,omitempty"` // RFC3339Nano UTC
	Reaping   bool   `json:"reaping,omitempty"`
	// Deleting and DeletingAt are ops.Destroy's own CAS claim (Milestone 4
	// Task 6b), the same shape as Reaping/TouchedAt above but a DELIBERATELY
	// SEPARATE field, not a unification of the two: Destroy CAS-writes
	// Deleting=true (stamping DeletingAt) before it does anything
	// irreversible, so a lease acquired in the window between Destroy's
	// initial GetRef and its actual delete refuses outright (AcquireLease
	// checks this exactly like it already checks Reaping) instead of racing
	// a branch out from under its new holder — the Destroy TOCTOU the M2
	// review documented. This was scoped as a sibling to Reaping rather than
	// a generalized {none,reaping,deleting} claim enum per Milestone 4 Task
	// 6b's timebox (PM Amendment 9): Reaping's CAS mechanics are
	// torture/race-tested and deliberately left untouched here. DeletingAt
	// (RFC3339Nano UTC, stamped at claim time) is Deleting's own staleness
	// clock — see ops.ClearStaleDeleteClaims — since a Deleting claim, unlike
	// Reaping, has no TTL/activity deadline to recompute against; it just
	// self-heals by age. Both omitempty: a ref decodes with Deleting false
	// and DeletingAt empty exactly like every pre-Task-6b ref.
	Deleting   bool   `json:"deleting,omitempty"`
	DeletingAt string `json:"deleting_at,omitempty"`
	// Meta is a small user-supplied string->string map describing this
	// branch's lineage (e.g. eval run id, git SHA, agent id), set by Fork
	// and capped at the ops layer (ops.ValidateMeta). Branch-level lineage
	// is the grain — no row-level provenance. Omitempty, no schema bump: a
	// ref written before this field existed decodes with it nil, same as
	// the TTL fields above.
	Meta map[string]string `json:"meta,omitempty"`
	// Base is the copy-on-write shared-fork pointer: when set, this
	// lineage's objects alone do not cover reads at targetTXID <= Base.TXID
	// — resolution must fall through to Base.Lineage (transitively, if that
	// lineage has its own Base) for anything at or below the fork point.
	// This is DELIBERATELY DISTINCT from Parent (a human-readable breadcrumb
	// with no resolution or GC meaning): Base is live, load-bearing data
	// that both Chain resolution (chainFrom's fall-through) and GC
	// reachability (ops.reachableObjects' base-spine marking) consult. Nil
	// means this ref has no base — every pre-CoW ref, and every fork that
	// fully materializes (a plain Fork SHARES by default, setting this,
	// whenever the resolved chain is below the fork-time depth bound; it
	// materializes — nil Base — only at the floor or under the test hooks,
	// see ops.Fork). No epoch field: an epoch here would let a fenced writer's
	// stale base survive past the point it was superseded, re-creating the
	// fenced-orphan bug epoch fencing exists to prevent — see the design
	// spec's "The base pointer" section. Omitempty, no schema bump: a ref
	// written before this field existed decodes with it nil.
	Base *BasePointer `json:"base,omitempty"`
}

// BasePointer names the fork point a shared child reads through for
// anything at or below Base.TXID: Lineage identifies the ancestor lineage,
// TXID is the fork point within it. Deliberately just these two fields — no
// epoch (see Ref.Base's doc comment for why).
type BasePointer struct {
	Lineage string `json:"lineage"`
	TXID    uint64 `json:"txid"`
}

// SetCheckpoint records name -> cp, allocating the map if needed.
func (r *Ref) SetCheckpoint(name string, cp Checkpoint) {
	if r.Checkpoints == nil {
		r.Checkpoints = map[string]Checkpoint{}
	}
	r.Checkpoints[name] = cp
}

// Touch stamps the ref's activity clock. Reaping (ops.Reap) measures TTL
// from the later of this stamp and the lease expiry, so any durable write
// or lease activity defers expiry.
func (r *Ref) Touch(now time.Time) {
	r.TouchedAt = now.UTC().Format(time.RFC3339Nano)
}

// refWire is the tolerant on-disk shape: checkpoints are either v1 numbers or
// v2 objects, so one decode handles both schemas.
type refWire struct {
	Schema      int                        `json:"schema"`
	Lineage     string                     `json:"lineage"`
	Epoch       uint64                     `json:"epoch"`
	HeadTXID    uint64                     `json:"head_txid"`
	HeadEpoch   uint64                     `json:"head_epoch"`
	Checkpoints map[string]json.RawMessage `json:"checkpoints"`
	Parent      string                     `json:"parent,omitempty"`
	Protected   bool                       `json:"protected"`
	LeaseHolder string                     `json:"lease_holder,omitempty"`
	LeaseExpiry string                     `json:"lease_expiry,omitempty"`
	TTL         string                     `json:"ttl,omitempty"`
	TouchedAt   string                     `json:"touched_at,omitempty"`
	Reaping     bool                       `json:"reaping,omitempty"`
	Deleting    bool                       `json:"deleting,omitempty"`
	DeletingAt  string                     `json:"deleting_at,omitempty"`
	Meta        map[string]string          `json:"meta,omitempty"`
	Base        *BasePointer               `json:"base,omitempty"`
}

// decodeRef parses the on-disk ref shape, upgrading a v1 ref (schema 1,
// checkpoints as name -> number) to v2 in memory: every checkpoint's epoch
// becomes the ref's own epoch (that is where everything a v1 binary wrote
// actually lived), and so does HeadEpoch. A schema newer than this binary
// understands is refused rather than guessed at.
func decodeRef(data []byte) (Ref, error) {
	var w refWire
	if err := json.Unmarshal(data, &w); err != nil {
		return Ref{}, err
	}
	if w.Schema > RefSchema {
		return Ref{}, fmt.Errorf(
			"store: ref schema %d is newer than this binary supports (%d)", w.Schema, RefSchema)
	}
	r := Ref{
		Schema: RefSchema, Lineage: w.Lineage, Epoch: w.Epoch,
		HeadTXID: w.HeadTXID, HeadEpoch: w.HeadEpoch,
		Parent: w.Parent, Protected: w.Protected,
		LeaseHolder: w.LeaseHolder, LeaseExpiry: w.LeaseExpiry,
		TTL: w.TTL, TouchedAt: w.TouchedAt, Reaping: w.Reaping,
		Deleting: w.Deleting, DeletingAt: w.DeletingAt,
		Meta: w.Meta, Base: w.Base,
	}
	// A v1 ref predates per-checkpoint epochs: everything it references was
	// written under the ref's own epoch.
	if r.HeadEpoch == 0 {
		r.HeadEpoch = w.Epoch
	}
	for name, raw := range w.Checkpoints {
		if string(raw) == "null" {
			return Ref{}, fmt.Errorf("store: bad checkpoint %q: null", name)
		}
		var cp Checkpoint
		if err := json.Unmarshal(raw, &cp); err == nil && cp.TXID != 0 {
			r.SetCheckpoint(name, cp)
			continue
		}
		var txid uint64
		if err := json.Unmarshal(raw, &txid); err != nil {
			return Ref{}, fmt.Errorf("store: bad checkpoint %q: %w", name, err)
		}
		r.SetCheckpoint(name, Checkpoint{TXID: txid, Epoch: w.Epoch})
	}
	return r, nil
}

type Store struct {
	B Backend

	// layoutAtLeastV2 memoizes EnsureLayoutV2's "manifest already >=
	// LayoutVersion" observation so repeated shared forks pay the manifest
	// Get once per process instead of once per fork. Safe to cache because
	// the layout version is monotonic (nothing ever lowers it): the flag
	// only goes false -> true, a stale false merely re-does the (correct)
	// Get, and a true is always still true. Atomic because a Store is
	// shared across goroutines (e.g. the daemon's ops all hold one).
	layoutAtLeastV2 atomic.Bool
}

func (s *Store) InitManifest() error {
	m := Manifest{LayoutVersion: LayoutVersion, CreatedAt: time.Now().UTC().Format(time.RFC3339)}
	data, _ := json.Marshal(m)
	_, err := s.B.PutIf(manifestKey, data, "")
	if err != nil {
		return fmt.Errorf("store: init: %w", err)
	}
	return nil
}

func (s *Store) CheckManifest() error {
	data, _, err := s.B.Get(manifestKey)
	if err != nil {
		return err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("store: corrupt manifest: %w", err)
	}
	if m.LayoutVersion > LayoutVersion {
		return fmt.Errorf("store: layout version %d is newer than this binary supports (%d)",
			m.LayoutVersion, LayoutVersion)
	}
	return nil
}

// EnsureLayoutV2 CAS-bumps the store's manifest to LayoutVersion 2 if it is
// currently below that; it is a no-op if the manifest is already at 2 or
// newer. It is idempotent and CAS-safe under concurrent callers: if this
// call's CAS loses the race because another caller already bumped the
// manifest to >= 2, that is re-checked and treated as success, not failure.
// Called by ops.Fork's SHARE path before writing the first base ref, since
// a base pointer must never land in a store an old binary could still open.
//
// The ">= 2" observation is memoized on the Store (layoutAtLeastV2, see its
// field comment for why that is safe): after the first success this costs
// zero RPCs, so a swarm of shared forks pays the manifest Get once, not once
// per fork.
func (s *Store) EnsureLayoutV2() error {
	if s.layoutAtLeastV2.Load() {
		return nil
	}
	for {
		data, etag, err := s.B.Get(manifestKey)
		if err != nil {
			return err
		}
		var m Manifest
		if err := json.Unmarshal(data, &m); err != nil {
			return fmt.Errorf("store: corrupt manifest: %w", err)
		}
		if m.LayoutVersion >= LayoutVersion {
			s.layoutAtLeastV2.Store(true)
			return nil
		}
		m.LayoutVersion = LayoutVersion
		newData, err := json.Marshal(m)
		if err != nil {
			return err
		}
		if _, err := s.B.PutIf(manifestKey, newData, etag); err != nil {
			if errors.Is(err, ErrCAS) {
				// Someone else changed the manifest since our Get; re-read
				// and either retry the bump or discover they already did it.
				continue
			}
			return err
		}
		s.layoutAtLeastV2.Store(true)
		return nil
	}
}

func ValidateName(name string) error {
	if name == "" || len(name) > maxNameLen {
		return fmt.Errorf("store: invalid name %q (1-%d chars)", name, maxNameLen)
	}
	// "." and ".." are directory-traversal segments once a name is joined
	// into a key path (e.g. refs/./main collapses to refs/main on the local
	// backend, silently dropping the db path component into an invisible
	// ref that GC then reaps the data for). Reject them, and any name
	// containing ".." as a substring, outright.
	if name == "." || name == ".." || strings.Contains(name, "..") {
		return fmt.Errorf("store: invalid name %q (must not be \".\" or \"..\", or contain \"..\")", name)
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return fmt.Errorf("store: invalid name %q (allowed: [a-z0-9-_.])", name)
		}
	}
	return nil
}

func NewLineageID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	return hex.EncodeToString(b)
}

func RefKey(db, branch string) string { return "refs/" + db + "/" + branch }

func SnapshotKey(lineage string, epoch, txid uint64) string {
	return fmt.Sprintf("data/%s/%d/snapshot-%016x.ltx", lineage, epoch, txid)
}

func LineagePrefix(lineage string) string { return "data/" + lineage + "/" }

// BaseKey locates a lineage's durable base pointer. It lives under
// LineagePrefix(lineage) alongside the lineage's snapshot/segment objects, so
// it is swept together with the lineage by GC. It is deliberately NEITHER
// snapshot- nor segment-prefixed, so ParseMemberKey returns ok=false for it
// and Chain never mistakes it for a materialization member.
func BaseKey(lineage string) string { return "data/" + lineage + "/base.json" }

// lineageBase loads the durable per-lineage base pointer — the copy-on-write
// resolution source. It is a per-lineage OBJECT (not a field read from the
// ref) precisely because a shared parent branch can be destroyed (its ref
// deleted) while a live descendant still bases on it: resolution must follow
// the base pointer of a lineage that no longer has a ref at all. Returns
// (nil, nil) when the lineage has no base object — a non-shared, pre-CoW
// lineage (today's path). Ref.Base is only a reporting mirror; it is NEVER the
// resolution source.
func (s *Store) lineageBase(lineage string) (*BasePointer, error) {
	data, _, err := s.B.Get(BaseKey(lineage))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var b BasePointer
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("store: base %s: %w", lineage, err)
	}
	return &b, nil
}

// WriteLineageBase durably records a lineage's base pointer, create-only: a
// lineage's base is immutable once written, so a second write is refused with
// ErrCAS (surfaced as-is). The shared-fork path (ops.Fork) is the real
// caller; it writes the base BEFORE the child ref CAS so resolution's source
// of truth exists by the time any reader can see the child.
//
// A base naming the lineage itself is refused outright: Chain resolves a
// based lineage by recursing into its base, so a self-base would recurse
// forever. (A longer cycle through several lineages would too, but bases are
// create-only and always point at an ALREADY-EXISTING lineage, so a cycle
// cannot be constructed through this writer; the self-base is the one cheap,
// local mistake worth guarding here.)
func (s *Store) WriteLineageBase(lineage string, b BasePointer) error {
	if b.Lineage == lineage {
		return fmt.Errorf("store: lineage %s cannot be its own base", lineage)
	}
	data, err := json.Marshal(b)
	if err != nil {
		return err
	}
	if _, err := s.B.PutIf(BaseKey(lineage), data, ""); err != nil {
		return err
	}
	return nil
}

// BaseSpine returns every ANCESTOR lineage in lineage's base spine, nearest
// first: the lineage's own base, that base's base, and so on until a lineage
// with no base object. The starting lineage itself is NOT included; a lineage
// with no base returns an empty spine. It is a thin walk over lineageBase —
// deliberately NOT a reimplementation of Chain's member resolution — for
// callers (GC's reachability mark) that need the set of lineages whose
// base.json objects resolution will read, independent of which of them
// contribute chain members: a pass-through lineage (forked but never
// diverged) contributes zero members yet its base.json IS read.
//
// An INVARIANT of the walk: lineage L's base.json exists exactly when the
// walk out of L found a base — i.e. BaseKey(L) exists for the starting
// lineage iff the spine is non-empty, and for spine[i] iff i+1 < len(spine)
// (the walk ended at spine[len-1] precisely because it has no base object).
// GC leans on this to mark exactly the existing base.json objects without
// extra existence probes.
//
// WriteLineageBase's create-only, must-point-at-an-existing-lineage rules
// mean a cycle cannot be constructed through this binary's writers, but the
// walk still guards against one defensively (a corrupt or hand-edited
// store): revisiting a lineage fails loudly rather than looping forever.
func (s *Store) BaseSpine(lineage string) ([]string, error) {
	var spine []string
	seen := map[string]bool{lineage: true}
	cur := lineage
	for {
		b, err := s.lineageBase(cur)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return spine, nil
		}
		if seen[b.Lineage] {
			return nil, fmt.Errorf("store: base spine of %s revisits lineage %s (cycle)", lineage, b.Lineage)
		}
		seen[b.Lineage] = true
		spine = append(spine, b.Lineage)
		cur = b.Lineage
	}
}

// SegmentKey locates an incremental segment covering (minTXID, maxTXID].
// Sorting by key sorts by maxTXID, so a lexical List is already in apply
// order.
func SegmentKey(lineage string, epoch, minTXID, maxTXID uint64) string {
	return fmt.Sprintf("data/%s/%d/segment-%016x-%016x.ltx", lineage, epoch, maxTXID, minTXID)
}

// ChainMember identifies one object in a materialization chain.
type ChainMember struct {
	Key              string
	Snapshot         bool
	MinTXID, MaxTXID uint64
	Epoch            uint64
}

// ParseMemberKey parses a snapshot or segment key back into a ChainMember.
func ParseMemberKey(key string) (ChainMember, bool) {
	rest, ok := strings.CutPrefix(key, "data/")
	if !ok {
		return ChainMember{}, false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 3 {
		return ChainMember{}, false
	}
	lineage, epochStr, file := parts[0], parts[1], parts[2]
	if lineage == "" {
		return ChainMember{}, false
	}
	epoch, err := strconv.ParseUint(epochStr, 10, 64)
	if err != nil {
		return ChainMember{}, false
	}
	switch {
	case strings.HasPrefix(file, "snapshot-") && strings.HasSuffix(file, ".ltx"):
		hexStr := strings.TrimSuffix(strings.TrimPrefix(file, "snapshot-"), ".ltx")
		txid, err := strconv.ParseUint(hexStr, 16, 64)
		if err != nil {
			return ChainMember{}, false
		}
		return ChainMember{Key: key, Snapshot: true, MinTXID: 0, MaxTXID: txid, Epoch: epoch}, true
	case strings.HasPrefix(file, "segment-") && strings.HasSuffix(file, ".ltx"):
		hexStr := strings.TrimSuffix(strings.TrimPrefix(file, "segment-"), ".ltx")
		fields := strings.Split(hexStr, "-")
		if len(fields) != 2 {
			return ChainMember{}, false
		}
		maxTXID, err := strconv.ParseUint(fields[0], 16, 64)
		if err != nil {
			return ChainMember{}, false
		}
		minTXID, err := strconv.ParseUint(fields[1], 16, 64)
		if err != nil {
			return ChainMember{}, false
		}
		return ChainMember{Key: key, Snapshot: false, MinTXID: minTXID, MaxTXID: maxTXID, Epoch: epoch}, true
	default:
		return ChainMember{}, false
	}
}

// keepHighestEpoch collapses members covering an identical TXID range down to
// the one written under the highest epoch. A lower-epoch duplicate is an
// orphan left by a writer that was fenced before its ref write landed, so it
// must never be chosen — see the fencing note in Chain.
func keepHighestEpoch(members []ChainMember) []ChainMember {
	type rng struct{ min, max uint64 }
	best := make(map[rng]ChainMember, len(members))
	for _, m := range members {
		k := rng{m.MinTXID, m.MaxTXID}
		if cur, ok := best[k]; !ok || m.Epoch > cur.Epoch {
			best[k] = m
		}
	}
	out := make([]ChainMember, 0, len(best))
	for _, m := range best {
		out = append(out, m)
	}
	return out
}

// Chain returns the members needed to materialize lineage at target: the
// newest snapshot with MaxTXID <= target, followed by every segment after it
// up to target, in apply order. It returns an error when no snapshot covers
// the target or the segments do not form a contiguous run — a caller must
// never be handed a chain with a hole.
//
// It lists LineagePrefix(lineage) once and works from the parsed keys, so it
// costs one List regardless of epoch count — segments from superseded
// epochs are simply members like any other, since an epoch bump does not
// move objects.
//
// Copy-on-write base following (the one hard rule): if the lineage has a
// durable base pointer (see lineageBase), resolution follows it under a
// STRICT, non-negotiable invariant — resolution NEVER merges members across
// lineages. Each half of a shared chain is resolved wholly within one
// lineage's List, and keepHighestEpoch is only ever run on a single lineage's
// members. A cross-lineage union would let a higher-epoch parent object win
// the child's own txid range and silently serve the parent's timeline.
//
// Like BaseSpine, the base recursion guards defensively against a cycle:
// WriteLineageBase's create-only, no-self-base, must-point-at-an-existing-
// lineage rules mean this binary's writers cannot construct one, but a
// corrupt or hand-edited store could — revisiting a lineage fails loudly
// with a cycle error rather than recursing until the stack overflows.
func (s *Store) Chain(lineage string, target uint64) ([]ChainMember, error) {
	return s.chainFrom(lineage, target, map[string]bool{lineage: true})
}

// chainFrom is Chain's recursive body, threading the set of lineages already
// visited along the base recursion (seeded with the starting lineage) so a
// corrupt store's base-pointer cycle is detected instead of recursed into —
// see Chain's doc comment. Chain is the only entry point; both recursion
// sites below go through chainFrom so the same seen set covers the whole
// resolution.
func (s *Store) chainFrom(lineage string, target uint64, seen map[string]bool) ([]ChainMember, error) {
	base, err := s.lineageBase(lineage)
	if err != nil {
		return nil, err
	}
	if base == nil {
		// No base pointer: the self-contained single-lineage path, unchanged
		// for every pre-CoW branch.
		return s.chainSelf(lineage, target)
	}
	if seen[base.Lineage] {
		return nil, fmt.Errorf("store: chain %s: base pointer cycle detected at lineage %s",
			lineage, base.Lineage)
	}
	seen[base.Lineage] = true
	if target <= base.TXID {
		// At or below the fork point: resolve entirely in the base lineage
		// (transitively, via the base's own base). The child contributes
		// nothing.
		return s.chainFrom(base.Lineage, target, seen)
	}
	// Above the fork point: if the child has grown its OWN snapshot covering
	// target (a divergence floor — ops.Checkpoint always writes one, and the
	// session's snapshot cadence will too), resolution anchors there and
	// never touches the base: the whole chain lives in this one lineage.
	// A shared child is born with zero objects and only ever writes txids
	// above its fork point, so a child snapshot is necessarily > base.TXID
	// and chainSelf succeeding is always a complete, single-lineage answer.
	// Fall through to the seam path ONLY when chainSelf failed specifically
	// because no child snapshot covers target (the sentinel) — the expected
	// state of a shared child that has not written its own floor yet. Every
	// other chainSelf error (a backend List failure, a hole past the child's
	// own snapshot) propagates unchanged: swallowing it into the seam path
	// would misreport e.g. a transient backend error as a bogus "hole".
	// One List serves BOTH walks below: the snapshot-anchored self attempt
	// and, if that reports no own snapshot, the seam-range walk over the
	// SAME parsed segments. Both operate strictly on THIS lineage's members
	// (listMembers already ran keepHighestEpoch per-lineage), so sharing the
	// parse changes nothing about the never-merge-across-lineages rule — it
	// only removes a second List of the identical prefix.
	snapshots, segments, err := s.listMembers(lineage)
	if err != nil {
		return nil, err
	}
	selfChain, selfErr := resolveSelf(lineage, target, snapshots, segments)
	if selfErr == nil {
		return selfChain, nil
	}
	if !errors.Is(selfErr, errNoSnapshotCoversTarget) {
		return nil, selfErr
	}
	// Concatenate two INDEPENDENTLY-resolved halves.
	// The base half is resolved wholly within the base lineage (and its
	// ancestors, via recursion) up to base.TXID; the child half is this
	// lineage's own contiguous segment run in (base.TXID, target]. Neither
	// half ever sees the other's members.
	parentChain, err := s.chainFrom(base.Lineage, base.TXID, seen)
	if err != nil {
		return nil, fmt.Errorf("store: chain %s@%d: resolving base %s@%d: %w",
			lineage, target, base.Lineage, base.TXID, err)
	}
	childSegs, err := resolveRange(lineage, base.TXID, target, segments)
	if err != nil {
		return nil, fmt.Errorf("store: chain %s@%d: %w", lineage, target, err)
	}
	// Verify the seam is contiguous: the base half must end exactly at
	// base.TXID and the child half must begin at base.TXID+1 (the latter is
	// enforced inside resolveRange). Error loudly if the base half did not
	// land where the fork point says it should.
	if len(parentChain) == 0 || parentChain[len(parentChain)-1].MaxTXID != base.TXID {
		var got uint64
		if len(parentChain) > 0 {
			got = parentChain[len(parentChain)-1].MaxTXID
		}
		return nil, fmt.Errorf(
			"store: chain %s@%d: base seam broken (base %s@%d resolved to end %d, want %d)",
			lineage, target, base.Lineage, base.TXID, got, base.TXID)
	}
	return append(parentChain, childSegs...), nil
}

// errNoSnapshotCoversTarget is chainSelf's distinguishable "this lineage has
// no snapshot at or below target" failure — the one chainSelf error Chain's
// base path treats as expected (a shared child that has not written its own
// divergence-floor snapshot yet) and answers via the seam path instead. Its
// wording is part of chainSelf's error message ("store: chain X@N: no
// snapshot covers target"), preserved verbatim from before it became a
// sentinel. Every other chainSelf error must propagate, never be swallowed
// into a seam-path retry.
var errNoSnapshotCoversTarget = errors.New("no snapshot covers target")

// listMembers Lists ONE lineage's prefix exactly once and parses it into
// that lineage's snapshot and segment members, each collapsed by
// keepHighestEpoch and sorted by TXID. It is the single List+parse step
// behind chain resolution: chainSelf feeds its output to resolveSelf, and
// chainFrom's seam path feeds the SAME parsed slices to both resolveSelf and
// resolveRange, so a shared child's prefix is Listed once per resolution, not
// twice. keepHighestEpoch runs here, on this one lineage's members only —
// never on a cross-lineage union (the one hard rule; see Chain).
func (s *Store) listMembers(lineage string) (snapshots, segments []ChainMember, err error) {
	keys, err := s.B.List(LineagePrefix(lineage))
	if err != nil {
		return nil, nil, err
	}
	for _, k := range keys {
		m, ok := ParseMemberKey(k)
		if !ok {
			continue
		}
		if m.Snapshot {
			snapshots = append(snapshots, m)
		} else {
			segments = append(segments, m)
		}
	}
	// Two members can cover the same TXID range under different epochs: a
	// writer uploads its object, loses the ref CAS, and is fenced, leaving an
	// orphan; because HeadTXID only advances on a *successful* ref write, the
	// next holder writes the same TXID under its own, higher epoch. Epochs
	// only ever increase, so the live object is always the higher-epoch one
	// and the lower-epoch one is by definition the superseded writer's.
	// Keeping only the highest epoch per range is what makes Plan 4's fencing
	// guarantee hold on this read path — otherwise a fenced writer's data
	// could be materialized.
	snapshots = keepHighestEpoch(snapshots)
	segments = keepHighestEpoch(segments)
	// List sorts lexically on the full key, which only sorts by TXID within
	// a single epoch directory (the epoch path segment isn't zero-padded, so
	// e.g. epoch 10 would sort before epoch 2). A chain can span epochs, so
	// sort explicitly by TXID here rather than trusting raw List order.
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].MaxTXID < snapshots[j].MaxTXID })
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].MaxTXID != segments[j].MaxTXID {
			return segments[i].MaxTXID < segments[j].MaxTXID
		}
		return segments[i].MinTXID < segments[j].MinTXID
	})
	return snapshots, segments, nil
}

// chainSelf is the single-lineage resolver: the newest snapshot with
// MaxTXID <= target followed by every segment up to target, in apply order,
// resolved wholly within this one lineage's List. It is the base==nil path of
// Chain and, via recursion, resolves each ancestor half of a shared chain.
func (s *Store) chainSelf(lineage string, target uint64) ([]ChainMember, error) {
	snapshots, segments, err := s.listMembers(lineage)
	if err != nil {
		return nil, err
	}
	return resolveSelf(lineage, target, snapshots, segments)
}

// resolveSelf is chainSelf's pure resolution half, operating on one
// lineage's already-parsed members (listMembers' output — deduped and
// sorted). Split out so chainFrom's seam path can run it and, on the
// no-own-snapshot sentinel, run resolveRange over the SAME parsed segments
// without a second List of the prefix. lineage and target appear only in
// error messages.
func resolveSelf(lineage string, target uint64, snapshots, segments []ChainMember) ([]ChainMember, error) {
	// Newest snapshot with MaxTXID <= target: snapshot keys sort by TXID
	// ascending, so scan from the end.
	var base *ChainMember
	for i := len(snapshots) - 1; i >= 0; i-- {
		if snapshots[i].MaxTXID <= target {
			base = &snapshots[i]
			break
		}
	}
	if base == nil {
		return nil, fmt.Errorf("store: chain %s@%d: %w", lineage, target, errNoSnapshotCoversTarget)
	}
	chain := []ChainMember{*base}
	if base.MaxTXID == target {
		return chain, nil
	}
	prevMax := base.MaxTXID
	for _, seg := range segments {
		if seg.MaxTXID <= prevMax {
			continue
		}
		if seg.MinTXID != prevMax+1 {
			return nil, fmt.Errorf(
				"store: chain %s@%d: hole before segment %s (expected minTXID %d, got %d)",
				lineage, target, seg.Key, prevMax+1, seg.MinTXID)
		}
		chain = append(chain, seg)
		prevMax = seg.MaxTXID
		if prevMax == target {
			return chain, nil
		}
		if prevMax > target {
			break
		}
	}
	return nil, fmt.Errorf("store: chain %s@%d: no contiguous run reaches target (stopped at %d)",
		lineage, target, prevMax)
}

// resolveRange resolves ONE lineage's own contiguous segment run covering
// (lo, hi], with NO leading snapshot — the child half of a shared chain has no
// snapshot of its own in this range (its snapshot, if any, lives in the base
// lineage). It operates on the lineage's already-parsed segments (listMembers'
// output: keepHighestEpoch was run on THIS lineage's segments alone, never a
// cross-lineage union — the one hard rule — and they arrive sorted), walking
// for contiguity: the first segment must start at lo+1, the run must have no
// hole, and it must reach hi exactly. It errors, mirroring Chain, when the
// run has a hole or stops short. lineage appears only in error messages.
func resolveRange(lineage string, lo, hi uint64, segments []ChainMember) ([]ChainMember, error) {
	var run []ChainMember
	prevMax := lo
	for _, seg := range segments {
		if seg.MaxTXID <= lo {
			continue // wholly at/below the fork point — belongs to the base half
		}
		if seg.MinTXID != prevMax+1 {
			return nil, fmt.Errorf(
				"store: segments %s(%d,%d]: hole before segment %s (expected minTXID %d, got %d)",
				lineage, lo, hi, seg.Key, prevMax+1, seg.MinTXID)
		}
		run = append(run, seg)
		prevMax = seg.MaxTXID
		if prevMax == hi {
			return run, nil
		}
		if prevMax > hi {
			break
		}
	}
	return nil, fmt.Errorf(
		"store: segments %s(%d,%d]: no contiguous run reaches target (stopped at %d)",
		lineage, lo, hi, prevMax)
}

func (s *Store) GetRef(db, branch string) (Ref, string, error) {
	data, etag, err := s.B.Get(RefKey(db, branch))
	if err != nil {
		return Ref{}, "", err
	}
	r, err := decodeRef(data)
	if err != nil {
		return Ref{}, "", fmt.Errorf("store: ref %s@%s: %w", db, branch, err)
	}
	return r, etag, nil
}

func (s *Store) PutRef(db, branch string, r Ref, ifMatch string) (string, error) {
	if err := ValidateName(db); err != nil {
		return "", err
	}
	if err := ValidateName(branch); err != nil {
		return "", err
	}
	r.Schema = RefSchema
	if r.HeadEpoch == 0 {
		r.HeadEpoch = r.Epoch
	}
	data, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	return s.B.PutIf(RefKey(db, branch), data, ifMatch)
}

// ConditionalDeleter is an optional Backend capability: DeleteIf removes key
// only if its current content still matches ifMatch's etag, refusing with
// ErrCAS if the key has since changed or is already gone. It is the delete-
// side counterpart to PutIf's CAS.
//
// Local implements this via the exact same per-key lock PutIf itself uses
// (see local.go's DeleteIf) — a true conditional delete. S3's DeleteObject
// API has no compare-and-delete precondition to give it (If-Match/If-None-
// Match are PUT/GET-only headers there; DELETE ignores them entirely), so
// S3 deliberately does NOT implement this interface rather than pretending
// to honor a condition it cannot actually enforce — see s3.go's Delete doc
// comment and DeleteRefIf below for what a caller gets instead.
type ConditionalDeleter interface {
	DeleteIf(key, ifMatch string) error
}

// DeleteRefIf deletes db@branch's ref, conditional on etag when the backend
// can actually honor that (ConditionalDeleter — today, Local) and
// unconditional otherwise (S3). Milestone 4 Task 6b's Destroy claim-guard
// (ops.Workspace.Destroy) is what actually closes the GetRef -> lease-check
// -> delete TOCTOU on EVERY backend, by CAS-writing a Deleting claim before
// calling this — see Ref.Deleting's doc comment. DeleteRefIf's own
// etag-conditioning is a belt-and-suspenders extra on a backend that can
// give it for free, never the primary safety mechanism, precisely because
// S3 cannot give it at all: pretending otherwise here would be the "pretend
// S3 DeleteObject has preconditions" mistake the design review flagged.
func (s *Store) DeleteRefIf(db, branch, etag string) error {
	key := RefKey(db, branch)
	if cd, ok := s.B.(ConditionalDeleter); ok {
		return cd.DeleteIf(key, etag)
	}
	return s.B.Delete(key)
}

func (s *Store) ListRefs() (map[string][]string, error) {
	keys, err := s.B.List("refs/")
	if err != nil {
		return nil, err
	}
	m := map[string][]string{}
	for _, k := range keys {
		parts := strings.Split(k, "/")
		if len(parts) != 3 {
			continue
		}
		m[parts[1]] = append(m[parts[1]], parts[2])
	}
	for db := range m {
		sort.Strings(m[db])
	}
	return m, nil
}
