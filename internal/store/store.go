package store

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	LayoutVersion = 1
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
	// Epoch identifies a lineage's current writer generation. Local (Plan-2)
	// mode never re-acquires a lineage, so Epoch stays 1 for the lifetime of
	// every ref this binary writes; bumping it is a Plan-3 daemon concern.
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
		Meta: w.Meta,
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

type Store struct{ B Backend }

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
func (s *Store) Chain(lineage string, target uint64) ([]ChainMember, error) {
	keys, err := s.B.List(LineagePrefix(lineage))
	if err != nil {
		return nil, err
	}
	var snapshots, segments []ChainMember
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
		return nil, fmt.Errorf("store: chain %s@%d: no snapshot covers target", lineage, target)
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

func (s *Store) DeleteRef(db, branch string) error {
	return s.B.Delete(RefKey(db, branch))
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
