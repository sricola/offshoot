package store

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
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
}

// SetCheckpoint records name at (txid, epoch), allocating the map if needed.
func (r *Ref) SetCheckpoint(name string, txid, epoch uint64) {
	if r.Checkpoints == nil {
		r.Checkpoints = map[string]Checkpoint{}
	}
	r.Checkpoints[name] = Checkpoint{TXID: txid, Epoch: epoch}
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
	}
	// A v1 ref predates per-checkpoint epochs: everything it references was
	// written under the ref's own epoch.
	if r.HeadEpoch == 0 {
		r.HeadEpoch = w.Epoch
	}
	for name, raw := range w.Checkpoints {
		var cp Checkpoint
		if err := json.Unmarshal(raw, &cp); err == nil && cp.TXID != 0 {
			r.SetCheckpoint(name, cp.TXID, cp.Epoch)
			continue
		}
		var txid uint64
		if err := json.Unmarshal(raw, &txid); err != nil {
			return Ref{}, fmt.Errorf("store: bad checkpoint %q: %w", name, err)
		}
		r.SetCheckpoint(name, txid, w.Epoch)
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
