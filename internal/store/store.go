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
	manifestKey   = "offshoot.json"
	maxNameLen    = 128
)

type Manifest struct {
	LayoutVersion int    `json:"layout_version"`
	CreatedAt     string `json:"created_at"`
}

type Ref struct {
	Schema      int               `json:"schema"`
	Lineage     string            `json:"lineage"`
	Epoch       uint64            `json:"epoch"`
	HeadTXID    uint64            `json:"head_txid"`
	Checkpoints map[string]uint64 `json:"checkpoints"`
	Parent      string            `json:"parent,omitempty"`
	Protected   bool              `json:"protected"`
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
	var r Ref
	if err := json.Unmarshal(data, &r); err != nil {
		return Ref{}, "", fmt.Errorf("store: corrupt ref %s@%s: %w", db, branch, err)
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
