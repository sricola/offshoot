package ltxio

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// randomPagesDB writes a synthetic database of nPages random 4 KiB pages
// with a header checkDBHeader accepts, and returns its path and bytes.
func randomPagesDB(t *testing.T, nPages int) (string, []byte) {
	t.Helper()
	const pageSize = 4096
	data := make([]byte, nPages*pageSize)
	rand.New(rand.NewSource(int64(nPages))).Read(data)
	copy(data, dbHeaderMagic)
	binary.BigEndian.PutUint16(data[16:18], pageSize)
	binary.BigEndian.PutUint32(data[28:32], uint32(nPages))
	path := filepath.Join(t.TempDir(), fmt.Sprintf("pages-%d.db", nPages))
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path, data
}

// TestStreamChecksumSplitFoldMatchesForEveryPageCount: the two-half fold of
// a block equals the sequential fold ChecksumDatabase makes for every page
// count from 1 to 40 with 64 KiB blocks (16 pages per block), so blocks
// with an odd number of pages, a single page, and a short last block of
// every size split and recombine correctly, and the tee still sees every
// byte.
func TestStreamChecksumSplitFoldMatchesForEveryPageCount(t *testing.T) {
	withStreamBlockSize(t, 65536)
	for nPages := 1; nPages <= 40; nPages++ {
		path, data := randomPagesDB(t, nPages)
		want, err := ChecksumDatabase(path)
		if err != nil {
			t.Fatalf("%d pages: ChecksumDatabase: %v", nPages, err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.New()
		got, err := StreamChecksum(f, h)
		f.Close()
		if err != nil {
			t.Fatalf("%d pages: StreamChecksum: %v", nPages, err)
		}
		if got != want {
			t.Fatalf("%d pages: StreamChecksum %016x, ChecksumDatabase %016x", nPages, got, want)
		}
		if !bytes.Equal(h.Sum(nil), func() []byte { s := sha256.Sum256(data); return s[:] }()) {
			t.Fatalf("%d pages: tee digest differs from the file's", nPages)
		}
	}
}
