package ltxio

import (
	"crypto/sha256"
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// streamBenchDB writes a synthetic 4 KiB-page database of size bytes: the
// SQLite magic, the page size and page count at their header offsets, and
// random page bodies, so neither digest short-circuits on zeros. It is a
// header checkDBHeader accepts; nothing opens it with SQLite.
func streamBenchDB(b *testing.B, size int) string {
	b.Helper()
	const pageSize = 4096
	data := make([]byte, size)
	rand.New(rand.NewSource(1)).Read(data)
	copy(data, dbHeaderMagic)
	binary.BigEndian.PutUint16(data[16:18], pageSize)
	binary.BigEndian.PutUint32(data[28:32], uint32(size/pageSize))
	path := filepath.Join(b.TempDir(), "bench.db")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		b.Fatal(err)
	}
	return path
}

// BenchmarkStreamChecksum times one pass of StreamChecksum over a 64 MiB
// database with a SHA-256 tee: the stamp's one read of a checkout, as
// ops.fileSums makes it. The file is in the page cache after the first
// iteration, so this measures the two digests, not the disk.
func BenchmarkStreamChecksum(b *testing.B) {
	const size = 64 << 20
	path := streamBenchDB(b, size)
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		h := sha256.New()
		if _, err := StreamChecksum(f, h); err != nil {
			b.Fatal(err)
		}
		h.Sum(nil)
		f.Close()
	}
}

// BenchmarkStreamChecksumSHAOnly is the floor: the same read with the
// SHA-256 tee alone, no page fold, which is what the sidecar hash cost
// before the fold joined it.
func BenchmarkStreamChecksumSHAOnly(b *testing.B) {
	const size = 64 << 20
	path := streamBenchDB(b, size)
	buf := make([]byte, streamBlockSize)
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		h := sha256.New()
		for {
			n, err := f.Read(buf)
			if n > 0 {
				h.Write(buf[:n])
			}
			if err != nil {
				break
			}
		}
		h.Sum(nil)
		f.Close()
	}
}
