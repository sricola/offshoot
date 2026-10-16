package ltxio

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// TestReadDBHeaderPageSizeBounds pins readDBHeader's verdict at the edges
// of the documented range: a page size must be a power of two in
// [512, 65536] (65536 is encoded as 1 in the header), and the file must
// carry the SQLite magic. gremlins reported both bounds as surviving a
// boundary mutation because every test used 4096.
func TestReadDBHeaderPageSizeBounds(t *testing.T) {
	header := func(magic []byte, encodedPageSize uint16, nPages uint32) []byte {
		hdr := make([]byte, dbHeaderSize)
		copy(hdr, magic)
		binary.BigEndian.PutUint16(hdr[16:18], encodedPageSize)
		binary.BigEndian.PutUint32(hdr[28:32], nPages)
		return hdr
	}
	for _, c := range []struct {
		encoded  uint16
		wantSize uint32
	}{{512, 512}, {1024, 1024}, {4096, 4096}, {32768, 32768}, {1, 65536}} {
		size, n, err := ReadDBHeader(bytes.NewReader(header(dbHeaderMagic, c.encoded, 7)))
		if err != nil || size != c.wantSize || n != 7 {
			t.Errorf("encoded page size %d: size=%d n=%d err=%v, want %d, 7, nil", c.encoded, size, n, err, c.wantSize)
		}
	}
	for _, encoded := range []uint16{0, 2, 256, 511, 513, 1000, 3072, 65535} {
		if _, _, err := ReadDBHeader(bytes.NewReader(header(dbHeaderMagic, encoded, 7))); err == nil || !strings.Contains(err.Error(), "not a power of two in [512, 65536]") {
			t.Errorf("encoded page size %d: err = %v, want the page-size refusal", encoded, err)
		}
	}
	if _, _, err := ReadDBHeader(bytes.NewReader(header([]byte("not a database!!"), 4096, 7))); err == nil || !strings.Contains(err.Error(), "magic") {
		t.Errorf("wrong magic: err = %v, want the magic refusal", err)
	}
	if _, _, err := ReadDBHeader(bytes.NewReader(make([]byte, dbHeaderSize-1))); err == nil {
		t.Error("a short header must be an error")
	}
}
