package ltxio

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/superfly/ltx"
)

// maxCommit is SQLite's ceiling on a database's page count (the largest
// value PRAGMA max_page_count accepts): an LTX header declaring more pages
// than SQLite can address is not a database this package will materialize.
const maxCommit = 0xFFFFFFFE

// frameGuard sits between an LTX object's bytes (downloaded from an object
// store, so untrusted until the trailer's CRC64 verifies at Close) and the
// ltx decoder, and refuses — before the decoder sees them — the structural
// fields the decoder would otherwise trust ahead of that CRC:
//
//   - the header's PageSize must be one SQLite allows (a power of two in
//     [512, 65536]) and its Commit at most maxCommit;
//   - each page frame's compressed-size prefix (the block format every
//     encoder since github.com/superfly/ltx v0.5.3 writes) must be at most
//     the LZ4 worst-case bound for one page. ltx's Decoder.DecodePage
//     allocates that many bytes before reading them, so without this a
//     single corrupted 4-byte field costs an allocation of up to 4 GiB;
//   - each block of an LZ4-frame-format page (what ltx v0.5.1, which this
//     repo used until 2026-09-26, wrote — stores hold such objects) must be
//     at most its frame's declared maximum block size.
//
// It is a pass-through parser: bytes reach the decoder unchanged and in
// order, only ever after the field that governs them has been checked, so
// a well-formed object decodes exactly as it would without the guard. Once
// the page block's terminating empty page header has passed, the rest (page
// index and trailer, which the decoder reads with bounded allocations) is
// passed through untouched.
type frameGuard struct {
	r        io.Reader
	pending  []byte // validated bytes not yet handed to the caller
	payload  int64  // opaque bytes to pass through before the next field
	state    guardState
	pageSize uint32
	err      error // sticky: returned once pending drains

	// LZ4 frame state for the current page (frame format only).
	blockChecksum   bool
	contentChecksum bool
	maxBlock        uint32
}

type guardState int

const (
	guardHeader guardState = iota
	guardPageHeader
	guardBlockSize
	guardFrameHeader
	guardFrameBlock
	guardRest
)

// lz4BlockBound is lz4.CompressBlockBound: the largest compressed size LZ4
// can produce for n input bytes.
func lz4BlockBound(n uint32) uint32 { return n + n/255 + 16 }

func newFrameGuard(r io.Reader) *frameGuard { return &frameGuard{r: r} }

func (g *frameGuard) Read(p []byte) (int, error) {
	for len(g.pending) == 0 {
		if g.err != nil {
			return 0, g.err
		}
		if g.payload > 0 || g.state == guardRest {
			n := len(p)
			if g.state != guardRest && int64(n) > g.payload {
				n = int(g.payload)
			}
			n, err := g.r.Read(p[:n])
			if g.state != guardRest {
				g.payload -= int64(n)
			}
			return n, err
		}
		g.step()
	}
	n := copy(p, g.pending)
	g.pending = g.pending[n:]
	return n, nil
}

// field reads exactly n structural bytes. On a short read it queues
// whatever arrived (the decoder reports the truncation itself) and records
// the error.
func (g *frameGuard) field(n int) ([]byte, bool) {
	b := make([]byte, n)
	m, err := io.ReadFull(g.r, b)
	if err != nil {
		g.pending, g.err = b[:m], err
		return nil, false
	}
	return b, true
}

func (g *frameGuard) fail(format string, args ...any) {
	g.err = fmt.Errorf("ltxio: "+format, args...)
}

// step parses the next structural field, queueing it in pending if valid.
func (g *frameGuard) step() {
	switch g.state {
	case guardHeader:
		b, ok := g.field(ltx.HeaderSize)
		if !ok {
			return
		}
		var hdr ltx.Header
		if err := hdr.UnmarshalBinary(b); err != nil {
			g.fail("decode header: %w", err)
			return
		}
		if !ltx.IsValidPageSize(hdr.PageSize) {
			g.fail("header PageSize %d is not a power of two in [512, 65536]", hdr.PageSize)
			return
		}
		if hdr.Commit > maxCommit {
			g.fail("header Commit %d exceeds SQLite's maximum page count %d", hdr.Commit, uint32(maxCommit))
			return
		}
		g.pageSize = hdr.PageSize
		g.pending, g.state = b, guardPageHeader
	case guardPageHeader:
		b, ok := g.field(ltx.PageHeaderSize)
		if !ok {
			return
		}
		g.pending = b
		var ph ltx.PageHeader
		_ = ph.UnmarshalBinary(b) // cannot fail: b is PageHeaderSize bytes
		switch {
		case ph.IsZero():
			g.state = guardRest
		case ph.Flags&ltx.PageHeaderFlagSize != 0:
			g.state = guardBlockSize
		default:
			g.state = guardFrameHeader
		}
	case guardBlockSize:
		b, ok := g.field(4)
		if !ok {
			return
		}
		size := binary.BigEndian.Uint32(b)
		if bound := lz4BlockBound(g.pageSize); size > bound {
			g.fail("page frame compressed size %d exceeds the LZ4 bound %d for a %d-byte page", size, bound, g.pageSize)
			return
		}
		g.pending, g.payload, g.state = b, int64(size), guardPageHeader
	case guardFrameHeader:
		g.frameHeader()
	case guardFrameBlock:
		b, ok := g.field(4)
		if !ok {
			return
		}
		g.pending = b
		size := binary.LittleEndian.Uint32(b)
		if size == 0 { // EndMark
			if g.contentChecksum {
				g.payload = 4
			}
			g.state = guardPageHeader
			return
		}
		size &^= 1 << 31 // high bit: block stored uncompressed
		if size > g.maxBlock {
			g.pending = nil
			g.fail("LZ4 block size %d exceeds the frame's %d-byte maximum", size, g.maxBlock)
			return
		}
		g.payload = int64(size)
		if g.blockChecksum {
			g.payload += 4
		}
	}
}

// frameHeader parses an LZ4 frame descriptor: magic, FLG, BD, the optional
// content size and dictionary ID, and the header checksum byte.
func (g *frameGuard) frameHeader() {
	b, ok := g.field(6) // magic, FLG, BD
	if !ok {
		return
	}
	if magic := binary.LittleEndian.Uint32(b); magic != 0x184D2204 {
		g.fail("page frame is neither block-format nor an LZ4 frame (magic %#x)", magic)
		return
	}
	flg, bd := b[4], b[5]
	if flg>>6 != 1 {
		g.fail("LZ4 frame version %d is not 1", flg>>6)
		return
	}
	switch (bd >> 4) & 7 {
	case 4:
		g.maxBlock = 64 << 10
	case 5:
		g.maxBlock = 256 << 10
	case 6:
		g.maxBlock = 1 << 20
	case 7:
		g.maxBlock = 4 << 20
	default:
		g.fail("LZ4 frame block-size code %d is invalid", (bd>>4)&7)
		return
	}
	g.blockChecksum = flg&(1<<4) != 0
	g.contentChecksum = flg&(1<<2) != 0
	extra := 1 // header checksum
	if flg&(1<<3) != 0 {
		extra += 8 // content size
	}
	if flg&1 != 0 {
		extra += 4 // dictionary ID
	}
	rest, ok := g.field(extra)
	if !ok {
		g.pending = append(b, g.pending...)
		return
	}
	g.pending = append(b, rest...)
	g.state = guardFrameBlock
}
