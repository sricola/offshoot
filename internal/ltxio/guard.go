package ltxio

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
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
//   - an LZ4-frame-format page (what ltx v0.5.1, which this repo used until
//     2026-09-26, wrote, so stores hold such objects) must have exactly the
//     shape v0.5.1 wrote: see frameHeader and frameBlock. Pinning the shape
//     is what keeps the guard in step with the decoder. ltx hands frame
//     pages to pierrec's lz4.Reader, which on other shapes reads bytes the
//     guard has not parsed: a 4-byte peek for a concatenated frame after an
//     unchecksummed frame, past the EndMark when the frame holds less than
//     a page, and never the dictionary ID. The guard would then be checking
//     a different byte stream from the one the decoder consumes.
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
}

type guardState int

const (
	guardHeader guardState = iota
	guardPageHeader
	guardBlockSize
	guardFrameHeader
	guardFrameBlock
	guardFrameEnd
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
		g.frameBlock()
	case guardFrameEnd:
		// EndMark (a zero block size), then the 4-byte content checksum,
		// which pierrec verifies. pierrec's next read, a peek for a
		// concatenated frame, is cut off by ltx's 8-byte footer limit
		// (decoder.go: lr.N = lz4FrameFooterSize), so it consumes nothing
		// past these 8 bytes.
		b, ok := g.field(8)
		if !ok {
			return
		}
		if end := binary.LittleEndian.Uint32(b); end != 0 {
			g.fail("LZ4 frame holds more than one block (next block size %#x, want the EndMark)", end)
			return
		}
		g.pending, g.state = b, guardPageHeader
	}
}

// v051FLG and v051BD are the LZ4 frame descriptor bytes every ltx v0.5.1
// page frame carries (lz4.NewWriter with Block64Kb, defaults otherwise):
// FLG = version 01, independent blocks, content checksum, and no block
// checksum, content size, dictionary ID or reserved bits; BD = 64 KiB
// maximum block size. A 64 KiB block holds the largest SQLite page, so
// v0.5.1 wrote every page as exactly one block.
const (
	v051FLG = 0x64
	v051BD  = 0x40
)

// frameHeader checks an LZ4 frame descriptor: the magic, then exactly the
// FLG and BD bytes v0.5.1 wrote, then the header checksum byte (verified by
// pierrec). That is seven bytes, exactly what pierrec's frame reader
// consumes for this descriptor.
func (g *frameGuard) frameHeader() {
	b, ok := g.field(7)
	if !ok {
		return
	}
	if magic := binary.LittleEndian.Uint32(b); magic != 0x184D2204 {
		g.fail("page frame is neither block-format nor an LZ4 frame (magic %#x)", magic)
		return
	}
	if flg, bd := b[4], b[5]; flg != v051FLG || bd != v051BD {
		g.fail("LZ4 frame descriptor FLG %#02x BD %#02x is not the shape ltx v0.5.1 writes (%#02x %#02x)", flg, bd, v051FLG, v051BD)
		return
	}
	g.pending, g.state = b, guardFrameBlock
}

// frameBlock checks the frame's one data block: its size field, then the
// block itself, which must hold exactly one page, either stored (size ==
// PageSize) or compressed and decompressing, here, to exactly PageSize
// bytes. pierrec then serves the decoder's full-page read from this block
// alone and never reads ahead for more data.
func (g *frameGuard) frameBlock() {
	b, ok := g.field(4)
	if !ok {
		return
	}
	raw := binary.LittleEndian.Uint32(b)
	stored := raw&(1<<31) != 0
	size := raw &^ (1 << 31)
	switch {
	case raw == 0:
		g.fail("LZ4 frame has no data block")
		return
	case stored && size != g.pageSize:
		g.fail("stored LZ4 block is %d bytes, want the page size %d", size, g.pageSize)
		return
	case size > 64<<10:
		g.fail("LZ4 block size %d exceeds the frame's 65536-byte maximum", size)
		return
	}
	data, ok := g.field(int(size))
	if !ok {
		g.pending = append(b, g.pending...)
		return
	}
	if !stored {
		page := make([]byte, g.pageSize)
		if n, err := lz4.UncompressBlock(data, page); err != nil || n != int(g.pageSize) {
			g.fail("LZ4 block decompresses to %d bytes (err %v), want the page size %d", n, err, g.pageSize)
			return
		}
	}
	g.pending, g.state = append(b, data...), guardFrameEnd
}
