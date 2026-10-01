package ltxio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/superfly/ltx"
)

// oversizedBlockSnapshot is the crasher shape FuzzDecodeSnapshot found: a
// real snapshot whose first page frame's compressed-size prefix (bytes
// 106:110, right after the 100-byte header and 6-byte page header) is
// rewritten to nearly 4 GiB, which ltx's DecodePage would allocate before
// reading — and before any CRC check.
func oversizedBlockSnapshot(tb testing.TB) []byte {
	tb.Helper()
	snap, _, _ := fuzzSeedChain(tb)
	bad := bytes.Clone(snap)
	if flags := binary.BigEndian.Uint16(bad[104:106]); flags&ltx.PageHeaderFlagSize == 0 {
		tb.Fatalf("seed page 1 is not block format (flags %#x)", flags)
	}
	binary.BigEndian.PutUint32(bad[106:110], 0xfffffff0)
	return bad
}

// TestDecodeRejectsOversizedPageFrame: every decode path refuses a page
// frame claiming more compressed bytes than one page can need, by name,
// without allocating it.
func TestDecodeRejectsOversizedPageFrame(t *testing.T) {
	bad := oversizedBlockSnapshot(t)
	dir := t.TempDir()
	var before, after runtimeAlloc
	before.read()
	_, _, err := MaterializeChain(bytes.NewReader(bad), nil, filepath.Join(dir, "a"))
	after.read()
	if err == nil || !strings.Contains(err.Error(), "compressed size") {
		t.Fatalf("MaterializeChain err = %v, want the compressed-size refusal", err)
	}
	if grew := after.total - before.total; grew > 64<<20 {
		t.Fatalf("decode allocated %d MiB before refusing", grew>>20)
	}
	if _, err := Materialize(bytes.NewReader(bad), filepath.Join(dir, "b")); err == nil || !strings.Contains(err.Error(), "compressed size") {
		t.Fatalf("Materialize err = %v, want the compressed-size refusal", err)
	}
	if _, err := TrailerPostApplyChecksum(bad); err == nil || !strings.Contains(err.Error(), "compressed size") {
		t.Fatalf("TrailerPostApplyChecksum err = %v, want the compressed-size refusal", err)
	}
	requireNoLeftovers(t, dir)
}

// TestDecodeRejectsIllegalPageSize: a header PageSize SQLite cannot have is
// refused, naming the field, before any page is decoded.
func TestDecodeRejectsIllegalPageSize(t *testing.T) {
	snap, segs, _ := fuzzSeedChain(t)
	for _, ps := range []uint32{0, 256, 1000, 1 << 17, 0xffffffff} {
		for name, obj := range map[string][]byte{"snapshot": snap, "segment": segs[0]} {
			bad := bytes.Clone(obj)
			binary.BigEndian.PutUint32(bad[8:12], ps)
			_, err := TrailerPostApplyChecksum(bad)
			if err == nil || !strings.Contains(err.Error(), "PageSize") {
				t.Errorf("%s PageSize %d: err = %v, want a PageSize refusal", name, ps, err)
			}
		}
		bad := bytes.Clone(snap)
		binary.BigEndian.PutUint32(bad[8:12], ps)
		if _, _, err := MaterializeChain(bytes.NewReader(bad), nil, filepath.Join(t.TempDir(), "x")); err == nil || !strings.Contains(err.Error(), "PageSize") {
			t.Errorf("MaterializeChain PageSize %d: err = %v, want a PageSize refusal", ps, err)
		}
	}
}

// hugeCommitSegment is the shape found by fuzzing ApplySegments: a
// CRC-valid segment with no pages declaring a commit of 2^30 pages.
func hugeCommitSegment(tb testing.TB, preApply uint64) []byte {
	tb.Helper()
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		tb.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version: ltx.Version, PageSize: 4096, Commit: 1 << 30,
		MinTXID: 2, MaxTXID: 2, Timestamp: 1, PreApplyChecksum: ltx.Checksum(preApply),
	}); err != nil {
		tb.Fatal(err)
	}
	enc.SetPostApplyChecksum(ltx.ChecksumFlag | 1)
	if err := enc.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

// TestApplyRejectsUncarriedGrowth: a segment declaring a commit past the
// pages it carries is refused promptly (it used to fold 2^30 zero pages
// into the checksum one by one), through both ApplySegments and
// MaterializeChain, leaving nothing behind.
func TestApplyRejectsUncarriedGrowth(t *testing.T) {
	snap, _, _ := fuzzSeedChain(t)
	dir := t.TempDir()
	start := filepath.Join(dir, "start.sqlite")
	_, startSum, err := MaterializeChain(bytes.NewReader(snap), nil, start)
	if err != nil {
		t.Fatal(err)
	}
	seg := hugeCommitSegment(t, startSum)

	outDir := t.TempDir()
	t0 := time.Now()
	_, _, err = ApplySegments(start, startSum, []io.Reader{bytes.NewReader(seg)}, filepath.Join(outDir, "a"))
	if !errors.Is(err, ErrUncarriedGrowth) {
		t.Fatalf("ApplySegments err = %v, want ErrUncarriedGrowth", err)
	}
	_, _, err = MaterializeChain(bytes.NewReader(snap), []io.Reader{bytes.NewReader(seg)}, filepath.Join(outDir, "b"))
	if !errors.Is(err, ErrUncarriedGrowth) {
		t.Fatalf("MaterializeChain err = %v, want ErrUncarriedGrowth", err)
	}
	if d := time.Since(t0); d > 5*time.Second {
		t.Fatalf("refusal took %v", d)
	}
	requireNoLeftovers(t, outDir)
}

// TestCheckGrowth pins the rule, including the lock page, which is never
// carried and never counted.
func TestCheckGrowth(t *testing.T) {
	set := func(pgnos ...uint32) map[uint32]bool {
		m := map[uint32]bool{}
		for _, p := range pgnos {
			m[p] = true
		}
		return m
	}
	const lock = 100
	for _, tc := range []struct {
		name         string
		commit, prev uint32
		carried      map[uint32]bool
		ok           bool
	}{
		{"shrink", 3, 5, set(), true},
		{"same size", 5, 5, set(2), true},
		{"grow, all carried", 7, 5, set(1, 6, 7), true},
		{"grow, one missing", 7, 5, set(7), false},
		{"grow, none carried", 1 << 30, 5, set(), false},
		{"pages past commit do not count", 6, 5, set(7, 8), false},
		{"crosses the lock page", 102, 98, set(99, 101, 102), true},
		{"crosses the lock page, one missing", 102, 98, set(99, 102), false},
		{"lock page neither required nor counted", 101, 99, set(100, 101), true},
		{"a carried lock page does not stand in for a missing page", 101, 99, set(100), false},
	} {
		err := checkGrowth(tc.commit, tc.prev, lock, tc.carried)
		if (err == nil) != tc.ok {
			t.Errorf("%s: err = %v, want ok=%v", tc.name, err, tc.ok)
		}
		if err != nil && !errors.Is(err, ErrUncarriedGrowth) {
			t.Errorf("%s: err = %v, not ErrUncarriedGrowth", tc.name, err)
		}
	}
}

// TestDecodesLTXv051FrameFormat: objects written by github.com/superfly/ltx
// v0.5.1 — LZ4 frame page format, what this repo wrote until 2026-09-26 —
// still decode through the guard, snapshot and growing segment alike.
// Fixtures from a one-off generator built against v0.5.1 (see
// testdata/ltx-v0.5.1); final.sqlite is the database they reproduce.
func TestDecodesLTXv051FrameFormat(t *testing.T) {
	read := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join("testdata", "ltx-v0.5.1", name))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	snap, seg, final := read("snapshot.ltx"), read("segment.ltx"), read("final.sqlite")
	if flags := binary.BigEndian.Uint16(snap[104:106]); flags&ltx.PageHeaderFlagSize != 0 {
		t.Fatal("fixture is not frame format")
	}
	dst := filepath.Join(t.TempDir(), "db.sqlite")
	txid, sum, err := MaterializeChain(bytes.NewReader(snap), []io.Reader{bytes.NewReader(seg)}, dst)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(dst)
	if txid != 2 || !bytes.Equal(got, final) {
		t.Fatalf("txid %d, content equal %v", txid, bytes.Equal(got, final))
	}
	if declared, err := TrailerPostApplyChecksum(seg); err != nil || declared != sum {
		t.Fatalf("TrailerPostApplyChecksum = %016x, %v; want %016x", declared, err, sum)
	}
}

// lz4Frame is one page frame as pierrec's lz4.Writer produces it with opts
// (ltx v0.5.1 used Block64Kb and defaults otherwise), holding content.
func lz4Frame(tb testing.TB, content []byte, opts ...lz4.Option) []byte {
	tb.Helper()
	var b bytes.Buffer
	zw := lz4.NewWriter(&b)
	if err := zw.Apply(opts...); err != nil {
		tb.Fatal(err)
	}
	if _, err := zw.Write(content); err != nil {
		tb.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		tb.Fatal(err)
	}
	return b.Bytes()
}

// frameObject is an LTX segment (4096-byte pages) whose page 1 is the given
// LZ4 frame, followed by tail.
func frameObject(tb testing.TB, frame, tail []byte) []byte {
	tb.Helper()
	h := ltx.Header{Version: ltx.Version, PageSize: 4096, Commit: 1 << 20, MinTXID: 2, MaxTXID: 2, Timestamp: 1, PreApplyChecksum: ltx.ChecksumFlag | 1}
	hb, err := h.MarshalBinary()
	if err != nil {
		tb.Fatal(err)
	}
	var o bytes.Buffer
	o.Write(hb)
	o.Write([]byte{0, 0, 0, 1, 0, 0}) // page 1, frame format (no size flag)
	o.Write(frame)
	o.Write(tail)
	o.Write(make([]byte, 32))
	return o.Bytes()
}

// frameDesyncTail is what a decoder that has drifted out of step with the
// guard would read as a block-format page with a ~4 GiB size prefix, if
// the guard took it for the start of the next page header — the PoC's
// payload.
var frameDesyncTail = []byte{0x04, 0x22, 0x4D, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0xFF, 0xFF, 0xFF, 0xF0}

// TestFrameGuardPinsTheV051FrameShape: an LZ4-frame page in any shape other
// than the one ltx v0.5.1 wrote is refused before pierrec can consume bytes
// the guard has not parsed, so the 4 GiB allocation the guard exists to
// prevent stays unreachable. Each case is a route a review found by which
// pierrec reads past what a field-by-field parse accounts for.
func TestFrameGuardPinsTheV051FrameShape(t *testing.T) {
	page := make([]byte, 4096)
	for i := range page[16:] {
		page[16+i] = byte(i * 7)
	}
	v051 := lz4Frame(t, page, lz4.BlockSizeOption(lz4.Block64Kb))
	withDict := bytes.Clone(v051)
	withDict[4] |= 1 // FLG dictionary-ID bit; pierrec never reads the ID

	for _, tc := range []struct {
		name string
		obj  []byte
		want string
	}{
		// (a) No content checksum: after the EndMark pierrec peeks 4 bytes
		// for a concatenated frame, bytes the guard would take as the next
		// page header.
		{"no content checksum", frameObject(t, lz4Frame(t, page, lz4.BlockSizeOption(lz4.Block64Kb), lz4.ChecksumOption(false)), frameDesyncTail), "descriptor"},
		// (b) Dictionary ID flagged: 4 bytes pierrec does not read.
		{"dictionary ID", frameObject(t, withDict, frameDesyncTail), "descriptor"},
		// (c) Frame holding less than a page: pierrec reads on past the
		// EndMark into a "concatenated frame" during the page read.
		{"short frame", frameObject(t, lz4Frame(t, page[:1000], lz4.BlockSizeOption(lz4.Block64Kb)), frameDesyncTail), "decompresses"},
		// Larger block maximum than v0.5.1's 64 KiB.
		{"256 KiB blocks", frameObject(t, lz4Frame(t, page, lz4.BlockSizeOption(lz4.Block256Kb)), frameDesyncTail), "descriptor"},
		// A second data block where the EndMark belongs.
		{"two blocks", frameObject(t, append(bytes.Clone(v051[:len(v051)-8]), 0x10, 0, 0, 0), frameDesyncTail), "end frame"}, // ltx reports the guard's refusal as its own trailer error
	} {
		var before, after runtimeAlloc
		before.read()
		_, err := TrailerPostApplyChecksum(tc.obj)
		after.read()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want a refusal mentioning %q", tc.name, err, tc.want)
		}
		if grew := after.total - before.total; grew > 8<<20 {
			t.Errorf("%s: decode allocated %d MiB before refusing", tc.name, grew>>20)
		}
	}

	// Control: the v0.5.1 shape itself passes the guard and decodes; the
	// object then fails only where it is meant to (the bogus next page).
	if _, err := TrailerPostApplyChecksum(frameObject(t, v051, []byte{0, 0, 0, 2, 0, 1, 0xFF, 0xFF, 0xFF, 0xF0})); err == nil || !strings.Contains(err.Error(), "compressed size") {
		t.Fatalf("control: err = %v, want the next page's compressed-size refusal", err)
	}
}

// frameSeeds are the frame-format fuzz seeds: the real ltx v0.5.1 snapshot
// and segment fixtures, and the review's desync PoC (an unchecksummed frame
// followed by a fake ~4 GiB block page). The block-format encoder produces
// none of these, so without them the fuzzer barely reaches the guard's
// frame half.
func frameSeeds(tb testing.TB) [][]byte {
	tb.Helper()
	var out [][]byte
	for _, name := range []string{"snapshot.ltx", "segment.ltx"} {
		b, err := os.ReadFile(filepath.Join("testdata", "ltx-v0.5.1", name))
		if err != nil {
			tb.Fatal(err)
		}
		out = append(out, b)
	}
	page := make([]byte, 4096)
	page[100] = 1
	poc := frameObject(tb, lz4Frame(tb, page, lz4.BlockSizeOption(lz4.Block64Kb), lz4.ChecksumOption(false)), frameDesyncTail)
	return append(out, poc)
}

type runtimeAlloc struct{ total uint64 }

func (a *runtimeAlloc) read() {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	a.total = ms.TotalAlloc
}
