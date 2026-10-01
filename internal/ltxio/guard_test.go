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

type runtimeAlloc struct{ total uint64 }

func (a *runtimeAlloc) read() {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	a.total = ms.TotalAlloc
}
