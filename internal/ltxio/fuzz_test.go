package ltxio

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

// maxFuzzInput caps every fuzz input: the LTX decoders stream, so 1 MiB is
// plenty to reach every code path while keeping one execution cheap.
const maxFuzzInput = 1 << 20

// fuzzDB writes a small quiesced SQLite database at path by running stmts,
// using only bounded, fixed SQL (fuzz input never reaches cgo SQLite).
func fuzzDB(tb testing.TB, path string, stmts ...string) {
	tb.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		tb.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	for _, s := range append([]string{"PRAGMA journal_mode=WAL"}, stmts...) {
		if _, err := db.Exec(s); err != nil {
			tb.Fatalf("%s: %v", s, err)
		}
	}
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		tb.Fatal(err)
	}
	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}
}

// pagesOf splits a quiesced database file into its pages.
func pagesOf(tb testing.TB, path string) (pageSize, commit uint32, pages []Page) {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatal(err)
	}
	defer f.Close()
	pageSize, commit, err = readDBHeader(f)
	if err != nil {
		tb.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	for i := uint32(0); i < commit; i++ {
		off := int(i) * int(pageSize)
		pages = append(pages, Page{Pgno: i + 1, Data: data[off : off+int(pageSize)]})
	}
	return pageSize, commit, pages
}

// fuzzSeedChain builds a tiny real chain with the real encoders: a snapshot
// of a one-table database (txid 1), then two segments — one that grows the
// database (txid 2) and one that shrinks it with VACUUM (txid 3). It returns
// the encoded objects plus the database each one produces.
func fuzzSeedChain(tb testing.TB) (snap []byte, segs [][]byte, states []string) {
	tb.Helper()
	dir := tb.TempDir()
	prev := filepath.Join(dir, "v1.sqlite")
	fuzzDB(tb, prev, "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)",
		"INSERT INTO t (v) VALUES (randomblob(64))")
	var sb bytes.Buffer
	if _, err := EncodeSnapshot(prev, 1, &sb); err != nil {
		tb.Fatal(err)
	}
	states = append(states, prev)
	steps := [][]string{
		{"INSERT INTO t (v) SELECT randomblob(900) FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6)"},
		{"DELETE FROM t WHERE id > 2", "VACUUM"},
	}
	for i, stmts := range steps {
		next := filepath.Join(dir, "v"+string(rune('2'+i))+".sqlite")
		if err := copyFileForTest(prev, next); err != nil {
			tb.Fatal(err)
		}
		fuzzDB(tb, next, stmts...)
		_, _, before := pagesOf(tb, prev)
		pageSize, commit, after := pagesOf(tb, next)
		pre, err := ChecksumDatabase(prev)
		if err != nil {
			tb.Fatal(err)
		}
		post, err := ChecksumDatabase(next)
		if err != nil {
			tb.Fatal(err)
		}
		txid := uint64(i + 2)
		var seg bytes.Buffer
		if err := EncodeSegment(pageSize, commit, txid, txid, pre, post, diffPages(before, after), &seg); err != nil {
			tb.Fatal(err)
		}
		segs = append(segs, seg.Bytes())
		states = append(states, next)
		prev = next
	}
	return sb.Bytes(), segs, states
}

// rescan is the materialized file's rolling checksum computed from its
// actual bytes in the chain's page size — independent of whatever page 1's
// SQLite header now claims — so a decode that returned a checksum the
// bytes do not embody is caught even when page 1 itself was rewritten.
func rescan(t *testing.T, path string, pageSize uint32) uint64 {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size()%int64(pageSize) != 0 {
		t.Fatalf("materialized file is %d bytes, not a whole number of %d-byte pages", fi.Size(), pageSize)
	}
	c, err := checksumPages(f, pageSize, uint32(fi.Size()/int64(pageSize)))
	if err != nil {
		t.Fatal(err)
	}
	return uint64(c)
}

// requireConsistent is the shared success-side property: the returned
// checksum is what the file's bytes actually fold to, it is what the last
// object's trailer declared, and — whenever page 1 still describes the
// file — ChecksumDatabase agrees too. A mutated input can therefore never
// yield a silently wrong file.
func requireConsistent(t *testing.T, path string, pageSize uint32, sum uint64, last []byte) {
	t.Helper()
	if got := rescan(t, path, pageSize); got != sum {
		t.Fatalf("returned checksum %016x, file's bytes fold to %016x", sum, got)
	}
	if declared, err := TrailerPostApplyChecksum(last); err != nil {
		t.Fatalf("decode succeeded but TrailerPostApplyChecksum rejects the same object: %v", err)
	} else if declared != sum {
		t.Fatalf("returned checksum %016x, trailer declares %016x", sum, declared)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ps, n, herr := readDBHeader(f)
	fi, serr := f.Stat()
	f.Close()
	if herr == nil && serr == nil && ps == pageSize && int64(n)*int64(ps) == fi.Size() {
		if full, err := ChecksumDatabase(path); err != nil || full != sum {
			t.Fatalf("ChecksumDatabase = %016x (err %v), returned %016x", full, err, sum)
		}
	}
}

// hangAfter bounds one fuzz execution. Go's fuzzer has no per-input
// timeout: a hung input just stalls its worker until -fuzztime runs out,
// and the run still reports PASS. Panicking from a timer instead crashes
// the worker, which makes the coordinator record the input under
// testdata/fuzz as a failing, reproducible crasher.
const hangAfter = 10 * time.Second

// watchdog panics if the current execution outlives hangAfter; call the
// returned stop func when the execution is done.
func watchdog() (stop func()) {
	tm := time.AfterFunc(hangAfter, func() {
		panic("fuzz execution exceeded " + hangAfter.String() + ": a hang")
	})
	return func() { tm.Stop() }
}

// requireNoLeftovers: a failed decode leaves neither the destination nor a
// temp file behind in dir.
func requireNoLeftovers(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed decode left %d entries behind (first %q)", len(entries), entries[0].Name())
	}
}

// FuzzDecodeSnapshot feeds arbitrary bytes to MaterializeChain as a
// snapshot (and to Materialize and TrailerPostApplyChecksum). The property:
// never panic or hang; either fail closed (no destination, no temp file) or
// produce a file whose checksum equals the trailer's post-apply checksum.
//
// It found that github.com/superfly/ltx's Decoder.DecodePage allocates a
// page frame's compressed-size prefix before reading it, ahead of any CRC
// check — up to 4 GiB for one corrupted field. frameGuard now refuses that
// field; the crasher shape is seeded below.
func FuzzDecodeSnapshot(f *testing.F) {
	snap, segs, _ := fuzzSeedChain(f)
	f.Add(snap)
	f.Add(segs[0]) // a segment is not a snapshot: must be refused
	f.Add(snap[:len(snap)/2])
	flipped := bytes.Clone(snap)
	flipped[len(flipped)/2] ^= 0x01
	f.Add(flipped)
	f.Add([]byte{})
	f.Add(snap[:ltx.HeaderSize])
	f.Add(oversizedBlockSnapshot(f))
	for _, s := range frameSeeds(f) {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxFuzzInput {
			t.Skip()
		}
		defer watchdog()()
		dir := t.TempDir()
		dst := filepath.Join(dir, "db.sqlite")
		_, sum, err := MaterializeChain(bytes.NewReader(data), nil, dst)
		if err != nil {
			requireNoLeftovers(t, dir)
			if _, merr := Materialize(bytes.NewReader(data), dst); merr == nil {
				t.Fatalf("Materialize accepted a snapshot MaterializeChain rejected (%v)", err)
			}
			requireNoLeftovers(t, dir)
			return
		}
		hdr := ltx.Header{}
		if err := hdr.UnmarshalBinary(data[:ltx.HeaderSize]); err != nil {
			t.Fatalf("decode succeeded on an unparseable header: %v", err)
		}
		requireConsistent(t, dst, hdr.PageSize, sum, data)

		dst2 := filepath.Join(dir, "plain.sqlite")
		if _, err := Materialize(bytes.NewReader(data), dst2); err != nil {
			t.Fatalf("Materialize rejected a snapshot MaterializeChain accepted: %v", err)
		}
		a, _ := os.ReadFile(dst)
		b, _ := os.ReadFile(dst2)
		if !bytes.Equal(a, b) {
			t.Fatal("Materialize and MaterializeChain disagree on the same snapshot")
		}
	})
}

// FuzzApplySegments applies arbitrary bytes as one segment on top of a real
// start database, two ways per input:
//
//   - raw: data itself is the segment. Mutations mostly die at the
//     decoder's CRC64 file checksum, so this explores header, page-frame
//     and LZ4 parsing ahead of that check.
//   - structured: data is read as a recipe (commit size, pages, which
//     checksum to corrupt) that is encoded with a valid CRC, so the input
//     reaches applySegments' post-Close logic — shrink, growth, page
//     placement, pre/post-apply checksum verification — and the result is
//     compared with an in-memory model of the same apply.
//
// Property: never panic or hang; fail closed with no destination or temp
// file, or produce a file whose checksum equals the segment's declared
// post-apply checksum (and, structured, whose bytes equal the model's).
func FuzzApplySegments(f *testing.F) {
	snap, segs, _ := fuzzSeedChain(f)
	startDir := f.TempDir()
	start := filepath.Join(startDir, "start.sqlite")
	_, startSum, err := MaterializeChain(bytes.NewReader(snap), nil, start)
	if err != nil {
		f.Fatal(err)
	}
	startBytes, err := os.ReadFile(start)
	if err != nil {
		f.Fatal(err)
	}
	pageSize, _, _ := pagesOf(f, start)

	for _, s := range segs {
		f.Add(s)
		f.Add(s[:len(s)-1])
	}
	f.Add(snap)
	f.Add([]byte{})
	// The CRC-valid Commit = 2^30 segment that used to hang applySegments.
	f.Add(hugeCommitSegment(f, startSum))
	for _, s := range frameSeeds(f) {
		f.Add(s)
	}
	// Structured-mode recipes (see structuredSegment): a carried growth, a
	// shrink to one page, a wrong post-apply checksum, an uncarried growth,
	// and a wild commit of 2^30 pages.
	f.Add([]byte{0, 4, 2, 0xaa, 3, 0x55})
	f.Add([]byte{0, 0x80})
	f.Add([]byte{1, 1, 1, 0x11})
	f.Add([]byte{0, 7})
	f.Add([]byte{4, 0x40, 0, 0, 0, 1, 0x22})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxFuzzInput {
			t.Skip()
		}
		defer watchdog()()
		// Raw.
		dir := t.TempDir()
		dst := filepath.Join(dir, "dst.sqlite")
		_, sum, err := ApplySegments(start, startSum, []io.Reader{bytes.NewReader(data)}, dst)
		if err != nil {
			requireNoLeftovers(t, dir)
		} else {
			requireConsistent(t, dst, pageSize, sum, data)
		}
		if got, _ := os.ReadFile(start); !bytes.Equal(got, startBytes) {
			t.Fatal("ApplySegments modified its start file")
		}

		// Structured.
		seg, want, wantSum, outcome := structuredSegment(startBytes, pageSize, startSum, data)
		if seg == nil {
			return
		}
		sdir := t.TempDir()
		sdst := filepath.Join(sdir, "dst.sqlite")
		_, sum, err = ApplySegments(start, startSum, []io.Reader{bytes.NewReader(seg)}, sdst)
		switch {
		case outcome == recipeValid && err != nil:
			t.Fatalf("well-formed segment with correct checksums rejected: %v", err)
		case outcome == recipeValid:
			if sum != wantSum {
				t.Fatalf("returned checksum %016x, model %016x", sum, wantSum)
			}
			got, _ := os.ReadFile(sdst)
			if !bytes.Equal(got, want) {
				t.Fatal("applied file differs from the model of the same apply")
			}
			requireConsistent(t, sdst, pageSize, sum, seg)
		case err == nil:
			t.Fatalf("malformed segment (outcome %d) was accepted", outcome)
		case outcome == recipeUncarried && !errors.Is(err, ErrUncarriedGrowth):
			t.Fatalf("segment growing past its pages refused for another reason: %v", err)
		default:
			requireNoLeftovers(t, sdir)
		}
	})
}

// recipeOutcome is what applying a structured segment must do.
type recipeOutcome int

const (
	recipeValid     recipeOutcome = iota // apply succeeds and matches the model
	recipeBadPre                         // corrupted pre-apply checksum: any error
	recipeUncarried                      // grows past its pages: ErrUncarriedGrowth
	recipeBadPost                        // corrupted post-apply checksum: any error
)

// maxRecipePages bounds a recipe's page count, and with it the model's
// memory (a carried growth adds at most this many pages).
const maxRecipePages = 64

// structuredSegment decodes data as a segment recipe and encodes it with
// the real ltx encoder (valid CRC), returning the segment, the expected
// outcome of applying it to base and, for recipeValid, the modeled result
// and its checksum. Recipe: data[0] bit 0 corrupts the post-apply checksum,
// bit 1 the pre-apply checksum, bit 2 selects a "wild" commit — any uint32,
// read from data[1:5] — instead of one in [0, base pages + 8] picked by
// data[1]; each following byte pair is (page selector, fill byte), at most
// maxRecipePages of them. A wild commit is almost always a growth the
// segment does not carry (the Commit = 2^30 shape that used to hang
// applySegments), which must be refused with ErrUncarriedGrowth. Returns a
// nil segment for a recipe the encoder itself refuses.
func structuredSegment(base []byte, pageSize uint32, startSum uint64, data []byte) (seg, want []byte, wantSum uint64, outcome recipeOutcome) {
	if len(data) < 2 {
		return nil, nil, 0, 0
	}
	basePages := uint32(len(base)) / pageSize
	var commit uint32
	var rest []byte
	if data[0]&4 != 0 {
		if len(data) < 5 {
			return nil, nil, 0, 0
		}
		commit, rest = binary.BigEndian.Uint32(data[1:5]), data[5:]
	} else {
		commit, rest = uint32(data[1])%(basePages+9), data[2:]
	}
	lockPgno := ltx.LockPgno(pageSize)

	byPgno := map[uint32]byte{}
	carried := map[uint32]bool{}
	if commit > 0 {
		for n := 0; len(rest) >= 2 && n < maxRecipePages; rest, n = rest[2:], n+1 {
			pgno := 1 + uint32(rest[0])%commit
			if pgno != lockPgno {
				byPgno[pgno] = rest[1]
				carried[pgno] = true
			}
		}
	}
	pgnos := make([]uint32, 0, len(byPgno))
	for p := range byPgno {
		pgnos = append(pgnos, p)
	}
	sort.Slice(pgnos, func(i, j int) bool { return pgnos[i] < pgnos[j] })
	pages := make([]Page, len(pgnos))
	for i, p := range pgnos {
		buf := bytes.Repeat([]byte{byPgno[p]}, int(pageSize))
		binary.BigEndian.PutUint32(buf, p)
		pages[i] = Page{Pgno: p, Data: buf}
	}

	switch {
	case data[0]&2 != 0:
		outcome = recipeBadPre
	case checkGrowth(commit, basePages, lockPgno, carried) != nil:
		outcome = recipeUncarried
	case data[0]&1 != 0:
		outcome = recipeBadPost
	}

	post := uint64(ltx.ChecksumFlag | 1)
	if checkGrowth(commit, basePages, lockPgno, carried) == nil {
		// Model: base, pages overwritten, then truncated or extended to
		// commit — exactly what a correct apply must produce. Bounded:
		// commit <= base pages + maxRecipePages here.
		want = make([]byte, max(len(base), int(commit)*int(pageSize)))
		copy(want, base)
		for _, p := range pages {
			copy(want[int(p.Pgno-1)*int(pageSize):], p.Data)
		}
		want = want[:int(commit)*int(pageSize)]
		c, err := checksumPages(bytes.NewReader(want), pageSize, commit)
		if err != nil {
			return nil, nil, 0, 0
		}
		wantSum = uint64(c)
		post = wantSum
	}
	pre := startSum
	if data[0]&1 != 0 {
		post ^= 2 // keeps ltx.ChecksumFlag set
	}
	if data[0]&2 != 0 {
		pre ^= 2
	}

	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		return nil, nil, 0, 0
	}
	hdr := ltx.Header{
		Version: ltx.Version, PageSize: pageSize, Commit: commit,
		MinTXID: 2, MaxTXID: 2, PreApplyChecksum: ltx.Checksum(pre),
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		return nil, nil, 0, 0
	}
	for _, p := range pages {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: p.Pgno}, p.Data); err != nil {
			return nil, nil, 0, 0
		}
	}
	enc.SetPostApplyChecksum(ltx.Checksum(post))
	if err := enc.Close(); err != nil {
		return nil, nil, 0, 0
	}
	return buf.Bytes(), want, wantSum, outcome
}
