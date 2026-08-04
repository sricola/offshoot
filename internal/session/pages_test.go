package session

import (
	"testing"

	"github.com/offshoot-db/offshoot/internal/wal"
)

func frame(pgno uint32, b byte) wal.Frame {
	d := make([]byte, 8)
	for i := range d {
		d[i] = b
	}
	return wal.Frame{Header: wal.FrameHeader{Pgno: pgno}, Data: d}
}

func TestPageSetKeepsLatestAndSorts(t *testing.T) {
	p := newPageSet()
	p.record([]wal.Frame{frame(3, 'a'), frame(1, 'a')})
	p.record([]wal.Frame{frame(3, 'b')}) // page 3 written again
	if p.len() != 2 {
		t.Fatalf("len = %d, want 2 distinct pages", p.len())
	}
	got := p.drain()
	if len(got) != 2 || got[0].Pgno != 1 || got[1].Pgno != 3 {
		t.Fatalf("drain must sort by pgno: %+v", got)
	}
	if got[1].Data[0] != 'b' {
		t.Fatalf("page 3 must hold its latest content, got %q", got[1].Data[0])
	}
	if p.len() != 0 {
		t.Fatal("drain must reset the set")
	}
}

func TestPageSetCopiesFrameData(t *testing.T) {
	p := newPageSet()
	f := frame(1, 'x')
	p.record([]wal.Frame{f})
	f.Data[0] = 'z' // the caller reuses its buffer
	got := p.drain()
	if got[0].Data[0] != 'x' {
		t.Fatal("pageSet must copy frame data, not alias the caller's buffer")
	}
}

// TestPageSetDropAboveClearsStaleShrunkPages guards recordApply's shrink
// path: a page recorded by one transaction (here, pgno 5) can be dropped by
// a LATER transaction that shrinks the database without that later
// transaction's own frames ever mentioning pgno 5 (there is nothing new to
// write to a page that is going away). Without dropAbove, drain() would
// still hand that stale, now-nonexistent page to EncodeSegment, which
// rejects any page number beyond the segment's declared commit size.
func TestPageSetDropAboveClearsStaleShrunkPages(t *testing.T) {
	p := newPageSet()
	p.record([]wal.Frame{frame(1, 'a'), frame(3, 'a'), frame(5, 'a')})
	p.dropAbove(3)
	if p.len() != 2 {
		t.Fatalf("len = %d, want 2 (pgno 5 dropped)", p.len())
	}
	got := p.drain()
	if len(got) != 2 || got[0].Pgno != 1 || got[1].Pgno != 3 {
		t.Fatalf("dropAbove(3) must leave only pgnos <= 3: %+v", got)
	}
}

func TestPageSetDropAboveIsANoOpWhenNothingExceedsCommit(t *testing.T) {
	p := newPageSet()
	p.record([]wal.Frame{frame(1, 'a'), frame(2, 'a')})
	p.dropAbove(5)
	if p.len() != 2 {
		t.Fatalf("len = %d, want 2 (nothing above commit 5)", p.len())
	}
}
