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

// TestPageSetRecordThenDropAboveOrderMatters guards the exact defect fixed
// in Session.recordApply (see its "record must run BEFORE dropAbove"
// comment): dropAbove only clears pages already present in the set at the
// moment it runs. Calling it BEFORE the record() that introduces a page
// beyond the new commit size leaves that page behind — record-then-
// dropAbove, unconditionally in that order, is the only sequence that
// actually guarantees pageSet never holds anything past the current commit.
// The other pageSet tests in this file only ever exercise the correct
// order; this one demonstrates why the order is load-bearing by contrasting
// it against the wrong one.
func TestPageSetRecordThenDropAboveOrderMatters(t *testing.T) {
	// Wrong order (what recordApply used to do inside its shrink branch):
	// dropAbove runs first, so it cannot see a page record() is about to
	// introduce above the new commit size — that page survives.
	wrong := newPageSet()
	wrong.dropAbove(3)
	wrong.record([]wal.Frame{frame(1, 'a'), frame(5, 'a')})
	if wrong.len() != 2 {
		t.Fatalf("dropAbove-before-record len = %d, want 2 (pgno 5 wrongly survives)", wrong.len())
	}

	// Correct order (what recordApply does now, unconditionally): record
	// first, then dropAbove(3) — the stale page above commit is gone.
	right := newPageSet()
	right.record([]wal.Frame{frame(1, 'a'), frame(5, 'a')})
	right.dropAbove(3)
	if right.len() != 1 {
		t.Fatalf("record-then-dropAbove len = %d, want 1 (pgno 5 dropped)", right.len())
	}
	got := right.drain()
	if len(got) != 1 || got[0].Pgno != 1 {
		t.Fatalf("record-then-dropAbove left %+v, want only pgno 1", got)
	}
}
