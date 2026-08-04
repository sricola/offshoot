package session

import (
	"sort"

	"github.com/offshoot-db/offshoot/internal/ltxio"
	"github.com/offshoot-db/offshoot/internal/wal"
)

// pageSet accumulates the latest content of every page changed since the last
// flush. It is the segment's payload: the capture engine hands us exactly the
// pages SQLite wrote, so a flush never has to diff the database.
type pageSet struct {
	pages map[uint32][]byte
}

func newPageSet() *pageSet {
	return &pageSet{pages: make(map[uint32][]byte)}
}

// record keeps the newest version of each page; a page written twice between
// flushes is stored once, at its latest content. Frame data is copied: the
// capture engine reuses its frame buffers across calls (see
// TestPageSetCopiesFrameData), so aliasing would let a later Apply silently
// rewrite a page this set already recorded.
func (p *pageSet) record(frames []wal.Frame) {
	for _, f := range frames {
		data := make([]byte, len(f.Data))
		copy(data, f.Data)
		p.pages[f.Header.Pgno] = data
	}
}

// drain returns the accumulated pages sorted by Pgno and resets the set.
func (p *pageSet) drain() []ltxio.Page {
	out := make([]ltxio.Page, 0, len(p.pages))
	for pgno, data := range p.pages {
		out = append(out, ltxio.Page{Pgno: pgno, Data: data})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Pgno < out[j].Pgno })
	p.pages = make(map[uint32][]byte)
	return out
}

func (p *pageSet) len() int { return len(p.pages) }
