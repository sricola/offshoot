package wal

import (
	"errors"
	"io"
	"os"
)

var ErrWALRestarted = errors.New("wal: salt changed — WAL was restarted")

type Reader struct {
	path     string
	bound    bool
	needSeed bool
	off      int64
	salt1    uint32
	salt2    uint32
	s1, s2   uint32 // running checksum at off
	hdr      Header
}

func NewReader(walPath string) *Reader { return &Reader{path: walPath} }

func (r *Reader) Offset() (int64, uint32, uint32) { return r.off, r.salt1, r.salt2 }

func (r *Reader) Bind(off int64, salt1, salt2 uint32) {
	// Rebinding at an offset requires the checksum seed at that offset; for
	// crash recovery we only ever Bind at HeaderSize with the header seed —
	// the engine rebases in all other cases (see capture.State). We don't
	// have the header's checksum words here, so mark the seed as pending
	// and derive it from the on-disk header on the next Next() call.
	r.bound, r.off, r.salt1, r.salt2 = true, off, salt1, salt2
	r.needSeed = true
}

func (r *Reader) Next() ([]Frame, error) {
	f, err := os.Open(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // no WAL yet
		}
		return nil, err
	}
	defer f.Close()

	hb := make([]byte, HeaderSize)
	if _, err := io.ReadFull(f, hb); err != nil {
		return nil, nil // header not fully written yet
	}
	hdr, err := ParseHeader(hb)
	if err != nil {
		return nil, nil // header torn mid-write; try again later
	}
	if !r.bound {
		r.bound = true
		r.off = HeaderSize
		r.salt1, r.salt2 = hdr.Salt1, hdr.Salt2
		r.s1, r.s2 = hdr.Cksum1, hdr.Cksum2
		r.hdr = hdr
	} else if hdr.Salt1 != r.salt1 || hdr.Salt2 != r.salt2 {
		return nil, ErrWALRestarted
	} else if r.needSeed {
		r.s1, r.s2 = hdr.Cksum1, hdr.Cksum2
		r.needSeed = false
	}
	r.hdr = hdr

	fsz := int64(hdr.FrameSize())
	var frames []Frame
	off, s1, s2 := r.off, r.s1, r.s2
	buf := make([]byte, fsz)
	for {
		if _, err := f.ReadAt(buf, off); err != nil {
			return nil, nil // incomplete frame at tail — wait
		}
		fh := ParseFrameHeader(buf[:FrameHeaderSize])
		if fh.Salt1 != hdr.Salt1 || fh.Salt2 != hdr.Salt2 {
			return nil, nil // stale/unwritten region past valid frames
		}
		s1, s2 = Checksum(hdr.ChecksumByteOrder(), s1, s2, buf[:8])
		s1, s2 = Checksum(hdr.ChecksumByteOrder(), s1, s2, buf[FrameHeaderSize:])
		if s1 != fh.Cksum1 || s2 != fh.Cksum2 {
			return nil, nil // torn frame at tail — wait
		}
		data := make([]byte, hdr.PageSize)
		copy(data, buf[FrameHeaderSize:])
		frames = append(frames, Frame{Header: fh, Data: data})
		off += fsz
		if fh.CommitSize != 0 {
			r.off, r.s1, r.s2 = off, s1, s2
			return frames, nil
		}
	}
}
