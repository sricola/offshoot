// Package wal parses the SQLite write-ahead-log binary format.
// Reference: https://www.sqlite.org/fileformat2.html#walformat
package wal

import (
	"encoding/binary"
	"errors"
)

const (
	HeaderSize      = 32
	FrameHeaderSize = 24

	magicLE = 0x377f0682 // checksum words read little-endian
	magicBE = 0x377f0683 // checksum words read big-endian
	version = 3007000
)

var (
	ErrBadMagic   = errors.New("wal: bad magic")
	ErrBadVersion = errors.New("wal: unsupported version")
	ErrChecksum   = errors.New("wal: header checksum mismatch")
)

type Header struct {
	Magic          uint32
	Version        uint32
	PageSize       uint32
	CheckpointSeq  uint32
	Salt1, Salt2   uint32
	Cksum1, Cksum2 uint32
}

type FrameHeader struct {
	Pgno           uint32
	CommitSize     uint32
	Salt1, Salt2   uint32
	Cksum1, Cksum2 uint32
}

type Frame struct {
	Header FrameHeader
	Data   []byte
}

func ParseHeader(b []byte) (Header, error) {
	h := Header{
		Magic:         binary.BigEndian.Uint32(b[0:4]),
		Version:       binary.BigEndian.Uint32(b[4:8]),
		PageSize:      binary.BigEndian.Uint32(b[8:12]),
		CheckpointSeq: binary.BigEndian.Uint32(b[12:16]),
		Salt1:         binary.BigEndian.Uint32(b[16:20]),
		Salt2:         binary.BigEndian.Uint32(b[20:24]),
		Cksum1:        binary.BigEndian.Uint32(b[24:28]),
		Cksum2:        binary.BigEndian.Uint32(b[28:32]),
	}
	if h.Magic != magicLE && h.Magic != magicBE {
		return h, ErrBadMagic
	}
	if h.Version != version {
		return h, ErrBadVersion
	}
	s1, s2 := Checksum(h.ChecksumByteOrder(), 0, 0, b[:24])
	if s1 != h.Cksum1 || s2 != h.Cksum2 {
		return h, ErrChecksum
	}
	return h, nil
}

func ParseFrameHeader(b []byte) FrameHeader {
	return FrameHeader{
		Pgno:       binary.BigEndian.Uint32(b[0:4]),
		CommitSize: binary.BigEndian.Uint32(b[4:8]),
		Salt1:      binary.BigEndian.Uint32(b[8:12]),
		Salt2:      binary.BigEndian.Uint32(b[12:16]),
		Cksum1:     binary.BigEndian.Uint32(b[16:20]),
		Cksum2:     binary.BigEndian.Uint32(b[20:24]),
	}
}

func (h Header) FrameSize() int { return FrameHeaderSize + int(h.PageSize) }

func (h Header) ChecksumByteOrder() binary.ByteOrder {
	if h.Magic == magicBE {
		return binary.BigEndian
	}
	return binary.LittleEndian
}

// Checksum implements SQLite's cumulative WAL checksum. len(b) must be a
// multiple of 8.
func Checksum(bo binary.ByteOrder, s1, s2 uint32, b []byte) (uint32, uint32) {
	for i := 0; i+8 <= len(b); i += 8 {
		s1 += bo.Uint32(b[i:i+4]) + s2
		s2 += bo.Uint32(b[i+4:i+8]) + s1
	}
	return s1, s2
}
