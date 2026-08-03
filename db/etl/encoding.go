// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package etl

import (
	"encoding/binary"
	"io"
	"math"
)

// On-disk record format for spilled buffers: a two-byte header holding one
// length byte per field, then the key and value bytes. Both lengths arrive in a
// single load, and decoding is branch-and-shift rather than a call - the varint
// this replaced could not inline, so it cost a call per field per entry.
//
// Spill files never outlive the run that wrote them, so this format carries no
// compatibility obligation.
const (
	entryHeaderSize    = 2                     // one length byte per field
	entryHeaderMaxSize = entryHeaderSize + 2*4 // plus a uint32 per escaped field
	lenInlineMax       = 252                   // longest length a header byte holds
	lenNil             = 253                   // nil field, distinct from empty
	lenLong            = 254                   // uint32 length follows, key before value
)

// putEntryHeader encodes the lengths of k and v into buf, which must be at least
// entryHeaderMaxSize long, and returns the encoded prefix.
func putEntryHeader(buf []byte, k, v []byte) []byte {
	buf[0], buf[1] = lenCode(k), lenCode(v)
	n := entryHeaderSize
	if buf[0] == lenLong {
		binary.LittleEndian.PutUint32(buf[n:], uint32(len(k))) //nolint:gosec
		n += 4
	}
	if buf[1] == lenLong {
		binary.LittleEndian.PutUint32(buf[n:], uint32(len(v))) //nolint:gosec
		n += 4
	}
	return buf[:n]
}

func lenCode(field []byte) byte {
	if field == nil {
		return lenNil
	}
	if len(field) > lenInlineMax {
		if uint64(len(field)) > math.MaxUint32 {
			panic("etl: field longer than the record format's uint32 length")
		}
		return lenLong
	}
	return byte(len(field))
}

// mmapBytesReader tracks position for reading from mmap'd data
type mmapBytesReader struct {
	data []byte // mmap'd file content
	pos  int    // current read position
}

// nextEntry returns the next key and value as zero-copy slices into the mmap'd
// data. A nil field decodes back to nil, not to an empty slice.
func (m *mmapBytesReader) nextEntry() ([]byte, []byte, error) {
	data, pos := m.data, m.pos
	if pos == len(data) {
		return nil, nil, io.EOF
	}
	if pos+entryHeaderSize > len(data) {
		return nil, nil, io.ErrUnexpectedEOF
	}
	h := binary.LittleEndian.Uint16(data[pos:])
	pos += entryHeaderSize

	kCode, vCode := int(h&0xFF), int(h>>8)
	kLen, pos, err := fieldLen(data, pos, kCode)
	if err != nil {
		return nil, nil, err
	}
	vLen, pos, err := fieldLen(data, pos, vCode)
	if err != nil {
		return nil, nil, err
	}
	if pos+kLen+vLen > len(data) {
		return nil, nil, io.ErrUnexpectedEOF
	}

	k := data[pos : pos+kLen : pos+kLen]
	pos += kLen
	v := data[pos : pos+vLen : pos+vLen]
	m.pos = pos + vLen
	if kCode == lenNil {
		k = nil
	}
	if vCode == lenNil {
		v = nil
	}
	return k, v, nil
}

func fieldLen(data []byte, pos, code int) (length, next int, err error) {
	switch {
	case code <= lenInlineMax:
		return code, pos, nil
	case code == lenNil:
		return 0, pos, nil
	case code == lenLong:
		if pos+4 > len(data) {
			return 0, 0, io.ErrUnexpectedEOF
		}
		return int(binary.LittleEndian.Uint32(data[pos:])), pos + 4, nil
	default:
		return 0, 0, io.ErrUnexpectedEOF
	}
}
