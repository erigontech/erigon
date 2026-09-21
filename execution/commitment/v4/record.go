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

package v4

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

const (
	recordFormat   byte = 0
	hdrFormatMask       = 0x0f
	hdrHasSelfExt       = 1 << 4
	hdrHasChildExt      = 1 << 5
	hdrHasEmb           = 1 << 6
	hdrIsLeafRoot       = 1 << 7
)

var (
	ErrInvalidRecord   = errors.New("commitment v4: invalid record")
	ErrRecordTruncated = errors.New("commitment v4: truncated record")
	ErrRecordMasks     = errors.New("commitment v4: invalid record masks")
	ErrRecordTrailer   = errors.New("commitment v4: invalid record trailer")
	ErrRecordRoot      = errors.New("commitment v4: invalid root record")
	ErrRecordFormat    = errors.New("commitment v4: unsupported record format")
)

type Record struct {
	data  []byte
	depth int
}

func NewRecord(data []byte, depth int) Record {
	return Record{data: data, depth: depth}
}

func (r Record) ChildMask() uint16 {
	if r.isLeafRoot() {
		return 0
	}
	child, _, _, _, _, ok := r.maskLayout()
	if !ok {
		return 0
	}
	return child
}

func (r Record) LeafMask() uint16 {
	if r.isLeafRoot() {
		return 0
	}
	_, leaf, _, _, _, ok := r.maskLayout()
	if !ok {
		return 0
	}
	return leaf
}

func (r Record) ExtMask() uint16 {
	if r.isLeafRoot() || r.data == nil || len(r.data) == 0 {
		return 0
	}
	_, _, ext, _, _, ok := r.maskLayout()
	if !ok {
		return 0
	}
	return ext
}

func (r Record) EmbMask() uint16 {
	if r.isLeafRoot() || r.data == nil || len(r.data) == 0 {
		return 0
	}
	_, _, _, emb, _, ok := r.maskLayout()
	if !ok {
		return 0
	}
	return emb
}

func (r Record) SelfExt() []byte {
	if len(r.data) == 0 || r.data[0]&hdrHasSelfExt == 0 || r.isLeafRoot() || len(r.data) < 2 {
		return nil
	}
	n := packedLen(int(r.data[1]))
	if len(r.data) < 2+n {
		return nil
	}
	return r.data[1 : 2+n]
}

func (r Record) SlotAt(nib int) []byte {
	if nib < 0 || nib > 15 || r.isLeafRoot() {
		return nil
	}
	child, leaf, _, emb, off, ok := r.maskLayout()
	if !ok {
		return nil
	}
	tree := child &^ leaf &^ emb
	if tree&(uint16(1)<<nib) == 0 {
		return nil
	}
	off += 32 * bits.OnesCount16(tree&((uint16(1)<<nib)-1))
	if len(r.data) < off+32 {
		return nil
	}
	return r.data[off : off+32]
}

func (r Record) LeafAt(nib int) (suffix, value []byte) {
	if nib < 0 || nib > 15 || r.isLeafRoot() || r.LeafMask()&(uint16(1)<<nib) == 0 {
		return nil, nil
	}
	off, ok := r.trailerStart()
	if !ok {
		return nil, nil
	}
	if mask := r.ExtMask(); mask != 0 {
		for i := range 16 {
			if mask&(uint16(1)<<i) == 0 {
				continue
			}
			if len(r.data) <= off {
				return nil, nil
			}
			n := packedLen(int(r.data[off]))
			end := off + 1 + n
			if end > len(r.data) {
				return nil, nil
			}
			off = end
		}
	}
	if mask := r.EmbMask(); mask != 0 {
		for i := range 16 {
			if mask&(uint16(1)<<i) == 0 {
				continue
			}
			if len(r.data) <= off {
				return nil, nil
			}
			n := int(r.data[off])
			end := off + 1 + n
			if end > len(r.data) {
				return nil, nil
			}
			off = end
		}
	}
	for i := range 16 {
		if r.LeafMask()&(uint16(1)<<i) == 0 {
			continue
		}
		suffixEnd := off + packedLen(64-r.depth-1)
		if suffixEnd >= len(r.data) {
			return nil, nil
		}
		valueEnd := suffixEnd + 1 + int(r.data[suffixEnd])
		if valueEnd > len(r.data) {
			return nil, nil
		}
		if i == nib {
			return r.data[off:suffixEnd], r.data[suffixEnd+1 : valueEnd]
		}
		off = valueEnd
	}
	return nil, nil
}

func (r Record) ExtAt(nib int) []byte {
	if nib < 0 || nib > 15 || r.isLeafRoot() || r.ExtMask()&(uint16(1)<<nib) == 0 {
		return nil
	}
	off, ok := r.trailerStart()
	if !ok {
		return nil
	}
	for i := range 16 {
		if r.ExtMask()&(uint16(1)<<i) == 0 {
			continue
		}
		if len(r.data) <= off {
			return nil
		}
		end := off + 1 + packedLen(int(r.data[off]))
		if end > len(r.data) {
			return nil
		}
		if i == nib {
			return r.data[off:end]
		}
		off = end
	}
	return nil
}

func (r Record) EmbAt(nib int) []byte {
	if nib < 0 || nib > 15 || r.isLeafRoot() || r.EmbMask()&(uint16(1)<<nib) == 0 {
		return nil
	}
	off, ok := r.trailerStart()
	if !ok {
		return nil
	}
	for i := range 16 {
		if r.ExtMask()&(uint16(1)<<i) != 0 {
			if len(r.data) <= off {
				return nil
			}
			off += 1 + packedLen(int(r.data[off]))
			if off > len(r.data) {
				return nil
			}
		}
	}
	for i := range 16 {
		if r.EmbMask()&(uint16(1)<<i) == 0 {
			continue
		}
		if len(r.data) <= off {
			return nil
		}
		end := off + 1 + int(r.data[off])
		if end > len(r.data) {
			return nil
		}
		if i == nib {
			return r.data[off+1 : end]
		}
		off = end
	}
	return nil
}

func (r Record) LeafRootBody() (hashedKey, value []byte) {
	if !r.isLeafRoot() || len(r.data) < 34 {
		return nil, nil
	}
	valueEnd := 34 + int(r.data[33])
	if valueEnd > len(r.data) {
		return nil, nil
	}
	return r.data[1:33], r.data[34:valueEnd]
}

func Validate(data []byte, depth int) error {
	r := NewRecord(data, depth)
	if err := r.validateBase(); err != nil {
		return err
	}
	if r.isLeafRoot() {
		if len(data) < 34 {
			return ErrRecordTruncated
		}
		if len(data) != 34+int(data[33]) {
			return fmt.Errorf("%w: leaf-root body length", ErrRecordTruncated)
		}
		return nil
	}
	child := r.ChildMask()
	leaf := r.LeafMask()
	ext := r.ExtMask()
	emb := r.EmbMask()
	if leaf&^child != 0 || ext&leaf != 0 || ext&^child != 0 || emb&leaf != 0 || emb&^child != 0 || ext&emb != 0 {
		return ErrRecordMasks
	}
	if r.data[0]&hdrHasSelfExt != 0 && bits.OnesCount16(child) != 1 {
		return fmt.Errorf("%w: self extension needs one child", ErrRecordRoot)
	}
	if r.data[0]&hdrHasSelfExt != 0 && depth != 0 {
		return fmt.Errorf("%w: self extension is root-only", ErrRecordRoot)
	}
	if depth < 0 || depth > 63 {
		return fmt.Errorf("%w: depth %d", ErrInvalidRecord, depth)
	}
	off, ok := r.trailerStart()
	if !ok {
		return ErrRecordTruncated
	}
	suffixCount := 64 - depth - 1
	for i := range 16 {
		if ext&(uint16(1)<<i) == 0 {
			continue
		}
		if off >= len(data) {
			return ErrRecordTruncated
		}
		extLen := int(data[off])
		if extLen > suffixCount {
			return fmt.Errorf("%w: extension length %d", ErrRecordTrailer, extLen)
		}
		end := off + 1 + packedLen(extLen)
		if end > len(data) {
			return ErrRecordTruncated
		}
		if extLen&1 != 0 && data[end-1]&0x0f != 0 {
			return ErrRecordTrailer
		}
		off = end
	}
	for i := range 16 {
		if emb&(uint16(1)<<i) == 0 {
			continue
		}
		if off >= len(data) {
			return ErrRecordTruncated
		}
		end := off + 1 + int(data[off])
		if end > len(data) {
			return ErrRecordTruncated
		}
		off = end
	}
	suffixLen := packedLen(suffixCount)
	for i := range 16 {
		if leaf&(uint16(1)<<i) == 0 {
			continue
		}
		if off+suffixLen >= len(data) {
			return ErrRecordTruncated
		}
		if suffixCount&1 != 0 && data[off+suffixLen-1]&0x0f != 0 {
			return ErrRecordTrailer
		}
		valueEnd := off + suffixLen + 1 + int(data[off+suffixLen])
		if valueEnd > len(data) {
			return ErrRecordTruncated
		}
		off = valueEnd
	}
	if off != len(data) {
		return fmt.Errorf("%w: trailing bytes", ErrRecordTrailer)
	}
	return nil
}

func (r Record) validateBase() error {
	if len(r.data) == 0 {
		return ErrRecordTruncated
	}
	if r.data[0]&hdrFormatMask != recordFormat {
		return ErrRecordFormat
	}
	if r.isLeafRoot() {
		if r.data[0]&hdrHasSelfExt != 0 {
			return ErrRecordRoot
		}
		if r.data[0]&(hdrHasChildExt|hdrHasEmb) != 0 {
			return ErrRecordRoot
		}
		if r.depth != 0 {
			return ErrRecordRoot
		}
		if len(r.data) < 34 {
			return ErrRecordTruncated
		}
		return nil
	}
	if r.depth < 0 || r.depth > 63 {
		return fmt.Errorf("%w: depth %d", ErrInvalidRecord, r.depth)
	}
	if r.data[0]&hdrHasSelfExt != 0 {
		if len(r.data) < 2 {
			return ErrRecordTruncated
		}
		extLen := int(r.data[1])
		end := 2 + packedLen(extLen)
		if extLen > 64 {
			return fmt.Errorf("%w: self extension length %d", ErrRecordRoot, extLen)
		}
		if end > len(r.data) {
			return ErrRecordTruncated
		}
		if extLen&1 != 0 && r.data[end-1]&0x0f != 0 {
			return ErrRecordTrailer
		}
	}
	_, _, ok := r.base()
	if !ok {
		return ErrRecordTruncated
	}
	return nil
}

func (r Record) isLeafRoot() bool {
	return len(r.data) != 0 && r.data[0]&hdrIsLeafRoot != 0
}

func (r Record) base() (selfEnd, body int, ok bool) {
	_, _, _, _, slot, ok := r.maskLayout()
	return 0, slot, ok
}

func (r Record) maskLayout() (child, leaf, ext, emb uint16, slot int, ok bool) {
	if len(r.data) == 0 {
		return 0, 0, 0, 0, 0, false
	}
	off := 1
	if r.data[0]&hdrHasSelfExt != 0 {
		if len(r.data) < off+1 {
			return 0, 0, 0, 0, 0, false
		}
		n := int(r.data[off])
		off += 1 + packedLen(n)
	}
	if len(r.data) < off+4 {
		return 0, 0, 0, 0, 0, false
	}
	off += 4
	maskOff := off - 4
	child = binary.BigEndian.Uint16(r.data[maskOff : maskOff+2])
	leaf = binary.BigEndian.Uint16(r.data[maskOff+2 : maskOff+4])
	if r.data[0]&hdrHasChildExt != 0 {
		if len(r.data) < off+2 {
			return 0, 0, 0, 0, 0, false
		}
		ext = binary.BigEndian.Uint16(r.data[off : off+2])
		off += 2
	}
	if r.data[0]&hdrHasEmb != 0 {
		if len(r.data) < off+2 {
			return 0, 0, 0, 0, 0, false
		}
		emb = binary.BigEndian.Uint16(r.data[off : off+2])
		off += 2
	}
	if len(r.data) < off {
		return 0, 0, 0, 0, 0, false
	}
	return child, leaf, ext, emb, off, true
}

func (r Record) trailerStart() (int, bool) {
	_, slot, ok := r.base()
	if !ok {
		return 0, false
	}
	child := r.ChildMask()
	leaf := r.LeafMask()
	emb := r.EmbMask()
	tree := child &^ leaf &^ emb
	start := slot + 32*bits.OnesCount16(tree)
	if start > len(r.data) {
		return 0, false
	}
	return start, true
}
