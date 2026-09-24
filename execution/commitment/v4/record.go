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

type layout struct {
	child, leaf, ext uint16
	slotOff          int
	selfExtLen       int
	ok               bool
}

func (l layout) tree() uint16 { return l.child &^ l.leaf }

func (l layout) trailerStart() int { return l.slotOff + 32*bits.OnesCount16(l.tree()) }

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

func (r Record) ExtAt(nib int) []byte { return r.extAt(r.layout(), nib) }

func (r Record) slotAt(l layout, nib int) []byte {
	tree := l.tree()
	if nib < 0 || nib > 15 || !l.ok || tree&(uint16(1)<<nib) == 0 {
		return nil
	}
	off := l.slotOff + 32*bits.OnesCount16(tree&((uint16(1)<<nib)-1))
	if len(r.data) < off+32 {
		return nil
	}
	return r.data[off : off+32]
}

func (r Record) leafAt(l layout, nib int) (suffix, value []byte) {
	if nib < 0 || nib > 15 || !l.ok || l.leaf&(uint16(1)<<nib) == 0 {
		return nil, nil
	}
	off, ok := r.skipExt(l, 16)
	if !ok {
		return nil, nil
	}
	suffixLen := packedLen(64 - r.depth - 1)
	for i := range 16 {
		if l.leaf&(uint16(1)<<i) == 0 {
			continue
		}
		suffixEnd := off + suffixLen
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

func (r Record) extAt(l layout, nib int) []byte {
	if nib < 0 || nib > 15 || !l.ok || l.ext&(uint16(1)<<nib) == 0 {
		return nil
	}
	off, ok := r.skipExt(l, nib)
	if !ok || off >= len(r.data) {
		return nil
	}
	end := off + 1 + packedLen(int(r.data[off]))
	if end > len(r.data) {
		return nil
	}
	return r.data[off:end]
}

func (r Record) skipExt(l layout, upto int) (int, bool) {
	off := l.trailerStart()
	if off > len(r.data) {
		return 0, false
	}
	for i := range upto {
		if l.ext&(uint16(1)<<i) == 0 {
			continue
		}
		if len(r.data) <= off {
			return 0, false
		}
		off += 1 + packedLen(int(r.data[off]))
		if off > len(r.data) {
			return 0, false
		}
	}
	return off, true
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
	if len(data) == 0 {
		return ErrRecordTruncated
	}
	if data[0]&(hdrFormatMask|hdrHasEmb) != recordFormat {
		return ErrRecordFormat
	}
	r := NewRecord(data, depth)
	if r.isLeafRoot() {
		if data[0]&(hdrHasSelfExt|hdrHasChildExt) != 0 || depth != 0 {
			return ErrRecordRoot
		}
		if len(data) < 34 {
			return ErrRecordTruncated
		}
		if len(data) != 34+int(data[33]) {
			return fmt.Errorf("%w: leaf-root body length", ErrRecordTruncated)
		}
		return nil
	}
	if depth < 0 || depth > 63 {
		return fmt.Errorf("%w: depth %d", ErrInvalidRecord, depth)
	}
	if data[0]&hdrHasSelfExt != 0 {
		if len(data) < 2 {
			return ErrRecordTruncated
		}
		extLen := int(data[1])
		end := 2 + packedLen(extLen)
		if extLen > 64 {
			return fmt.Errorf("%w: self extension length %d", ErrRecordRoot, extLen)
		}
		if end > len(data) {
			return ErrRecordTruncated
		}
		if extLen&1 != 0 && data[end-1]&0x0f != 0 {
			return ErrRecordTrailer
		}
	}
	l := r.layout()
	if !l.ok {
		return ErrRecordTruncated
	}
	child, leaf, ext := l.child, l.leaf, l.ext
	if leaf&^child != 0 || ext&leaf != 0 || ext&^child != 0 {
		return ErrRecordMasks
	}
	if data[0]&hdrHasSelfExt != 0 && bits.OnesCount16(child) != 1 {
		return fmt.Errorf("%w: self extension needs one child", ErrRecordRoot)
	}
	if data[0]&hdrHasSelfExt != 0 && depth != 0 {
		return fmt.Errorf("%w: self extension is root-only", ErrRecordRoot)
	}
	off := l.trailerStart()
	if off > len(data) {
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

func (r Record) isLeafRoot() bool {
	return len(r.data) != 0 && r.data[0]&hdrIsLeafRoot != 0
}

func (r Record) layout() (l layout) {
	if len(r.data) == 0 || r.isLeafRoot() {
		return l
	}
	off := 1
	if r.data[0]&hdrHasSelfExt != 0 {
		if len(r.data) < off+1 {
			return layout{}
		}
		l.selfExtLen = int(r.data[off])
		off += 1 + packedLen(l.selfExtLen)
	}
	if len(r.data) < off+4 {
		return layout{}
	}
	l.child = binary.BigEndian.Uint16(r.data[off : off+2])
	l.leaf = binary.BigEndian.Uint16(r.data[off+2 : off+4])
	off += 4
	if r.data[0]&hdrHasChildExt != 0 {
		if len(r.data) < off+2 {
			return layout{}
		}
		l.ext = binary.BigEndian.Uint16(r.data[off : off+2])
		off += 2
	}
	l.slotOff, l.ok = off, true
	return l
}

func encodeRecord(n *node, depth int, dst []byte) []byte {
	if n.childMask == 0 {
		return dst[:0]
	}
	if n.leafMask&^n.childMask != 0 {
		panic("commitment v4: leaf mask is not a subset of child mask")
	}

	if depth == 0 && n.leafMask == n.childMask && bits.OnesCount16(n.childMask) == 1 {
		return encodeLeafRoot(n, dst)
	}

	selfExt := depth == 0 && len(n.path) != 0
	if selfExt {
		if bits.OnesCount16(n.childMask) != 1 || n.leafMask != 0 {
			panic("commitment v4: root extension needs one non-leaf child")
		}
	} else if depth == 0 && bits.OnesCount16(n.childMask) < 2 {
		panic("commitment v4: root branch needs at least two children")
	}

	extMask := uint16(0)
	for nib := range 16 {
		bit := uint16(1) << nib
		if n.childMask&bit != 0 && n.leafMask&bit == 0 && n.hasChildExt(nib) {
			extMask |= bit
		}
	}
	if selfExt && extMask != 0 {
		panic("commitment v4: root extension cannot have a child extension")
	}

	flags := recordFormat
	if selfExt {
		flags |= hdrHasSelfExt
	}
	if extMask != 0 {
		flags |= hdrHasChildExt
	}
	treeMask := n.childMask &^ n.leafMask
	headerLen := 1 + 4
	if selfExt {
		headerLen += 1 + packedLen(len(n.path))
	}
	if extMask != 0 {
		headerLen += 2
	}
	out := dst[:0]
	out = append(out, make([]byte, headerLen)...)
	out[0] = flags
	pos := 1
	if selfExt {
		out[pos] = byte(len(n.path))
		pos++
		packPath(n.path, out[pos:pos+packedLen(len(n.path))])
		pos += packedLen(len(n.path))
	}
	binary.BigEndian.PutUint16(out[pos:], n.childMask)
	binary.BigEndian.PutUint16(out[pos+2:], n.leafMask)
	if extMask != 0 {
		binary.BigEndian.PutUint16(out[pos+4:], extMask)
	}

	slotStart := len(out)
	slotCount := bits.OnesCount16(treeMask)
	out = append(out, make([]byte, slotCount*32)...)
	for nib := range 16 {
		bit := uint16(1) << nib
		if treeMask&bit == 0 {
			continue
		}
		hash := n.childHashAt(nib)
		if len(hash) != 32 {
			panic(fmt.Sprintf("commitment v4: child %d has no stored hash", nib))
		}
		copy(out[slotStart:], hash)
		slotStart += 32
	}

	for nib := range 16 {
		bit := uint16(1) << nib
		if extMask&bit == 0 {
			continue
		}
		out = n.appendChildExt(out, nib)
	}

	suffixCount := 64 - depth - 1
	suffixLen := packedLen(suffixCount)
	for nib := range 16 {
		bit := uint16(1) << nib
		if n.leafMask&bit == 0 {
			continue
		}
		suffix := n.leafSuffixAt(nib)
		if len(suffix) != suffixLen {
			panic(fmt.Sprintf("commitment v4: leaf %d has suffix length %d, want %d", nib, len(suffix), suffixLen))
		}
		value := n.leafValueAt(nib)
		if len(value) > 255 {
			panic(fmt.Sprintf("commitment v4: leaf %d value is too long", nib))
		}
		out = append(out, suffix...)
		out = append(out, byte(len(value)))
		out = append(out, value...)
	}
	return out
}

func encodeLeafRoot(n *node, dst []byte) []byte {
	nib := bits.TrailingZeros16(n.childMask)
	suffix := n.leafSuffixAt(nib)
	if len(n.path) != 0 || len(suffix) != packedLen(63) {
		panic("commitment v4: invalid leaf root path")
	}
	value := n.leafValueAt(nib)
	if len(value) > 255 {
		panic("commitment v4: leaf root value is too long")
	}
	var pathScratch [64]byte
	fullPath := pathScratch[:64]
	fullPath[0] = byte(nib)
	unpackPath(suffix, 63, fullPath[1:64:64])
	var keyScratch [32]byte
	key := packPath(fullPath, keyScratch[:0])
	out := dst[:0]
	out = append(out, hdrIsLeafRoot)
	out = append(out, key...)
	out = append(out, byte(len(value)))
	return append(out, value...)
}
