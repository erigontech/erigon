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

package eip8297

import (
	"errors"
	"fmt"
	"math/bits"
)

const (
	// MaxPathBits is the longest EIP-8297 tree key: 66 bytes for a storage leaf.
	MaxPathBits = 528
	PathWords   = (MaxPathBits + 63) / 64
)

// Bitpath is a path of up to 528 bits through the binary tree, held as
// big-endian words so that word order equals descent order and divergence is a
// XOR plus LeadingZeros64. Bits at or past bitLen are not part of the path and
// may hold anything; every reader either clamps by bitLen or masks first.
type Bitpath struct {
	Words  [PathWords]uint64
	BitLen int16
}

func PathFromBytes(b []byte) Bitpath {
	return PathFromBits(b, int16(len(b)*8))
}

func PathFromBits(b []byte, bitLen int16) Bitpath {
	if bitLen < 0 || bitLen > MaxPathBits {
		panic(fmt.Sprintf("pbin: bit length %d out of range", bitLen))
	}
	var p Bitpath
	n := min((int(bitLen)+7)/8, len(b))
	for i := range n {
		p.Words[i/8] |= uint64(b[i]) << (56 - 8*uint(i%8))
	}
	p.BitLen = bitLen
	p.MaskTail()
	return p
}

func (p *Bitpath) Bit(d int16) uint64 {
	if d < 0 || d >= p.BitLen {
		panic(fmt.Sprintf("pbin: bit %d out of range for %d-bit path", d, p.BitLen))
	}
	return (p.Words[d/64] >> (63 - uint(d%64))) & 1
}

func (p *Bitpath) SetBitAt(d int16, v uint64) {
	if d < 0 || d >= MaxPathBits {
		panic(fmt.Sprintf("pbin: bit %d out of range", d))
	}
	m := uint64(1) << (63 - uint(d%64))
	if v != 0 {
		p.Words[d/64] |= m
	} else {
		p.Words[d/64] &^= m
	}
}

func (p *Bitpath) MaskTail() {
	wi, off := int(p.BitLen)/64, uint(p.BitLen)%64
	if off == 0 {
		p.Words[wi] = 0
	} else {
		p.Words[wi] &= ^uint64(0) << (64 - off)
	}
	for i := wi + 1; i < PathWords; i++ {
		p.Words[i] = 0
	}
}

func (p *Bitpath) Truncate(bitLen int16) {
	if bitLen < 0 || bitLen > p.BitLen {
		panic(fmt.Sprintf("pbin: cannot truncate %d-bit path to %d bits", p.BitLen, bitLen))
	}
	p.BitLen = bitLen
	p.MaskTail()
}

func (p *Bitpath) Slice(from, to int16) Bitpath {
	if from < 0 || to < from || to > p.BitLen {
		panic(fmt.Sprintf("pbin: slice [%d,%d) out of range for %d-bit path", from, to, p.BitLen))
	}
	var r Bitpath
	for src, dst := from, int16(0); dst < to-from; {
		take := min(int16(64-dst%64), to-from-dst)
		word := p.wordAt(src)
		if take < 64 {
			word &= ^uint64(0) << (64 - uint(take))
		}
		r.Words[dst/64] |= word >> uint(dst%64)
		src += take
		dst += take
	}
	r.BitLen = to - from
	r.MaskTail()
	return r
}

func (p *Bitpath) AppendBit(v uint64) {
	if p.BitLen < 0 || p.BitLen >= MaxPathBits {
		panic(fmt.Sprintf("pbin: bit %d out of range", p.BitLen))
	}
	if v != 0 {
		p.Words[p.BitLen/64] |= uint64(1) << (63 - uint(p.BitLen%64))
	}
	p.BitLen++
}

func (p *Bitpath) Append(o *Bitpath) {
	if int(p.BitLen)+int(o.BitLen) > MaxPathBits {
		panic(fmt.Sprintf("pbin: appending %d bits to %d-bit path overflows", o.BitLen, p.BitLen))
	}
	for src, dst := int16(0), p.BitLen; src < o.BitLen; {
		take := min(int16(64-dst%64), o.BitLen-src)
		word := o.wordAt(src)
		if take < 64 {
			word &= ^uint64(0) << (64 - uint(take))
		}
		p.Words[dst/64] |= word >> uint(dst%64)
		src += take
		dst += take
	}
	p.BitLen += o.BitLen
	p.MaskTail()
}

func (p *Bitpath) wordAt(offset int16) uint64 {
	word := p.Words[offset/64] << uint(offset%64)
	if shift := uint(offset % 64); shift != 0 && int(offset/64)+1 < PathWords {
		word |= p.Words[offset/64+1] >> (64 - shift)
	}
	return word
}

func (p *Bitpath) HasPrefix(o *Bitpath) bool {
	return o.BitLen <= p.BitLen && CommonPrefixBitsAt(p, 0, o) == o.BitLen
}

// CommonPrefixBitsAt reports how many leading bits of prefix agree with key
// read from bit `from`, clamped to what both operands hold.
func CommonPrefixBitsAt(key *Bitpath, from int16, prefix *Bitpath) int16 {
	limit := min(key.BitLen-from, prefix.BitLen)
	if limit <= 0 {
		return 0
	}
	shift := uint(from % 64)
	n := int16(0)
	for wi := int(from / 64); n < limit; wi++ {
		w := key.Words[wi] << shift
		if shift != 0 && wi+1 < PathWords {
			w |= key.Words[wi+1] >> (64 - shift)
		}
		if x := w ^ prefix.Words[n/64]; x != 0 {
			n += int16(bits.LeadingZeros64(x))
			break
		}
		n += 64
	}
	return min(n, limit)
}

// appendPackedBits appends the path's bits MSB-first, zero-padded to a byte
// boundary.
func (p *Bitpath) AppendPackedBits(dst []byte) []byte {
	for i := range (int(p.BitLen) + 7) / 8 {
		dst = append(dst, byte(p.Words[i/8]>>(56-8*uint(i%8))))
	}
	if used := p.BitLen % 8; used != 0 {
		dst[len(dst)-1] &= ^byte(0) << (8 - uint(used))
	}
	return dst
}

var (
	ErrEmptyBitPath    = errors.New("pbin: empty bit-path key")
	ErrNonCanonicalPad = errors.New("pbin: non-canonical padding in bit-path key")
)

// AppendBitPath appends the DB key for p: packed bits followed by one byte
// holding bitLen mod 8. The count is a suffix so that a subtree stays
// contiguous; a leading length field would scatter its records across the
// keyspace. The order is not ancestors-before-descendants, and callers must not
// assume it is.
func AppendBitPath(dst []byte, p *Bitpath) []byte {
	return append(p.AppendPackedBits(dst), byte(p.BitLen%8))
}

func EncodeBitPath(p *Bitpath) []byte {
	return AppendBitPath(make([]byte, 0, (int(p.BitLen)+7)/8+1), p)
}

// DecodeBitPath inverts AppendBitPath, rejecting non-canonical
// spellings so that one path has exactly one DB key.
func DecodeBitPath(buf []byte) (Bitpath, error) {
	var p Bitpath
	if len(buf) == 0 {
		return p, ErrEmptyBitPath
	}
	tailBits, packed := buf[len(buf)-1], buf[:len(buf)-1]
	if tailBits > 7 {
		return p, fmt.Errorf("pbin: invalid trailing bit count %d in bit-path key", tailBits)
	}
	bitLen := len(packed) * 8
	if tailBits != 0 {
		if len(packed) == 0 {
			return p, fmt.Errorf("pbin: trailing bit count %d with no payload", tailBits)
		}
		bitLen = bitLen - 8 + int(tailBits)
	}
	if bitLen > MaxPathBits {
		return p, fmt.Errorf("pbin: bit path of %d bits exceeds %d", bitLen, MaxPathBits)
	}
	if used := bitLen % 8; used != 0 && packed[len(packed)-1]&(0xFF>>used) != 0 {
		return Bitpath{}, ErrNonCanonicalPad
	}
	return PathFromBits(packed, int16(bitLen)), nil
}
