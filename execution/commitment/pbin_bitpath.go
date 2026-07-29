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

package commitment

import (
	"errors"
	"fmt"
	"math/bits"
)

const (
	// pbinMaxPathBits is the longest EIP-8297 tree key: 66 bytes for a storage leaf.
	pbinMaxPathBits = 528
	pbinPathWords   = (pbinMaxPathBits + 63) / 64
)

// pbinBitpath is a path of up to 528 bits through the binary tree, held as
// big-endian words so that word order equals descent order and divergence is a
// XOR plus LeadingZeros64. Bits at or past bitLen are not part of the path and
// may hold anything; every reader either clamps by bitLen or masks first.
type pbinBitpath struct {
	w      [pbinPathWords]uint64
	bitLen int16
}

func pbinPathFromBytes(b []byte) pbinBitpath {
	return pbinPathFromBits(b, int16(len(b)*8))
}

func pbinPathFromBits(b []byte, bitLen int16) pbinBitpath {
	if bitLen < 0 || bitLen > pbinMaxPathBits {
		panic(fmt.Sprintf("pbin: bit length %d out of range", bitLen))
	}
	var p pbinBitpath
	n := min((int(bitLen)+7)/8, len(b))
	for i := range n {
		p.w[i/8] |= uint64(b[i]) << (56 - 8*uint(i%8))
	}
	p.bitLen = bitLen
	p.maskTail()
	return p
}

func (p *pbinBitpath) bit(d int16) uint64 {
	if d < 0 || d >= p.bitLen {
		panic(fmt.Sprintf("pbin: bit %d out of range for %d-bit path", d, p.bitLen))
	}
	return (p.w[d/64] >> (63 - uint(d%64))) & 1
}

func (p *pbinBitpath) setBitAt(d int16, v uint64) {
	if d < 0 || d >= pbinMaxPathBits {
		panic(fmt.Sprintf("pbin: bit %d out of range", d))
	}
	m := uint64(1) << (63 - uint(d%64))
	if v != 0 {
		p.w[d/64] |= m
	} else {
		p.w[d/64] &^= m
	}
}

func (p *pbinBitpath) maskTail() {
	wi, off := int(p.bitLen)/64, uint(p.bitLen)%64
	if off == 0 {
		p.w[wi] = 0
	} else {
		p.w[wi] &= ^uint64(0) << (64 - off)
	}
	for i := wi + 1; i < pbinPathWords; i++ {
		p.w[i] = 0
	}
}

func (p *pbinBitpath) truncate(bitLen int16) {
	if bitLen < 0 || bitLen > p.bitLen {
		panic(fmt.Sprintf("pbin: cannot truncate %d-bit path to %d bits", p.bitLen, bitLen))
	}
	p.bitLen = bitLen
	p.maskTail()
}

func (p *pbinBitpath) slice(from, to int16) pbinBitpath {
	if from < 0 || to < from || to > p.bitLen {
		panic(fmt.Sprintf("pbin: slice [%d,%d) out of range for %d-bit path", from, to, p.bitLen))
	}
	var r pbinBitpath
	for i := from; i < to; i++ {
		r.setBitAt(i-from, p.bit(i))
	}
	r.bitLen = to - from
	return r
}

func (p *pbinBitpath) appendBit(v uint64) {
	p.setBitAt(p.bitLen, v)
	p.bitLen++
}

func (p *pbinBitpath) append(o *pbinBitpath) {
	if int(p.bitLen)+int(o.bitLen) > pbinMaxPathBits {
		panic(fmt.Sprintf("pbin: appending %d bits to %d-bit path overflows", o.bitLen, p.bitLen))
	}
	for i := int16(0); i < o.bitLen; i++ {
		p.setBitAt(p.bitLen+i, o.bit(i))
	}
	p.bitLen += o.bitLen
}

func (p *pbinBitpath) hasPrefix(o *pbinBitpath) bool {
	return o.bitLen <= p.bitLen && pbinCommonPrefixBits(p, o) == o.bitLen
}

// pbinCommonPrefixBits reports how many leading bits a and b share, never more
// than the shorter path holds.
func pbinCommonPrefixBits(a, b *pbinBitpath) int16 {
	limit := min(a.bitLen, b.bitLen)
	n := int16(0)
	for i := 0; i < pbinPathWords && n < limit; i++ {
		if x := a.w[i] ^ b.w[i]; x != 0 {
			n += int16(bits.LeadingZeros64(x))
			break
		}
		n += 64
	}
	return min(n, limit)
}

// pbinCommonPrefixBitsAt reports how many leading bits of prefix agree with key
// read from bit `from`, never past the end of either operand.
func pbinCommonPrefixBitsAt(key *pbinBitpath, from int16, prefix *pbinBitpath) int16 {
	limit := min(key.bitLen-from, prefix.bitLen)
	n := int16(0)
	for n < limit && key.bit(from+n) == prefix.bit(n) {
		n++
	}
	return n
}

// pbinAppendPackedBits appends the path's bits MSB-first, zero-padded to a byte
// boundary.
func (p *pbinBitpath) appendPackedBits(dst []byte) []byte {
	for i := range (int(p.bitLen) + 7) / 8 {
		dst = append(dst, byte(p.w[i/8]>>(56-8*uint(i%8))))
	}
	if used := p.bitLen % 8; used != 0 {
		dst[len(dst)-1] &= ^byte(0) << (8 - uint(used))
	}
	return dst
}

var (
	errPBinEmptyBitPath    = errors.New("pbin: empty bit-path key")
	errPBinNonCanonicalPad = errors.New("pbin: non-canonical padding in bit-path key")
)

// pbinAppendBitPath appends the DB key for p: packed bits followed by a single
// byte holding bitLen mod 8. The trailing count is a suffix on purpose — a
// leading length field would scatter one subtree's records across the keyspace,
// whereas this layout keeps a subtree contiguous. It does not order ancestors
// before descendants, and callers must not assume it does.
func pbinAppendBitPath(dst []byte, p *pbinBitpath) []byte {
	return append(p.appendPackedBits(dst), byte(p.bitLen%8))
}

func pbinEncodeBitPath(p *pbinBitpath) []byte {
	return pbinAppendBitPath(make([]byte, 0, (int(p.bitLen)+7)/8+1), p)
}

// pbinDecodeBitPath is the inverse of pbinAppendBitPath and rejects every
// non-canonical spelling, so one path has exactly one DB key.
func pbinDecodeBitPath(buf []byte) (pbinBitpath, error) {
	var p pbinBitpath
	if len(buf) == 0 {
		return p, errPBinEmptyBitPath
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
	if bitLen > pbinMaxPathBits {
		return p, fmt.Errorf("pbin: bit path of %d bits exceeds %d", bitLen, pbinMaxPathBits)
	}
	for i, b := range packed {
		p.w[i/8] |= uint64(b) << (56 - 8*uint(i%8))
	}
	p.bitLen = int16(bitLen)

	masked := p
	masked.maskTail()
	if masked.w != p.w {
		return pbinBitpath{}, errPBinNonCanonicalPad
	}
	return p, nil
}
