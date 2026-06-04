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

package recsplit

import (
	"encoding/binary"
	"math/bits"
)

const (
	murmurC1 = 0x87c37b91114253d5
	murmurC2 = 0x4cf5ad432745937f
)

// murmur128WithSeed is MurmurHash3 x64 128-bit, bit-identical to
// github.com/spaolacci/murmur3.Sum128WithSeed but allocation- and
// indirection-free, which is measurably faster for the short keys
// hashed on every index lookup.
func murmur128WithSeed(key []byte, seed uint32) (uint64, uint64) {
	h1, h2 := uint64(seed), uint64(seed)
	clen := len(key)

	for len(key) >= 16 {
		k1 := binary.LittleEndian.Uint64(key)
		k2 := binary.LittleEndian.Uint64(key[8:])

		k1 *= murmurC1
		k1 = bits.RotateLeft64(k1, 31)
		k1 *= murmurC2
		h1 ^= k1

		h1 = bits.RotateLeft64(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729

		k2 *= murmurC2
		k2 = bits.RotateLeft64(k2, 33)
		k2 *= murmurC1
		h2 ^= k2

		h2 = bits.RotateLeft64(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5

		key = key[16:]
	}

	if n := len(key); n > 0 {
		var k1 uint64
		if n > 8 {
			// overlapping load of the last 8 bytes, shifted so only bytes 8..n-1 remain
			k2 := binary.LittleEndian.Uint64(key[n-8:]) >> (8 * (16 - n))
			k2 *= murmurC2
			k2 = bits.RotateLeft64(k2, 33)
			k2 *= murmurC1
			h2 ^= k2
			k1 = binary.LittleEndian.Uint64(key)
		} else {
			k1 = loadPartial(key, n)
		}
		k1 *= murmurC1
		k1 = bits.RotateLeft64(k1, 31)
		k1 *= murmurC2
		h1 ^= k1
	}

	h1 ^= uint64(clen)
	h2 ^= uint64(clen)

	h1 += h2
	h2 += h1

	h1 = murmurFmix64(h1)
	h2 = murmurFmix64(h2)

	h1 += h2
	h2 += h1

	return h1, h2
}

// loadPartial reads 1..8 bytes little-endian without crossing the slice end
func loadPartial(p []byte, n int) uint64 {
	if n >= 8 {
		return binary.LittleEndian.Uint64(p)
	}
	if n >= 4 {
		lo := uint64(binary.LittleEndian.Uint32(p))
		hi := uint64(binary.LittleEndian.Uint32(p[n-4:]))
		return lo | hi<<(8*(n-4))
	}
	v := uint64(p[0])
	if n >= 2 {
		v |= uint64(p[1]) << 8
	}
	if n == 3 {
		v |= uint64(p[2]) << 16
	}
	return v
}

func murmurFmix64(k uint64) uint64 {
	k ^= k >> 33
	k *= 0xff51afd7ed558ccd
	k ^= k >> 33
	k *= 0xc4ceb9fe1a85ec53
	k ^= k >> 33
	return k
}
