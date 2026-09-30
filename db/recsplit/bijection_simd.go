// Copyright 2021 The Erigon Authors
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

//go:build go1.27 && goexperiment.simd && amd64

package recsplit

import "simd/archsimd"

var hasAVX512 = archsimd.X86.AVX512()

// saltVectors is how many 8-lane salt batches are searched per pass. splitmix64
// is a serial dependency chain, so one vector leaves the multiplier idle; four
// independent chains keep it fed, which is the same reason the scalar form
// unrolls eight salts by hand.
const saltVectors = 4

// findBijection is findBijectionGeneric with the salt candidates held in 512-bit
// registers. AVX2 cannot host it: the 64x64 multiply both splitmix64 and remap16
// need is VPMULLQ (AVX512DQ), and emulating it costs more than the unrolled
// scalar form saves.
func findBijection(bucket []uint64, salt uint64) uint64 {
	if !hasAVX512 {
		return findBijectionGeneric(bucket, salt)
	}
	m := uint16(len(bucket))
	fullMask := uint64(1)<<m - 1

	lanes := [8]uint64{0, 1, 2, 3, 4, 5, 6, 7}
	offsets := archsimd.LoadUint64x8Array(&lanes)
	mask48v := archsimd.BroadcastUint64x8(mask48)
	// The generic form masks the modulus to keep the shift in range; MaxLeafSize
	// holds m far below that, but mirror it so the two cannot diverge.
	modulus := archsimd.BroadcastUint64x8(uint64(m & 31))
	one := archsimd.BroadcastUint64x8(1)
	c1 := archsimd.BroadcastUint64x8(0xbf58476d1ce4e5b9)
	c2 := archsimd.BroadcastUint64x8(0x94d049bb133111eb)

	var salts, acc [saltVectors]archsimd.Uint64x8
	var out [8]uint64
	for {
		for v := range salts {
			salts[v] = archsimd.BroadcastUint64x8(salt + uint64(8*v)).Add(offsets)
			acc[v] = archsimd.Uint64x8{}
		}
		for _, key := range bucket {
			k := archsimd.BroadcastUint64x8(key)
			for v := range salts {
				z := k.Add(salts[v])
				z = z.Xor(z.ShiftAllRight(30)).Mul(c1)
				z = z.Xor(z.ShiftAllRight(27)).Mul(c2)
				z = z.Xor(z.ShiftAllRight(31))
				// remap16: ((z & mask48) * m) >> 48, then set that bit.
				acc[v] = acc[v].Or(one.ShiftLeft(z.And(mask48v).Mul(modulus).ShiftAllRight(48)))
			}
		}
		for v := range acc {
			acc[v].StoreArray(&out)
			for i, bits := range out {
				if bits == fullMask {
					return salt + uint64(8*v+i)
				}
			}
		}
		salt += 8 * saltVectors
	}
}
