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

// findBijection is findBijectionGeneric with the eight salt candidates held in
// one 512-bit register. AVX2 cannot host it: the 64x64 multiply both splitmix64
// and remap16 need is VPMULLQ (AVX512DQ), and emulating it costs more than the
// unrolled scalar form saves.
func findBijection(bucket []uint64, salt uint64) uint64 {
	if !hasAVX512 {
		return findBijectionGeneric(bucket, salt)
	}
	m := uint16(len(bucket))
	fullMask := uint64(1)<<m - 1

	lanes := [8]uint64{0, 1, 2, 3, 4, 5, 6, 7}
	offsets := archsimd.LoadUint64x8Array(&lanes)
	mask48v := archsimd.BroadcastUint64x8(mask48)
	modulus := archsimd.BroadcastUint64x8(uint64(m))
	one := archsimd.BroadcastUint64x8(1)
	c1 := archsimd.BroadcastUint64x8(0xbf58476d1ce4e5b9)
	c2 := archsimd.BroadcastUint64x8(0x94d049bb133111eb)

	var out [8]uint64
	for {
		salts := archsimd.BroadcastUint64x8(salt).Add(offsets)
		acc := archsimd.BroadcastUint64x8(0)
		for _, key := range bucket {
			z := archsimd.BroadcastUint64x8(key).Add(salts)
			z = z.Xor(z.ShiftAllRight(30)).Mul(c1)
			z = z.Xor(z.ShiftAllRight(27)).Mul(c2)
			z = z.Xor(z.ShiftAllRight(31))
			// remap16: ((z & mask48) * m) >> 48, then set that bit.
			acc = acc.Or(one.ShiftLeft(z.And(mask48v).Mul(modulus).ShiftAllRight(48)))
		}
		acc.StoreArray(&out)
		for i, bits := range out {
			if bits == fullMask {
				return salt + uint64(i)
			}
		}
		salt += 8
	}
}
