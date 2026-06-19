//go:build goexperiment.simd && amd64

package recsplit

import (
	"math/bits"

	"simd/archsimd"
)

// useSIMD requires AVX512 because the 8-lane uint64 multiply (VPMULLQ) used by
// remixVec is only available with AVX512DQ, which archsimd.X86.AVX512 includes.
var useSIMD = archsimd.X86.AVX512()

var simdLanes = [8]uint64{0, 1, 2, 3, 4, 5, 6, 7}

func findSplitVec(bucket []uint64, salt uint64, fanout, unit uint16, count []uint16) uint64 {
	if useSIMD {
		return findSplitSIMD(bucket, salt, fanout, unit, count)
	}
	return findSplit(bucket, salt, fanout, unit, count)
}

func findBijectionVec(bucket []uint64, salt uint64) uint64 {
	if useSIMD {
		return findBijectionSIMD(bucket, salt)
	}
	return findBijection(bucket, salt)
}

// remixVec is the lane-wise vector form of remix.
func remixVec(z, c1, c2 archsimd.Uint64x8) archsimd.Uint64x8 {
	z = z.Xor(z.ShiftAllRight(30)).Mul(c1)
	z = z.Xor(z.ShiftAllRight(27)).Mul(c2)
	return z.Xor(z.ShiftAllRight(31))
}

// findBijectionSIMD vectorizes the 8-way salt parallelism of findBijection across
// the 8 lanes of a uint64 vector: each lane carries salt+laneIndex and accumulates
// its own position bitmask.
func findBijectionSIMD(bucket []uint64, salt uint64) uint64 {
	m := uint16(len(bucket))
	full := archsimd.BroadcastUint64x8(uint64(1)<<m - 1)
	lanes := archsimd.LoadUint64x8(&simdLanes)
	mask48v := archsimd.BroadcastUint64x8(mask48)
	mVec := archsimd.BroadcastUint64x8(uint64(m))
	c1 := archsimd.BroadcastUint64x8(0xbf58476d1ce4e5b9)
	c2 := archsimd.BroadcastUint64x8(0x94d049bb133111eb)
	one := archsimd.BroadcastUint64x8(1)
	for {
		saltVec := archsimd.BroadcastUint64x8(salt).Add(lanes)
		maskVec := archsimd.BroadcastUint64x8(0)
		for i := uint16(0); i < m; i++ {
			z := remixVec(archsimd.BroadcastUint64x8(bucket[i]).Add(saltVec), c1, c2)
			pos := z.And(mask48v).Mul(mVec).ShiftAllRight(48) // remap16 -> [0,m)
			maskVec = maskVec.Or(one.ShiftLeft(pos))
		}
		if good := maskVec.Equal(full).ToBits(); good != 0 {
			return salt + uint64(bits.TrailingZeros8(good))
		}
		salt += 8
	}
}

// findSplitSIMD vectorizes the remix+remap hashing of findSplit (the multiply-heavy
// part) across 8 salt lanes; the per-partition count scatter stays scalar.
func findSplitSIMD(bucket []uint64, salt uint64, fanout, unit uint16, count []uint16) uint64 {
	m := uint16(len(bucket))
	c0 := count[0*fanout : 1*fanout : 1*fanout]
	c1c := count[1*fanout : 2*fanout : 2*fanout]
	c2c := count[2*fanout : 3*fanout : 3*fanout]
	c3 := count[3*fanout : 4*fanout : 4*fanout]
	c4 := count[4*fanout : 5*fanout : 5*fanout]
	c5 := count[5*fanout : 6*fanout : 6*fanout]
	c6 := count[6*fanout : 7*fanout : 7*fanout]
	c7 := count[7*fanout : 8*fanout : 8*fanout]
	lanes := archsimd.LoadUint64x8(&simdLanes)
	mask48v := archsimd.BroadcastUint64x8(mask48)
	mVec := archsimd.BroadcastUint64x8(uint64(m))
	c1 := archsimd.BroadcastUint64x8(0xbf58476d1ce4e5b9)
	c2 := archsimd.BroadcastUint64x8(0x94d049bb133111eb)
	var part [8]uint64
	for {
		clear(count[:8*fanout])
		saltVec := archsimd.BroadcastUint64x8(salt).Add(lanes)
		for i := uint16(0); i < m; i++ {
			z := remixVec(archsimd.BroadcastUint64x8(bucket[i]).Add(saltVec), c1, c2)
			z.And(mask48v).Mul(mVec).ShiftAllRight(48).Store(&part) // remap16 -> [0,m)
			c0[uint16(part[0])/unit]++
			c1c[uint16(part[1])/unit]++
			c2c[uint16(part[2])/unit]++
			c3[uint16(part[3])/unit]++
			c4[uint16(part[4])/unit]++
			c5[uint16(part[5])/unit]++
			c6[uint16(part[6])/unit]++
			c7[uint16(part[7])/unit]++
		}
		var bad0, bad1, bad2, bad3, bad4, bad5, bad6, bad7 uint16
		for i := uint16(0); i < fanout-1; i++ {
			bad0 |= c0[i] ^ unit
			bad1 |= c1c[i] ^ unit
			bad2 |= c2c[i] ^ unit
			bad3 |= c3[i] ^ unit
			bad4 |= c4[i] ^ unit
			bad5 |= c5[i] ^ unit
			bad6 |= c6[i] ^ unit
			bad7 |= c7[i] ^ unit
		}
		switch {
		case bad0 == 0:
			return salt
		case bad1 == 0:
			return salt + 1
		case bad2 == 0:
			return salt + 2
		case bad3 == 0:
			return salt + 3
		case bad4 == 0:
			return salt + 4
		case bad5 == 0:
			return salt + 5
		case bad6 == 0:
			return salt + 6
		case bad7 == 0:
			return salt + 7
		}
		salt += 8
	}
}
