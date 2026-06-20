//go:build goexperiment.simd && amd64

package recsplit

import (
	"math/bits"

	"simd/archsimd"
)

// useSIMD requires AVX512 because the 8-lane uint64 multiply (VPMULLQ) used by
// remixVec is only available with AVX512DQ, which archsimd.X86.AVX512 includes.
var useSIMD = archsimd.X86.AVX512()

var (
	simdLanes  = [8]uint64{0, 1, 2, 3, 4, 5, 6, 7}
	simdLanes8 = [8]uint64{8, 9, 10, 11, 12, 13, 14, 15}
)

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

// findBijectionSIMD dispatches to a 16-salt m=8 fast path or the general 8-salt path.
func findBijectionSIMD(bucket []uint64, salt uint64) uint64 {
	if uint16(len(bucket)) == 8 {
		return findBijectionSIMD8(bucket, salt)
	}
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

// findBijectionSIMD8 specializes for m=8 leaf buckets: it tests 16 salt candidates
// per iteration via two Uint64x8 vectors, replaces the remap16 VPMULLQ with z>>45&7
// (valid because m=8 is a power of two), and unrolls the m=8 inner loop so the 8 remix
// chains stay independent and the Or-accumulation forms a latency-minimizing tree.
func findBijectionSIMD8(bucket []uint64, salt uint64) uint64 {
	full := archsimd.BroadcastUint64x8(0xFF)
	lanes0 := archsimd.LoadUint64x8(&simdLanes)
	lanes1 := archsimd.LoadUint64x8(&simdLanes8)
	c1 := archsimd.BroadcastUint64x8(0xbf58476d1ce4e5b9)
	c2 := archsimd.BroadcastUint64x8(0x94d049bb133111eb)
	seven := archsimd.BroadcastUint64x8(7)
	one := archsimd.BroadcastUint64x8(1)

	bk0 := archsimd.BroadcastUint64x8(bucket[0])
	bk1 := archsimd.BroadcastUint64x8(bucket[1])
	bk2 := archsimd.BroadcastUint64x8(bucket[2])
	bk3 := archsimd.BroadcastUint64x8(bucket[3])
	bk4 := archsimd.BroadcastUint64x8(bucket[4])
	bk5 := archsimd.BroadcastUint64x8(bucket[5])
	bk6 := archsimd.BroadcastUint64x8(bucket[6])
	bk7 := archsimd.BroadcastUint64x8(bucket[7])

	for {
		base := archsimd.BroadcastUint64x8(salt)
		sv0 := base.Add(lanes0)
		sv1 := base.Add(lanes1)

		z0_0 := remixVec(bk0.Add(sv0), c1, c2)
		z0_1 := remixVec(bk1.Add(sv0), c1, c2)
		z0_2 := remixVec(bk2.Add(sv0), c1, c2)
		z0_3 := remixVec(bk3.Add(sv0), c1, c2)
		z0_4 := remixVec(bk4.Add(sv0), c1, c2)
		z0_5 := remixVec(bk5.Add(sv0), c1, c2)
		z0_6 := remixVec(bk6.Add(sv0), c1, c2)
		z0_7 := remixVec(bk7.Add(sv0), c1, c2)
		z1_0 := remixVec(bk0.Add(sv1), c1, c2)
		z1_1 := remixVec(bk1.Add(sv1), c1, c2)
		z1_2 := remixVec(bk2.Add(sv1), c1, c2)
		z1_3 := remixVec(bk3.Add(sv1), c1, c2)
		z1_4 := remixVec(bk4.Add(sv1), c1, c2)
		z1_5 := remixVec(bk5.Add(sv1), c1, c2)
		z1_6 := remixVec(bk6.Add(sv1), c1, c2)
		z1_7 := remixVec(bk7.Add(sv1), c1, c2)

		pos := func(z archsimd.Uint64x8) archsimd.Uint64x8 { // remap16(z, 8) = z>>45 & 7
			return z.ShiftAllRight(45).And(seven)
		}

		m0_0 := one.ShiftLeft(pos(z0_0))
		m0_1 := one.ShiftLeft(pos(z0_1))
		m0_2 := one.ShiftLeft(pos(z0_2))
		m0_3 := one.ShiftLeft(pos(z0_3))
		m0_4 := one.ShiftLeft(pos(z0_4))
		m0_5 := one.ShiftLeft(pos(z0_5))
		m0_6 := one.ShiftLeft(pos(z0_6))
		m0_7 := one.ShiftLeft(pos(z0_7))
		m1_0 := one.ShiftLeft(pos(z1_0))
		m1_1 := one.ShiftLeft(pos(z1_1))
		m1_2 := one.ShiftLeft(pos(z1_2))
		m1_3 := one.ShiftLeft(pos(z1_3))
		m1_4 := one.ShiftLeft(pos(z1_4))
		m1_5 := one.ShiftLeft(pos(z1_5))
		m1_6 := one.ShiftLeft(pos(z1_6))
		m1_7 := one.ShiftLeft(pos(z1_7))

		mv0 := m0_0.Or(m0_1).Or(m0_2.Or(m0_3)).Or(m0_4.Or(m0_5).Or(m0_6.Or(m0_7)))
		mv1 := m1_0.Or(m1_1).Or(m1_2.Or(m1_3)).Or(m1_4.Or(m1_5).Or(m1_6.Or(m1_7)))

		if g := mv0.Equal(full).ToBits(); g != 0 {
			return salt + uint64(bits.TrailingZeros8(g))
		}
		if g := mv1.Equal(full).ToBits(); g != 0 {
			return salt + 8 + uint64(bits.TrailingZeros8(g))
		}
		salt += 16
	}
}

// findSplitSIMD tests 8 salt candidates at once. Keys are processed 8-at-a-time per
// salt; per-partition counts come from Equal+ToBits+OnesCount on the lane mask instead
// of a Store+scatter. partition(z) = remap16(z,m)/unit = (z & mask48) * m >> (48+log2(unit)),
// so when unit (and, on the fast path, m) is a power of two the divide and the Mul(m)
// collapse into shifts. The scalar findSplit handles the non-power-of-two case.
func findSplitSIMD(bucket []uint64, salt uint64, fanout, unit uint16, count []uint16) uint64 {
	var cmpVec [16]archsimd.Uint64x8
	if unit&(unit-1) != 0 || int(fanout) > len(cmpVec) {
		return findSplit(bucket, salt, fanout, unit, count)
	}
	m := uint16(len(bucket))
	c1 := archsimd.BroadcastUint64x8(0xbf58476d1ce4e5b9)
	c2 := archsimd.BroadcastUint64x8(0x94d049bb133111eb)

	unitLog2 := uint64(bits.TrailingZeros16(unit))
	for p := uint16(0); p < fanout; p++ {
		cmpVec[p] = archsimd.BroadcastUint64x8(uint64(p))
	}

	counts := count[:8*fanout]
	batchEnd := m &^ 7

	checkAndReturn := func(s0 uint64) (uint64, bool) {
		for s := uint16(0); s < 8; s++ {
			sc := counts[s*fanout:]
			var bad uint16
			for p := uint16(0); p < fanout-1; p++ {
				bad |= sc[p] ^ unit
			}
			if bad == 0 {
				return s0 + uint64(s), true
			}
		}
		return 0, false
	}

	scalarTail := func(s0 uint64) {
		for i := batchEnd; i < m; i++ {
			for s := uint16(0); s < 8; s++ {
				z := remix(bucket[i] + s0 + uint64(s))
				counts[s*fanout+remap16(z, m)/unit]++
			}
		}
	}

	if m&(m-1) == 0 && fanout&(fanout-1) == 0 { // m and fanout powers of two: shift only, no VPMULLQ
		mLog2 := uint64(bits.TrailingZeros16(m))
		partShift := 48 + unitLog2 - mLog2
		partMask := archsimd.BroadcastUint64x8(uint64(fanout - 1))
		for {
			clear(counts)
			for b := uint16(0); b < batchEnd; b += 8 {
				keyVec := archsimd.LoadUint64x8((*[8]uint64)(bucket[b:]))
				for s := uint16(0); s < 8; s++ {
					z := remixVec(keyVec.Add(archsimd.BroadcastUint64x8(salt+uint64(s))), c1, c2)
					pv := z.ShiftAllRight(partShift).And(partMask)
					sc := counts[s*fanout:]
					for p := uint16(0); p < fanout; p++ {
						sc[p] += uint16(bits.OnesCount8(pv.Equal(cmpVec[p]).ToBits()))
					}
				}
			}
			scalarTail(salt)
			if v, ok := checkAndReturn(salt); ok {
				return v
			}
			salt += 8
		}
	}

	mask48v := archsimd.BroadcastUint64x8(mask48)
	mVec := archsimd.BroadcastUint64x8(uint64(m))
	totalShift := 48 + unitLog2
	for {
		clear(counts)
		for b := uint16(0); b < batchEnd; b += 8 {
			keyVec := archsimd.LoadUint64x8((*[8]uint64)(bucket[b:]))
			for s := uint16(0); s < 8; s++ {
				z := remixVec(keyVec.Add(archsimd.BroadcastUint64x8(salt+uint64(s))), c1, c2)
				pv := z.And(mask48v).Mul(mVec).ShiftAllRight(totalShift)
				sc := counts[s*fanout:]
				for p := uint16(0); p < fanout; p++ {
					sc[p] += uint16(bits.OnesCount8(pv.Equal(cmpVec[p]).ToBits()))
				}
			}
		}
		scalarTail(salt)
		if v, ok := checkAndReturn(salt); ok {
			return v
		}
		salt += 8
	}
}
