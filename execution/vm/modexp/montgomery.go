package modexp

import "math/bits"

// montgomeryMul computes x = a * b / R mod m (Montgomery multiplication).
// T is a scratch buffer of length >= 2*n that is zeroed by the caller.
// All inputs must have the same number of limbs as m.
func (x *nat) montgomeryMul(a, b *nat, m *modulus, T []uint) *nat {
	n := m.size()
	aLimbs := a.limbs[:n]
	bLimbs := b.limbs[:n]
	mLimbs := m.nat.limbs[:n]

	// Only clear the first n positions (used by first addMulVVW).
	// T[n:2n] is set position-by-position via bits.Add below.
	clear(T[:n])

	var c uint
	for i := 0; i < n; i++ {
		c1 := addMulVVW(T[i:n+i], aLimbs, bLimbs[i])
		c2 := addMulVVW(T[i:n+i], mLimbs, T[i]*m.m0inv)
		T[n+i], c = bits.Add(c1, c2, c)
	}

	xLimbs := x.limbs[:n]
	copy(xLimbs, T[n:n*2])
	x.maybeSubtractModulus(c, m, T[:n])
	return x
}

// montgomeryRepresentation computes x = x * R mod m.
func (x *nat) montgomeryRepresentation(m *modulus, T []uint) *nat {
	return x.montgomeryMul(x, m.rr, m, T)
}

// montgomeryReduction computes x = x / R mod m.
func (x *nat) montgomeryReduction(m *modulus, T []uint, one *nat) *nat {
	return x.montgomeryMul(x, one, m, T)
}

// exp computes out = base^exp mod m using 4-bit windowed Montgomery exponentiation.
// m must be odd. base must be reduced modulo m with the same number of limbs.
// exp is big-endian bytes.
func (out *nat) exp(base *nat, e []byte, m *modulus) *nat {
	n := m.size()

	// Scan exponent to find max nibble (for lazy table building)
	// and first non-zero nibble position (to skip leading zeros).
	var maxNibble byte
	for _, b := range e {
		hi := (b >> 4) & 0xf
		lo := b & 0xf
		if hi > maxNibble {
			maxNibble = hi
		}
		if lo > maxNibble {
			maxNibble = lo
		}
	}
	tableLen := int(maxNibble) // table[0..maxNibble-1]
	if tableLen == 0 {
		tableLen = 1 // always need at least table[0] for montgomeryRepresentation
	}

	// Single allocation for all working storage:
	// T (2n) + one (n) + table (tableLen*n) + tmp (n)
	slabSize := (3 + tableLen + 1) * n
	slab := make([]uint, slabSize)
	T := slab[:2*n]
	oneLimbs := slab[2*n : 3*n]
	oneLimbs[0] = 1
	one := &nat{limbs: oneLimbs}

	// Build table[i] = base^(i+1) * R mod m, only up to maxNibble
	var table [15]*nat
	for i := 0; i < tableLen; i++ {
		off := (3 + i) * n
		table[i] = &nat{limbs: slab[off : off+n]}
	}
	table[0].set(base).montgomeryRepresentation(m, T)
	for i := 1; i < tableLen; i++ {
		table[i].montgomeryMul(table[i-1], table[0], m, T)
	}

	tmp := &nat{limbs: slab[(3+tableLen)*n : (3+tableLen+1)*n]}

	// Find first non-zero nibble to seed out, skipping leading zero squarings.
	started := false
	out.reset(n)
	for _, b := range e {
		for _, j := range [2]int{4, 0} {
			k := (b >> j) & 0xf
			if !started {
				if k == 0 {
					continue
				}
				// First non-zero nibble: seed out directly from table
				copy(out.limbs, table[k-1].limbs)
				started = true
				continue
			}
			// Square four times
			out.montgomeryMul(out, out, m, T)
			out.montgomeryMul(out, out, m, T)
			out.montgomeryMul(out, out, m, T)
			out.montgomeryMul(out, out, m, T)

			// Multiply by table[k-1] if k != 0
			if k != 0 {
				tmp.montgomeryMul(out, table[k-1], m, T)
				copy(out.limbs, tmp.limbs)
			}
		}
	}

	if !started {
		// exp == 0: result is 1 mod m
		out.limbs[0] = 1
		out.montgomeryRepresentation(m, T)
	}

	return out.montgomeryReduction(m, T, one)
}

// modReduce computes x mod m via shift-and-subtract division.
// x can be up to 2*n limbs.
func modReduce(x *nat, m *modulus) *nat {
	n := m.size()
	mBits := m.bitLen()

	xLen := len(x.limbs)
	for xLen > 0 && x.limbs[xLen-1] == 0 {
		xLen--
	}
	xBits := 0
	if xLen > 0 {
		xBits = (xLen-1)*_W + bits.Len(x.limbs[xLen-1])
	}

	if xBits == 0 {
		return newNat(n)
	}

	shift := xBits - mBits
	if shift < 0 {
		result := newNat(n)
		copy(result.limbs, x.limbs[:min(xLen, n)])
		return result
	}

	rem := &nat{limbs: make([]uint, xLen)}
	copy(rem.limbs, x.limbs[:xLen])

	shifted := &nat{limbs: make([]uint, xLen)}
	shiftLeft(shifted, m.nat, shift, xLen)

	for s := shift; s >= 0; s-- {
		if rem.cmpGeqLen(shifted, xLen) == 1 {
			subLen(rem, shifted, xLen)
		}
		if s > 0 {
			rshift1Full(shifted, xLen)
		}
	}

	result := newNat(n)
	copy(result.limbs, rem.limbs[:min(xLen, n)])
	return result
}

func shiftLeft(dst, m *nat, shift int, dstLen int) {
	wordShift := shift / _W
	bitShift := uint(shift % _W)

	clear(dst.limbs[:dstLen])
	for i := 0; i < len(m.limbs); i++ {
		j := i + wordShift
		if j < dstLen {
			dst.limbs[j] |= m.limbs[i] << bitShift
		}
		if bitShift > 0 && j+1 < dstLen {
			dst.limbs[j+1] |= m.limbs[i] >> (_W - bitShift)
		}
	}
}

func rshift1Full(x *nat, n int) {
	for i := 0; i < n-1; i++ {
		x.limbs[i] = (x.limbs[i] >> 1) | (x.limbs[i+1] << (_W - 1))
	}
	x.limbs[n-1] >>= 1
}

func (x *nat) cmpGeqLen(y *nat, n int) uint {
	for i := n - 1; i >= 0; i-- {
		if x.limbs[i] > y.limbs[i] {
			return 1
		}
		if x.limbs[i] < y.limbs[i] {
			return 0
		}
	}
	return 1
}

func subLen(x, y *nat, n int) {
	var borrow uint
	for i := 0; i < n; i++ {
		x.limbs[i], borrow = bits.Sub(x.limbs[i], y.limbs[i], borrow)
	}
}
