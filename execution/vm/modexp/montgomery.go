package modexp

import "math/bits"

// montgomeryMul computes out = a * b / R mod m (Montgomery multiplication).
// All slice parameters are n limbs, T is scratch of 2*n limbs.
// T[:n] is zeroed internally; T[n:2n] is overwritten.
func montgomeryMul(out, a, b, mLimbs []uint, m0inv uint, T []uint) {
	n := len(mLimbs)

	// Only clear the first n positions (used by first addMulVVW).
	// T[n:2n] is set position-by-position via bits.Add below.
	clear(T[:n])

	var c uint
	for i := 0; i < n; i++ {
		c1 := addMulVVW(T[i:n+i], a, b[i])
		c2 := addMulVVW(T[i:n+i], mLimbs, T[i]*m0inv)
		T[n+i], c = bits.Add(c1, c2, c)
	}

	copy(out, T[n:n*2])

	// maybeSubtractModulus: if c != 0 or out >= m, subtract m
	scratch := T[:n]
	copy(scratch, out)
	var borrow uint
	for i := 0; i < n; i++ {
		scratch[i], borrow = bits.Sub(scratch[i], mLimbs[i], borrow)
	}
	keep := (1 - borrow) | c
	mask := 0 - keep
	for i := range out[:n] {
		out[i] = (out[i] &^ mask) | (scratch[i] & mask)
	}
}
