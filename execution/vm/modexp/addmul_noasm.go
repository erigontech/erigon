//go:build !amd64

package modexp

import "math/bits"

// addMulVVW multiplies the multi-word value x by the single-word value y,
// adding the result to the multi-word value z and returning the final carry.
// This is the pure Go fallback for non-amd64 platforms.
func addMulVVW(z, x []uint, y uint) (carry uint) {
	_ = x[len(z)-1] // bounds check elimination hint
	for i := range z {
		hi, lo := bits.Mul(x[i], y)
		lo, c := bits.Add(lo, z[i], 0)
		hi, _ = bits.Add(hi, 0, c)
		lo, c = bits.Add(lo, carry, 0)
		hi, _ = bits.Add(hi, 0, c)
		carry = hi
		z[i] = lo
	}
	return carry
}
