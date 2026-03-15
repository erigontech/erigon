package modexp

import "math/big"

// smallExpThreshold is the max exponent byte length for which we use
// big.Int.Exp directly instead of our Montgomery path. For small exponents,
// big.Int.Exp is faster because Montgomery setup (computeRR, table building)
// dominates the cost.
const smallExpThreshold = 8

// Exp computes base^exp mod mod and returns the result as a byte slice
// of length equal to len(modBytes). Edge cases per EIP-198:
//   - mod == 0 → zero-length result (caller should handle)
//   - mod == 1 → zero bytes of length 1
//   - exp == 0 → 1 mod mod
//   - base == 0 → 0
func Exp(base, exp, modBytes []byte) []byte {
	modLen := len(modBytes)
	if modLen == 0 {
		return nil
	}

	// Check if mod is zero or one
	mod := newModulus(modBytes)
	if mod == nil {
		// mod <= 1
		return make([]byte, modLen)
	}

	// Check if exp is all zeros
	if isAllZero(exp) {
		// base^0 = 1 mod m (for m > 1)
		out := make([]byte, modLen)
		out[modLen-1] = 1
		return out
	}

	// Check if base is all zeros
	if isAllZero(base) {
		return make([]byte, modLen)
	}

	// For small exponents, big.Int.Exp is faster (avoids Montgomery setup cost).
	expLen := len(stripLeadingZeros(exp))
	if expLen <= smallExpThreshold {
		return expBigInt(base, exp, modBytes)
	}

	// Route based on odd/even modulus
	if mod.odd {
		return expOdd(base, exp, mod, modLen)
	}
	return expEven(base, exp, modBytes)
}

// expBigInt uses math/big.Int.Exp for small exponents where Montgomery
// setup cost would dominate.
func expBigInt(base, exp, mod []byte) []byte {
	b := new(big.Int).SetBytes(base)
	e := new(big.Int).SetBytes(exp)
	m := new(big.Int).SetBytes(mod)
	result := new(big.Int).Exp(b, e, m)
	return leftPad(result.Bytes(), len(mod))
}

// expOdd computes base^exp mod m where m is odd, using Montgomery exponentiation.
func expOdd(baseBytes, expBytes []byte, m *modulus, modLen int) []byte {
	n := m.size()

	// Parse base at full width, then reduce mod m
	baseFull := natFromBytes(baseBytes)
	b := reduceNat(baseFull, m)

	// Strip leading zero bytes from exp for efficiency
	exp := stripLeadingZeros(expBytes)

	out := newNat(n)
	out.exp(b, exp, m)
	return out.bytes(modLen)
}

// reduceNat reduces x mod m, returning a nat with exactly m.size() limbs.
func reduceNat(x *nat, m *modulus) *nat {
	n := m.size()
	// If x fits in n limbs and x < m, just expand/copy
	if len(x.limbs) <= n {
		result := newNat(n)
		copy(result.limbs, x.limbs)
		if result.cmpGeq(m.nat) == 1 {
			return modReduce(result, m)
		}
		return result
	}
	// x has more limbs than m: need full reduction
	x.expand(max(len(x.limbs), n))
	return modReduce(x, m)
}

func isAllZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func stripLeadingZeros(b []byte) []byte {
	for i, v := range b {
		if v != 0 {
			return b[i:]
		}
	}
	return b[len(b)-1:] // keep at least one byte
}
