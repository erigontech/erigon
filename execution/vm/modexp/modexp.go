package modexp

import (
	"math/big"
	"math/bits"
)

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

	// Check if exp is all zeros (before any expensive parsing)
	if isAllZero(exp) {
		// base^0 mod m: result is 1 if m > 1, else 0
		if isModOne(modBytes) {
			return make([]byte, modLen)
		}
		out := make([]byte, modLen)
		out[modLen-1] = 1
		return out
	}

	// Check if base is all zeros
	if isAllZero(base) {
		return make([]byte, modLen)
	}

	// For small exponents, use big.Int.Exp directly (avoids Montgomery setup).
	// Also handles even moduli efficiently.
	expLen := len(stripLeadingZeros(exp))
	if expLen <= smallExpThreshold {
		return expBigInt(base, exp, modBytes)
	}

	// Parse modulus (trim leading zeros to get limb count)
	nLimbs := bytesToLimbCount(modBytes)
	if nLimbs == 0 {
		return make([]byte, modLen) // mod == 0
	}

	// Allocate limb buffer for modulus + base together to reduce allocs.
	// mLimbs uses first nLimbs slots.
	baseLimbCount := bytesToLimbCount(base)
	if baseLimbCount == 0 {
		baseLimbCount = 1
	}
	limbBuf := make([]uint, nLimbs+baseLimbCount)
	mLimbs := limbBuf[:nLimbs]
	fillLimbs(modBytes, mLimbs)

	if nLimbs == 1 && mLimbs[0] == 1 {
		return make([]byte, modLen)
	}

	// Even moduli: use big.Int.Exp
	if mLimbs[0]&1 == 0 {
		return expEven(base, exp, modBytes)
	}

	// Parse base into the second half of limbBuf
	rawBase := limbBuf[nLimbs:]
	fillLimbs(base, rawBase)

	// Odd moduli: Montgomery exponentiation with pooled workspace
	return expOddPooled(rawBase, baseLimbCount, exp, mLimbs, nLimbs, modLen)
}

// isModOne checks if mod is 0 or 1 without full parsing.
func isModOne(mod []byte) bool {
	for i := 0; i < len(mod)-1; i++ {
		if mod[i] != 0 {
			return false
		}
	}
	return len(mod) == 0 || mod[len(mod)-1] <= 1
}

// expBigInt uses math/big.Int.Exp for small exponents where Montgomery
// setup cost would dominate.
func expBigInt(base, exp, mod []byte) []byte {
	b := new(big.Int).SetBytes(base)
	e := new(big.Int).SetBytes(exp)
	m := new(big.Int).SetBytes(mod)
	if m.Sign() <= 0 {
		return make([]byte, len(mod))
	}
	result := new(big.Int).Exp(b, e, m)
	return leftPad(result.Bytes(), len(mod))
}

// expOddPooled computes base^exp mod m where m is odd, using Montgomery
// exponentiation with a single slab allocation for all working storage.
// rawBase is the base as little-endian limbs (rawBaseLen trimmed length), already parsed.
func expOddPooled(rawBase []uint, rawBaseLen int, expBytes []byte, mLimbs []uint, n, modLen int) []byte {
	// Compute Montgomery constants
	m0inv := minusInverseModW(mLimbs[0])

	// Compute R^2 mod m using big.Int (one-time setup)
	rrLimbs := computeRRInto(mLimbs, n)

	// Reduce base mod m
	baseLimbs := reduceRawLimbs(rawBase, rawBaseLen, mLimbs, n)

	// Strip leading zeros from exponent
	exp := stripLeadingZeros(expBytes)

	// Scan exponent for max nibble
	var maxNibble byte
	for _, b := range exp {
		hi := (b >> 4) & 0xf
		lo := b & 0xf
		if hi > maxNibble {
			maxNibble = hi
		}
		if lo > maxNibble {
			maxNibble = lo
		}
	}
	tableLen := int(maxNibble)
	if tableLen == 0 {
		tableLen = 1
	}

	// Single slab allocation: T(2n) + one(n) + out(n) + table(tableLen*n) + tmp(n) + rr(n) + base(n)
	totalLimbs := (6 + tableLen) * n
	slab := make([]uint, totalLimbs)

	T := slab[:2*n]
	one := slab[2*n : 3*n]
	one[0] = 1
	outLimbs := slab[3*n : 4*n]
	tableBase := 4 * n
	tmp := slab[(4+tableLen)*n : (5+tableLen)*n]

	// Copy rr and base into workspace
	rr := slab[(5+tableLen)*n : (6+tableLen)*n]
	copy(rr, rrLimbs)
	base := baseLimbs // already n limbs

	// Build table[i] = base^(i+1) * R mod m
	t0 := slab[tableBase : tableBase+n]
	copy(t0, base)
	montgomeryMul(t0, t0, rr, mLimbs, m0inv, T)

	for i := 1; i < tableLen; i++ {
		prev := slab[tableBase+(i-1)*n : tableBase+i*n]
		cur := slab[tableBase+i*n : tableBase+(i+1)*n]
		montgomeryMul(cur, prev, t0, mLimbs, m0inv, T)
	}

	// Windowed exponentiation
	started := false
	for _, b := range exp {
		for _, j := range [2]int{4, 0} {
			k := (b >> j) & 0xf
			if !started {
				if k == 0 {
					continue
				}
				entry := slab[tableBase+int(k-1)*n : tableBase+int(k)*n]
				copy(outLimbs, entry)
				started = true
				continue
			}
			montgomeryMul(outLimbs, outLimbs, outLimbs, mLimbs, m0inv, T)
			montgomeryMul(outLimbs, outLimbs, outLimbs, mLimbs, m0inv, T)
			montgomeryMul(outLimbs, outLimbs, outLimbs, mLimbs, m0inv, T)
			montgomeryMul(outLimbs, outLimbs, outLimbs, mLimbs, m0inv, T)

			if k != 0 {
				entry := slab[tableBase+int(k-1)*n : tableBase+int(k)*n]
				montgomeryMul(tmp, outLimbs, entry, mLimbs, m0inv, T)
				copy(outLimbs, tmp)
			}
		}
	}

	if !started {
		outLimbs[0] = 1
		montgomeryMul(outLimbs, outLimbs, rr, mLimbs, m0inv, T)
	}

	// Montgomery reduction
	montgomeryMul(outLimbs, outLimbs, one, mLimbs, m0inv, T)

	// Convert to bytes
	return limbsToBytes(outLimbs, n, modLen)
}

// bytesToLimbCount returns the number of limbs needed for big-endian bytes,
// after stripping leading zeros.
func bytesToLimbCount(b []byte) int {
	start := 0
	for start < len(b) && b[start] == 0 {
		start++
	}
	if start == len(b) {
		return 0
	}
	effectiveLen := len(b) - start
	return (effectiveLen + _S - 1) / _S
}

// fillLimbs converts big-endian bytes into pre-allocated little-endian limbs.
func fillLimbs(b []byte, limbs []uint) {
	n := len(limbs)
	clear(limbs)
	i, k := len(b), 0
	for k < n && i >= _S {
		limbs[k] = beUint(b[i-_S : i])
		i -= _S
		k++
	}
	for s := 0; s < _W && k < n && i > 0; s += 8 {
		limbs[k] |= uint(b[i-1]) << s
		i--
	}
}

// reduceRawLimbs reduces rawBase (rawBaseLen trimmed limbs) mod m, returning exactly n limbs.
func reduceRawLimbs(rawBase []uint, rawBaseLen int, mLimbs []uint, n int) []uint {
	// Trim trailing zero limbs
	for rawBaseLen > 0 && rawBase[rawBaseLen-1] == 0 {
		rawBaseLen--
	}

	if rawBaseLen <= n {
		// Fits in n limbs — expand and check
		result := make([]uint, n)
		copy(result, rawBase[:rawBaseLen])
		if cmpGeqLimbs(result, mLimbs, n) {
			return reduceLimbs(result, rawBaseLen, mLimbs, n)
		}
		return result
	}
	// More limbs than m: need full reduction
	return reduceLimbs(rawBase[:rawBaseLen], rawBaseLen, mLimbs, n)
}

// cmpGeqLimbs returns true if a >= b (both n limbs).
func cmpGeqLimbs(a, b []uint, n int) bool {
	var borrow uint
	for i := 0; i < n; i++ {
		_, borrow = bits.Sub(a[i], b[i], borrow)
	}
	return borrow == 0
}

// reduceLimbs reduces x mod m via shift-and-subtract.
func reduceLimbs(x []uint, xLen int, mLimbs []uint, n int) []uint {
	mBits := limbsBitLen(mLimbs, n)

	for xLen > 0 && x[xLen-1] == 0 {
		xLen--
	}
	if xLen == 0 {
		return make([]uint, n)
	}
	xBits := (xLen-1)*_W + bits.Len(x[xLen-1])

	shift := xBits - mBits
	if shift < 0 {
		result := make([]uint, n)
		copy(result, x[:min(xLen, n)])
		return result
	}

	rem := make([]uint, xLen)
	copy(rem, x[:xLen])

	shifted := make([]uint, xLen)
	shiftLeftLimbs(shifted, mLimbs, n, shift, xLen)

	for s := shift; s >= 0; s-- {
		if cmpGeqLimbsN(rem, shifted, xLen) {
			subLimbs(rem, shifted, xLen)
		}
		if s > 0 {
			rshift1Limbs(shifted, xLen)
		}
	}

	result := make([]uint, n)
	copy(result, rem[:min(xLen, n)])
	return result
}

func limbsBitLen(limbs []uint, n int) int {
	for i := n - 1; i >= 0; i-- {
		if limbs[i] != 0 {
			return i*_W + bits.Len(limbs[i])
		}
	}
	return 0
}

func shiftLeftLimbs(dst, m []uint, mLen, shift, dstLen int) {
	wordShift := shift / _W
	bitShift := uint(shift % _W)
	clear(dst[:dstLen])
	for i := 0; i < mLen; i++ {
		j := i + wordShift
		if j < dstLen {
			dst[j] |= m[i] << bitShift
		}
		if bitShift > 0 && j+1 < dstLen {
			dst[j+1] |= m[i] >> (_W - bitShift)
		}
	}
}

func cmpGeqLimbsN(a, b []uint, n int) bool {
	for i := n - 1; i >= 0; i-- {
		if a[i] > b[i] {
			return true
		}
		if a[i] < b[i] {
			return false
		}
	}
	return true
}

func subLimbs(x, y []uint, n int) {
	var borrow uint
	for i := 0; i < n; i++ {
		x[i], borrow = bits.Sub(x[i], y[i], borrow)
	}
}

func rshift1Limbs(x []uint, n int) {
	for i := 0; i < n-1; i++ {
		x[i] = (x[i] >> 1) | (x[i+1] << (_W - 1))
	}
	x[n-1] >>= 1
}

// computeRRInto computes R^2 mod m and returns the result as n limbs.
// Uses big.Int for the modular reduction (one-time setup cost per call).
func computeRRInto(mLimbs []uint, n int) []uint {
	bigM := bigIntPool.Get().(*big.Int)
	bigRR := bigIntPool.Get().(*big.Int)

	mWords := make([]big.Word, n)
	for i := 0; i < n; i++ {
		mWords[i] = big.Word(mLimbs[i])
	}
	bigM.SetBits(mWords)

	bigRR.Lsh(bigRR.SetInt64(1), uint(2*_W*n))
	bigRR.Mod(bigRR, bigM)

	rr := make([]uint, n)
	rrWords := bigRR.Bits()
	for i := 0; i < len(rrWords) && i < n; i++ {
		rr[i] = uint(rrWords[i])
	}

	bigIntPool.Put(bigM)
	bigIntPool.Put(bigRR)
	return rr
}

// limbsToBytes converts little-endian limbs to big-endian bytes, zero-padded to size.
func limbsToBytes(limbs []uint, n, size int) []byte {
	out := make([]byte, size)
	i := size
	for k := 0; k < n; k++ {
		limb := limbs[k]
		for j := 0; j < _S; j++ {
			i--
			if i < 0 {
				break
			}
			out[i] = byte(limb)
			limb >>= 8
		}
	}
	return out
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
