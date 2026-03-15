// Package modexp implements modular exponentiation for the EVM MODEXP precompile.
//
// It uses Word-by-Word Montgomery multiplication for odd moduli and CRT
// decomposition for even moduli. The core addMulVVW function is pure Go
// for now, with assembly acceleration planned as a follow-up.
package modexp

import (
	"math/big"
	"math/bits"
)

const (
	// _W is the size in bits of a limb.
	_W = bits.UintSize
	// _S is the size in bytes of a limb.
	_S = _W / 8
)

// nat represents an arbitrary natural number as a little-endian slice of
// machine-word-sized limbs.
type nat struct {
	limbs []uint
}

// newNat returns a zero-valued nat with the given number of limbs.
func newNat(n int) *nat {
	return &nat{limbs: make([]uint, n)}
}

// natFromBytes creates a nat from big-endian bytes. Leading zeros are trimmed.
func natFromBytes(b []byte) *nat {
	n := (len(b) + _S - 1) / _S
	x := newNat(n)
	i, k := len(b), 0
	for k < n && i >= _S {
		x.limbs[k] = beUint(b[i-_S : i])
		i -= _S
		k++
	}
	for s := 0; s < _W && k < n && i > 0; s += 8 {
		x.limbs[k] |= uint(b[i-1]) << s
		i--
	}
	return x.trim()
}

// natFromBytesFixed creates a nat from big-endian bytes with exactly n limbs
// (no trimming). If b is shorter than n limbs, the upper limbs are zero.
func natFromBytesFixed(b []byte, n int) *nat {
	x := newNat(n)
	i, k := len(b), 0
	for k < n && i >= _S {
		x.limbs[k] = beUint(b[i-_S : i])
		i -= _S
		k++
	}
	for s := 0; s < _W && k < n && i > 0; s += 8 {
		x.limbs[k] |= uint(b[i-1]) << s
		i--
	}
	return x
}

// beUint reads a big-endian uint from exactly _S bytes.
func beUint(b []byte) uint {
	if _W == 64 {
		_ = b[7]
		return uint(b[7]) | uint(b[6])<<8 | uint(b[5])<<16 | uint(b[4])<<24 |
			uint(b[3])<<32 | uint(b[2])<<40 | uint(b[1])<<48 | uint(b[0])<<56
	}
	_ = b[3]
	return uint(b[3]) | uint(b[2])<<8 | uint(b[1])<<16 | uint(b[0])<<24
}

// trim removes leading zero limbs.
func (x *nat) trim() *nat {
	for i := len(x.limbs) - 1; i >= 0; i-- {
		if x.limbs[i] != 0 {
			x.limbs = x.limbs[:i+1]
			return x
		}
	}
	x.limbs = x.limbs[:0]
	return x
}

// isZero returns true if x == 0.
func (x *nat) isZero() bool {
	for _, l := range x.limbs {
		if l != 0 {
			return false
		}
	}
	return true
}

// bytes returns x as a big-endian byte slice, zero-padded to size bytes.
func (x *nat) bytes(size int) []byte {
	out := make([]byte, size)
	i := size
	for _, limb := range x.limbs {
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

// expand grows x to n limbs (zero-extending), or does nothing if already >= n.
func (x *nat) expand(n int) *nat {
	if len(x.limbs) >= n {
		return x
	}
	newLimbs := make([]uint, n)
	copy(newLimbs, x.limbs)
	x.limbs = newLimbs
	return x
}

// set copies y into x (resizing).
func (x *nat) set(y *nat) *nat {
	if cap(x.limbs) < len(y.limbs) {
		x.limbs = make([]uint, len(y.limbs))
	} else {
		x.limbs = x.limbs[:len(y.limbs)]
	}
	copy(x.limbs, y.limbs)
	return x
}

// reset sets x to zero with n limbs.
func (x *nat) reset(n int) *nat {
	if cap(x.limbs) < n {
		x.limbs = make([]uint, n)
	} else {
		x.limbs = x.limbs[:n]
		clear(x.limbs)
	}
	return x
}

// bitLen returns the bit length of x (variable time).
func (x *nat) bitLen() int {
	for i := len(x.limbs) - 1; i >= 0; i-- {
		if x.limbs[i] != 0 {
			return i*_W + bits.Len(x.limbs[i])
		}
	}
	return 0
}

// cmpGeq returns 1 if x >= y (same length), 0 otherwise.
func (x *nat) cmpGeq(y *nat) uint {
	var borrow uint
	for i := 0; i < len(x.limbs); i++ {
		_, borrow = bits.Sub(x.limbs[i], y.limbs[i], borrow)
	}
	// borrow == 0 means x >= y
	return 1 - borrow
}

// sub computes x -= y, returns borrow.
func (x *nat) sub(y *nat) uint {
	var borrow uint
	for i := 0; i < len(x.limbs); i++ {
		x.limbs[i], borrow = bits.Sub(x.limbs[i], y.limbs[i], borrow)
	}
	return borrow
}

// add computes x += y, returns carry.
func (x *nat) add(y *nat) uint {
	var carry uint
	for i := 0; i < len(x.limbs); i++ {
		x.limbs[i], carry = bits.Add(x.limbs[i], y.limbs[i], carry)
	}
	return carry
}

// modulus holds precomputed values for Montgomery multiplication.
type modulus struct {
	nat   *nat
	odd   bool
	m0inv uint // -nat.limbs[0]^{-1} mod 2^_W
	rr    *nat // R*R mod m, where R = 2^(_W * n)
}

// newModulus creates a modulus from big-endian bytes.
// Returns nil if m <= 1.
func newModulus(b []byte) *modulus {
	n := natFromBytes(b)
	if len(n.limbs) == 0 {
		return nil // m == 0
	}
	// Check if m == 1
	if len(n.limbs) == 1 && n.limbs[0] == 1 {
		return nil
	}

	m := &modulus{nat: n}
	if n.limbs[0]&1 == 1 {
		m.odd = true
		m.m0inv = minusInverseModW(n.limbs[0])
		m.rr = computeRR(m)
	}
	return m
}

// size returns the number of limbs.
func (m *modulus) size() int {
	return len(m.nat.limbs)
}

// bitLen returns the bit length of the modulus.
func (m *modulus) bitLen() int {
	return m.nat.bitLen()
}

// byteLen returns the byte length of the modulus.
func (m *modulus) byteLen() int {
	return (m.bitLen() + 7) / 8
}

// minusInverseModW computes -x^{-1} mod 2^_W with x odd.
func minusInverseModW(x uint) uint {
	// Newton's method: y = y * (2 - x*y), converges in 5 iterations for 64 bits.
	y := x
	for i := 0; i < 5; i++ {
		y = y * (2 - x*y)
	}
	return -y
}

// computeRR computes R*R mod m where R = 2^(_W * n).
// Uses big.Int for the modular reduction (one-time setup cost).
func computeRR(m *modulus) *nat {
	n := m.size()

	// Convert m to big.Int (little-endian limbs → big.Int words).
	bigM := new(big.Int)
	mWords := make([]big.Word, n)
	for i := 0; i < n; i++ {
		mWords[i] = big.Word(m.nat.limbs[i])
	}
	bigM.SetBits(mWords)

	// R = 2^(_W * n), R^2 = 2^(2 * _W * n).
	bigRR := new(big.Int).Lsh(big.NewInt(1), uint(2*_W*n))
	bigRR.Mod(bigRR, bigM)

	// Convert back to nat.
	rr := newNat(n)
	rrWords := bigRR.Bits()
	for i := 0; i < len(rrWords) && i < n; i++ {
		rr.limbs[i] = uint(rrWords[i])
	}
	return rr
}

// maybeSubtractModulus subtracts m if x >= m or if overflow is set.
// scratch must have len >= len(x.limbs); if nil, it allocates.
func (x *nat) maybeSubtractModulus(overflow uint, m *modulus, scratch []uint) {
	n := len(x.limbs)
	if len(scratch) < n {
		scratch = make([]uint, n)
	}
	copy(scratch[:n], x.limbs)
	var borrow uint
	for i := 0; i < n; i++ {
		scratch[i], borrow = bits.Sub(scratch[i], m.nat.limbs[i], borrow)
	}
	// Keep result if didn't underflow, or if overflow was set.
	keep := (1 - borrow) | overflow
	mask := 0 - keep
	for i := range x.limbs {
		x.limbs[i] = (x.limbs[i] &^ mask) | (scratch[i] & mask)
	}
}

