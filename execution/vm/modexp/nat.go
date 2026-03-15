// Package modexp implements modular exponentiation for the EVM MODEXP precompile.
//
// It uses Word-by-Word Montgomery multiplication for odd moduli and
// big.Int.Exp for even moduli. The core addMulVVW function uses ADX/BMI2
// assembly on amd64, with a pure Go fallback on other platforms.
package modexp

import (
	"math/big"
	"math/bits"
	"sync"
)

const (
	// _W is the size in bits of a limb.
	_W = bits.UintSize
	// _S is the size in bytes of a limb.
	_S = _W / 8
)

// nat represents an arbitrary natural number as a little-endian slice of
// machine-word-sized limbs. Used by unit tests and the natFromBytes helper.
type nat struct {
	limbs []uint
}

// natFromBytes creates a nat from big-endian bytes. Leading zeros are trimmed.
func natFromBytes(b []byte) *nat {
	n := (len(b) + _S - 1) / _S
	x := &nat{limbs: make([]uint, n)}
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
	// trim leading zero limbs
	for j := len(x.limbs) - 1; j >= 0; j-- {
		if x.limbs[j] != 0 {
			x.limbs = x.limbs[:j+1]
			return x
		}
	}
	x.limbs = x.limbs[:0]
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

// minusInverseModW computes -x^{-1} mod 2^_W with x odd.
func minusInverseModW(x uint) uint {
	// Newton's method: y = y * (2 - x*y), converges in 5 iterations for 64 bits.
	y := x
	for i := 0; i < 5; i++ {
		y = y * (2 - x*y)
	}
	return -y
}

// bigIntPool reduces allocations in computeRRInto for repeated calls.
var bigIntPool = sync.Pool{
	New: func() any { return new(big.Int) },
}
