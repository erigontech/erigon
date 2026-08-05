// Copyright 2025 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0

package vm

import (
	"bytes"
	"math/big"
	"math/rand"
	"testing"
)

// TestModexpU256 cross-checks the fixed-width uint256 MODEXP fast path against
// math/big across odd/even moduli, base 0/1/>mod, and exponent 0/small/large.
func TestModexpU256(t *testing.T) {
	be := func(x *big.Int) []byte { return x.Bytes() }
	i := big.NewInt
	twoTo256m1 := new(big.Int).Sub(new(big.Int).Lsh(i(1), 256), i(1)) // odd
	twoTo255 := new(big.Int).Lsh(i(1), 255)                           // even, needs 32 bytes
	twoTo300 := new(big.Int).Add(new(big.Int).Lsh(i(1), 300), i(7))   // base > 256 bits (38 bytes)
	twoTo500 := new(big.Int).Add(new(big.Int).Lsh(i(1), 500), i(3))   // base >> 256 bits (63 bytes)

	cases := []struct{ base, exp, mod *big.Int }{
		{i(2), i(65537), twoTo256m1},                           // odd modulus, small exp
		{i(2), new(big.Int).Sub(twoTo256m1, i(1)), twoTo256m1}, // odd modulus, ~256-bit exp
		{i(0), i(5), i(7)},                                     // base 0 -> 0
		{i(3), i(0), i(7)},                                     // exp 0 -> 1
		{i(1), i(12345), i(7)},                                 // base 1 -> 1
		{i(10), i(3), i(6)},                                    // even modulus
		{i(100), i(2), i(7)},                                   // base > modulus
		{i(3), i(100), twoTo255},                               // even 256-bit modulus
		{i(3), i(5), i(2)},                                     // tiny modulus
		{twoTo300, i(65537), twoTo256m1},                       // base > 256 bits, odd modulus
		{twoTo500, i(100), i(7)},                               // base >> 256 bits, tiny modulus
		{twoTo300, i(9), twoTo255},                             // base > 256 bits, even modulus
	}
	for idx, c := range cases {
		modLen := len(be(c.mod))
		if modLen == 0 {
			modLen = 1
		}
		dst := make([]byte, modLen)
		modexpU256(dst, be(c.base), be(c.exp), be(c.mod))

		want := make([]byte, modLen)
		new(big.Int).Exp(c.base, c.exp, c.mod).FillBytes(want)
		if !bytes.Equal(dst, want) {
			t.Errorf("case %d (base=%s exp=%s mod=%s): got %x want %x",
				idx, c.base, c.exp, c.mod, dst, want)
		}
	}
}

// TestModexpU256Random fuzzes the uint256 path against math/big over random
// inputs, including bases longer than the 256-bit modulus and small moduli.
func TestModexpU256Random(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	randBytes := func(maxLen int) []byte {
		b := make([]byte, rng.Intn(maxLen+1))
		rng.Read(b)
		return b
	}
	one := big.NewInt(1)
	for iter := range 20000 {
		base := randBytes(48) // up to 48 bytes exercises the base-reduction path
		exp := randBytes(20)
		modB := randBytes(32) // modulus fits in 256 bits
		m := new(big.Int).SetBytes(modB)
		if m.Cmp(one) <= 0 {
			continue // mod 0/1 are handled by earlier switch cases
		}
		modLen := len(modB)
		dst := make([]byte, modLen)
		modexpU256(dst, base, exp, modB)

		want := make([]byte, modLen)
		new(big.Int).Exp(new(big.Int).SetBytes(base), new(big.Int).SetBytes(exp), m).FillBytes(want)
		if !bytes.Equal(dst, want) {
			t.Fatalf("iter %d: base=%x exp=%x mod=%x\n got %x\nwant %x", iter, base, exp, modB, dst, want)
		}
	}
}
