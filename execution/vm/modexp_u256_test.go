// Copyright 2025 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0

package vm

import (
	"bytes"
	"math/big"
	"math/rand"
	"testing"

	evmone "github.com/erigontech/evmone_precompiles"
)

func bigLsh(n uint) *big.Int { return new(big.Int).Lsh(big.NewInt(1), n) }

// TestModexpU256Applicable pins the routing boundaries: a base wider than 256
// bits (which uint256.SetBytes would silently truncate) and a modulus below
// 2^192 (where the reciprocal is unavailable) must not reach modexpU256.
func TestModexpU256Applicable(t *testing.T) {
	be := func(n int, v *big.Int) []byte {
		b := make([]byte, n)
		v.FillBytes(b)
		return b
	}
	mod256 := new(big.Int).Sub(bigLsh(256), big.NewInt(1))
	mod192 := bigLsh(192)
	mod192m1 := new(big.Int).Sub(mod192, big.NewInt(1))

	cases := []struct {
		name string
		base []byte
		mod  []byte
		want bool
	}{
		{"32-byte base, full 256-bit modulus", be(32, mod256), be(32, mod256), true},
		{"empty base", nil, be(32, mod256), true},
		{"modulus exactly 2^192", be(32, mod256), be(25, mod192), true},
		{"2^192 in a padded 32-byte field", be(32, mod256), be(32, mod192), true},
		{"modulus 2^192-1", be(32, mod256), be(24, mod192m1), false},
		{"2^192-1 in a padded 32-byte field", be(32, mod256), be(32, mod192m1), false},
		{"128-bit modulus in a 32-byte field", be(32, mod256), be(32, bigLsh(128)), false},
		{"33-byte base", be(33, mod256), be(32, mod256), false},
		{"1024-byte base", be(1024, mod256), be(32, mod256), false},
		{"33-byte modulus", be(32, mod256), be(33, mod256), false},
	}
	for _, c := range cases {
		if got := modexpU256Applicable(c.base, c.mod); got != c.want {
			t.Errorf("%s: modexpU256Applicable = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestModexpU256 cross-checks the fixed-width uint256 MODEXP fast path against
// math/big across odd/even moduli, base 0/1/>mod, and exponent 0/small/large.
func TestModexpU256(t *testing.T) {
	be := func(x *big.Int) []byte { return x.Bytes() }
	i := big.NewInt
	twoTo256m1 := new(big.Int).Sub(bigLsh(256), i(1))      // odd, 256-bit
	twoTo255 := bigLsh(255)                                // even, 32 bytes
	twoTo192 := bigLsh(192)                                // smallest accepted modulus, 25 bytes
	twoTo192p1 := new(big.Int).Add(twoTo192, i(1))         // odd, 25 bytes
	bigExp := new(big.Int).Sub(twoTo256m1, i(1))           // ~256-bit exponent
	bigBase := new(big.Int).Sub(bigLsh(256), i(1234567))   // 32-byte base > modulus 2^192
	oddMod := new(big.Int).Sub(twoTo256m1, i(58))          // odd, 256-bit
	evenMod := new(big.Int).Sub(bigLsh(250), i(1024*1024)) // even, 32 bytes

	cases := []struct{ base, exp, mod *big.Int }{
		{i(2), i(65537), twoTo256m1},    // odd modulus, small exp
		{i(2), bigExp, twoTo256m1},      // odd modulus, ~256-bit exp
		{i(0), i(5), oddMod},            // base 0 -> 0
		{i(3), i(0), oddMod},            // exp 0 -> 1
		{i(1), i(12345), oddMod},        // base 1 -> 1
		{i(10), i(3), evenMod},          // even modulus
		{bigBase, i(2), twoTo192},       // base > modulus, power-of-two modulus
		{bigBase, i(65537), twoTo192p1}, // base > modulus, odd 25-byte modulus
		{i(3), i(100), twoTo255},        // even 256-bit modulus
		{bigBase, bigExp, oddMod},       // full-width base and exponent
	}
	for idx, c := range cases {
		modBytes := be(c.mod)
		if !modexpU256Applicable(be(c.base), modBytes) {
			t.Fatalf("case %d: test vector is outside the routed domain", idx)
		}
		dst := make([]byte, len(modBytes))
		modexpU256(dst, be(c.base), be(c.exp), modBytes)

		want := make([]byte, len(modBytes))
		new(big.Int).Exp(c.base, c.exp, c.mod).FillBytes(want)
		if !bytes.Equal(dst, want) {
			t.Errorf("case %d (base=%s exp=%s mod=%s): got %x want %x",
				idx, c.base, c.exp, c.mod, dst, want)
		}
	}
}

// TestModexpU256PaddedAndZero covers the operand shapes whose cost is driven by
// the declared field width rather than the value: an exponent field padded with
// leading zeros, and a base that reduces to zero. 0^0 mod m stays 1.
func TestModexpU256PaddedAndZero(t *testing.T) {
	pad := func(n int, tail ...byte) []byte {
		b := make([]byte, n)
		copy(b[n-len(tail):], tail)
		return b
	}
	mod := pad(32, 0xff)
	mod[0] = 0xff // full-width odd modulus
	modBig := new(big.Int).SetBytes(mod)

	cases := []struct {
		name      string
		base, exp []byte
	}{
		{"1024-byte exponent field holding 0", pad(32, 7), pad(1024)},
		{"1024-byte exponent field holding 1", pad(32, 7), pad(1024, 1)},
		{"zero base, exponent 0", pad(32), pad(1024)},
		{"zero base, all-ones exponent", pad(32), bytes.Repeat([]byte{0xff}, 1024)},
		{"base equal to the modulus", mod, bytes.Repeat([]byte{0xff}, 1024)},
		{"empty base and empty exponent", nil, nil},
	}
	for _, c := range cases {
		dst := make([]byte, len(mod))
		modexpU256(dst, c.base, c.exp, mod)

		want := make([]byte, len(mod))
		new(big.Int).Exp(new(big.Int).SetBytes(c.base), new(big.Int).SetBytes(c.exp), modBig).FillBytes(want)
		if !bytes.Equal(dst, want) {
			t.Errorf("%s: got %x want %x", c.name, dst, want)
		}
	}
}

// TestModexpU256Random fuzzes the uint256 path against math/big over random
// operands drawn from the routed domain, including modulus fields padded with
// leading zero bytes.
func TestModexpU256Random(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	randBytes := func(minLen, maxLen int) []byte {
		b := make([]byte, minLen+rng.Intn(maxLen-minLen+1))
		rng.Read(b)
		return b
	}
	for iter := range 20000 {
		base := randBytes(0, 32)
		exp := randBytes(0, 20)
		modB := randBytes(25, 32)
		// Force the modulus to at least 2^192, then zero a random prefix so that
		// padded fields (and their boundary) get exercised too.
		modB[0] |= 1
		for j := range rng.Intn(len(modB) - 24) {
			modB[j] = 0
		}
		if !modexpU256Applicable(base, modB) {
			continue
		}
		m := new(big.Int).SetBytes(modB)
		dst := make([]byte, len(modB))
		modexpU256(dst, base, exp, modB)

		want := make([]byte, len(modB))
		new(big.Int).Exp(new(big.Int).SetBytes(base), new(big.Int).SetBytes(exp), m).FillBytes(want)
		if !bytes.Equal(dst, want) {
			t.Fatalf("iter %d: base=%x exp=%x mod=%x\n got %x\nwant %x", iter, base, exp, modB, dst, want)
		}
	}
}

// benchModexpCase is one operand triple at its declared field widths, as the
// precompile sees them.
type benchModexpCase struct {
	name           string
	base, exp, mod []byte
}

func benchModexpCases() []benchModexpCase {
	rng := rand.New(rand.NewSource(7))
	fill := func(n int) []byte {
		b := make([]byte, n)
		rng.Read(b)
		b[0] |= 0x80 // keep the value at its full declared width
		return b
	}
	// A value of the given bit width placed in a field of n bytes.
	widthIn := func(n int, bits uint) []byte {
		b := make([]byte, n)
		v := new(big.Int).Sub(bigLsh(bits), big.NewInt(1))
		v.FillBytes(b)
		return b
	}
	base32, base1024 := fill(32), fill(1024)
	mod256, mod512, mod1024, mod2048 := fill(32), fill(64), fill(128), fill(256)
	mod256[31] |= 1
	mod512[63] |= 1
	mod1024[127] |= 1
	mod2048[255] |= 1
	exp3 := []byte{3}
	exp65537 := []byte{0x01, 0x00, 0x01}
	exp64, exp256, exp512, exp2048, exp8192 := fill(8), fill(32), fill(64), fill(256), fill(1024)

	return []benchModexpCase{
		{"mod256/exp65537", base32, exp65537, mod256},
		{"mod256/exp64bit", base32, exp64, mod256},
		{"mod256/exp256bit", base32, exp256, mod256},
		{"mod256/exp512bit", base32, exp512, mod256},
		{"mod256/exp2048bit", base32, exp2048, mod256},
		{"mod256/exp8192bit", base32, exp8192, mod256},
		{"mod2^192/exp64bit", base32, exp64, widthIn(32, 193)},
		{"mod128bit/exp64bit", base32, exp64, widthIn(32, 128)},
		{"mod64bit/exp64bit", base32, exp64, widthIn(32, 64)},
		{"base1024/mod256/exp65537", base1024, exp65537, mod256},
		{"mod512/exp65537", base32, exp65537, mod512},
		{"mod512/exp256bit", base32, exp256, mod512},
		{"mod1024/exp65537", base32, exp65537, mod1024},
		{"mod1024/exp256bit", base32, exp256, mod1024},
		{"mod2048/exp3", base32, exp3, mod2048},
		{"mod2048/exp256bit", base32, exp256, mod2048},
		{"mod2048/exp65537", base32, exp65537, mod2048},
		{"mod2048/exp2048bit", base32, exp2048, mod2048},
	}
}

// BenchmarkModexpBackends measures each MODEXP backend on the same operands, so
// that the routing in bigModExp.Run can be re-derived when a backend changes.
func BenchmarkModexpBackends(b *testing.B) {
	for _, c := range benchModexpCases() {
		dst := make([]byte, len(c.mod))
		b.Run(c.name+"/big", func(b *testing.B) {
			for range b.N {
				new(big.Int).Exp(new(big.Int).SetBytes(c.base), new(big.Int).SetBytes(c.exp),
					new(big.Int).SetBytes(c.mod)).FillBytes(dst)
			}
		})
		b.Run(c.name+"/evmone", func(b *testing.B) {
			for range b.N {
				evmone.ModExp(dst, c.base, c.exp, c.mod)
			}
		})
		if !modexpU256Applicable(c.base, c.mod) {
			continue
		}
		b.Run(c.name+"/u256", func(b *testing.B) {
			for range b.N {
				clear(dst)
				modexpU256(dst, c.base, c.exp, c.mod)
			}
		})
	}
}

// BenchmarkModexpRouted measures the precompile as callers see it, backend
// selection included.
func BenchmarkModexpRouted(b *testing.B) {
	c7883 := &bigModExp{osaka: true}
	for _, c := range benchModexpCases() {
		input := make([]byte, 0, 96+len(c.base)+len(c.exp)+len(c.mod))
		for _, n := range []int{len(c.base), len(c.exp), len(c.mod)} {
			input = append(input, make([]byte, 24)...)
			input = append(input, new(big.Int).SetInt64(int64(n)).FillBytes(make([]byte, 8))...)
		}
		input = append(input, c.base...)
		input = append(input, c.exp...)
		input = append(input, c.mod...)
		b.Run(c.name, func(b *testing.B) {
			for range b.N {
				if _, err := c7883.Run(input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestModexpBigIntFaster pins the measured math/big vs evmone boundary. math/big
// only reaches its windowed Montgomery path above a one-word exponent, so the
// modulus width at which it overtakes evmone differs sharply either side of that.
func TestModexpBigIntFaster(t *testing.T) {
	exp := func(n int) []byte {
		b := make([]byte, n)
		b[0] = 0xff
		return b
	}
	padded := func(n int) []byte { // n-byte field holding a 64-bit value
		b := make([]byte, n)
		b[n-8] = 0xff
		return b
	}
	cases := []struct {
		name   string
		exp    []byte
		modLen uint64
		want   bool
	}{
		{"256-bit modulus stays off math/big", exp(32), 32, false},
		{"320-bit modulus, one-word exponent", exp(1), 40, false},
		{"320-bit modulus, wide exponent", exp(16), 40, false},
		{"512-bit modulus, one-word exponent", exp(8), 64, false},
		{"512-bit modulus, wide exponent", exp(16), 64, true},
		{"512-bit modulus, exponent just over one word", exp(9), 64, true},
		{"512-bit modulus, one word in a padded field", padded(9), 64, false},
		{"768-bit modulus, one-word exponent", exp(8), 96, false},
		{"768-bit modulus, wide exponent", exp(32), 96, true},
		{"1024-bit modulus, one-byte exponent", exp(1), 128, true},
		{"1024-bit modulus, wide exponent", exp(32), 128, true},
		{"2048-bit modulus, 65537", []byte{0x01, 0x00, 0x01}, 256, true},
		{"8192-bit modulus, full exponent", exp(1024), 1024, true},
		{"empty exponent, 512-bit modulus", nil, 64, false},
		{"empty exponent, 1024-bit modulus", nil, 128, true},
	}
	for _, c := range cases {
		if got := modexpBigIntFaster(c.exp, c.modLen); got != c.want {
			t.Errorf("%s: modexpBigIntFaster = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestModexpRoutingAgrees checks that every backend Run can select returns the
// same bytes, over operand shapes that straddle each routing boundary.
func TestModexpRoutingAgrees(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	c := &bigModExp{osaka: true}
	pack := func(base, exp, mod []byte) []byte {
		out := make([]byte, 0, 96+len(base)+len(exp)+len(mod))
		for _, n := range []int{len(base), len(exp), len(mod)} {
			var field [32]byte
			big.NewInt(int64(n)).FillBytes(field[:])
			out = append(out, field[:]...)
		}
		return append(append(append(out, base...), exp...), mod...)
	}
	for _, modLen := range []int{24, 25, 32, 33, 40, 63, 64, 65, 96, 127, 128, 129, 256} {
		for _, expLen := range []int{0, 1, 3, 8, 9, 16, 64} {
			for _, baseLen := range []int{0, 1, 32, 33, 64} {
				base, exp, mod := make([]byte, baseLen), make([]byte, expLen), make([]byte, modLen)
				rng.Read(base)
				rng.Read(exp)
				rng.Read(mod)
				mod[modLen-1] |= 1 // keep the modulus above 1

				got, err := c.Run(pack(base, exp, mod))
				if err != nil {
					t.Fatalf("mod %d exp %d base %d: %v", modLen, expLen, baseLen, err)
				}
				want := make([]byte, modLen)
				new(big.Int).Exp(new(big.Int).SetBytes(base), new(big.Int).SetBytes(exp),
					new(big.Int).SetBytes(mod)).FillBytes(want)
				if !bytes.Equal(got, want) {
					t.Fatalf("mod %d exp %d base %d:\n got %x\nwant %x", modLen, expLen, baseLen, got, want)
				}
			}
		}
	}
}

// TestModexpU256Windows walks exponent bit lengths across every window-width
// boundary, with bit patterns that stress window edges: all-ones (every window
// full), a lone top bit, alternating bits, and sparse ones separated by runs of
// zeros longer than the widest window.
func TestModexpU256Windows(t *testing.T) {
	mod := make([]byte, 32)
	for i := range mod {
		mod[i] = 0xd7
	}
	mod[31] |= 1
	modBig := new(big.Int).SetBytes(mod)
	base := bytes.Repeat([]byte{0x9c}, 32)
	baseBig := new(big.Int).SetBytes(base)

	patterns := map[string]func(n int) *big.Int{
		"all ones": func(n int) *big.Int { return new(big.Int).Sub(bigLsh(uint(n)), big.NewInt(1)) },
		"top bit":  func(n int) *big.Int { return bigLsh(uint(n - 1)) },
		"alternating": func(n int) *big.Int {
			return new(big.Int).Div(new(big.Int).Sub(bigLsh(uint(n+1)), big.NewInt(1)), big.NewInt(3))
		},
		"sparse": func(n int) *big.Int {
			e := bigLsh(uint(n - 1))
			for s := n - 1 - 7; s > 0; s -= 7 {
				e.SetBit(e, s, 1)
			}
			return e
		},
	}
	var bitLens []int
	for _, edge := range []int{1, 7, 25, 81, 241, 1024} {
		for _, d := range []int{-1, 0, 1, 2} {
			if n := edge + d; n >= 1 {
				bitLens = append(bitLens, n)
			}
		}
	}
	for name, gen := range patterns {
		for _, n := range bitLens {
			expBig := gen(n)
			if expBig.BitLen() != n {
				continue // pattern cannot express this width
			}
			dst := make([]byte, 32)
			modexpU256(dst, base, expBig.Bytes(), mod)

			want := make([]byte, 32)
			new(big.Int).Exp(baseBig, expBig, modBig).FillBytes(want)
			if !bytes.Equal(dst, want) {
				t.Fatalf("%s, %d-bit exponent:\n got %x\nwant %x", name, n, dst, want)
			}
		}
	}
}
