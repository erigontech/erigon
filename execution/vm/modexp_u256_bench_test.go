// Copyright 2025 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0

package vm

import (
	"bytes"
	"fmt"
	"math/big"
	"math/rand"
	"testing"

	evmone "github.com/erigontech/evmone_precompiles"
)

// benchModexpCase is one operand triple at its declared field widths, as the
// precompile sees them.
type benchModexpCase struct {
	name           string
	base, exp, mod []byte
}

// benchModexpCases sweeps the routing inputs: modulus width, exponent width
// either side of the one-word boundary, and modulus parity. Parity is part of
// the sweep because an even modulus takes a different path in every backend.
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
	setParity := func(mod []byte, odd bool) []byte {
		m := bytes.Clone(mod)
		if odd {
			m[len(m)-1] |= 1
		} else {
			m[len(m)-1] &^= 1
		}
		return m
	}
	base32, base1024 := fill(32), fill(1024)
	mod256, mod512, mod768 := fill(32), fill(64), fill(96)
	mod1024, mod1536, mod2048 := fill(128), fill(192), fill(256)
	exp65537, exp64 := []byte{0x01, 0x00, 0x01}, fill(8)
	exps := []struct {
		name string
		b    []byte
	}{
		{"exp65537", exp65537},
		{"exp64bit", exp64},
		{"exp256bit", fill(32)},
		{"exp2048bit", fill(256)},
	}

	var cases []benchModexpCase
	for _, mod := range [][]byte{mod256, mod512, mod768, mod1024, mod1536, mod2048} {
		for _, odd := range []bool{true, false} {
			parity := "odd"
			if !odd {
				parity = "even"
			}
			for _, e := range exps {
				cases = append(cases, benchModexpCase{
					name: fmt.Sprintf("mod%d%s/%s", len(mod)*8, parity, e.name),
					base: base32, exp: e.b, mod: setParity(mod, odd),
				})
			}
		}
	}
	oddMod256 := setParity(mod256, true)
	return append(cases,
		benchModexpCase{"mod256odd/exp8192bit", base32, fill(1024), oddMod256},
		benchModexpCase{"mod2^192/exp64bit", base32, exp64, widthIn(32, 193)},
		benchModexpCase{"mod128bit/exp64bit", base32, exp64, widthIn(32, 128)},
		benchModexpCase{"mod64bit/exp64bit", base32, exp64, widthIn(32, 64)},
		benchModexpCase{"base1024/mod256odd/exp65537", base1024, exp65537, oddMod256},
	)
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
		input := packModexpInput(c.base, c.exp, c.mod)
		b.Run(c.name, func(b *testing.B) {
			for range b.N {
				if _, err := c7883.Run(input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
