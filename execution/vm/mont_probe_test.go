package vm

import (
	"testing"

	"github.com/holiman/uint256"
)

func BenchmarkMontProbe(b *testing.B) {
	m := uint256.MustFromHex("0xc5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5")
	x := uint256.MustFromHex("0x27a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7")
	inv := m[0]
	for range 5 {
		inv *= 2 - m[0]*inv
	}
	inv = -inv
	b.Run("mont", func(b *testing.B) {
		z := *x
		for b.Loop() {
			z = montMulU256(&z, &z, m, inv)
		}
	})
	b.Run("recip", func(b *testing.B) {
		mu := uint256.Reciprocal(m)
		z := *x
		for b.Loop() {
			z.MulModWithReciprocal(&z, &z, m, &mu)
		}
	})
	b.Run("setup", func(b *testing.B) {
		for b.Loop() {
			var r, r2 uint256.Int
			r.Sub(&r, m).Mod(&r, m)
			r2.MulMod(&r, &r, m)
		}
	})
}
