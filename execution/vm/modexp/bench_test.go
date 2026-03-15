package modexp

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"testing"
)

// BenchmarkModExpPrecompileVectors benchmarks using the same test vectors as
// BenchmarkPrecompiledModExpEip2565 in execution/vm/contracts_test.go.
// This allows direct comparison between our implementation and the precompile path.
func BenchmarkModExpPrecompileVectors(b *testing.B) {
	for _, file := range []string{"modexp", "modexp_eip2565"} {
		b.Run(file, func(b *testing.B) {
			tests := loadPrecompileVectors(b, file)
			for _, tc := range tests {
				if tc.NoBenchmark {
					continue
				}
				base, exp, mod := parseModexpInput(tc.Input)
				if len(mod) == 0 {
					continue
				}

				b.Run(tc.Name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						Exp(base, exp, mod)
					}
				})
			}
		})
	}
}

// BenchmarkModExpSizes benchmarks modexp at various operand sizes.
func BenchmarkModExpSizes(b *testing.B) {
	sizes := []struct {
		name    string
		bits    int
		expBits int // 0 = same as bits
	}{
		{"256bit", 256, 0},
		{"512bit", 512, 0},
		{"1024bit", 1024, 0},
		{"2048bit", 2048, 0},
		{"4096bit", 4096, 0},
	}

	for _, s := range sizes {
		byteLen := s.bits / 8
		expLen := byteLen
		if s.expBits > 0 {
			expLen = s.expBits / 8
		}

		base := randomOddBench(byteLen)
		exp := randomBench(expLen)
		mod := randomOddBench(byteLen)

		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				Exp(base, exp, mod)
			}
		})
	}
}

// BenchmarkModExpSmallExp benchmarks with small exponents (common in practice).
func BenchmarkModExpSmallExp(b *testing.B) {
	cases := []struct {
		name    string
		modBits int
		exp     int64
	}{
		{"256bit-exp3", 256, 3},
		{"256bit-exp65537", 256, 65537},
		{"1024bit-exp3", 1024, 3},
		{"1024bit-exp65537", 1024, 65537},
		{"2048bit-exp3", 2048, 3},
		{"2048bit-exp65537", 2048, 65537},
	}

	for _, tc := range cases {
		byteLen := tc.modBits / 8
		base := randomOddBench(byteLen)
		exp := big.NewInt(tc.exp).Bytes()
		mod := randomOddBench(byteLen)

		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				Exp(base, exp, mod)
			}
		})
	}
}

// BenchmarkModExpVsBigInt directly compares our implementation against Go's math/big.
func BenchmarkModExpVsBigInt(b *testing.B) {
	sizes := []int{256, 512, 1024, 2048}

	for _, bits := range sizes {
		byteLen := bits / 8
		baseBytes := randomOddBench(byteLen)
		expBytes := randomBench(byteLen)
		modBytes := randomOddBench(byteLen)

		baseBig := new(big.Int).SetBytes(baseBytes)
		expBig := new(big.Int).SetBytes(expBytes)
		modBig := new(big.Int).SetBytes(modBytes)

		b.Run(fmt.Sprintf("ours/%dbit", bits), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				Exp(baseBytes, expBytes, modBytes)
			}
		})

		b.Run(fmt.Sprintf("big.Int/%dbit", bits), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				new(big.Int).Exp(baseBig, expBig, modBig)
			}
		})
	}
}

func randomBench(n int) []byte {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck
	return b
}

func randomOddBench(n int) []byte {
	b := randomBench(n)
	b[0] |= 0x80 // ensure MSB set (full width)
	b[n-1] |= 1  // ensure odd
	return b
}

// BenchmarkAddMulVVW benchmarks the core inner loop function.
func BenchmarkAddMulVVW(b *testing.B) {
	for _, n := range []int{4, 8, 16, 32, 64} {
		z := make([]uint, n)
		x := make([]uint, n)
		for i := range x {
			x[i] = ^uint(0)
		}
		y := ^uint(0)

		b.Run(fmt.Sprintf("%d-limbs", n), func(b *testing.B) {
			for b.Loop() {
				addMulVVW(z, x, y)
			}
		})
	}
}
