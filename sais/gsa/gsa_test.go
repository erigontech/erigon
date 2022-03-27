package gsa

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/sais"
	"github.com/stretchr/testify/assert"
)

func TestExampleGSA(t *testing.T) {
	R := [][]byte{
		[]byte("hihihi"),
		[]byte("hihihi"),
		[]byte("hihihi"),
	}
	str, n := ConcatAll(R)
	sa := make([]uint, n)
	lcp := make([]int, n)
	da := make([]int32, n)
	_ = GSA(str, sa, lcp, da)
	PrintArrays(str, sa, lcp, da, n)
}

func TestGSA(t *testing.T) {
	R := [][]byte{{4, 5, 6, 4, 5, 6, 4, 5, 6}}
	str, n := ConcatAll(R)
	sa := make([]uint, n)
	lcp := make([]int, n)
	da := make([]int32, n)
	_ = GSA(str, sa, lcp, da)
	assert.Equal(t, []uint{10, 9, 6, 3, 0, 7, 4, 1, 8, 5, 2}, sa[:n])
}

const N = 100_000

func BenchmarkName(b *testing.B) {
	R := make([][]byte, 0, N)
	for i := 0; i < N; i++ {
		R = append(R, []byte("hihihi"))
	}
	superstring := make([]byte, 0, 1024)

	for _, a := range R {
		for _, b := range a {
			superstring = append(superstring, 1, b)
		}
		superstring = append(superstring, 0, 0)
	}

	sa := make([]int32, len(superstring))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := sais.Sais(superstring, sa)
		if err != nil {
			panic(err)
		}
	}
}
func BenchmarkName2(b *testing.B) {
	R := make([][]byte, 0, N)
	for i := 0; i < N; i++ {
		R = append(R, []byte("hihihi"))
	}
	str, n := ConcatAll(R)
	sa := make([]uint, n)
	lcp := make([]int, n)
	da := make([]int32, n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = GSA(str, sa, lcp, da)
	}
}
