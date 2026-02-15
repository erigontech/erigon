package sais

import (
	"fmt"
	"index/suffixarray"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSais(t *testing.T) {
	data := []byte{4, 5, 6, 4, 5, 6, 4, 5, 6}
	sa := make([]int32, len(data))
	err := Sais(data, sa)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, []int32{6, 3, 0, 7, 4, 1, 8, 5, 2}, sa)
}

func BenchmarkSais(b *testing.B) {
	for _, size := range []int{1024, 64 * 1024, 1024 * 1024, 16 * 1024 * 1024} {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			rng := rand.New(rand.NewSource(0))
			data := make([]byte, size)
			rng.Read(data)
			sa := make([]int32, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := Sais(data, sa); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkStdlibSuffixArray(b *testing.B) {
	for _, size := range []int{1024, 64 * 1024, 1024 * 1024, 16 * 1024 * 1024} {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			rng := rand.New(rand.NewSource(0))
			data := make([]byte, size)
			rng.Read(data)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				suffixarray.New(data)
			}
		})
	}
}
