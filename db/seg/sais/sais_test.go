package sais

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestSaisText(t *testing.T) {
	data := []byte("abracadabra")
	sa := make([]int32, len(data))
	require.NoError(t, Sais(data, sa))
	// Verify suffix array is sorted
	for i := 1; i < len(sa); i++ {
		a := string(data[sa[i-1]:])
		b := string(data[sa[i]:])
		if a >= b {
			t.Fatalf("sa not sorted at %d: %q >= %q", i, a, b)
		}
	}
}

func TestSaisRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for _, n := range []int{2, 3, 10, 100, 1000, 10000} {
		data := make([]byte, n)
		rng.Read(data)
		sa := make([]int32, n)
		require.NoError(t, Sais(data, sa))

		// Verify: sa is a permutation and suffixes are sorted
		seen := make([]bool, n)
		for _, v := range sa {
			require.False(t, seen[v], "duplicate in sa")
			seen[v] = true
		}
		for i := 1; i < n; i++ {
			a := string(data[sa[i-1]:])
			b := string(data[sa[i]:])
			if a >= b {
				t.Fatalf("n=%d: sa not sorted at %d", n, i)
			}
		}
	}
}

func TestSaisEdgeCases(t *testing.T) {
	// Empty
	require.NoError(t, Sais(nil, nil))

	// Single byte
	sa := make([]int32, 1)
	require.NoError(t, Sais([]byte{42}, sa))
	assert.Equal(t, []int32{0}, sa)

	// All same bytes
	data := make([]byte, 100)
	sa = make([]int32, 100)
	require.NoError(t, Sais(data, sa))
	expected := make([]int32, 100)
	for i := range expected {
		expected[i] = int32(99 - i)
	}
	assert.Equal(t, expected, sa)
}

func BenchmarkSais(b *testing.B) {
	for _, size := range []int{16 * 1024 * 1024} {
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
