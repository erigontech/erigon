package sais

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// verifySA16 checks that sa is a permutation of [0,n) whose suffixes of data are
// in strictly ascending lexicographic order.
func verifySA16(t *testing.T, data []uint16, sa []int32) {
	t.Helper()
	n := len(data)
	seen := make([]bool, n)
	for _, v := range sa {
		require.GreaterOrEqual(t, int(v), 0)
		require.Less(t, int(v), n)
		require.False(t, seen[v], "duplicate in sa")
		seen[v] = true
	}
	less := func(i, j int32) bool {
		for int(i) < n && int(j) < n {
			if data[i] != data[j] {
				return data[i] < data[j]
			}
			i++
			j++
		}
		return i > j // the shorter (later-starting) suffix is the smaller
	}
	for k := 1; k < n; k++ {
		require.Truef(t, less(sa[k-1], sa[k]), "sa not sorted at %d", k)
	}
}

func TestSais16Random(t *testing.T) {
	var buf []int32
	rng := rand.New(rand.NewSource(42))
	for _, n := range []int{2, 3, 10, 100, 1000, 10000} {
		data := make([]uint16, n)
		for i := range data {
			data[i] = uint16(rng.Intn(257)) // full alphabet incl. the 256 symbol
		}
		sa := make([]int32, n)
		require.NoError(t, Sais16(data, 257, sa, &buf))
		verifySA16(t, data, sa)
	}
}

func TestSais16EdgeCases(t *testing.T) {
	var buf []int32
	require.NoError(t, Sais16(nil, 257, nil, &buf))

	sa := make([]int32, 1)
	require.NoError(t, Sais16([]uint16{256}, 257, sa, &buf))
	assert.Equal(t, []int32{0}, sa)

	// All same symbol: suffixes sort by descending start position.
	data := make([]uint16, 100)
	for i := range data {
		data[i] = 256
	}
	sa = make([]int32, 100)
	require.NoError(t, Sais16(data, 257, sa, &buf))
	expected := make([]int32, 100)
	for i := range expected {
		expected[i] = int32(99 - i)
	}
	assert.Equal(t, expected, sa)
}

// TestSais16Superstring runs Sais16 over superstring-shaped input (separator->0,
// byte b->b+1, alphabet 257) — the exact encoding parallel_compress builds — and
// checks the resulting suffix array is a correctly sorted permutation.
func TestSais16Superstring(t *testing.T) {
	var buf []int32
	rng := rand.New(rand.NewSource(99))
	for iter := 0; iter < 300; iter++ {
		var code []uint16 // one symbol per cell
		nwords := 1 + rng.Intn(8)
		for w := 0; w < nwords; w++ {
			for k := rng.Intn(6); k > 0; k-- {
				code = append(code, uint16(rng.Intn(256))+1)
			}
			code = append(code, 0) // word boundary
		}
		n := len(code)
		sa := make([]int32, n)
		require.NoError(t, Sais16(code, 257, sa, &buf))
		verifySA16(t, code, sa)
	}
}
