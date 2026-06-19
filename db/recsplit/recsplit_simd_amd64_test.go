//go:build goexperiment.simd && amd64

package recsplit

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/murmur3"
)

func makeBucket(prefix string, size int) []uint64 {
	bucket := make([]uint64, size)
	for i := range bucket {
		key := fmt.Appendf(nil, "%s_%d_%d", prefix, size, i)
		_, lo := murmur3.Sum128WithSeed(key, 1)
		bucket[i] = lo
	}
	return bucket
}

func TestFindBijectionSIMDMatchesScalar(t *testing.T) {
	if !useSIMD {
		t.Skip("AVX512 not available")
	}
	for size := 1; size <= 24; size++ {
		bucket := makeBucket("simd_bij", size)
		require.Equal(t, findBijection(bucket, 0), findBijectionSIMD(bucket, 0), "size %d", size)
	}
}

func TestFindSplitSIMDMatchesScalar(t *testing.T) {
	if !useSIMD {
		t.Skip("AVX512 not available")
	}
	const (
		leafSize           = uint16(8)
		primaryAggrBound   = uint16(32)
		secondaryAggrBound = uint16(96)
	)
	for _, m := range []uint16{32, 48, 64, 96} {
		bucket := makeBucket("simd_split", int(m))
		fanout, unit := splitParams(m, leafSize, primaryAggrBound, secondaryAggrBound)
		want := findSplit(bucket, 0, fanout, unit, make([]uint16, secondaryAggrBound))
		got := findSplitSIMD(bucket, 0, fanout, unit, make([]uint16, secondaryAggrBound))
		require.Equal(t, want, got, "m %d", m)
	}
}
