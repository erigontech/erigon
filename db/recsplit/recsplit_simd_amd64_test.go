//go:build goexperiment.simd && amd64

package recsplit

import (
	"fmt"
	"math"
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

func aggrBounds(leafSize uint16) (primary, secondary uint16) {
	primary = leafSize * uint16(math.Max(2, math.Ceil(0.35*float64(leafSize)+0.5)))
	if leafSize < 7 {
		secondary = primary * 2
	} else {
		secondary = primary * uint16(math.Ceil(0.21*float64(leafSize)+0.9))
	}
	return
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

// TestFindSplitSIMDMatchesScalar covers both a power-of-two leaf size (SIMD fast/general
// paths) and a non-power-of-two one (unit not a power of two -> scalar fallback guard).
func TestFindSplitSIMDMatchesScalar(t *testing.T) {
	if !useSIMD {
		t.Skip("AVX512 not available")
	}
	for _, leafSize := range []uint16{8, 12} {
		primary, secondary := aggrBounds(leafSize)
		for _, m := range []uint16{leafSize * 2, primary, primary + leafSize, secondary, secondary + 1} {
			bucket := makeBucket("simd_split", int(m))
			fanout, unit := splitParams(m, leafSize, primary, secondary)
			want := findSplit(bucket, 0, fanout, unit, make([]uint16, secondary*2))
			got := findSplitSIMD(bucket, 0, fanout, unit, make([]uint16, secondary*2))
			require.Equal(t, want, got, "leafSize %d m %d fanout %d unit %d", leafSize, m, fanout, unit)
		}
	}
}
