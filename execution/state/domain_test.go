package state

import (
	"testing"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/stretchr/testify/require"
)

func TestBranchCacheTrunk(t *testing.T) {
	t.Parallel()

	cache, err := newBranchCache(500_000)
	require.NoError(t, err)

	for i := byte(0); i < 16; i++ {
		cache.Add(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i})), commitment.Branch{i}, 0)
		for j := byte(0); j < 16; j++ {
			cache.Add(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i, j})), commitment.Branch{i, j}, 0)
			for k := byte(0); k < 16; k++ {
				cache.Add(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i, j, k})), commitment.Branch{i, j, k}, 0)
				for l := byte(0); l < 16; l++ {
					cache.Add(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i, j, k, l})), commitment.Branch{i, j, k, l}, 0)
				}
			}
		}
	}

	for i := byte(0); i < 16; i++ {
		valueWithStep, ok := cache.Get(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i})))
		require.True(t, ok)
		require.Equal(t, ValueWithStep[commitment.Branch]{Value: commitment.Branch{i}}, valueWithStep)
		for j := byte(0); j < 16; j++ {
			valueWithStep, ok := cache.Get(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i, j})))
			require.True(t, ok)
			require.Equal(t, ValueWithStep[commitment.Branch]{Value: commitment.Branch{i, j}}, valueWithStep)
			for k := byte(0); k < 16; k++ {
				valueWithStep, ok := cache.Get(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i, j, k})))
				require.True(t, ok)
				require.Equal(t, ValueWithStep[commitment.Branch]{Value: commitment.Branch{i, j, k}}, valueWithStep)
				for l := byte(0); l < 16; l++ {
					valueWithStep, ok := cache.Get(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i, j, k, l})))
					require.True(t, ok)
					require.Equal(t, ValueWithStep[commitment.Branch]{Value: commitment.Branch{i, j, k, l}}, valueWithStep)
				}
			}
		}
	}

	for i := byte(0); i < 16; i++ {
		evicted := cache.Remove(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i})))
		require.True(t, evicted)
		for j := byte(0); j < 16; j++ {
			evicted := cache.Remove(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i, j})))
			require.True(t, evicted)
			for k := byte(0); k < 16; k++ {
				evicted := cache.Remove(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i, j, k})))
				require.True(t, evicted)
				for l := byte(0); l < 16; l++ {
					evicted := cache.Remove(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i, j, k, l})))
					require.True(t, evicted)
				}
			}
		}
	}

	for i := byte(0); i < 16; i++ {
		_, ok := cache.Get(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i})))
		require.False(t, ok)
		for j := byte(0); j < 16; j++ {
			_, ok := cache.Get(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i, j})))
			require.False(t, ok)
			for k := byte(0); k < 16; k++ {
				_, ok := cache.Get(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i, j, k})))
				require.False(t, ok)
				for l := byte(0); l < 16; l++ {
					_, ok := cache.Get(commitment.InternPath(commitment.HexNibblesToCompactBytes([]byte{i, j, k, l})))
					require.False(t, ok)
				}
			}
		}
	}
}
