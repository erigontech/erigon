package solid

import (
	"encoding/binary"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/merkle_tree"
)

func TestUint64ListProgressiveRootMatchesReference(t *testing.T) {
	for _, count := range []int{0, 1, 4, 5, 20, 21, 84, 85} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			list := NewUint64ListSSZ(1_000_000).(*uint64ListSSZ)
			for i := range count {
				list.Append(uint64(i + 1))
			}

			want, err := merkle_tree.ProgressiveBasicListRoot(list.Bytes(), uint64(list.Length()))
			require.NoError(t, err)
			got, err := list.HashSSZProgressive()
			require.NoError(t, err)
			require.Equal(t, want, got)
			require.NotNil(t, list.u.progressiveTrees)
		})
	}
}

func TestUint64ListProgressiveRootAfterSet(t *testing.T) {
	values := make([]uint64, 85)
	for i := range values {
		values[i] = uint64(i + 1)
	}
	list := NewUint64ListSSZFromSlice(1_000_000, values).(*uint64ListSSZ)
	_, err := list.HashSSZProgressive()
	require.NoError(t, err)

	list.Set(40, 1_000)

	assertUint64ListProgressiveRoot(t, list)
}

func TestUint64ListProgressiveRootAfterAppend(t *testing.T) {
	for _, count := range []int{1, 4, 20, 36, 41, 84} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			list := NewUint64ListSSZ(1_000_000).(*uint64ListSSZ)
			for i := range count {
				list.Append(uint64(i + 1))
			}
			_, err := list.HashSSZProgressive()
			require.NoError(t, err)

			list.Append(1_000)

			assertUint64ListProgressiveRoot(t, list)
		})
	}
}

func TestUint64ListProgressiveRootAfterClearAndAppend(t *testing.T) {
	list := NewUint64ListSSZFromSlice(1_000_000, []uint64{1, 2, 3, 4, 5}).(*uint64ListSSZ)
	_, err := list.HashSSZProgressive()
	require.NoError(t, err)

	list.Clear()
	list.Append(42)

	assertUint64ListProgressiveRoot(t, list)
}

func TestUint64ListProgressiveCacheResetsOnReplacement(t *testing.T) {
	newWarmList := func(t *testing.T) *uint64ListSSZ {
		t.Helper()
		values := make([]uint64, 85)
		for i := range values {
			values[i] = uint64(i + 1)
		}
		list := NewUint64ListSSZFromSlice(1_000_000, values).(*uint64ListSSZ)
		_, err := list.HashSSZProgressive()
		require.NoError(t, err)
		require.NotNil(t, list.u.progressiveTrees)
		return list
	}

	t.Run("pop", func(t *testing.T) {
		list := newWarmList(t)
		require.Equal(t, uint64(85), list.Pop())
		require.Nil(t, list.u.progressiveTrees)
		assertUint64ListProgressiveRoot(t, list)
	})

	t.Run("decode", func(t *testing.T) {
		list := newWarmList(t)
		encoded := make([]byte, 16)
		binary.LittleEndian.PutUint64(encoded, 7)
		binary.LittleEndian.PutUint64(encoded[8:], 8)
		require.NoError(t, list.DecodeSSZ(encoded, 0))
		require.Nil(t, list.u.progressiveTrees)
		assertUint64ListProgressiveRoot(t, list)

		require.NoError(t, list.DecodeSSZ(nil, 0))
		assertUint64ListProgressiveRoot(t, list)
	})
}

func TestUint64ListProgressiveCopyIsIndependent(t *testing.T) {
	values := make([]uint64, 85)
	for i := range values {
		values[i] = uint64(i + 1)
	}
	source := NewUint64ListSSZFromSlice(1_000_000, values).(*uint64ListSSZ)
	_, err := source.HashSSZProgressive()
	require.NoError(t, err)
	target := NewUint64ListSSZ(1_000_000).(*uint64ListSSZ)

	source.CopyTo(target)

	require.NotNil(t, target.u.progressiveTrees)
	require.NotSame(t, source.u.progressiveTrees[2], target.u.progressiveTrees[2])
	source.Set(40, 1_000)
	target.Set(44, 2_000)
	assertUint64ListProgressiveRoot(t, source)
	assertUint64ListProgressiveRoot(t, target)
}

func TestUint64ListProgressiveCopyAcrossBackingRepresentations(t *testing.T) {
	encoded := make([]byte, 85*8)
	for i := range 85 {
		binary.LittleEndian.PutUint64(encoded[i*8:], uint64(i+1))
	}
	newCompact := func(t *testing.T) *uint64ListSSZ {
		t.Helper()
		list := NewUint64ListSSZ(1_000_000).(*uint64ListSSZ)
		require.NoError(t, list.DecodeSSZ(encoded, 0))
		return list
	}
	newOverallocated := func() *uint64ListSSZ {
		list := NewUint64ListSSZ(1_000_000).(*uint64ListSSZ)
		for i := range 200 {
			list.Append(uint64(i))
		}
		list.Clear()
		for i := range 85 {
			list.Append(uint64(i + 1))
		}
		return list
	}

	for _, tc := range []struct {
		name   string
		source func() *uint64ListSSZ
		target func() *uint64ListSSZ
	}{
		{name: "compact_to_overallocated", source: func() *uint64ListSSZ { return newCompact(t) }, target: newOverallocated},
		{name: "overallocated_to_compact", source: newOverallocated, target: func() *uint64ListSSZ { return newCompact(t) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := tc.source()
			target := tc.target()
			_, err := source.HashSSZProgressive()
			require.NoError(t, err)

			source.CopyTo(target)
			source.Set(40, 1_000)
			target.Set(44, 2_000)
			assertUint64ListProgressiveRoot(t, source)
			assertUint64ListProgressiveRoot(t, target)
		})
	}
}

func TestUint64ListCopyClearsTargetProgressiveCache(t *testing.T) {
	source := NewUint64ListSSZFromSlice(1_000_000, []uint64{1, 2, 3, 4, 5}).(*uint64ListSSZ)
	target := NewUint64ListSSZFromSlice(1_000_000, []uint64{6, 7, 8, 9, 10}).(*uint64ListSSZ)
	_, err := target.HashSSZProgressive()
	require.NoError(t, err)

	source.CopyTo(target)

	require.Nil(t, target.u.progressiveTrees)
	assertUint64ListProgressiveRoot(t, target)
}

func TestUint64ListCopyReusesTargetProgressiveTrees(t *testing.T) {
	values := make([]uint64, 85)
	source := NewUint64ListSSZFromSlice(1_000_000, values).(*uint64ListSSZ)
	target := NewUint64ListSSZFromSlice(1_000_000, values).(*uint64ListSSZ)
	_, err := source.HashSSZProgressive()
	require.NoError(t, err)
	_, err = target.HashSSZProgressive()
	require.NoError(t, err)
	trees := append([]*merkle_tree.MerkleTree(nil), target.u.progressiveTrees...)

	source.CopyTo(target)

	for i, tree := range trees {
		require.Same(t, tree, target.u.progressiveTrees[i])
	}
	assertUint64ListProgressiveRoot(t, target)
}

func TestUint64ListSwitchesMerkleCacheMode(t *testing.T) {
	list := NewUint64ListSSZFromSlice(16, []uint64{1, 2, 3, 4, 5}).(*uint64ListSSZ)
	_, err := list.HashSSZ()
	require.NoError(t, err)
	require.NotNil(t, list.u.MerkleTree)

	list.SetProgressiveHashing(true)
	require.Nil(t, list.u.MerkleTree)
	_, err = list.HashSSZProgressive()
	require.NoError(t, err)
	require.NotNil(t, list.u.progressiveTrees)

	list.SetProgressiveHashing(false)
	require.Nil(t, list.u.progressiveTrees)
	_, err = list.HashSSZ()
	require.NoError(t, err)
	require.NotNil(t, list.u.MerkleTree)
}

func TestUint64ListProgressiveRootRandomOperations(t *testing.T) {
	for _, initialLength := range []int{0, 3, 4, 19, 20, 83, 84} {
		t.Run(strconv.Itoa(initialLength), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(initialLength + 1)))
			values := make([]uint64, initialLength, 128)
			list := NewUint64ListSSZ(1_000_000).(*uint64ListSSZ)
			for i := range values {
				values[i] = rng.Uint64()
				list.Append(values[i])
			}

			for range 200 {
				switch rng.Intn(4) {
				case 0:
					if len(values) > 0 {
						index := rng.Intn(len(values))
						value := rng.Uint64()
						values[index] = value
						list.Set(index, value)
					}
				case 1:
					if len(values) < 128 {
						value := rng.Uint64()
						values = append(values, value)
						list.Append(value)
					}
				case 2:
					if len(values) > 0 {
						require.Equal(t, values[len(values)-1], list.Pop())
						values = values[:len(values)-1]
					}
				case 3:
					encoded := make([]byte, len(values)*8)
					for i, value := range values {
						binary.LittleEndian.PutUint64(encoded[i*8:], value)
					}
					require.NoError(t, list.DecodeSSZ(encoded, 0))
				}
				assertUint64ListProgressiveRoot(t, list)
			}
		})
	}
}

func TestUint64ListWarmProgressiveHashIsIncremental(t *testing.T) {
	const listLength = 1_140_000
	list := NewUint64ListSSZ(listLength).(*uint64ListSSZ)
	for i := range listLength {
		list.Append(uint64(i))
	}
	_, err := list.HashSSZProgressive()
	require.NoError(t, err)
	assertUint64ListProgressiveRoot(t, list)

	leafHashes := 0
	start := 0
	capacity := 1
	for _, tree := range list.u.progressiveTrees {
		leafFn := list.u.progressiveLeafFn(start)
		tree.SetComputeLeafFn(func(index int, out []byte) {
			leafHashes++
			leafFn(index, out)
		})
		start += capacity
		capacity *= 4
	}
	for i := range 600 {
		list.Set(i*4, uint64(i+listLength))
	}

	_, err = list.HashSSZProgressive()
	require.NoError(t, err)
	require.Positive(t, leafHashes)
	require.LessOrEqual(t, leafHashes, 1_200)
	assertUint64ListProgressiveRoot(t, list)
}

func assertUint64ListProgressiveRoot(t *testing.T, list *uint64ListSSZ) {
	t.Helper()
	want, err := merkle_tree.ProgressiveBasicListRoot(list.Bytes(), uint64(list.Length()))
	require.NoError(t, err)
	got, err := list.HashSSZProgressive()
	require.NoError(t, err)
	require.Equal(t, want, got)
}
