package merkle_tree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildFromLeafRootsMatchesComputeRoot(t *testing.T) {
	const count = 50_003
	leaves := make([][32]byte, count)
	leafRoots := make([]byte, count*32)
	for i := range leaves {
		for j := range leaves[i] {
			leaves[i][j] = byte(i*31 + j)
		}
		copy(leafRoots[i*32:(i+1)*32], leaves[i][:])
	}
	computeLeaf := func(index int, out []byte) {
		copy(out, leaves[index][:])
	}
	limit := uint64(1_000_000)
	serial := &MerkleTree{}
	serial.Initialize(count, OptimalMaxTreeCacheDepth, computeLeaf, &limit)
	parallel, root := BuildFromLeafRoots(leafRoots, count, OptimalMaxTreeCacheDepth, &limit, 4, computeLeaf)
	require.Equal(t, serial.ComputeRoot(), root)
	want, err := MerkleizeVector(append([][32]byte(nil), leaves...), limit)
	require.NoError(t, err)
	require.Equal(t, want[:], root[:])

	leaves[0][0]++
	serial.MarkLeafAsDirty(0)
	parallel.MarkLeafAsDirty(0)
	require.Equal(t, serial.ComputeRoot(), parallel.ComputeRoot())
}
