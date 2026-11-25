package merkle_tree_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

func getExpectedRoot(t *testing.T, testBuffer []byte) common.Hash {
	var root common.Hash
	require.NoError(t, merkle_tree.MerkleRootFromFlatLeaves(testBuffer, root[:]))
	return root
}

func getExpectedRootWithLimit(t *testing.T, testBuffer []byte, limit int) common.Hash {
	var root common.Hash
	require.NoError(t, merkle_tree.MerkleRootFromFlatLeavesWithLimit(testBuffer, root[:], uint64(limit)))
	return root
}

func TestPowerOf2MerkleTree(t *testing.T) {
	mt := merkle_tree.MerkleTree{}
	testBuffer := make([]byte, 4*length.Hash)
	testBuffer[0] = 1
	testBuffer[32] = 2
	testBuffer[64] = 3
	testBuffer[96] = 9
	mt.Initialize(4, 6, func(idx int, out []byte) {
		copy(out, testBuffer[idx*length.Hash:(idx+1)*length.Hash])
	}, nil)
	expectedRoot1 := getExpectedRoot(t, testBuffer)
	require.Equal(t, expectedRoot1, mt.ComputeRoot())
	testBuffer[64] = 4
	require.Equal(t, expectedRoot1, mt.ComputeRoot())
	mt.MarkLeafAsDirty(2)
	expectedRoot2 := getExpectedRoot(t, testBuffer)
	require.Equal(t, expectedRoot2, mt.ComputeRoot())
	testBuffer[64] = 3
	mt.MarkLeafAsDirty(2)
	require.Equal(t, expectedRoot1, mt.ComputeRoot())
}

func TestMerkleTreeAppendLeaf(t *testing.T) {
	mt := merkle_tree.MerkleTree{}
	testBuffer := make([]byte, 4*length.Hash)
	testBuffer[0] = 1
	testBuffer[32] = 2
	testBuffer[64] = 3
	testBuffer[96] = 9
	mt.Initialize(4, 6, func(idx int, out []byte) {
		copy(out, testBuffer[idx*length.Hash:(idx+1)*length.Hash])
	}, nil)
	// Test AppendLeaf
	mt.AppendLeaf()
	testBuffer = append(testBuffer, make([]byte, 4*length.Hash)...) //nolint:makezero
	testBuffer[128] = 5
	expectedRoot1 := getExpectedRoot(t, testBuffer)
	require.Equal(t, expectedRoot1, mt.ComputeRoot())
	// adding 3 more empty leaves should not change the root
	mt.AppendLeaf()
	mt.AppendLeaf()
	mt.AppendLeaf()
	require.Equal(t, expectedRoot1, mt.ComputeRoot())
}

func TestMerkleTreeRootEmpty(t *testing.T) {
	mt := merkle_tree.MerkleTree{}
	mt.Initialize(0, 6, func(idx int, out []byte) {
	}, nil)
	require.Equal(t, "0x0000000000000000000000000000000000000000000000000000000000000000", mt.ComputeRoot().String())
}

func TestMerkleTreeRootSingleElement(t *testing.T) {
	mt := merkle_tree.MerkleTree{}
	testBuffer := make([]byte, length.Hash)
	testBuffer[0] = 1
	mt.Initialize(1, 6, func(idx int, out []byte) {
		copy(out, testBuffer)
	}, nil)
	require.Equal(t, "0x0100000000000000000000000000000000000000000000000000000000000000", mt.ComputeRoot().String())
}

func TestMerkleTreeAppendLeafWithLowMaxDepth(t *testing.T) {
	mt := merkle_tree.MerkleTree{}
	testBuffer := make([]byte, 4*length.Hash)
	testBuffer[0] = 1
	testBuffer[32] = 2
	testBuffer[64] = 3
	testBuffer[96] = 9
	mt.Initialize(4, 2, func(idx int, out []byte) {
		copy(out, testBuffer[idx*length.Hash:(idx+1)*length.Hash])
	}, nil)
	// Test AppendLeaf
	mt.AppendLeaf()
	testBuffer = append(testBuffer, make([]byte, 4*length.Hash)...) //nolint:makezero
	testBuffer[128] = 5
	expectedRoot := getExpectedRoot(t, testBuffer)
	require.Equal(t, expectedRoot, mt.ComputeRoot())
	// adding 3 more empty leaves should not change the root
	mt.AppendLeaf()
	mt.AppendLeaf()
	mt.AppendLeaf()
	require.Equal(t, expectedRoot, mt.ComputeRoot())
}

func TestMerkleTree17Elements(t *testing.T) {
	mt := merkle_tree.MerkleTree{}
	testBuffer := make([]byte, 17*length.Hash)
	testBuffer[0] = 1
	testBuffer[32] = 2
	testBuffer[64] = 3
	testBuffer[96] = 9
	testBuffer[128] = 5
	mt.Initialize(17, 2, func(idx int, out []byte) {
		copy(out, testBuffer[idx*length.Hash:(idx+1)*length.Hash])
	}, nil)
	// Test AppendLeaf
	expectedRoot := getExpectedRoot(t, testBuffer)
	require.Equal(t, expectedRoot, mt.ComputeRoot())
}

func TestMerkleTreeAppendLeafWithLowMaxDepthAndLimitAndTestWR(t *testing.T) {
	mt := merkle_tree.MerkleTree{}
	testBuffer := make([]byte, 4*length.Hash)
	testBuffer[0] = 1
	testBuffer[32] = 2
	testBuffer[64] = 3
	testBuffer[96] = 9
	lm := uint64(1 << 12)
	mt.Initialize(4, 2, func(idx int, out []byte) {
		copy(out, testBuffer[idx*length.Hash:(idx+1)*length.Hash])
	}, &lm)
	// Test AppendLeaf
	mt.AppendLeaf()
	testBuffer = append(testBuffer, make([]byte, 4*length.Hash)...) //nolint:makezero
	testBuffer[128] = 5
	expectedRoot := getExpectedRootWithLimit(t, testBuffer, int(lm))
	require.Equal(t, expectedRoot, mt.ComputeRoot())
}

func TestMerkleTreeBaseLevel(t *testing.T) {
	const base = 5
	want := func(leaves [][32]byte, limit uint64) common.Hash {
		var root [32]byte
		require.NoError(t, merkle_tree.MerkleRootFromFlatFromIntermediateLevelWithLimit(flatChunks(leaves), root[:], int(limit)<<base, base))
		return root
	}
	for _, limit := range []uint64{1, 4, 16, 5000} {
		for count := 1; count <= min(int(limit), 40); count++ {
			t.Run(fmt.Sprintf("limit%d_leaves%d", limit, count), func(t *testing.T) {
				leaves := progressiveTestChunks(count)
				tree := &merkle_tree.MerkleTree{}
				tree.Initialize(count, merkle_tree.OptimalMaxTreeCacheDepth, func(idx int, out []byte) { copy(out, leaves[idx][:]) }, &limit)
				tree.SetBaseLevel(base)
				require.Equal(t, want(leaves, limit), tree.ComputeRoot())

				leaves[count-1][0] ^= 1
				tree.MarkLeafAsDirty(count - 1)
				require.Equal(t, want(leaves, limit), tree.ComputeRoot())

				if uint64(count) < limit {
					leaves = append(leaves, [32]byte{7})
					tree.AppendLeaf()
					tree.MarkLeafAsDirty(count)
					require.Equal(t, want(leaves, limit), tree.ComputeRoot())
				}

				var copied merkle_tree.MerkleTree
				tree.CopyInto(&copied)
				copied.SetComputeLeafFn(func(idx int, out []byte) { copy(out, leaves[idx][:]) })
				leaves[0][0] ^= 1
				copied.MarkLeafAsDirty(0)
				require.Equal(t, want(leaves, limit), copied.ComputeRoot())
			})
		}
	}
}

func TestMerkleTreeBaseLevelRequiresLimit(t *testing.T) {
	tree := &merkle_tree.MerkleTree{}
	tree.Initialize(1, merkle_tree.OptimalMaxTreeCacheDepth, func(int, []byte) {}, nil)
	require.Panics(t, func() { tree.SetBaseLevel(5) })
	require.NotPanics(t, func() { tree.SetBaseLevel(0) })
}
