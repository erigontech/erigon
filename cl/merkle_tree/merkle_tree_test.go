package merkle_tree_test

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/common/length"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/stretchr/testify/require"
)

func getExpectedRoot(testBuffer []byte) common.Hash {
	var root common.Hash
	merkle_tree.MerkleRootFromFlatLeaves(testBuffer, root[:])
	return root
}

func getExpectedRootWithLimit(testBuffer []byte, limit int) common.Hash {
	var root common.Hash
	merkle_tree.MerkleRootFromFlatLeavesWithLimit(testBuffer, root[:], uint64(limit))
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
	expectedRoot1 := getExpectedRoot(testBuffer)
	require.Equal(t, mt.ComputeRoot(), expectedRoot1)
	testBuffer[64] = 4
	require.Equal(t, mt.ComputeRoot(), expectedRoot1)
	mt.MarkLeafAsDirty(2)
	expectedRoot2 := getExpectedRoot(testBuffer)
	require.Equal(t, mt.ComputeRoot(), expectedRoot2)
	testBuffer[64] = 3
	mt.MarkLeafAsDirty(2)
	require.Equal(t, mt.ComputeRoot(), expectedRoot1)

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
	testBuffer = append(testBuffer, make([]byte, 4*length.Hash)...)
	testBuffer[128] = 5
	expectedRoot1 := getExpectedRoot(testBuffer)
	require.Equal(t, mt.ComputeRoot(), expectedRoot1)
	// adding 3 more empty leaves should not change the root
	mt.AppendLeaf()
	mt.AppendLeaf()
	mt.AppendLeaf()
	require.Equal(t, mt.ComputeRoot(), expectedRoot1)
}

func TestMerkleTreeRootEmpty(t *testing.T) {
	mt := merkle_tree.MerkleTree{}
	mt.Initialize(0, 6, func(idx int, out []byte) {
		return
	}, nil)
	require.Equal(t, mt.ComputeRoot().String(), "0x0000000000000000000000000000000000000000000000000000000000000000")
}

func TestMerkleTreeRootSingleElement(t *testing.T) {
	mt := merkle_tree.MerkleTree{}
	testBuffer := make([]byte, length.Hash)
	testBuffer[0] = 1
	mt.Initialize(1, 6, func(idx int, out []byte) {
		copy(out, testBuffer)
	}, nil)
	require.Equal(t, mt.ComputeRoot().String(), "0x0100000000000000000000000000000000000000000000000000000000000000")
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
	testBuffer = append(testBuffer, make([]byte, 4*length.Hash)...)
	testBuffer[128] = 5
	expectedRoot := getExpectedRoot(testBuffer)
	require.Equal(t, mt.ComputeRoot(), expectedRoot)
	// adding 3 more empty leaves should not change the root
	mt.AppendLeaf()
	mt.AppendLeaf()
	mt.AppendLeaf()
	require.Equal(t, mt.ComputeRoot(), expectedRoot)
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
	expectedRoot := getExpectedRoot(testBuffer)
	require.Equal(t, mt.ComputeRoot(), expectedRoot)
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
	testBuffer = append(testBuffer, make([]byte, 4*length.Hash)...)
	testBuffer[128] = 5
	expectedRoot := getExpectedRootWithLimit(testBuffer, int(lm))
	require.Equal(t, mt.ComputeRoot(), expectedRoot)
	// adding 3 more empty leaves should not change the root
	mt.AppendLeaf()
	mt.AppendLeaf()
	mt.AppendLeaf()

	var buffer bytes.Buffer
	require.NoError(t, mt.WriteMerkleTree(&buffer))

	mt2 := merkle_tree.MerkleTree{}
	require.NoError(t, mt2.ReadMerkleTree(&buffer))
	require.Equal(t, mt.ComputeRoot(), expectedRoot)
}
