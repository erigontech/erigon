package diffstorage

import (
	"math"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/base_encoding"
	"github.com/stretchr/testify/require"
)

// 1 -> 2 -> 3 -> 4 -> 5
//
//	    	 |
//			 --> 6
func TestDiffStorage(t *testing.T) {
	// decleare 5 nodes
	node1 := libcommon.Hash{1}
	node2 := libcommon.Hash{2}
	node3 := libcommon.Hash{3}
	node4 := libcommon.Hash{4}
	node5 := libcommon.Hash{5}
	node6 := libcommon.Hash{6}

	node1Content := []uint64{1, 2, 3, 4, 5}
	node2Content := []uint64{1, 2, 3, 4, 5, 6}
	node3Content := []uint64{1, 2, 3, 4, 5, 2, 7}
	node4Content := []uint64{1, 2, 3, 4, 5, 2, 7, 8}
	node5Content := []uint64{1, 6, 8, 4, 5, 2, 7, 8, 9}
	node6Content := []uint64{1, 2, 3, 4, 5, 2, 7, 10}

	exp1 := solid.NewUint64ListSSZFromSlice(math.MaxInt, node1Content)
	exp2 := solid.NewUint64ListSSZFromSlice(math.MaxInt, node2Content)
	exp3 := solid.NewUint64ListSSZFromSlice(math.MaxInt, node3Content)
	exp4 := solid.NewUint64ListSSZFromSlice(math.MaxInt, node4Content)
	exp5 := solid.NewUint64ListSSZFromSlice(math.MaxInt, node5Content)
	exp6 := solid.NewUint64ListSSZFromSlice(math.MaxInt, node6Content)

	enc1, err := exp1.EncodeSSZ(nil)
	require.NoError(t, err)
	enc2, err := exp2.EncodeSSZ(nil)
	require.NoError(t, err)
	enc3, err := exp3.EncodeSSZ(nil)
	require.NoError(t, err)
	enc4, err := exp4.EncodeSSZ(nil)
	require.NoError(t, err)
	enc5, err := exp5.EncodeSSZ(nil)
	require.NoError(t, err)
	enc6, err := exp6.EncodeSSZ(nil)
	require.NoError(t, err)

	diffStorage := NewChainDiffStorage(base_encoding.ComputeCompressedSerializedUint64ListDiff, base_encoding.ApplyCompressedSerializedUint64ListDiff)
	diffStorage.Insert(node1, libcommon.Hash{}, nil, enc1, true)
	diffStorage.Insert(node2, node1, enc1, enc2, false)
	diffStorage.Insert(node3, node2, enc2, enc3, false)
	diffStorage.Insert(node4, node3, enc3, enc4, false)
	diffStorage.Insert(node5, node4, enc4, enc5, false)
	diffStorage.Insert(node6, node2, enc2, enc6, false)

	d1, err := diffStorage.Get(node1)
	require.NoError(t, err)
	require.Equal(t, enc1, d1)

	d2, err := diffStorage.Get(node2)
	require.NoError(t, err)
	require.Equal(t, enc2, d2)

	d3, err := diffStorage.Get(node3)
	require.NoError(t, err)
	require.Equal(t, enc3, d3)

	d4, err := diffStorage.Get(node4)
	require.NoError(t, err)
	require.Equal(t, enc4, d4)

	d5, err := diffStorage.Get(node5)
	require.NoError(t, err)
	require.Equal(t, enc5, d5)

	d6, err := diffStorage.Get(node6)
	require.NoError(t, err)
	require.Equal(t, enc6, d6)
}
