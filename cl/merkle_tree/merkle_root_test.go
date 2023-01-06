package merkle_tree_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state/state_encoding"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestEmptyArraysRoot(t *testing.T) {
	expected := common.HexToHash("df6af5f5bbdb6be9ef8aa618e4bf8073960867171e29676f8b284dea6a08a85e")
	root, err := merkle_tree.ArraysRoot([][32]byte{}, state_encoding.StateRootsLength)
	require.NoError(t, err)
	require.Equal(t, expected, common.Hash(root))
}

func TestEmptyArraysWithLengthRoot(t *testing.T) {
	expected := common.HexToHash("0xf770287da731841c38eb035da016bd2daad53bf0bca607461c0685b0ea54c5f9")
	roots := [][32]byte{
		common.BytesToHash([]byte{1}),
		common.BytesToHash([]byte{2}),
		common.BytesToHash([]byte{3}),
		common.BytesToHash([]byte{4}),
		common.BytesToHash([]byte{5}),
		common.BytesToHash([]byte{6}),
		common.BytesToHash([]byte{7}),
		common.BytesToHash([]byte{8}),
	}
	root, err := merkle_tree.ArraysRootWithLimit(roots, 8192)
	require.NoError(t, err)
	require.Equal(t, expected, common.Hash(root))
}
