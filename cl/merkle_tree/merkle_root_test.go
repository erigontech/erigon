package merkle_tree_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/phase1/core/state/state_encoding"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

func TestEmptyArraysRoot(t *testing.T) {
	expected := libcommon.HexToHash("df6af5f5bbdb6be9ef8aa618e4bf8073960867171e29676f8b284dea6a08a85e")
	root, err := merkle_tree.ArraysRoot([][32]byte{}, state_encoding.StateRootsLength)
	require.NoError(t, err)
	require.Equal(t, expected, libcommon.Hash(root))
}
