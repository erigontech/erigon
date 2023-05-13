package merkle_tree_test

import (
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/state_encoding"
	"testing"

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

func TestEmptyArraysWithLengthRoot(t *testing.T) {
	expected := libcommon.HexToHash("0xf770287da731841c38eb035da016bd2daad53bf0bca607461c0685b0ea54c5f9")
	roots := [][32]byte{
		libcommon.BytesToHash([]byte{1}),
		libcommon.BytesToHash([]byte{2}),
		libcommon.BytesToHash([]byte{3}),
		libcommon.BytesToHash([]byte{4}),
		libcommon.BytesToHash([]byte{5}),
		libcommon.BytesToHash([]byte{6}),
		libcommon.BytesToHash([]byte{7}),
		libcommon.BytesToHash([]byte{8}),
	}
	root, err := merkle_tree.ArraysRootWithLimit(roots, 8192)
	require.NoError(t, err)
	require.Equal(t, expected, libcommon.Hash(root))
}

func TestUint64ListRootWithLimit(t *testing.T) {
	expected := libcommon.HexToHash("0xfbe583f8fbcc3683d98c12ae969e93aaa5ac472e15422c14759cb7f3ef60673c")
	nums := []uint64{1, 2, 4, 5, 2, 5, 6, 7, 1, 4, 3, 5, 100, 6, 64, 2}
	root, err := merkle_tree.Uint64ListRootWithLimit(nums, 274877906944)
	require.NoError(t, err)
	require.Equal(t, expected, libcommon.Hash(root))
}

func TestParticipationBitsRoot(t *testing.T) {
	expected := libcommon.HexToHash("0x8e6653ba3656afddaf5e6c69c149e63a2e26ff91a2e361b3c40b11f08c039572")
	bits := []byte{1, 2, 4, 5, 2, 5, 6, 7, 1, 4, 3, 5, 100, 6, 64, 2}
	root, err := merkle_tree.BitlistRootWithLimitForState(bits, 1099511627776)
	require.NoError(t, err)
	require.Equal(t, expected, libcommon.Hash(root))
}
