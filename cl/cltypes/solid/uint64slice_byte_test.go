package solid_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUint64SliceBasic(t *testing.T) {
	slice := solid.NewUint64Slice(8)

	slice.Append(3)
	slice.Append(2)
	slice.Append(1)

	assert.EqualValues(t, 3, slice.Get(0))
	assert.EqualValues(t, 2, slice.Get(1))
	assert.EqualValues(t, 1, slice.Get(2))

	out := [32]byte{}
	err := slice.HashSSZTo(out[:])
	require.NoError(t, err)

	nums := []uint64{3, 2, 1}
	root, err := merkle_tree.Uint64ListRootWithLimit(nums, 2)
	require.NoError(t, err)
	require.EqualValues(t, root, out)

}
