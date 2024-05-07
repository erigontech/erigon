package solid_test

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
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

	out, err := slice.HashListSSZ()
	require.NoError(t, err)
	require.EqualValues(t, common.HexToHash("eb8cec5eaec74a32e8b9b56cc42f7627cef722f81081ead786c97a4df1c8be5d"), out)

}

func TestUint64SliceCopyTo(t *testing.T) {
	num := 1000
	set := solid.NewUint64ListSSZ(100_000)
	set2 := solid.NewUint64ListSSZ(100_000)
	for i := 0; i < num; i++ {
		set.Append(uint64(i))
		set.HashSSZ()
	}
	firstHash, err := set.HashSSZ()
	require.NoError(t, err)

	set.CopyTo(set2)
	secondHash, err := set2.HashSSZ()
	require.NoError(t, err)

	require.Equal(t, firstHash, secondHash)
}
