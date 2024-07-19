// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package solid_test

import (
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes/solid"
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
