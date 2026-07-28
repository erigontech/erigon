// Copyright 2026 The Erigon Authors
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

package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/hexutil"
)

const witnessParserFixture = `{
  "blocks": [
    {"blocknumber": "1", "executionWitness": {"state": ["0x01", "0x0203"], "codes": ["0xaa"], "headers": ["0xf901"]}},
    {"blocknumber": "", "expectException": "TR_IntrinsicGas", "executionWitness": {"state": ["0xff"], "codes": [], "headers": []}},
    {"blocknumber": "2"},
    {"blocknumber": "0x10", "executionWitness": {"state": [], "codes": [], "headers": []}},
    {"blocknumber": "abc", "executionWitness": {"state": ["0x01"], "codes": [], "headers": []}}
  ]
}`

func TestWitnessBlockTestParser(t *testing.T) {
	var wbt WitnessBlockTest
	require.NoError(t, wbt.UnmarshalJSON([]byte(witnessParserFixture)))

	require.Equal(t, 5, wbt.NumBlocks())

	t.Run("block with witness", func(t *testing.T) {
		w := wbt.ExpectedWitnessForBlock(0)
		require.NotNil(t, w)
		require.Equal(t, []hexutil.Bytes{{0x01}, {0x02, 0x03}}, w.State)
		require.Equal(t, []hexutil.Bytes{{0xaa}}, w.Codes)
		require.Equal(t, []hexutil.Bytes{{0xf9, 0x01}}, w.Headers)

		n, ok := wbt.BlockNumberForBlock(0)
		require.True(t, ok)
		require.Equal(t, uint64(1), n)
		require.False(t, wbt.BlockExpectsException(0))
	})

	t.Run("exception block keeps witness but has no number", func(t *testing.T) {
		require.NotNil(t, wbt.ExpectedWitnessForBlock(1))
		require.True(t, wbt.BlockExpectsException(1))
		_, ok := wbt.BlockNumberForBlock(1)
		require.False(t, ok)
	})

	t.Run("block without witness", func(t *testing.T) {
		require.Nil(t, wbt.ExpectedWitnessForBlock(2))
		n, ok := wbt.BlockNumberForBlock(2)
		require.True(t, ok)
		require.Equal(t, uint64(2), n)
		require.False(t, wbt.BlockExpectsException(2))
	})

	t.Run("hex block number", func(t *testing.T) {
		n, ok := wbt.BlockNumberForBlock(3)
		require.True(t, ok)
		require.Equal(t, uint64(16), n)
	})

	t.Run("unparseable block number", func(t *testing.T) {
		_, ok := wbt.BlockNumberForBlock(4)
		require.False(t, ok)
	})

	t.Run("out of range indices", func(t *testing.T) {
		require.Nil(t, wbt.ExpectedWitnessForBlock(99))
		require.Nil(t, wbt.ExpectedWitnessForBlock(-1))
		for _, i := range []int{-1, 99} {
			_, ok := wbt.BlockNumberForBlock(i)
			require.False(t, ok)
			require.False(t, wbt.BlockExpectsException(i))
		}
	})
}
