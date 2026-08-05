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

package merkle_tree_test

import (
	_ "embed"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

//go:embed testdata/serialized.ssz_snappy
var beaconState []byte

func TestHashTreeRoot(t *testing.T) {
	bs := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(bs, beaconState, int(clparams.DenebVersion)))
	root, err := bs.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0x9f684cf34c4ac8eb9056051f93498c552b59de6b0977c453ee099be68e58d90c"))
}

func TestHashTreeRootEmptySchema(t *testing.T) {
	_, err := merkle_tree.HashTreeRoot()
	require.Error(t, err)
}

func TestProgressiveContainerRootUnsupportedTypeMessage(t *testing.T) {
	require.PanicsWithValue(t, "Can't create TreeRoot: unsupported type string at index 0", func() {
		_, _ = merkle_tree.ProgressiveContainerRootAll("bad")
	})
}

func TestProgressiveContainerRootInactiveFieldVector(t *testing.T) {
	first := common.Hash{1}
	third := common.Hash{2}
	root, err := merkle_tree.ProgressiveContainerRoot([]bool{true, false, true}, first[:], third[:])
	require.NoError(t, err)
	require.Equal(t, common.HexToHash("0x3a6584864e28437da67deac288c46c9b60cee55880b19b12cfe68a7d1d5bc491"), common.Hash(root))
}

func TestHashTreeRootTxs(t *testing.T) {
	txs := [][]byte{
		{1, 2, 3},
		{1, 2, 3},
		{1, 2, 3},
	}
	root, err := merkle_tree.TransactionsListRoot(txs)
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0x987269bc1075122edff32bfc38479757103cee5c1ed6e990de7ffee85b5dd18a"))
}

func TestProgressiveContainerProofRejectsOversizedSchema(t *testing.T) {
	schema := make([]any, 257)
	for i := range schema {
		schema[i] = uint64(i)
	}

	_, err := merkle_tree.ProgressiveContainerProofAll(0, schema...)
	require.Error(t, err)
}
