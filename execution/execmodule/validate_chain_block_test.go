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

package execmodule_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

func TestValidateChainExecutesTheGivenBlock(t *testing.T) {
	m, privKey, _ := newMetricsTester(t)
	pack, err := m.GenerateChain(1, sendTo(t, m, privKey, common.Address{0xaa}, 1))
	require.NoError(t, err)
	enc, err := rlp.EncodeToBytes(pack.Blocks[0])
	require.NoError(t, err)
	block := new(types.Block)
	require.NoError(t, rlp.DecodeBytes(enc, block))
	txn := block.Transactions()[0]
	_, ok := txn.GetSender()
	require.False(t, ok)

	status, err := m.InsertBlocks(t.Context(), []*types.Block{block})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := m.ValidateChain(t.Context(), block)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus)

	_, ok = txn.GetSender()
	require.True(t, ok, "validation executed a copy of the block instead of the transactions it was given")
}
