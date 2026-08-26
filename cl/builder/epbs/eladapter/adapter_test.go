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

package eladapter

import (
	"bytes"
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type assembledBlockModule struct {
	execmodule.ExecutionModule
	result execmodule.AssembledBlockResult
}

func (m assembledBlockModule) GetAssembledBlock(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
	return m.result, nil
}

func TestAdapterGetPayloadPreservesBlockAccessList(t *testing.T) {
	t.Parallel()

	bal := types.BlockAccessList{{Address: accounts.InternAddress(common.Address{19: 1})}}
	balSidecar := types.NewBlockAccessListSidecar(bal)
	balHash, err := balSidecar.Hash()
	require.NoError(t, err)
	parentRoot := common.Hash{31: 1}
	requestsHash := common.Hash{31: 2}
	slot := uint64(10)
	zero := uint64(0)
	header := &types.Header{
		Number:                *uint256.NewInt(1),
		BaseFee:               uint256.NewInt(1),
		GasLimit:              30_000_000,
		Time:                  1,
		ParentBeaconBlockRoot: &parentRoot,
		RequestsHash:          &requestsHash,
		BlockAccessListHash:   &balHash,
		SlotNumber:            &slot,
		BlobGasUsed:           &zero,
		ExcessBlobGas:         &zero,
	}
	block := types.NewBlock(header, nil, nil, nil, []*types.Withdrawal{}, balSidecar)
	module := assembledBlockModule{result: execmodule.AssembledBlockResult{
		Block:      &types.BlockWithReceipts{Block: block},
		BlockValue: uint256.NewInt(1),
	}}
	payload, err := NewAdapter(module, clparams.GloasVersion, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.NoError(t, err)

	wantBAL, err := balSidecar.Bytes()
	require.NoError(t, err)
	require.Equal(t, wantBAL, payload.Eth1Block.BlockAccessList.Bytes())
	decodedBAL, err := types.DecodeBlockAccessListSidecarOwned(bytes.Clone(payload.Eth1Block.BlockAccessList.Bytes()))
	require.NoError(t, err)
	rebuiltHeader, err := payload.Eth1Block.RlpHeader(&parentRoot, requestsHash, decodedBAL)
	require.NoError(t, err)
	require.Equal(t, block.Hash(), rebuiltHeader.Hash())
}
