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

package chainreader

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/types"
)

// assembledBlockStub answers GetAssembledBlock with a fixed result and panics on anything else, so
// a test can drive the one boundary it cares about.
type assembledBlockStub struct {
	execmodule.ExecutionModule
	result execmodule.AssembledBlockResult
}

func (s assembledBlockStub) GetAssembledBlock(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
	return s.result, nil
}

func TestGetAssembledBlockDistinguishesAnUnknownIdFromAnEmptyOne(t *testing.T) {
	unknown := ChainReaderWriterEth1{executionModule: assembledBlockStub{result: execmodule.AssembledBlockResult{Unknown: true}}}
	_, _, _, _, err := unknown.GetAssembledBlock(t.Context(), 1)

	// Nothing will ever arrive for an id with no builder behind it. Reporting that as an ordinary
	// empty result leaves a caller polling it for the rest of the slot.
	require.ErrorIs(t, err, ErrUnknownPayload)

	busy := ChainReaderWriterEth1{executionModule: assembledBlockStub{result: execmodule.AssembledBlockResult{Busy: true}}}
	_, _, _, _, err = busy.GetAssembledBlock(t.Context(), 1)
	require.ErrorIs(t, err, ErrExecutionBusy)

	// A builder that simply has nothing yet is neither: the caller should keep waiting.
	building := ChainReaderWriterEth1{executionModule: assembledBlockStub{}}
	block, _, _, _, err := building.GetAssembledBlock(t.Context(), 1)
	require.NoError(t, err)
	require.Nil(t, block)
}

func TestGetAssembledBlockCarriesGloasPayloadFields(t *testing.T) {
	sidecar := types.NewBlockAccessListSidecar(types.BlockAccessList{{Address: common.HexToAddress("0x01")}})
	balHash, err := sidecar.Hash()
	require.NoError(t, err)
	balBytes, err := sidecar.Bytes()
	require.NoError(t, err)

	slot := uint64(4242)
	blobGasUsed, excessBlobGas := uint64(0), uint64(0)
	beaconRoot := common.HexToHash("0xbeac")
	requestsHash := types.FlatRequests{}.Hash()
	withdrawalsHash := types.DeriveSha(types.Withdrawals{})
	header := &types.Header{
		ParentHash:            common.HexToHash("0x01"),
		UncleHash:             empty.UncleHash,
		Difficulty:            *merge.ProofOfStakeDifficulty,
		Nonce:                 merge.ProofOfStakeNonce,
		GasLimit:              30_000_000,
		Time:                  1_000,
		Extra:                 []byte{},
		BaseFee:               uint256.NewInt(7),
		TxHash:                empty.RootHash,
		ReceiptHash:           empty.RootHash,
		WithdrawalsHash:       &withdrawalsHash,
		BlobGasUsed:           &blobGasUsed,
		ExcessBlobGas:         &excessBlobGas,
		ParentBeaconBlockRoot: &beaconRoot,
		RequestsHash:          requestsHash,
		BlockAccessListHash:   &balHash,
		SlotNumber:            &slot,
	}
	header.Number.SetUint64(10)
	block := types.NewBlock(header, nil, nil, nil, types.Withdrawals{}, sidecar)

	reader := ChainReaderWriterEth1{executionModule: assembledBlockStub{result: execmodule.AssembledBlockResult{
		Block:      &types.BlockWithReceipts{Block: block, Requests: types.FlatRequests{}},
		BlockValue: uint256.NewInt(0),
	}}}
	assembled, _, _, _, err := reader.GetAssembledBlock(t.Context(), 1)
	require.NoError(t, err)
	require.Equal(t, slot, assembled.SlotNumber)
	require.NotNil(t, assembled.BlockAccessList)
	require.Equal(t, balBytes, assembled.BlockAccessList.Bytes())

	payload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	payload.ParentHash = assembled.ParentHash
	payload.FeeRecipient = assembled.FeeRecipient
	payload.StateRoot = assembled.StateRoot
	payload.ReceiptsRoot = assembled.ReceiptsRoot
	payload.LogsBloom = assembled.LogsBloom
	payload.PrevRandao = assembled.PrevRandao
	payload.BlockNumber = assembled.BlockNumber
	payload.GasLimit = assembled.GasLimit
	payload.GasUsed = assembled.GasUsed
	payload.Time = assembled.Time
	payload.Extra = assembled.Extra
	payload.BaseFeePerGas = assembled.BaseFeePerGas
	payload.BlockHash = assembled.BlockHash
	payload.Transactions = assembled.Transactions
	payload.Withdrawals = assembled.Withdrawals
	payload.BlobGasUsed = assembled.BlobGasUsed
	payload.ExcessBlobGas = assembled.ExcessBlobGas
	payload.BlockAccessList = assembled.BlockAccessList
	payload.SlotNumber = assembled.SlotNumber
	derived, err := payload.ComputeBlockHash(&beaconRoot, *requestsHash, nil)
	require.NoError(t, err)
	require.Equal(t, block.Hash(), derived)
}
