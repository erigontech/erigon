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

package engine_types

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestExecutionPayloadToEth1BlockRejectsNilConfig(t *testing.T) {
	t.Parallel()

	for _, version := range []clparams.StateVersion{clparams.Phase0Version, clparams.GloasVersion} {
		t.Run(version.String(), func(t *testing.T) {
			payload := &ExecutionPayload{}
			block, err := payload.ToEth1Block(version, nil)

			require.ErrorContains(t, err, "beacon config is required")
			require.Nil(t, block)
		})
	}
}

func TestExecutionPayloadBlockRoundTrip(t *testing.T) {
	t.Parallel()

	to := common.Address{1}
	txs := []types.Transaction{
		types.NewTransaction(1, to, uint256.NewInt(2), 30_000, uint256.NewInt(3), []byte{4}),
		&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: 5, GasLimit: 50_000, To: &to, Value: *uint256.NewInt(6), Data: []byte{7}},
			ChainID:  *uint256.NewInt(1),
			TipCap:   *uint256.NewInt(8),
			FeeCap:   *uint256.NewInt(9),
		},
	}
	receipts := []*types.Receipt{
		{
			Status:            types.ReceiptStatusSuccessful,
			CumulativeGasUsed: 21_000,
			Logs:              types.Logs{{Address: common.Address{2}, Topics: []common.Hash{{3}}, Data: []byte{4}}},
		},
		{Type: types.DynamicFeeTxType, Status: types.ReceiptStatusSuccessful, CumulativeGasUsed: 42_000},
	}
	withdrawals := types.Withdrawals{
		{Index: 10, Validator: 11, Address: common.Address{12}, Amount: 13},
		{Index: 14, Validator: 15, Address: common.Address{16}, Amount: 17},
	}
	sidecar := types.NewBlockAccessListSidecar(types.BlockAccessList{{Address: common.Address{18}}})
	balHash, err := sidecar.Hash()
	require.NoError(t, err)
	beaconRoot := common.Hash{19}
	requestsHash := types.FlatRequests{}.Hash()
	header := &types.Header{
		ParentHash:            common.Hash{20},
		Coinbase:              common.Address{21},
		Root:                  common.Hash{22},
		Number:                *uint256.NewInt(123),
		Difficulty:            *merge.ProofOfStakeDifficulty,
		Nonce:                 merge.ProofOfStakeNonce,
		GasLimit:              30_000_000,
		GasUsed:               42_000,
		Time:                  1_000,
		Extra:                 []byte{23, 24},
		MixDigest:             common.Hash{25},
		BaseFee:               uint256.MustFromHex("0x10000000000000001"),
		BlobGasUsed:           common.NewUint64(131_072),
		ExcessBlobGas:         common.NewUint64(262_144),
		ParentBeaconBlockRoot: &beaconRoot,
		RequestsHash:          requestsHash,
		BlockAccessListHash:   &balHash,
		SlotNumber:            common.NewUint64(4242),
	}
	block := types.NewBlock(header, txs, nil, receipts, withdrawals, sidecar)

	payload, err := ExecutionPayloadFromBlock(block)
	require.NoError(t, err)
	eth1Block, err := payload.ToEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	require.NoError(t, err)

	derived, err := eth1Block.ComputeBlockHash(&beaconRoot, *requestsHash, nil)
	require.NoError(t, err)
	require.Equal(t, block.Hash(), derived)
	require.Equal(t, block.Hash(), eth1Block.BlockHash)
}

func TestExecutionPayloadFromBlockIncludesCanonicalEmptyBAL(t *testing.T) {
	t.Parallel()

	emptyBAL := []byte{0xc0}
	sidecar, err := types.DecodeBlockAccessListSidecar(emptyBAL)
	require.NoError(t, err)
	baseFee := uint256.NewInt(1_000_000_000)
	emptyBALHash := empty.BlockAccessListHash
	header := &types.Header{
		Number:              *uint256.NewInt(101),
		Time:                1,
		BaseFee:             baseFee,
		GasLimit:            30_000_000,
		BlockAccessListHash: &emptyBALHash,
	}
	block := types.NewBlockWithHeader(header, sidecar)

	payload, err := ExecutionPayloadFromBlock(block)
	require.NoError(t, err)

	require.NotNil(t, payload.BlockAccessList)
	require.Equal(t, hexutil.Bytes(emptyBAL), *payload.BlockAccessList)
}

func TestExecutionPayloadFromBlockReturnsSidecarEncodingError(t *testing.T) {
	t.Parallel()

	balHash := common.Hash{1}
	header := &types.Header{
		Number:              *uint256.NewInt(101),
		BaseFee:             uint256.NewInt(1_000_000_000),
		GasLimit:            30_000_000,
		BlockAccessListHash: &balHash,
	}
	// A nil nested StorageChange is the encoding failure that survives an
	// account list of values.
	sidecar := types.NewBlockAccessListSidecar(types.BlockAccessList{{
		Address: common.Address{2},
		StorageChanges: []types.SlotChanges{{
			Slot:    accounts.InternKey(common.Hash{3}),
			Changes: []*types.StorageChange{nil},
		}},
	}})
	block := types.NewBlockWithHeader(header, sidecar)

	payload, err := ExecutionPayloadFromBlock(block)

	require.Nil(t, payload)
	require.ErrorContains(t, err, "encode block access list")
}
