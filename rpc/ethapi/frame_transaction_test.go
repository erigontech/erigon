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

package ethapi

import (
	"encoding/json"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestFrameRPCTransaction(t *testing.T) {
	tx := &types.FrameTransaction{NonceKeys: nil, ChainID: *uint256.NewInt(1), SenderAddress: common.Address{1}, Frames: []types.Frame{{Mode: 1, Flags: 3}}, Signatures: []types.FrameSignature{{Signature: []byte{1}}}}
	result := NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil, nil, 0)
	data, err := json.Marshal(result)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &fields))
	require.JSONEq(t, `[]`, string(fields["nonceKeys"]))
	require.Contains(t, fields, "frames")
	require.Contains(t, fields, "signatures")
	require.NotContains(t, fields, "maxFeePerGas")
	require.NotContains(t, fields, "maxFeePerBlobGas")
	require.JSONEq(t, `"0x0"`, string(fields["gasPrice"]))
	require.JSONEq(t, `"0x0"`, string(fields["maxFee"]))
}

func TestFrameRPCGasPriceMaxTip(t *testing.T) {
	tx := &types.FrameTransaction{Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth}}}
	tx.Fees.TipCap.SetAllOne()
	tx.Fees.MaxFee.SetUint64(13 * tx.GetGasLimit())
	result := NewRPCTransaction(tx, common.Hash{1}, 1, 1, 0, uint256.NewInt(10), nil, tx.GetGasLimit())
	require.Equal(t, uint256.NewInt(13), (*uint256.Int)(result.GasPrice))
}

func TestFrameRPCGasPriceUnaffordable(t *testing.T) {
	tx := &types.FrameTransaction{Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth}}}
	tx.Fees.MaxFee.SetUint64(tx.GetGasLimit() - 1)
	for _, gasUsed := range []uint64{0, tx.GetGasLimit()} {
		result := NewRPCTransaction(tx, common.Hash{1}, 1, 1, 0, uint256.NewInt(1), nil, gasUsed)
		data, err := json.Marshal(result)
		require.NoError(t, err)
		require.NotContainsf(t, string(data), `"gasPrice"`, "gas used: %d", gasUsed)
	}
}

func TestFrameRPCBlockGasPriceWithBlob(t *testing.T) {
	tx := &types.FrameTransaction{Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth}}, BlobVersionedHashes: []common.Hash{{1}}}
	tx.Fees.TipCap.SetAllOne()
	tx.Fees.MaxFee.SetUint64(10*tx.GetGasLimit() + tx.GetBlobGas())
	excess := uint64(0)
	header := &types.Header{Number: *uint256.NewInt(1), BaseFee: uint256.NewInt(10), ExcessBlobGas: &excess}
	ordinary := types.NewTransaction(0, common.Address{}, nil, 21_000, uint256.NewInt(11), nil)
	block := types.NewBlockFromStorage(common.Hash{1}, header, types.Transactions{ordinary, tx}, nil, nil, nil)
	result := RPCMarshalBlockWithFramePrices(block, true, true, uint256.NewInt(1), types.Receipts{nil, {GasUsed: tx.GetGasLimit() / 2}})
	transactions := result.Transactions.([]*RPCTransaction)
	require.Equal(t, uint256.NewInt(11), (*uint256.Int)(transactions[0].GasPrice))
	require.Equal(t, uint256.NewInt(20), (*uint256.Int)(transactions[1].GasPrice))
	estimated := RPCMarshalBlockWithFramePrices(block, true, true, uint256.NewInt(1), nil).Transactions.([]*RPCTransaction)
	require.Equal(t, uint256.NewInt(10), (*uint256.Int)(estimated[1].GasPrice))
}
