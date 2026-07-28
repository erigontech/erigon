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

package ethutils

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

// MarshalReceipt must reuse a Bloom the receipt already carries instead of
// hashing the logs again on every call, and compute it only when unset.
func TestMarshalReceiptReusesReceiptBloom(t *testing.T) {
	var preset types.Bloom
	preset[0] = 0x80
	preset[types.BloomByteLength-1] = 0x01
	receipt := &types.Receipt{
		Status:            types.ReceiptStatusSuccessful,
		CumulativeGasUsed: 21_000,
		Bloom:             preset,
		Logs: []*types.Log{{
			Address: common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Topics:  []common.Hash{common.HexToHash("0x01")},
		}},
		BlockNumber:      uint256.NewInt(1),
		TransactionIndex: 0,
	}
	txn := &types.LegacyTx{
		CommonTx: types.CommonTx{GasLimit: 21_000},
		GasPrice: *uint256.NewInt(1),
	}
	header := &types.Header{Number: *uint256.NewInt(1)}
	config := chain.TestChainBerlinConfig

	fields := MarshalReceipt(receipt, txn, config, header, common.HexToHash("0xbeef"), false, false)
	assert.Equal(t, preset, fields["logsBloom"])

	receipt.Bloom = types.Bloom{}
	fields = MarshalReceipt(receipt, txn, config, header, common.HexToHash("0xbeef"), false, false)
	assert.Equal(t, types.CreateBloom(types.Receipts{receipt}), fields["logsBloom"])
}
