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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

func BenchmarkMarshalReceipt(b *testing.B) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	txn := &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{Nonce: 3, GasLimit: 21000, To: &to, Value: *uint256.NewInt(5)},
		ChainID:  *uint256.NewInt(1337),
		TipCap:   *uint256.NewInt(2),
		FeeCap:   *uint256.NewInt(100),
	}
	header := &types.Header{Number: *uint256.NewInt(7), Time: 1_750_000_000, BaseFee: uint256.NewInt(50)}

	for _, logCount := range []int{0, 2} {
		logs := make(types.Logs, logCount)
		for i := range logs {
			logs[i] = &types.Log{Address: to, Topics: []common.Hash{{0x01}, {0x02}}, Data: make([]byte, 64)}
		}
		receipt := &types.Receipt{
			Status:            types.ReceiptStatusSuccessful,
			CumulativeGasUsed: 42_000,
			Logs:              logs,
			TxHash:            common.HexToHash("0xbeef"),
			GasUsed:           21_000,
			BlockHash:         common.HexToHash("0xb10c"),
			BlockNumber:       uint256.NewInt(7),
			TransactionIndex:  3,
		}
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})

		b.Run(fmt.Sprintf("logs=%d", logCount), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				out, err := json.Marshal(MarshalReceipt(receipt, txn, chain.TestChainOsakaConfig, header, receipt.TxHash, false, true))
				if err != nil || len(out) == 0 {
					b.Fatal(err)
				}
			}
		})
	}
}
