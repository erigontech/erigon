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
	"net/http/httptest"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func BenchmarkMarshalReceipt(b *testing.B) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	txn := dynamicFeeTx(&to)
	txn.SetSender(accounts.InternAddress(common.HexToAddress("0xabcdef0123456789abcdef0123456789abcdef03")))
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

		rpcReceipts := RPCReceipts{MarshalReceipt(receipt, &txn, chain.TestChainOsakaConfig, header, receipt.TxHash, true, true)}

		// The served path: the pooled stream a reply writes into, through the hand-written
		// encoder. The reflect arm is what it replaces.
		b.Run(fmt.Sprintf("logs=%d/stream", logCount), func(b *testing.B) {
			b.ReportAllocs()
			rec := httptest.NewRecorder()
			for b.Loop() {
				rec.Body.Reset()
				w := jsonstream.Get(rec)
				if err := rpcReceipts.MarshalFastJSONTo(w); err != nil {
					b.Fatal(err)
				}
				if err := w.Flush(); err != nil {
					b.Fatal(err)
				}
				jsonstream.Put(w)
			}
		})
		b.Run(fmt.Sprintf("logs=%d/reflect", logCount), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				out, err := json.Marshal(rpcReceipts)
				if err != nil || len(out) == 0 {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("logs=%d/build", logCount), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if MarshalReceipt(receipt, &txn, chain.TestChainOsakaConfig, header, receipt.TxHash, true, true) == nil {
					b.Fatal("nil")
				}
			}
		})
	}
}
