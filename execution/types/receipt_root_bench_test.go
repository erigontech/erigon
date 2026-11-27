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

package types

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon/common"
)

// transferReceipts builds n receipts with one EIP-7708 style transfer log each:
// the same emitter, event and recipient, a distinct sender per receipt.
func transferReceipts(n int) Receipts {
	emitter := common.HexToAddress("0xfffffffffffffffffffffffffffffffffffffffe")
	event := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	to := common.HexToHash("0x000000000000000000000000000000000000000000000000000000000000b0b0")
	receipts := make(Receipts, n)
	for i := range receipts {
		var from common.Hash
		binary.BigEndian.PutUint64(from[24:], uint64(i)+1)
		receipts[i] = &Receipt{
			Type:              DynamicFeeTxType,
			Status:            ReceiptStatusSuccessful,
			CumulativeGasUsed: uint64(i+1) * 21_000,
			Logs: []*Log{{
				Address: emitter,
				Topics:  []common.Hash{event, from, to},
				Data:    common.LeftPadBytes([]byte{1}, 32),
			}},
		}
	}
	return receipts
}

func BenchmarkTransferReceiptsBlooms(b *testing.B) {
	receipts := transferReceipts(7142)
	b.Run("per-receipt", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var bloom Bloom
			for _, r := range receipts {
				r.Bloom = CreateBloom(Receipts{r})
				bloom.Or(&r.Bloom)
			}
		}
	})
	b.Run("SetReceiptBlooms", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			SetReceiptBlooms(receipts)
		}
	})
}

func BenchmarkTransferReceiptsDeriveSha(b *testing.B) {
	receipts := transferReceipts(7142)
	for _, r := range receipts {
		r.Bloom = CreateBloom(Receipts{r})
	}
	b.ReportAllocs()
	for b.Loop() {
		DeriveSha(receipts)
	}
}

func BenchmarkTransferReceiptsEncode(b *testing.B) {
	receipts := transferReceipts(7142)
	for _, r := range receipts {
		r.Bloom = CreateBloom(Receipts{r})
	}
	var buf bytes.Buffer
	b.ReportAllocs()
	for b.Loop() {
		for i := range receipts {
			buf.Reset()
			receipts.EncodeIndex(i, &buf)
		}
	}
}
