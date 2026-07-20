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
	"testing"

	"github.com/erigontech/erigon/common"
)

func benchReceipts(nReceipts, nLogs, nTopics, dataLen int) Receipts {
	rs := make(Receipts, nReceipts)
	for i := range rs {
		logs := make(Logs, nLogs)
		for j := range logs {
			topics := make([]common.Hash, nTopics)
			for k := range topics {
				topics[k][0] = byte(i)
				topics[k][1] = byte(j)
				topics[k][31] = byte(k)
			}
			data := make([]byte, dataLen)
			for k := range data {
				data[k] = byte(k)
			}
			var addr common.Address
			addr[0], addr[19] = byte(i), byte(j)
			logs[j] = &Log{Address: addr, Topics: topics, Data: data}
		}
		r := &Receipt{
			Type:              DynamicFeeTxType,
			Status:            ReceiptStatusSuccessful,
			CumulativeGasUsed: uint64(21000 * (i + 1)),
			Logs:              logs,
		}
		r.Bloom = CreateBloom(Receipts{r})
		rs[i] = r
	}
	return rs
}

func BenchmarkReceiptsEncodeIndex(b *testing.B) {
	rs := benchReceipts(1, 4, 3, 96)
	var buf bytes.Buffer
	b.ReportAllocs()
	for b.Loop() {
		buf.Reset()
		rs.EncodeIndex(0, &buf)
	}
}

func BenchmarkDeriveShaReceipts(b *testing.B) {
	rs := benchReceipts(200, 4, 3, 96)
	b.ReportAllocs()
	for b.Loop() {
		DeriveSha(rs)
	}
}
