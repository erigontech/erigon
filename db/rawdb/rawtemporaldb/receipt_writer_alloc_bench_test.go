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

package rawtemporaldb_test

import (
	"testing"

	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
)

// BenchmarkPerTxReceiptWrite is what one applied transaction pays on the receipt
// path: the storage encoding into RCacheDomain plus the three ReceiptDomain
// counters.
func BenchmarkPerTxReceiptWrite(b *testing.B) {
	var w rawtemporaldb.ReceiptWriter
	receipt := receiptFixture()
	putter := &countingPutDel{}

	b.ReportAllocs()
	for b.Loop() {
		if err := w.Append(putter, receipt, 42); err != nil {
			b.Fatal(err)
		}
		if err := w.AppendMetadata(putter, 7, receipt.CumulativeGasUsed, 131072, 42); err != nil {
			b.Fatal(err)
		}
	}
}
