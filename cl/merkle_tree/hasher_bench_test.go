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

package merkle_tree_test

import (
	"testing"

	"github.com/erigontech/erigon/cl/merkle_tree"
)

func BenchmarkTransactionsListRoot(b *testing.B) {
	const (
		numTxs  = 256
		txBytes = 200
	)
	txs := make([][]byte, numTxs)
	for i := range txs {
		tx := make([]byte, txBytes)
		for j := range tx {
			tx[j] = byte(i + j)
		}
		txs[i] = tx
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := merkle_tree.TransactionsListRoot(txs); err != nil {
			b.Fatal(err)
		}
	}
}
