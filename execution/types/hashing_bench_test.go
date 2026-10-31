// Copyright 2024 The Erigon Authors
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
	"testing"
)

func BenchmarkLegacySmallList(b *testing.B) {
	for b.Loop() {
		legacyDeriveSha(smallTxList)
	}
}

func BenchmarkCurrentSmallList(b *testing.B) {
	for b.Loop() {
		DeriveSha(smallTxList)
	}
}

func BenchmarkLegacyLargeList(b *testing.B) {
	for b.Loop() {
		legacyDeriveSha(largeTxList)
	}
}

func BenchmarkCurrentLargeList(b *testing.B) {
	for b.Loop() {
		DeriveSha(largeTxList)
	}
}

func BenchmarkRlpHashHeader(b *testing.B) {
	for b.Loop() {
		RlpHash(benchHeader)
	}
}

func BenchmarkDeriveShaLargeBinaryTransactions(b *testing.B) {
	txs := make(BinaryTransactions, 5)
	for i := range txs {
		txs[i] = make([]byte, 4*1024*1024)
		txs[i][0] = byte(i)
	}
	b.SetBytes(int64(len(txs) * len(txs[0])))
	for b.Loop() {
		DeriveSha(txs)
	}
}
