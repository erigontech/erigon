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

package misc

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func BenchmarkValidateBlobs(b *testing.B) {
	var expected []common.Hash
	txns := make([]types.Transaction, 6)
	for i := range txns {
		h := common.Hash{0x01, byte(i)}
		txns[i] = &types.BlobTx{BlobVersionedHashes: []common.Hash{h}}
		expected = append(expected, h)
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := ValidateBlobs(0, 0, 9, expected, &txns); err != nil {
			b.Fatal(err)
		}
	}
}
