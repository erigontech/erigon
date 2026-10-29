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

package p2p

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

func BenchmarkDecodeBlockBodiesResponseMalformedTransactions(b *testing.B) {
	headers := []*types.Header{newMockHeaderForBody(1, &types.Body{})}

	for _, test := range []struct {
		name  string
		count int
	}{
		{name: "one", count: 1},
		{name: "10M", count: 10_000_000},
	} {
		b.Run(test.name, func(b *testing.B) {
			transactions := rlpTestList(bytes.Repeat([]byte{rlp.EmptyStringCode}, test.count))
			body := rlpTestList(append(transactions, rlp.EmptyListCode))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := decodeBlockBodiesResponse(body, headers); err == nil {
					b.Fatal("expected a transaction decoding error")
				}
			}
		})
	}
}
