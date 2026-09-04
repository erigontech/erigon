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

func BenchmarkDecodeBlockBodiesResponseRejectsTransactionAmplification(b *testing.B) {
	transaction := append([]byte{0xc9}, bytes.Repeat([]byte{0x80}, 9)...)
	header := newMockHeaderForBody(1, &types.Body{})

	for _, test := range []struct {
		name  string
		count int
	}{
		{name: "one", count: 1},
		{name: "4096", count: 4096},
	} {
		b.Run(test.name, func(b *testing.B) {
			transactions := rlpTestList(bytes.Repeat(transaction, test.count))
			body := rlpTestList(append(transactions, rlp.EmptyListCode))
			b.ReportAllocs()
			for b.Loop() {
				_, _ = decodeBlockBodiesResponse(body, []*types.Header{header})
			}
		})
	}
}
