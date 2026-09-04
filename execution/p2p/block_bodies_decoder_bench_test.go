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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

var benchmarkDecodedBlockBodies []*types.Body

func BenchmarkDecodeBlockBodiesResponse(b *testing.B) {
	for _, scenario := range []struct {
		name      string
		bodyCount int
		txCount   int
	}{
		{name: "one-body-200-transactions", bodyCount: 1, txCount: 200},
		{name: "sixteen-bodies-20-transactions", bodyCount: 16, txCount: 20},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			bodies := make([]*types.Body, scenario.bodyCount)
			headers := make([]*types.Header, scenario.bodyCount)
			for bodyIndex := range bodies {
				transactions := make([]types.Transaction, scenario.txCount)
				for txIndex := range transactions {
					transactions[txIndex] = types.NewTransaction(
						uint64(bodyIndex*scenario.txCount+txIndex),
						common.Address{1},
						uint256.NewInt(1),
						21_000,
						uint256.NewInt(1),
						bytes.Repeat([]byte{2}, 32),
					)
				}
				bodies[bodyIndex] = &types.Body{Transactions: transactions}
				headers[bodyIndex] = newMockHeaderForBody(uint64(bodyIndex+1), bodies[bodyIndex])
			}

			encoded, err := rlp.EncodeToBytes(bodies)
			if err != nil {
				b.Fatal(err)
			}
			encodedBodies, rest, err := rlp.SplitList(encoded)
			if err != nil {
				b.Fatal(err)
			}
			if len(rest) != 0 {
				b.Fatal(rlp.ErrMoreThanOneValue)
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(encodedBodies)))
			b.ResetTimer()
			for b.Loop() {
				benchmarkDecodedBlockBodies, err = decodeBlockBodiesResponse(encodedBodies, headers)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

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
