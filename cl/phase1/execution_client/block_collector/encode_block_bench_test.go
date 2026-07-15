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

package block_collector

import (
	"math/rand/v2"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// benchPayload builds a Deneb execution payload carrying txCount transactions
// of txSize incompressible bytes each.
func benchPayload(txCount, txSize int) *cltypes.Eth1Block {
	rnd := rand.NewChaCha8([32]byte{42})
	txs := make([][]byte, txCount)
	for i := range txs {
		txs[i] = make([]byte, txSize)
		rnd.Read(txs[i])
	}
	body := &types.RawBody{Transactions: txs, Withdrawals: []*types.Withdrawal{}}
	return cltypes.NewEth1BlockFromHeaderAndBody(makeTestHeader(12345, common.Hash{}, nil), body, &clparams.MainnetBeaconConfig)
}

func BenchmarkEncodeBlock(b *testing.B) {
	parentRoot := common.HexToHash("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd")
	for _, tc := range []struct {
		name    string
		txCount int
		txSize  int
	}{
		{"empty", 0, 0},
		{"100KB", 200, 500},
		{"1MB", 500, 2048},
	} {
		payload := benchPayload(tc.txCount, tc.txSize)
		b.Run(tc.name, func(b *testing.B) {
			p := &PersistentBlockCollector{}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := p.encodeBlock(payload, parentRoot, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
