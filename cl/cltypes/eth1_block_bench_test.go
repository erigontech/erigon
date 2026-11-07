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

package cltypes

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func BenchmarkEth1BlockHashSSZGloas(b *testing.B) {
	for _, test := range []struct {
		transactions int
		balBytes     int
	}{
		{transactions: 300, balBytes: 100_000},
		{transactions: 1500, balBytes: 400_000},
	} {
		b.Run(fmt.Sprintf("txs=%d/bal=%dKB", test.transactions, test.balBytes/1000), func(b *testing.B) {
			block, encoded := benchmarkGloasEth1Block(b, test.transactions, test.balBytes)

			b.Run("cold", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					decoded := NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
					require.NoError(b, decoded.DecodeSSZ(encoded, int(clparams.GloasVersion)))
					b.StartTimer()
					_, err := decoded.HashSSZ()
					require.NoError(b, err)
				}
			})

			b.Run("warm", func(b *testing.B) {
				_, err := block.HashSSZ()
				require.NoError(b, err)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					_, err := block.HashSSZ()
					require.NoError(b, err)
				}
			})
		})
	}
}

func benchmarkGloasEth1Block(b *testing.B, transactionCount, balBytes int) (*Eth1Block, []byte) {
	b.Helper()
	transactions := make([][]byte, transactionCount)
	for i := range transactions {
		transactions[i] = make([]byte, 256)
		transactions[i][0] = byte(i)
	}

	block := NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	block.Extra = solid.NewExtraData()
	block.Transactions = solid.NewProgressiveTransactionsSSZFromTransactions(transactions)
	block.Withdrawals = solid.NewStaticListSSZ[*Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)
	require.NoError(b, block.BlockAccessList.SetBytes(make([]byte, balBytes)))
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(b, err)
	return block, encoded
}
