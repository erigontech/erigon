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

package jsonrpc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/rpc"
)

func BenchmarkAccountReadsAtLatest(b *testing.B) {
	m := execmoduletester.New(b, execmoduletester.WithChainConfig(chain.TestChainBerlinConfig))
	blocks, err := m.GenerateChain(4, func(int, *blockgen.BlockGen) {})
	require.NoError(b, err)
	require.NoError(b, m.InsertChain(blocks))
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	ctx := b.Context()
	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

	for _, request := range []struct {
		name string
		call func() error
	}{
		{"balance", func() error { _, err := api.GetBalance(ctx, m.Address, &latest); return err }},
		{"transaction_count", func() error { _, err := api.GetTransactionCount(ctx, m.Address, &latest); return err }},
	} {
		b.Run(request.name, func(b *testing.B) {
			require.NoError(b, request.call())
			b.ReportAllocs()
			for b.Loop() {
				if err := request.call(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
