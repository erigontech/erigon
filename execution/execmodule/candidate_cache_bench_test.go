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

package execmodule_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func BenchmarkValidatedCandidateReuse(b *testing.B) {
	for _, retained := range []bool{false, true} {
		name := "discarded"
		if retained {
			name = "retained"
		}
		b.Run(name, func(b *testing.B) {
			b.StopTimer()
			senders := benchSenders(b)
			b.ReportAllocs()
			for range b.N {
				func() {
					b.StopTimer()
					m, blocks := benchFixture(b, warmAccounts, 10_000, 1, senders)
					defer m.Close()
					sibling, err := m.GenerateChainFrom(m.Genesis, 1, func(_ int, bg *blockgen.BlockGen) {
						bg.SetCoinbase(benchCoinbase)
						bg.SetExtra([]byte("sibling"))
						for _, txn := range blocks[0].Transactions() {
							bg.AddTx(txn)
						}
					})
					require.NoError(b, err)
					for _, block := range append(blocks, sibling.Blocks...) {
						_, err := m.InsertBlocks(b.Context(), []*types.Block{block})
						require.NoError(b, err)
						result, err := m.ValidateChain(b.Context(), block.Header())
						require.NoError(b, err)
						require.Equal(b, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
					}
					if !retained {
						m.ForkValidator.ClearWithUnwind()
					}
					b.StartTimer()
					result, err := m.UpdateForkChoice(b.Context(), blocks[0].Header())
					require.NoError(b, err)
					require.Equal(b, execmodule.ExecutionStatusSuccess, result.Status)
					b.StopTimer()
				}()
			}
		})
	}
}
