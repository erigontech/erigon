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
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func BenchmarkForkChoiceWithSideBlocks(b *testing.B) {
	for _, sideBlocks := range []int{0, 1, 3, 7} {
		b.Run(fmt.Sprintf("side=%d", sideBlocks), func(b *testing.B) {
			b.StopTimer()
			senders := benchSenders(b)
			b.ReportAllocs()
			var cgoCalls int64
			for range b.N {
				func() {
					b.StopTimer()
					m, blocks := benchFixture(b, warmAccounts, 10_000, 1, senders)
					defer m.Close()
					winner := blocks[0]
					payloads := make([]*types.Block, 0, sideBlocks+1)
					for i := range sideBlocks {
						chain, err := m.GenerateChainFrom(m.Genesis, 1, func(_ int, bg *blockgen.BlockGen) {
							bg.SetCoinbase(benchCoinbase)
							bg.SetExtra([]byte{byte(i + 1)})
							for _, txn := range winner.Transactions() {
								bg.AddTx(txn)
							}
						})
						require.NoError(b, err)
						payloads = append(payloads, chain.Blocks[0])
					}
					payloads = append(payloads, winner)
					for _, block := range payloads {
						_, err := m.InsertBlocks(b.Context(), []*types.Block{block})
						require.NoError(b, err)
						result, err := m.ValidateChain(b.Context(), block.Header())
						require.NoError(b, err)
						require.Equal(b, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
					}
					m.ExecModule.WaitIdle(b.Context())
					calls := runtime.NumCgoCall()
					b.StartTimer()
					result, err := m.UpdateForkChoice(b.Context(), winner.Header())
					m.ExecModule.WaitIdle(b.Context())
					b.StopTimer()
					cgoCalls += runtime.NumCgoCall() - calls
					require.NoError(b, err)
					require.Equal(b, execmodule.ExecutionStatusSuccess, result.Status)
				}()
			}
			b.ReportMetric(float64(cgoCalls)/float64(b.N), "cgo-calls/op")
		})
	}
}
