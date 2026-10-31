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
	"github.com/erigontech/erigon/execution/types"
)

type benchmarkIteration struct {
	*testing.B
	cleanups []func()
}

func (b *benchmarkIteration) Cleanup(f func()) {
	b.cleanups = append(b.cleanups, f)
}

func (b *benchmarkIteration) close() {
	if len(b.cleanups) > 0 {
		defer b.close()
		last := len(b.cleanups) - 1
		cleanup := b.cleanups[last]
		b.cleanups = b.cleanups[:last]
		cleanup()
	}
}

func BenchmarkBALPhantomReads(b *testing.B) {
	for _, workers := range []int{1, 4} {
		for _, workload := range []string{"valid", "reads", "attack"} {
			b.Run(fmt.Sprintf("workers=%d/%s", workers, workload), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					func() {
						b.StopTimer()
						iteration := &benchmarkIteration{B: b}
						defer iteration.close()
						m, block := phantomReadFixture(iteration, workload, workers)
						status, err := m.InsertBlocks(b.Context(), []*types.Block{block})
						require.NoError(b, err)
						require.Equal(b, execmodule.ExecutionStatusSuccess, status)
						runtime.GC()
						b.StartTimer()
						result, err := m.ValidateChain(b.Context(), block.Header())
						require.NoError(b, err)
						if workload == "attack" {
							require.Equal(b, execmodule.ExecutionStatusBadBlock, result.ValidationStatus)
							require.Contains(b, result.ValidationError, "block access list mismatch")
						} else {
							require.Equal(b, execmodule.ExecutionStatusSuccess, result.ValidationStatus, result.ValidationError)
						}
						m.ReadAheader.WaitForWarmup(b.Context())
						b.StopTimer()
					}()
				}
			})
		}
	}
}
