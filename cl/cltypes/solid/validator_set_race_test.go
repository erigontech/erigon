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

package solid

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every goroutine owns its registry exclusively, except the shared source,
// which is only copied. This is the access pattern the state locks allow.
func TestValidatorSetSiblingsShareChunksUnderConcurrentUse(t *testing.T) {
	for _, progressive := range []bool{false, true} {
		t.Run(fmt.Sprintf("progressive=%v", progressive), func(t *testing.T) {
			source := allTestSet(t, 20_000, progressive)
			if progressive {
				_, err := source.hash()
				require.NoError(t, err)
			}
			const siblings = 8
			lives := make([]*liveSet, siblings)
			for i := range lives {
				lives[i] = &liveSet{set: NewValidatorSet(1 << 20), model: newValidatorModel(1 << 20)}
			}
			var wg sync.WaitGroup
			for i, live := range lives {
				wg.Go(func() {
					rng := rand.New(rand.NewSource(int64(i)))
					for round := range 6 {
						source.copyInto(live)
						switch (i + round) % 3 {
						case 0:
							_, err := live.hash()
							assert.NoError(t, err)
						case 1:
							for range 20 {
								live.applyRandomSetter(rng)
							}
							live.appendRandom(rng)
						default:
							spare := &liveSet{set: NewValidatorSet(1 << 20), model: newValidatorModel(1 << 20)}
							live.copyInto(spare)
							spare.applyRandomSetter(rng)
							_, err := spare.hash()
							assert.NoError(t, err)
						}
					}
				})
			}
			wg.Wait()
			source.check(t, "source")
			for i, live := range lives {
				live.check(t, fmt.Sprintf("sibling %d", i))
			}
		})
	}
}

func TestValidatorSetParallelWritersMatchSerialRun(t *testing.T) {
	for _, progressive := range []bool{false, true} {
		t.Run(fmt.Sprintf("progressive=%v", progressive), func(t *testing.T) {
			const count = 10_000
			source := allTestSet(t, count, progressive)
			_, err := source.hash()
			require.NoError(t, err)

			parallel := &liveSet{set: NewValidatorSet(1 << 20), model: newValidatorModel(1 << 20)}
			source.copyInto(parallel)
			serial := &liveSet{set: NewValidatorSet(1 << 20), model: newValidatorModel(1 << 20)}
			source.copyInto(serial)

			workers := max(4, runtime.NumCPU())
			var wg sync.WaitGroup
			for w := range workers {
				wg.Go(func() {
					// Each worker owns every workers-th index, so all of them contend for every chunk.
					for i := w; i < count; i += workers {
						_ = parallel.set.Get(i).EffectiveBalance()
						parallel.set.SetEffectiveBalanceForValidatorAtIndex(i, uint64(i)*3)
					}
				})
			}
			wg.Wait()
			for i := range count {
				serial.set.SetEffectiveBalanceForValidatorAtIndex(i, uint64(i)*3)
			}
			for _, live := range []*liveSet{parallel, serial} {
				for i := range count {
					live.model.mutate(i, func(v Validator) { v.SetEffectiveBalance(uint64(i) * 3) })
				}
			}
			parallelRoot, err := parallel.hash()
			require.NoError(t, err)
			serialRoot, err := serial.hash()
			require.NoError(t, err)
			require.Equal(t, serialRoot, parallelRoot)
			parallel.check(t, "parallel writers")
			serial.check(t, "serial writer")
			source.check(t, "source")
		})
	}
}
