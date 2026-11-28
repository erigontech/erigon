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

package stagedsync

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
)

func TestWorkerCacheNeverGivesOneWorkerToTwoCalls(t *testing.T) {
	const n = 2
	cache := &execWorkerCache{}
	var inUse sync.Map
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 200 {
				workers := cache.take(n)
				if workers == nil {
					for range n {
						workers = append(workers, exec.NewWorkerContext(t.Context(), false, nil, nil, nil, chain.AllProtocolChanges, nil, nil, datadir.Dirs{}, log.New()))
					}
				}
				for _, w := range workers {
					_, busy := inUse.LoadOrStore(w, struct{}{})
					assert.False(t, busy, "one worker went to two calls at the same time")
				}
				for _, w := range workers {
					inUse.Delete(w)
				}
				cache.put(workers)
			}
		})
	}
	wg.Wait()
	closeWorkers(cache.take(n))
}
