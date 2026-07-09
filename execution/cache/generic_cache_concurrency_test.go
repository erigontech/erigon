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

package cache

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/c2h5oh/datasize"
)

// TestGenericCache_ConcurrentPutAcrossGrow guards the jump-grow data race:
// curCap is written under resizeMu in maybeGrow but read on the put fast-path
// outside it, so concurrent writers crossing the grow threshold raced on it
// (surfaced by the -race eest shard). Many goroutines insert enough distinct
// keys to trigger several grow steps while others put concurrently; run with
// -race, this must stay clean.
func TestGenericCache_ConcurrentPutAcrossGrow(t *testing.T) {
	// Budget well above the start size (1024 slots) so maybeGrow fires repeatedly.
	c := NewGenericCache[[]byte](64*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU)

	const workers = 8
	const perWorker = 20_000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			key := make([]byte, 8)
			for i := 0; i < perWorker; i++ {
				binary.BigEndian.PutUint64(key, uint64(base*perWorker+i))
				c.Put(key, []byte{byte(i)}, uint64(i))
				c.Get(key)
			}
		}(w)
	}
	wg.Wait()
}
