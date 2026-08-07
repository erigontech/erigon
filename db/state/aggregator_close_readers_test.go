// Copyright 2024 The Erigon Authors
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

package state_test

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon/db/kv"
)

// Close must not unmap domain files out from under an in-flight reader:
// closeDirtyFiles unmaps while a concurrent AggregatorRoTx getter reads the same
// mapping under no shared lock. Without a reader-drain in Close this is a
// use-after-unmap, reported as "DATA RACE" (and often a SIGSEGV) under -race.
//
// Readers stay in their getter loop until stop is closed; Close runs
// concurrently. A correct Close blocks until every reader has released its pin
// before unmapping, so the two never overlap.
func TestAggregatorCloseWaitsForFileReaders(t *testing.T) {
	t.Parallel()
	const stepSize = 16
	_, agg := testDbAggregatorWithFiles(t, &testAggConfig{stepSize: stepSize})

	// Real keys pass the existence filter, and maxTxNum < MaxUint64 disables the
	// from-file cache, so every read reaches the decompressor mmap.
	keys, _ := generateInputData(t, 20, 5, stepSize*32)
	const readMaxTxNum = math.MaxUint64 - 1

	const readers = 32
	stop := make(chan struct{})
	started := make(chan struct{}, readers)
	var wg sync.WaitGroup
	for range readers {
		wg.Go(func() {
			at := agg.BeginFilesRo()
			defer at.Close()
			started <- struct{}{}
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_, _, _, _, _ = at.DebugGetLatestFromFiles(kv.AccountsDomain, keys[i%len(keys)], readMaxTxNum)
			}
		})
	}
	for range readers {
		<-started
	}

	closed := make(chan struct{})
	go func() {
		agg.Close()
		close(closed)
	}()

	time.Sleep(50 * time.Millisecond) // give Close time to reach the unmap while readers are mid-getter

	// Positive barrier assertion, independent of -race: readers still hold their
	// pins, so a correct Close must still be blocked in the drain. Without the
	// drain, Close unmaps and returns here — failing deterministically.
	select {
	case <-closed:
		t.Fatal("Close returned while file readers were still pinned")
	default:
	}

	close(stop) // readers release their pins; Close drains, then unmaps
	<-closed
	wg.Wait()
}
