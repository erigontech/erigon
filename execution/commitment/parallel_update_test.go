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

package commitment

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParallelUpdateConstruction(t *testing.T) {
	pu := newParallelUpdate()
	require.NotNil(t, pu)
	require.NotNil(t, pu.trie, "trie must be allocated")
	require.NotNil(t, pu.trie.root, "trie root must be allocated")
	require.NotNil(t, pu.splitMap, "splitMap must be allocated")
	assert.Equal(t, 0, pu.splitMap.Len(), "splitMap is empty on construction")
	assert.Empty(t, pu.leafQueue, "leafQueue is empty on construction")
	assert.Empty(t, pu.deferredCombined, "deferredCombined is empty on construction")
}

func TestParallelUpdateInsertDelegates(t *testing.T) {
	pu := newParallelUpdate()

	pu.Insert(nibs(0x01, 0x02, 0x03))
	pu.Insert(nibs(0x01, 0x02, 0x04))
	pu.Insert(nibs(0x05, 0x06, 0x07))

	// Verify the trie observed the inserts by comparing against a freshly built trie.
	expected := newPrefixTrie()
	expected.Insert(nibs(0x01, 0x02, 0x03))
	expected.Insert(nibs(0x01, 0x02, 0x04))
	expected.Insert(nibs(0x05, 0x06, 0x07))

	assert.Equal(t, expected.arena.nodeCount(), pu.trie.arena.nodeCount(),
		"parallelUpdate.Insert must produce same node count as direct trie.Insert")
	assert.Equal(t, expected.root.subtreeCount, pu.trie.root.subtreeCount,
		"subtreeCount must reflect all inserts delegated through parallelUpdate")
}

func TestParallelUpdateResetClearsAllState(t *testing.T) {
	pu := newParallelUpdate()

	// Populate every field.
	pu.Insert(nibs(0x01, 0x02, 0x03))
	pu.Insert(nibs(0x05, 0x06, 0x07))
	pu.splitMap.Set([]byte{0x01}, &splitPoint{prefix: []byte{0x01}, touchedBitmap: 0x05})
	pu.leafQueue = append(pu.leafQueue, leafTask{prefix: []byte{0x01}, keyCount: 7})
	pu.deferredCombined = append(pu.deferredCombined, &DeferredBranchUpdate{})

	require.Equal(t, uint32(2), pu.trie.root.subtreeCount)
	require.Equal(t, 1, pu.splitMap.Len())
	require.Len(t, pu.leafQueue, 1)
	require.Len(t, pu.deferredCombined, 1)

	pu.Reset()

	assert.Equal(t, uint32(0), pu.trie.root.subtreeCount, "trie must be reset")
	assert.Equal(t, 1, pu.trie.arena.nodeCount(), "arena must be reset to a single root node")
	assert.Equal(t, 0, pu.splitMap.Len(), "splitMap must be cleared")
	assert.Empty(t, pu.leafQueue, "leafQueue must be cleared")
	assert.Empty(t, pu.deferredCombined, "deferredCombined must be cleared")

	// Verify reusable: insert again works as on fresh instance.
	pu.Insert(nibs(0x0A, 0x0B))
	assert.Equal(t, uint32(1), pu.trie.root.subtreeCount)
}

func TestParallelUpdateClose(t *testing.T) {
	pu := newParallelUpdate()
	pu.Insert(nibs(0x01, 0x02))
	pu.splitMap.Set([]byte{0x01}, &splitPoint{})
	pu.leafQueue = append(pu.leafQueue, leafTask{})
	pu.deferredCombined = append(pu.deferredCombined, &DeferredBranchUpdate{})

	pu.Close()

	assert.Nil(t, pu.trie, "trie must be released")
	assert.Nil(t, pu.splitMap, "splitMap must be released")
	assert.Nil(t, pu.leafQueue, "leafQueue must be released")
	assert.Nil(t, pu.deferredCombined, "deferredCombined must be released")
}

func TestParallelUpdateAppendDeferredEmpty(t *testing.T) {
	pu := newParallelUpdate()
	pu.appendDeferred(nil)
	pu.appendDeferred([]*DeferredBranchUpdate{})
	assert.Empty(t, pu.deferredCombined, "empty inputs must not grow deferredCombined")
}

func TestParallelUpdateAppendDeferredSequential(t *testing.T) {
	pu := newParallelUpdate()
	a := &DeferredBranchUpdate{}
	b := &DeferredBranchUpdate{}
	c := &DeferredBranchUpdate{}

	pu.appendDeferred([]*DeferredBranchUpdate{a})
	pu.appendDeferred([]*DeferredBranchUpdate{b, c})

	require.Len(t, pu.deferredCombined, 3)
	assert.Same(t, a, pu.deferredCombined[0])
	assert.Same(t, b, pu.deferredCombined[1])
	assert.Same(t, c, pu.deferredCombined[2])
}

// TestParallelUpdateAppendDeferredConcurrent exercises the appendDeferred mutex
// under contention. Run with `go test -race` to verify there are no data races.
func TestParallelUpdateAppendDeferredConcurrent(t *testing.T) {
	pu := newParallelUpdate()

	const (
		workers      = 16
		perWorker    = 32
		expectedSize = workers * perWorker
	)

	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			batch := make([]*DeferredBranchUpdate, perWorker)
			for i := range batch {
				batch[i] = &DeferredBranchUpdate{}
			}
			pu.appendDeferred(batch)
		}()
	}
	wg.Wait()

	assert.Len(t, pu.deferredCombined, expectedSize,
		"appendDeferred must atomically merge every worker's batch")

	// All pointers must be non-nil — verifies no torn writes from concurrent appends.
	for i, upd := range pu.deferredCombined {
		assert.NotNil(t, upd, "deferredCombined[%d] must not be nil", i)
	}
}

// TestParallelUpdateSplitPointAtomicArrived sanity-checks the atomic counter
// inside splitPoint — this is a structural test, not an end-to-end barrier test.
func TestParallelUpdateSplitPointAtomicArrived(t *testing.T) {
	sp := &splitPoint{}
	sp.arrived.Store(3)

	// Three workers decrementing should observe 2, 1, 0.
	results := make([]int32, 3)
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = sp.arrived.Add(-1)
		}(i)
	}
	wg.Wait()

	// Initial value 3 minus three decrements lands at exactly 0.
	// results contain {2,1,0} in some order.
	assert.Equal(t, int32(0), sp.arrived.Load(), "atomic.Int32 must end at 0 after 3 decrements from 3")

	seen := map[int32]bool{}
	for _, r := range results {
		seen[r] = true
	}
	assert.Len(t, seen, 3, "every decrement must produce a distinct return value")
	for _, want := range []int32{0, 1, 2} {
		assert.True(t, seen[want], "expected to observe return value %d", want)
	}
}

func TestParallelUpdateLeafTaskZeroValue(t *testing.T) {
	var lt leafTask
	assert.Nil(t, lt.prefix)
	assert.Equal(t, uint32(0), lt.keyCount)
}

func TestParallelUpdateSplitPointZeroValue(t *testing.T) {
	var sp splitPoint
	assert.Nil(t, sp.prefix)
	assert.Equal(t, uint16(0), sp.touchedBitmap)
	assert.Equal(t, uint16(0), sp.dbBitmap)
	assert.False(t, sp.branchBefore)
	assert.Equal(t, int32(0), sp.arrived.Load())
	for i := range 16 {
		assert.Equal(t, cellEncodeData{}, sp.cells[i])
	}
}
