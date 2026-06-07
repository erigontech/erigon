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
	assert.Empty(t, pu.deferredCombined, "deferredCombined is empty on construction")
}

func TestParallelUpdateInsertDelegates(t *testing.T) {
	pu := newParallelUpdate()

	pu.Insert(nibs(0x01, 0x02, 0x03), nil, nil)
	pu.Insert(nibs(0x01, 0x02, 0x04), nil, nil)
	pu.Insert(nibs(0x05, 0x06, 0x07), nil, nil)

	// Verify the trie observed the inserts by comparing against a freshly built trie.
	expected := newPrefixTrie()
	expected.Insert(nibs(0x01, 0x02, 0x03), nil, nil)
	expected.Insert(nibs(0x01, 0x02, 0x04), nil, nil)
	expected.Insert(nibs(0x05, 0x06, 0x07), nil, nil)

	assert.Equal(t, expected.arena.nodeCount(), pu.trie.arena.nodeCount(),
		"parallelUpdate.Insert must produce same node count as direct trie.Insert")
	assert.Equal(t, expected.root.subtreeCount, pu.trie.root.subtreeCount,
		"subtreeCount must reflect all inserts delegated through parallelUpdate")
}

func TestParallelUpdateResetClearsAllState(t *testing.T) {
	pu := newParallelUpdate()

	pu.Insert(nibs(0x01, 0x02, 0x03), nil, nil)
	pu.Insert(nibs(0x05, 0x06, 0x07), nil, nil)
	pu.deferredCombined = append(pu.deferredCombined, &DeferredBranchUpdate{})

	require.Equal(t, uint32(2), pu.trie.root.subtreeCount)
	require.Len(t, pu.deferredCombined, 1)

	pu.Reset()

	assert.Equal(t, uint32(0), pu.trie.root.subtreeCount, "trie must be reset")
	assert.Equal(t, 1, pu.trie.arena.nodeCount(), "arena must be reset to a single root node")
	assert.Empty(t, pu.deferredCombined, "deferredCombined must be cleared")

	// Verify reusable: insert again works as on fresh instance.
	pu.Insert(nibs(0x0A, 0x0B), nil, nil)
	assert.Equal(t, uint32(1), pu.trie.root.subtreeCount)
}

func TestParallelUpdateClose(t *testing.T) {
	pu := newParallelUpdate()
	pu.Insert(nibs(0x01, 0x02), nil, nil)
	pu.deferredCombined = append(pu.deferredCombined, &DeferredBranchUpdate{})

	pu.Close()

	assert.Nil(t, pu.trie, "trie must be released")
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
