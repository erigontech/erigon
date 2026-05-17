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
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
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

// prepareMockCtx is a minimal PatriciaContext used by Prepare tests.
// Branch lookups return pre-set entries; Account/Storage/PutBranch are
// no-ops because Prepare only consults Branch.
type prepareMockCtx struct {
	branches  map[string][]byte
	branchHit map[string]int // counts lookups for assertions
}

func newPrepareMockCtx() *prepareMockCtx {
	return &prepareMockCtx{
		branches:  map[string][]byte{},
		branchHit: map[string]int{},
	}
}

func (m *prepareMockCtx) Branch(prefix []byte) ([]byte, kv.Step, error) {
	m.branchHit[string(prefix)]++
	if data, ok := m.branches[string(prefix)]; ok {
		return data, 0, nil
	}
	return nil, 0, nil
}
func (m *prepareMockCtx) PutBranch(prefix, data, prevData []byte) error { return nil }
func (m *prepareMockCtx) Account(plainKey []byte) (*Update, error)      { return nil, nil }
func (m *prepareMockCtx) Storage(plainKey []byte) (*Update, error)      { return nil, nil }

// buildBranchData synthesises a BranchData blob populated for the supplied
// nibbles. Each cell is given a unique hash so tests can verify the cell
// landed at the right slot after decode.
func buildBranchData(t *testing.T, nibblesPresent []int) []byte {
	t.Helper()
	require.NotEmpty(t, nibblesPresent, "branch must have at least one cell")

	row := make([]*cell, 16)
	var bm uint16
	for _, nib := range nibblesPresent {
		require.True(t, nib >= 0 && nib < 16, "nibble %d out of range", nib)
		c := new(cell)
		c.hashLen = 32
		// Fill hash with a deterministic pattern keyed on the nibble so cells are distinguishable.
		for i := range 32 {
			c.hash[i] = byte(nib) ^ byte(i)
		}
		row[nib] = c
		bm |= uint16(1) << nib
	}

	var cellData [16]cellEncodeData
	for bs := bm; bs != 0; {
		nib := bs & -bs
		idx := 0
		for nib > 1 {
			nib >>= 1
			idx++
		}
		cellData[idx] = cellEncodeDataFromCell(row[idx])
		bs &= bs - 1
	}

	encoder := NewBranchEncoder(256)
	out, err := encoder.EncodeBranch(bm, bm, bm, &cellData)
	require.NoError(t, err)
	return []byte(out)
}

// keysWithSharedPrefix returns n hashed keys (each 64 nibbles long) that all
// start with the given prefix and then diverge across the remaining nibbles.
func keysWithSharedPrefix(prefix []byte, n int) [][]byte {
	const totalLen = 64
	out := make([][]byte, n)
	for i := range n {
		k := make([]byte, totalLen)
		copy(k, prefix)
		// Fill the suffix with a unique deterministic pattern so keys diverge.
		for j := len(prefix); j < totalLen; j++ {
			k[j] = byte((i*0x97 + j*0x13) & 0x0F)
		}
		out[i] = k
	}
	return out
}

// lookupSplitPoint fetches a split-point from pu's splitMap. The map hashes its
// keys away so we cannot iterate keys directly — tests assert presence via the
// known prefix instead.
func lookupSplitPoint(pu *parallelUpdate, prefix []byte) (*splitPoint, bool) {
	return pu.splitMap.Get(prefix)
}

func TestPrepareEmptyTrie(t *testing.T) {
	pu := newParallelUpdate()
	ctx := newPrepareMockCtx()

	err := pu.Prepare(ctx)
	require.NoError(t, err)

	assert.Empty(t, pu.leafQueue, "empty trie must produce no leaf tasks")
	assert.Equal(t, 0, pu.splitMap.Len(), "empty trie must produce no split points")
}

func TestPrepareSmallTrieNoSplits(t *testing.T) {
	pu := newParallelUpdate()
	ctx := newPrepareMockCtx()

	// Insert MinSplitKeys-1 keys all sharing the same first nibble so no node
	// hits the split threshold; they diverge inside an extension so the trie
	// still has internal structure.
	pre := []byte{0x0A, 0x0B}
	keys := keysWithSharedPrefix(pre, int(MinSplitKeys)-1)
	for _, k := range keys {
		pu.Insert(k)
	}

	require.NoError(t, pu.Prepare(ctx))

	assert.Equal(t, 0, pu.splitMap.Len(), "small subtree must not produce a split-point")
	// Exactly one leafTask whose prefix begins with 0x0A (top-level nibble).
	require.Len(t, pu.leafQueue, 1)
	lt := pu.leafQueue[0]
	assert.Equal(t, byte(0x0A), lt.prefix[0], "leafTask must route via the shared top-level nibble")
	assert.GreaterOrEqual(t, len(lt.prefix), 1)
	assert.Equal(t, uint32(MinSplitKeys-1), lt.keyCount)
	assert.Equal(t, 0, ctx.branchHit[string(nibbles.HexToCompact(lt.prefix))],
		"Prepare must not query the DB for leafTask prefixes")
}

func TestPrepareTwoWayForkAboveThreshold(t *testing.T) {
	pu := newParallelUpdate()
	ctx := newPrepareMockCtx()

	// Fork node at prefix [0x0A] has subtreeCount = MinSplitKeys and fanout=2
	// so it qualifies as a split-point. Each side has MinSplitKeys/2 keys
	// (< MinSplitKeys) so no nested split-point emerges below.
	perSide := int(MinSplitKeys) / 2
	for _, k := range keysWithSharedPrefix([]byte{0x0A, 0x0B}, perSide) {
		pu.Insert(k)
	}
	for _, k := range keysWithSharedPrefix([]byte{0x0A, 0x0C}, perSide) {
		pu.Insert(k)
	}

	require.NoError(t, pu.Prepare(ctx))

	require.Equal(t, 1, pu.splitMap.Len(), "expected exactly one split-point")
	sp, ok := lookupSplitPoint(pu, []byte{0x0A})
	require.True(t, ok, "split-point must sit at the fork depth (prefix [0x0A])")
	require.NotNil(t, sp)
	assert.Equal(t, uint16(1<<0x0B|1<<0x0C), sp.touchedBitmap)
	assert.Equal(t, int32(2), sp.arrived.Load(), "arrived must equal popcount(touched); the worker whose Add(-1) returns 0 is the unique last-finisher")
	assert.False(t, sp.branchBefore, "no DB branch was provided")
	assert.Equal(t, uint16(0), sp.dbBitmap)

	require.Len(t, pu.leafQueue, 2)
	prefixes := map[string]uint32{}
	for _, lt := range pu.leafQueue {
		prefixes[string(lt.prefix)] = lt.keyCount
	}
	assert.Equal(t, uint32(perSide), prefixes[string([]byte{0x0A, 0x0B})])
	assert.Equal(t, uint32(perSide), prefixes[string([]byte{0x0A, 0x0C})])
}

func TestPrepareForkBelowThreshold(t *testing.T) {
	pu := newParallelUpdate()
	ctx := newPrepareMockCtx()

	// 8 keys total split between two branches under [0x0A]: below threshold,
	// must collapse into a single leafTask covering both branches.
	for _, k := range keysWithSharedPrefix([]byte{0x0A, 0x0B}, 4) {
		pu.Insert(k)
	}
	for _, k := range keysWithSharedPrefix([]byte{0x0A, 0x0C}, 4) {
		pu.Insert(k)
	}

	require.NoError(t, pu.Prepare(ctx))

	assert.Equal(t, 0, pu.splitMap.Len(), "sub-threshold fork must not produce a split-point")
	require.Len(t, pu.leafQueue, 1, "sub-threshold subtree must collapse into one leafTask")
	lt := pu.leafQueue[0]
	assert.Equal(t, []byte{0x0A}, lt.prefix, "collapsed leafTask prefix must be the fork prefix")
	assert.Equal(t, uint32(8), lt.keyCount)
}

func TestPrepareRootForkBelowThresholdProducesPerNibbleLeafTasks(t *testing.T) {
	pu := newParallelUpdate()
	ctx := newPrepareMockCtx()

	// 4 keys at 0x01... + 4 keys at 0x02... — root forks at depth 0 but the
	// total is below MinSplitKeys. Per the routing constraint, each top-level
	// nibble must become its own leafTask so the worker can scan a single
	// Updates.nibbles[i] bucket.
	for _, k := range keysWithSharedPrefix([]byte{0x01}, 4) {
		pu.Insert(k)
	}
	for _, k := range keysWithSharedPrefix([]byte{0x02}, 4) {
		pu.Insert(k)
	}

	require.NoError(t, pu.Prepare(ctx))

	assert.Equal(t, 0, pu.splitMap.Len(), "root fork below threshold must not produce a split-point")
	require.Len(t, pu.leafQueue, 2, "each top-level nibble must produce its own leafTask")

	heads := map[byte]bool{}
	for _, lt := range pu.leafQueue {
		require.NotEmpty(t, lt.prefix, "leafTask prefix must be non-empty for routing")
		heads[lt.prefix[0]] = true
		assert.Equal(t, uint32(4), lt.keyCount)
	}
	assert.True(t, heads[0x01])
	assert.True(t, heads[0x02])
}

func TestPrepareDeepStorageShapeSplitsAtStorageFork(t *testing.T) {
	pu := newParallelUpdate()
	ctx := newPrepareMockCtx()

	// 60-nibble shared account-prefix, then storage diverges in the 61st nibble.
	// MinSplitKeys/2 keys per side keeps each child subtree below MinSplitKeys
	// so the only split-point sits at the storage-fork depth.
	accountPrefix := make([]byte, 60)
	for i := range accountPrefix {
		accountPrefix[i] = 0x0A
	}
	perSide := int(MinSplitKeys) / 2
	for _, k := range keysWithSharedPrefix(append(append([]byte{}, accountPrefix...), 0x01), perSide) {
		pu.Insert(k)
	}
	for _, k := range keysWithSharedPrefix(append(append([]byte{}, accountPrefix...), 0x02), perSide) {
		pu.Insert(k)
	}

	require.NoError(t, pu.Prepare(ctx))

	require.Equal(t, 1, pu.splitMap.Len())
	sp, ok := lookupSplitPoint(pu, accountPrefix)
	require.True(t, ok, "split-point must sit exactly at the storage-fork prefix")
	require.NotNil(t, sp)
	assert.Equal(t, uint16(1<<0x01|1<<0x02), sp.touchedBitmap)
	assert.Equal(t, int32(2), sp.arrived.Load())

	require.Len(t, pu.leafQueue, 2)
}

func TestPrepareUntouchedNibblePrePopulation(t *testing.T) {
	pu := newParallelUpdate()
	ctx := newPrepareMockCtx()

	// Worker arrives at nibbles 0x03 and 0x07; the DB branch at prefix [0x0A]
	// has nibbles 0x03, 0x05, 0x07, 0x0A, 0x0F. 0x05/0x0A/0x0F are untouched
	// siblings and must survive into sp.cells after Prepare.
	// MinSplitKeys/2 keys per side keeps the fork at [0x0A] as the sole split-point.
	perSide := int(MinSplitKeys) / 2
	for _, k := range keysWithSharedPrefix([]byte{0x0A, 0x03}, perSide) {
		pu.Insert(k)
	}
	for _, k := range keysWithSharedPrefix([]byte{0x0A, 0x07}, perSide) {
		pu.Insert(k)
	}

	dbNibbles := []int{0x03, 0x05, 0x07, 0x0A, 0x0F}
	branch := buildBranchData(t, dbNibbles)
	ctx.branches[string(nibbles.HexToCompact([]byte{0x0A}))] = branch

	require.NoError(t, pu.Prepare(ctx))

	require.Equal(t, 1, pu.splitMap.Len())
	sp, ok := lookupSplitPoint(pu, []byte{0x0A})
	require.True(t, ok)
	require.NotNil(t, sp)

	expectedDB := uint16(0)
	for _, n := range dbNibbles {
		expectedDB |= uint16(1) << uint16(n)
	}
	expectedTouched := uint16(1<<0x03 | 1<<0x07)
	assert.Equal(t, expectedTouched, sp.touchedBitmap)
	assert.Equal(t, expectedDB, sp.dbBitmap)
	assert.True(t, sp.branchBefore)
	assert.Equal(t, int32(2), sp.arrived.Load(),
		"arrived must reflect touched fanout (count of workers depositing), not dbBitmap fanout")

	for _, n := range dbNibbles {
		var zero cellEncodeData
		assert.NotEqual(t, zero, sp.cells[n],
			"sp.cells[%x] must be pre-populated from the DB branch", n)
		assert.Equal(t, int16(32), sp.cells[n].hashLen)
		// Hash pattern check: cell hash byte 0 must equal the nibble we built it for.
		assert.Equal(t, byte(n), sp.cells[n].hash[0],
			"sp.cells[%x] cell must match the nibble it was built for", n)
	}

	// Untouched nibbles outside dbBitmap must remain zero.
	for n := range 16 {
		if expectedDB&(uint16(1)<<uint16(n)) == 0 {
			var zero cellEncodeData
			assert.Equal(t, zero, sp.cells[n], "sp.cells[%x] must stay zero", n)
		}
	}
}

func TestPrepareLeafQueueSortedByKeyCount(t *testing.T) {
	pu := newParallelUpdate()
	ctx := newPrepareMockCtx()

	// Three top-level nibbles, varying counts scaled by MinSplitKeys/4 so each
	// child subtree stays below MinSplitKeys but the root sums above it. Root
	// fanout = 3, root subtreeCount = 6*unit = 1.5*MinSplitKeys ≥ MinSplitKeys
	// so root is the sole split-point.
	unit := int(MinSplitKeys) / 4
	smallCount := unit   // 1u
	medCount := 2 * unit // 2u
	bigCount := 3 * unit // 3u, still < MinSplitKeys = 4u
	for _, k := range keysWithSharedPrefix([]byte{0x01}, medCount) {
		pu.Insert(k)
	}
	for _, k := range keysWithSharedPrefix([]byte{0x02}, bigCount) {
		pu.Insert(k)
	}
	for _, k := range keysWithSharedPrefix([]byte{0x03}, smallCount) {
		pu.Insert(k)
	}

	require.NoError(t, pu.Prepare(ctx))
	require.Equal(t, 1, pu.splitMap.Len(), "root must be the only split-point")
	require.Len(t, pu.leafQueue, 3)

	counts := make([]uint32, len(pu.leafQueue))
	for i, lt := range pu.leafQueue {
		counts[i] = lt.keyCount
	}
	require.True(t, sort.SliceIsSorted(counts, func(i, j int) bool { return counts[i] > counts[j] }),
		"leafQueue must be sorted by keyCount descending: %v", counts)
	assert.Equal(t, uint32(bigCount), counts[0])
	assert.Equal(t, uint32(medCount), counts[1])
	assert.Equal(t, uint32(smallCount), counts[2])
}

func TestPrepareSingleKey(t *testing.T) {
	pu := newParallelUpdate()
	ctx := newPrepareMockCtx()

	key := keysWithSharedPrefix([]byte{0x0A, 0x0B}, 1)[0]
	pu.Insert(key)

	require.NoError(t, pu.Prepare(ctx))

	assert.Equal(t, 0, pu.splitMap.Len())
	require.Len(t, pu.leafQueue, 1)
	lt := pu.leafQueue[0]
	// A single key with no fork collapses to the full key as the leafTask prefix.
	assert.Equal(t, byte(0x0A), lt.prefix[0])
	assert.Equal(t, uint32(1), lt.keyCount)
}

func TestPrepareCtxBranchErrorPropagates(t *testing.T) {
	pu := newParallelUpdate()
	ctx := &erroringPrepareCtx{prefixToFail: string(nibbles.HexToCompact([]byte{0x0A}))}

	// Force a split-point at [0x0A] so loadDBBranch fires for that prefix.
	perSide := int(MinSplitKeys) / 2
	for _, k := range keysWithSharedPrefix([]byte{0x0A, 0x01}, perSide) {
		pu.Insert(k)
	}
	for _, k := range keysWithSharedPrefix([]byte{0x0A, 0x02}, perSide) {
		pu.Insert(k)
	}

	err := pu.Prepare(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loadDBBranch")
}

// erroringPrepareCtx returns an error from Branch when looked up for a
// specific compact prefix.
type erroringPrepareCtx struct {
	prepareMockCtx
	prefixToFail string
}

func (e *erroringPrepareCtx) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if string(prefix) == e.prefixToFail {
		return nil, 0, fmt.Errorf("simulated branch read failure")
	}
	return nil, 0, nil
}
func (e *erroringPrepareCtx) PutBranch(prefix, data, prevData []byte) error { return nil }
func (e *erroringPrepareCtx) Account(plainKey []byte) (*Update, error)      { return nil, nil }
func (e *erroringPrepareCtx) Storage(plainKey []byte) (*Update, error)      { return nil, nil }

func TestPrepareResetClearsPreviousResults(t *testing.T) {
	pu := newParallelUpdate()
	ctx := newPrepareMockCtx()

	// First run: produces a split-point and two leafTasks.
	perSide := int(MinSplitKeys) / 2
	for _, k := range keysWithSharedPrefix([]byte{0x0A, 0x0B}, perSide) {
		pu.Insert(k)
	}
	for _, k := range keysWithSharedPrefix([]byte{0x0A, 0x0C}, perSide) {
		pu.Insert(k)
	}
	require.NoError(t, pu.Prepare(ctx))
	require.Equal(t, 1, pu.splitMap.Len())
	require.NotEmpty(t, pu.leafQueue)

	// Second run after Reset: fewer keys, no split-point, smaller leafQueue.
	pu.Reset()
	for _, k := range keysWithSharedPrefix([]byte{0x05}, 4) {
		pu.Insert(k)
	}
	require.NoError(t, pu.Prepare(ctx))
	assert.Equal(t, 0, pu.splitMap.Len(), "Prepare must not see stale entries from previous run")
	require.Len(t, pu.leafQueue, 1)
}
