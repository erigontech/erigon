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
	"sync/atomic"

	"github.com/erigontech/erigon/common/maphash"
)

// splitPoint is a barrier slot at a chosen trie prefix where multiple workers
// converge during the fold phase. Workers that arrive at the prefix deposit
// their produced upper-cell into cells[nib] and atomically decrement arrived.
// The last finisher (whose Add(-1) returns 0) sees all sibling writes via the
// happens-before edge of the atomic and continues folding upward through the
// merged grid.
//
// cells is laid out to match DeferredBranchUpdate.cells so that pre-populated
// untouched-nibble entries (loaded from ctx.Branch in Prepare) can be copied
// straight into a worker's grid via plain assignment.
type splitPoint struct {
	prefix        []byte
	cells         [16]cellEncodeData
	arrived       atomic.Int32
	touchedBitmap uint16
	dbBitmap      uint16
	branchBefore  bool
}

// leafTask is one unit of parallel work. It identifies a contiguous span of
// touched hashed-keys sharing prefix. keyCount is used to schedule large tasks
// first.
//
// The chain of ancestor split-points is discovered implicitly by the worker
// during its fold loop via splitMap lookups — there is no enclosingSP field.
type leafTask struct {
	prefix   []byte
	keyCount uint32
}

// parallelUpdate owns the per-batch state that drives parallel commitment:
// the path-compressed prefix trie of touched keys, the freeze-time split-point
// map, the leaf-task queue, and a mutex-guarded slice that collects deferred
// branch updates from all workers.
//
// All Insert calls must be serialized by the caller (same constraint as
// prefixTrie). splitMap and leafQueue are written only by Prepare and read
// concurrently afterwards. deferredCombined is the one mutable shared slice
// during the parallel phase — appendDeferred guards it.
type parallelUpdate struct {
	trie *prefixTrie

	splitMap  *maphash.NonConcurrentMap[*splitPoint]
	leafQueue []leafTask

	deferredMu       sync.Mutex
	deferredCombined []*DeferredBranchUpdate
}

func newParallelUpdate() *parallelUpdate {
	return &parallelUpdate{
		trie:     newPrefixTrie(),
		splitMap: maphash.NewNonConcurrentMap[*splitPoint](),
	}
}

// Insert adds a hashed key (in nibble form) to the prefix trie.
func (pu *parallelUpdate) Insert(hashedKey []byte) {
	pu.trie.Insert(hashedKey)
}

// Reset clears all per-batch state so the parallelUpdate can be reused.
// The underlying arena is recycled.
func (pu *parallelUpdate) Reset() {
	if pu.trie != nil {
		pu.trie.Reset()
	}
	if pu.splitMap != nil {
		pu.splitMap.Clear()
	}
	pu.leafQueue = pu.leafQueue[:0]
	pu.deferredMu.Lock()
	pu.deferredCombined = pu.deferredCombined[:0]
	pu.deferredMu.Unlock()
}

// Close releases references owned by the parallelUpdate. After Close the
// instance must not be reused.
func (pu *parallelUpdate) Close() {
	pu.trie = nil
	pu.splitMap = nil
	pu.leafQueue = nil
	pu.deferredMu.Lock()
	pu.deferredCombined = nil
	pu.deferredMu.Unlock()
}

// appendDeferred merges a worker's deferred branch updates into the shared
// slice. Safe for concurrent callers.
func (pu *parallelUpdate) appendDeferred(updates []*DeferredBranchUpdate) {
	if len(updates) == 0 {
		return
	}
	pu.deferredMu.Lock()
	pu.deferredCombined = append(pu.deferredCombined, updates...)
	pu.deferredMu.Unlock()
}
