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
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/maphash"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// MinSplitKeys is the minimum subtree size (touched-key count) below which a
// fork is collapsed into a single leafTask instead of being elevated to a
// split-point. Smaller subtrees do not benefit from parallel execution because
// the barrier coordination cost dominates the saved work.
const MinSplitKeys uint32 = 64

// PrefixTrieMaxDepth bounds the nibble depth Prepare is willing to traverse.
// A keccak256-hashed key is 64 bytes = 128 nibbles, so this is the worst-case
// path length. The constant exists to fail fast on malformed input rather than
// to recurse unboundedly.
const PrefixTrieMaxDepth = 128

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
	node     *prefixNode // subtree root the worker DFS-walks for its keys
	keyCount uint32
}

// plainKeyArena hands out stable plainKey copies from fixed chunks; a full chunk
// is replaced, not grown, so earlier sub-slices keep their backing until reset.
type plainKeyArena struct {
	buf []byte
}

const plainKeyArenaChunk = 64 * 1024

func (a *plainKeyArena) intern(b []byte) []byte {
	if len(b) > plainKeyArenaChunk {
		return append([]byte(nil), b...)
	}
	if cap(a.buf)-len(a.buf) < len(b) {
		a.buf = make([]byte, 0, plainKeyArenaChunk)
	}
	off := len(a.buf)
	a.buf = append(a.buf, b...)
	return a.buf[off : off+len(b) : off+len(b)]
}

func (a *plainKeyArena) reset() { a.buf = nil }

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

	splitMap *maphash.NonConcurrentMap[*splitPoint]
	// splitPoints mirrors splitMap so callers can iterate the emitted
	// split-points without poking inside the map (NonConcurrentMap has no
	// Range). Populated by Prepare in the same DFS order as splitMap.
	splitPoints []*splitPoint
	leafQueue   []leafTask

	// minSplitKeys overrides the global MinSplitKeys threshold for this
	// instance. Zero means "use the global default". Callers (e.g.
	// ParallelPatriciaHashed.Process or tests) set this before Prepare to
	// influence split-point emission — raising it above the touched-key count
	// suppresses all split-points so every leafTask runs without barriers.
	minSplitKeys uint32

	deferredMu       sync.Mutex
	deferredCombined []*DeferredBranchUpdate

	keyArena plainKeyArena
}

func newParallelUpdate() *parallelUpdate {
	return &parallelUpdate{
		trie:     newPrefixTrie(),
		splitMap: maphash.NewNonConcurrentMap[*splitPoint](),
	}
}

// Insert adds a hashed key (in nibble form), its plainKey, and an optional
// carried value (nil = fold re-reads from ctx) to the prefix trie.
func (pu *parallelUpdate) Insert(hashedKey, plainKey []byte, update *Update) {
	pu.trie.Insert(hashedKey, plainKey, update)
}

// internKey copies plainKey into the per-batch arena for stable trie retention.
func (pu *parallelUpdate) internKey(plainKey []byte) []byte {
	return pu.keyArena.intern(plainKey)
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
	pu.splitPoints = pu.splitPoints[:0]
	pu.leafQueue = pu.leafQueue[:0]
	pu.deferredMu.Lock()
	for _, upd := range pu.deferredCombined {
		putDeferredUpdate(upd)
	}
	pu.deferredCombined = pu.deferredCombined[:0]
	pu.deferredMu.Unlock()
	pu.keyArena.reset()
}

// Close releases references owned by the parallelUpdate. After Close the
// instance must not be reused.
func (pu *parallelUpdate) Close() {
	pu.trie = nil
	pu.splitMap = nil
	pu.splitPoints = nil
	pu.leafQueue = nil
	pu.deferredMu.Lock()
	for _, upd := range pu.deferredCombined {
		putDeferredUpdate(upd)
	}
	pu.deferredCombined = nil
	pu.deferredMu.Unlock()
	pu.keyArena.reset()
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

// Prepare walks the prefix trie in DFS order and partitions the touched-key
// space into split-points and leaf-tasks. A node becomes a split-point when it
// has >= 2 child branches AND its accumulated subtree contains at least
// MinSplitKeys touched keys — coarser forks are collapsed into a single
// leafTask.
//
// For every emitted split-point Prepare also loads the on-disk branch at that
// prefix via ctx.Branch and pre-populates sp.cells for nibbles that exist on
// disk but were not touched in this batch. Without this pre-population the
// last-finisher worker would fold a branch missing those untouched siblings
// and produce the wrong branch hash. sp.arrived is initialised to
// popcount(touchedBitmap) - 1 so that the worker whose atomic Add returns 0 is
// the unique last-finisher.
//
// Prepare must be called sequentially after all TouchPlainKey/Insert calls and
// before any worker reads splitMap or leafQueue.
func (pu *parallelUpdate) Prepare(ctx PatriciaContext) error {
	if pu.trie == nil || pu.trie.root == nil {
		return errors.New("parallelUpdate.Prepare: trie not initialised")
	}
	pu.leafQueue = pu.leafQueue[:0]
	pu.splitPoints = pu.splitPoints[:0]
	if pu.splitMap == nil {
		pu.splitMap = maphash.NewNonConcurrentMap[*splitPoint]()
	} else {
		pu.splitMap.Clear()
	}

	if err := pu.prepareDFS(ctx, pu.trie.root, nil, true); err != nil {
		return err
	}

	// Schedule larger leafTasks first for better worker utilisation: a single
	// long-running task blocking the tail of the queue is the worst case for
	// throughput, so dispatching the heaviest tasks earliest gives workers the
	// most overlap.
	slices.SortStableFunc(pu.leafQueue, func(a, b leafTask) int {
		switch {
		case a.keyCount > b.keyCount:
			return -1
		case a.keyCount < b.keyCount:
			return 1
		default:
			return 0
		}
	})
	return nil
}

// prepareDFS implements the recursive walk for Prepare. accPrefix is the
// nibble path from the trie root to (but not including) node.ext. isRoot marks
// the trie root, which needs special handling: leafTasks emerging from the
// root must carry a non-empty prefix, each rooting a distinct top-nibble
// subtree the worker DFS-walks.
func (pu *parallelUpdate) prepareDFS(ctx PatriciaContext, node *prefixNode, accPrefix []byte, isRoot bool) error {
	if node == nil {
		return nil
	}
	nodeDepth := len(accPrefix) + len(node.ext)
	if nodeDepth > PrefixTrieMaxDepth {
		return fmt.Errorf("parallelUpdate.Prepare: prefix depth %d exceeds max %d", nodeDepth, PrefixTrieMaxDepth)
	}

	subtrees := bits.OnesCount16(node.bitmap)

	if isRoot && subtrees == 0 {
		// Empty trie — nothing to do.
		return nil
	}

	threshold := pu.minSplitKeys
	if threshold == 0 {
		threshold = MinSplitKeys
	}
	qualifiesAsSplit := subtrees >= 2 && node.subtreeCount >= threshold

	// A node hosting a terminator key (one that ends exactly here, e.g. an
	// account at depth 64 above its storage) cannot be a split-point:
	// splitPoint.cells is indexed by child nibble and has no terminator slot,
	// so splitting would drop that key from the branch hash and diverge from
	// the sequential root.
	if qualifiesAsSplit && nodeHasTerminator(node) {
		qualifiesAsSplit = false
	}

	// Root that does not qualify as a split-point: still descend one level so
	// each emerging leafTask gets a non-empty prefix rooting a distinct
	// top-nibble subtree, instead of one task covering the whole trie.
	if isRoot && !qualifiesAsSplit {
		return pu.recurseChildren(ctx, node, nil)
	}

	if qualifiesAsSplit {
		nodePrefix := buildPrefix(accPrefix, node.ext)
		sp := &splitPoint{
			prefix:        nodePrefix,
			touchedBitmap: node.bitmap,
		}
		if err := pu.loadDBBranch(ctx, sp); err != nil {
			return err
		}
		// One depositor per subtree; the worker whose Add(-1) returns 0 is the
		// unique last finisher. (subtrees, not subtrees-1: at a 2-way fork the
		// latter would let both workers pass the barrier and publish two roots.)
		sp.arrived.Store(int32(subtrees))
		pu.splitMap.Set(nodePrefix, sp)
		pu.splitPoints = append(pu.splitPoints, sp)
		return pu.recurseChildren(ctx, node, nodePrefix)
	}

	// Non-root subtree that does not qualify as a split-point — collapse the
	// entire subtree (including this node's ext and all descendants) into a
	// single leafTask. The worker DFS-walks node to enumerate its keys.
	nodePrefix := buildPrefix(accPrefix, node.ext)
	pu.leafQueue = append(pu.leafQueue, leafTask{
		prefix:   nodePrefix,
		node:     node,
		keyCount: node.subtreeCount,
	})
	return nil
}

// recurseChildren walks every present child of node, appending the child's
// nibble to nodePrefix and continuing the DFS.
func (pu *parallelUpdate) recurseChildren(ctx PatriciaContext, node *prefixNode, nodePrefix []byte) error {
	childIdx := 0
	for bm := node.bitmap; bm != 0; {
		nib := byte(bits.TrailingZeros16(bm))
		child := node.children[childIdx]
		childAcc := make([]byte, len(nodePrefix)+1)
		copy(childAcc, nodePrefix)
		childAcc[len(nodePrefix)] = nib
		if err := pu.prepareDFS(ctx, child, childAcc, false); err != nil {
			return err
		}
		childIdx++
		bm &^= uint16(1) << nib
	}
	return nil
}

// loadDBBranch fetches the on-disk branch at sp.prefix (if any) and
// pre-populates sp.cells for every nibble the DB had populated.
//
// This is the critical "untouched-nibble" fix: a branch hash mixes ALL
// nibbles, not just the touched ones. Workers only write to touched slots, so
// the DB-only slots must be seeded here or the last-finisher would compute a
// hash over a partial set of cells.
func (pu *parallelUpdate) loadDBBranch(ctx PatriciaContext, sp *splitPoint) error {
	if ctx == nil {
		return nil
	}
	branchKey := nibbles.HexToCompact(sp.prefix)
	branch, _, err := ctx.Branch(branchKey)
	if err != nil {
		return fmt.Errorf("loadDBBranch(%x): %w", sp.prefix, err)
	}
	if len(branch) == 0 {
		return nil
	}
	_, afterMap, row, err := BranchData(branch).decodeCells()
	if err != nil {
		return fmt.Errorf("loadDBBranch(%x) decodeCells: %w", sp.prefix, err)
	}
	sp.dbBitmap = afterMap
	sp.branchBefore = true
	for bm := afterMap; bm != 0; {
		nib := bits.TrailingZeros16(bm)
		if row[nib] != nil {
			sp.cells[nib] = cellEncodeDataFromCell(row[nib])
		}
		bm &^= uint16(1) << nib
	}
	return nil
}

// buildPrefix concatenates the accumulated prefix and the node's compressed
// extension into a freshly-allocated slice. The returned slice is safe to
// retain (it does not alias the trie's arena nodes).
func buildPrefix(accPrefix, ext []byte) []byte {
	out := make([]byte, len(accPrefix)+len(ext))
	copy(out, accPrefix)
	copy(out[len(accPrefix):], ext)
	return out
}

// nodeHasTerminator reports whether at least one inserted key ends exactly at
// this node (subtreeCount > Σ children.subtreeCount, since Insert bumps the
// terminating node's count too). prepareDFS uses it to refuse split-point
// emission at nodes hosting inline leaves.
func nodeHasTerminator(node *prefixNode) bool {
	if node == nil {
		return false
	}
	var childSum uint32
	for _, c := range node.children {
		if c != nil {
			childSum += c.subtreeCount
		}
	}
	return node.subtreeCount > childSum
}
