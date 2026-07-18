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
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// BatchBranchResolver resolves a batch of compact-encoded CommitmentDomain
// keys to branch-node values, reading from the file layer only (no MDBX cursor,
// no per-key cgo crossing). Keys are passed in ascending key order so a
// contiguous-slice partition maps to contiguous file regions (page-cache
// readahead). Results MUST be returned in the same order as keys, with
// vals[i] == nil for keys not present.
type BatchBranchResolver func(keys [][]byte) (vals [][]byte, err error)

func estimatedEntryCost(key, value []byte) int {
	return estimatedEntryOverheadBytes + len(key) + len(value)
}

// minEntryBytes: true lower bound on estimatedEntryCost for a storage-trunk
// branch (33 = shortest HexToCompact key at depth >= 64; value may be empty).
// Bounds a wave's file fetch so the budget is guaranteed exhausted inside it.
const minEntryBytes = estimatedEntryOverheadBytes + 33

// maxStorageTrunkDepth: 64 (account path) + 64 (keccak256(slot)) = 128.
const maxStorageTrunkDepth = 128

type pathKey struct {
	path []byte // nibble path (1 byte / nibble)
	key  []byte // HexToCompact(path)
}

func toPathKey(path []byte) pathKey {
	// HexToCompact result may alias a reused buffer, so copy it.
	return pathKey{path: path, key: common.Copy(nibbles.HexToCompact(path))}
}

// ContractTrunkPreloadParallel is the wave-BFS analogue of ContractTrunkPreload.
// It walks one depth-level per wave and resolves missing branches through a
// batched, file-only BatchBranchResolver (no MDBX in the hot path). Each Run
// advances zero or more waves bounded by stepBudgetBytes; partial waves are
// truncated to fit the budget and resumed on the next Run.
//
// dbBranches shadows file values for the same key — DB is authoritative for
// steps not yet flushed to files. Pass nil for cold-snapshot / file-only mode.
//
// Not goroutine-safe. The resolver is passed per-Run (not held) so callers
// can supply a fresh tx-scoped resolver each block.
type ContractTrunkPreloadParallel struct {
	contractHash    []byte
	frontier        []pathKey // paths to process at depth = nextDepth
	pendingChildren []pathKey // accumulated children of pinned items at depth = nextDepth+1

	nextDepth       int // depth of the next wave (starts at 64)
	pinnedPrefixes  [][]byte
	pinned          int
	usedBytes       int
	maxDepthReached int
	dbHitsPinned    int
	// pinTxNum stamps pinned entries with the head txNum they were read at, so a
	// later unwind below that point evicts them via the BranchCache floor (a
	// txN=0 pin would escape it and be served stale after a deep unwind).
	pinTxNum uint64

	// Reusable per-wave partition scratch. Contents are copied into the next
	// frontier before the buffers are reused, so retaining the grown backing
	// across Run calls avoids re-allocating them for every budget-limited step.
	scratchDbHits   []pathKey
	scratchDbVals   [][]byte
	scratchFileMiss []pathKey
}

// NewContractTrunkPreloadParallel seeds a preload at depth 64 (storage subtree root).
func NewContractTrunkPreloadParallel(contractHash []byte) (*ContractTrunkPreloadParallel, error) {
	if len(contractHash) != 32 {
		return nil, fmt.Errorf("NewContractTrunkPreloadParallel: contractHash must be 32 bytes, got %d", len(contractHash))
	}
	contractHashCopy := common.Copy(contractHash)
	return &ContractTrunkPreloadParallel{
		contractHash:    contractHashCopy,
		frontier:        []pathKey{toPathKey(ContractNibbles(contractHashCopy))},
		nextDepth:       64,
		maxDepthReached: 64,
	}, nil
}

// sortAndPartitionFrontier sorts the frontier ascending by key, then splits it
// into DB-shadowed hits (with values and total cost) and file misses. Returned
// slices alias reusable scratch and are valid until the next call. Kept as its
// own method so profiles attribute the sort + partition cost here, not to Run.
func (p *ContractTrunkPreloadParallel) sortAndPartitionFrontier(dbBranches map[string][]byte) (dbHits []pathKey, dbVals [][]byte, fileMiss []pathKey, dbHitsBytes int) {
	t := time.Now()
	slices.SortFunc(p.frontier, func(a, b pathKey) int { return bytes.Compare(a.key, b.key) })

	dbHits = p.scratchDbHits[:0]
	dbVals = p.scratchDbVals[:0]
	fileMiss = p.scratchFileMiss[:0]
	for i := range p.frontier {
		pk := &p.frontier[i]
		if v, ok := dbBranches[string(pk.key)]; ok {
			dbHits = append(dbHits, *pk)
			dbVals = append(dbVals, v)
			dbHitsBytes += estimatedEntryCost(pk.key, v)
		} else {
			fileMiss = append(fileMiss, *pk)
		}
	}
	p.scratchDbHits, p.scratchDbVals, p.scratchFileMiss = dbHits, dbVals, fileMiss
	// Drop references in the reused tail so a large earlier wave doesn't pin its
	// path/key bytes (and stale dbBranches values) alive across Run calls.
	clear(dbHits[len(dbHits):cap(dbHits)])
	clear(dbVals[len(dbVals):cap(dbVals)])
	clear(fileMiss[len(fileMiss):cap(fileMiss)])
	log.Warn("[dbg] sortAndPartitionFrontier", "l", len(p.frontier), "took", time.Since(t))
	return dbHits, dbVals, fileMiss, dbHitsBytes
}

// Run advances the wave-BFS until stepBudgetBytes is exhausted, the frontier
// is empty, or maxStorageTrunkDepth is reached. On resolver error the partial
// pin set and wave position survive for retry on the next Run.
func (p *ContractTrunkPreloadParallel) Run(
	stepBudgetBytes int,
	dbBranches map[string][]byte,
	resolve BatchBranchResolver,
	cache *BranchCache,
	logger log.Logger,
) (newlyPinned int, queueEmpty bool, err error) {
	if cache == nil {
		return 0, false, fmt.Errorf("ContractTrunkPreloadParallel.Run: cache is nil")
	}
	if resolve == nil {
		return 0, false, fmt.Errorf("ContractTrunkPreloadParallel.Run: resolver is nil")
	}
	if stepBudgetBytes <= 0 {
		return 0, len(p.frontier) == 0, nil
	}
	defer p.releaseScratch()

	stepCap := p.usedBytes + stepBudgetBytes
	chunkPinned := 0
	budgetHit := false

	// pin records the entry and queues its children. Returns false on budget hit.
	pin := func(pk pathKey, v []byte, depth int, next *[]pathKey) bool {
		cost := estimatedEntryCost(pk.key, v)
		if p.usedBytes+cost > stepCap {
			budgetHit = true
			return false
		}
		// step=0: a storage-trunk branch resolved across merged files has no single
		// source step, and the pinTxNum stamp already gives unwind coherence — the
		// floor drops a preloaded pin before the cStep<=maxStep gate is consulted,
		// so leaving step unset only keeps that gate trivially true for live pins.
		cache.PinEntry(pk.key, v, 0, p.pinTxNum)
		p.pinnedPrefixes = append(p.pinnedPrefixes, common.Copy(pk.key))
		p.usedBytes += cost
		p.pinned++
		chunkPinned++
		if depth > p.maxDepthReached {
			p.maxDepthReached = depth
		}
		if logger != nil && p.pinned%5000 == 0 {
			logger.Info("[trunk-preload-parallel] progress",
				"pinned", p.pinned, "depth", depth, "used_mb", p.usedBytes/(1<<20))
		}
		if len(v) >= 4 { // 2-byte touchMap || 2-byte afterMap || per-child data
			bitmap := binary.BigEndian.Uint16(v[2:4])
			for n := range 16 {
				if bitmap&(1<<uint(n)) == 0 {
					continue
				}
				childPath := make([]byte, len(pk.path)+1)
				copy(childPath, pk.path)
				childPath[len(pk.path)] = byte(n)
				*next = append(*next, toPathKey(childPath))
			}
		}
		return true
	}

	for !budgetHit && p.nextDepth <= maxStorageTrunkDepth && len(p.frontier) > 0 {
		depth := p.nextDepth
		dbHits, dbVals, fileMiss, dbHitsBytes := p.sortAndPartitionFrontier(dbBranches)

		// Cap the file fetch by what the budget can absorb after dbHits.
		var fileMissDeferred []pathKey
		if fileBudget := stepCap - p.usedBytes - dbHitsBytes; fileBudget <= 0 {
			fileMissDeferred = fileMiss
			fileMiss = nil
		} else if maxFileFetch := fileBudget/minEntryBytes + 1; maxFileFetch < len(fileMiss) {
			fileMissDeferred = fileMiss[maxFileFetch:]
			fileMiss = fileMiss[:maxFileFetch]
		}

		var fileVals [][]byte
		if len(fileMiss) > 0 {
			keys := make([][]byte, len(fileMiss))
			for i := range fileMiss {
				keys[i] = fileMiss[i].key
			}
			fileVals, err = resolve(keys)
			if err != nil {
				return chunkPinned, false, fmt.Errorf("preload at depth %d: %w", depth, err)
			}
			if len(fileVals) != len(keys) {
				return chunkPinned, false, fmt.Errorf("preload at depth %d: resolver returned %d vals for %d keys", depth, len(fileVals), len(keys))
			}
		}

		dbHitStop := len(dbHits)
		for i, pk := range dbHits {
			if !pin(pk, dbVals[i], depth, &p.pendingChildren) {
				dbHitStop = i
				break
			}
			p.dbHitsPinned++
		}
		fileMissStop := len(fileMiss)
		if !budgetHit {
			for i, pk := range fileMiss {
				v := fileVals[i]
				if v == nil {
					continue
				}
				if !pin(pk, v, depth, &p.pendingChildren) {
					fileMissStop = i
					break
				}
			}
		}

		if budgetHit {
			// Preserve un-pinned items at current depth; pendingChildren stays
			// at depth+1 for when this depth is drained on a future Run.
			rest := make([]pathKey, 0, len(dbHits)-dbHitStop+len(fileMiss)-fileMissStop+len(fileMissDeferred))
			rest = append(rest, dbHits[dbHitStop:]...)
			rest = append(rest, fileMiss[fileMissStop:]...)
			rest = append(rest, fileMissDeferred...)
			p.frontier = rest
			break
		}

		if len(fileMissDeferred) > 0 {
			// Defensive: !budgetHit should mean no truncation. Clone out of the
			// scratch-aliased slice so the next wave's partition can't overwrite it.
			p.frontier = slices.Clone(fileMissDeferred)
		} else {
			p.frontier = p.pendingChildren
			p.pendingChildren = nil
			p.nextDepth++
		}
	}

	queueEmpty = (len(p.frontier) == 0 && len(p.pendingChildren) == 0) || p.nextDepth > maxStorageTrunkDepth
	if logger != nil && (chunkPinned > 0 || queueEmpty) {
		logger.Info("[trunk-preload-parallel] step",
			"step_budget_mb", stepBudgetBytes/(1<<20),
			"used_mb", p.usedBytes/(1<<20),
			"pinned_this_step", chunkPinned,
			"pinned", p.pinned,
			"db_hits", p.dbHitsPinned,
			"max_depth_reached", p.maxDepthReached,
			"queue_empty", queueEmpty,
			"next_depth", p.nextDepth,
			"frontier_size", len(p.frontier),
			"contract_hash", fmt.Sprintf("%x", p.contractHash))
	}
	return chunkPinned, queueEmpty, nil
}

func (p *ContractTrunkPreloadParallel) PinnedTotal() int     { return p.pinned }
func (p *ContractTrunkPreloadParallel) UsedBytes() int       { return p.usedBytes }
func (p *ContractTrunkPreloadParallel) MaxDepthReached() int { return p.maxDepthReached }
func (p *ContractTrunkPreloadParallel) DbHitsPinned() int    { return p.dbHitsPinned }
func (p *ContractTrunkPreloadParallel) ContractHash() []byte { return p.contractHash }

func (p *ContractTrunkPreloadParallel) QueueRemaining() int {
	return len(p.frontier) + len(p.pendingChildren)
}

// PinnedPrefixes returns slices aliasing internal storage — do not mutate.
func (p *ContractTrunkPreloadParallel) PinnedPrefixes() [][]byte { return p.pinnedPrefixes }

// PreloadContractTrunkParallel is the one-shot wrapper around
// NewContractTrunkPreloadParallel + Run.
func PreloadContractTrunkParallel(
	contractHash []byte,
	ramBudgetBytes int,
	dbBranches map[string][]byte,
	resolve BatchBranchResolver,
	cache *BranchCache,
	logger log.Logger,
) (pinned int, err error) {
	if ramBudgetBytes <= 0 {
		return 0, fmt.Errorf("PreloadContractTrunkParallel: ramBudgetBytes must be positive, got %d", ramBudgetBytes)
	}
	p, err := NewContractTrunkPreloadParallel(contractHash)
	if err != nil {
		return 0, err
	}
	if resolve == nil {
		return 0, fmt.Errorf("PreloadContractTrunkParallel: resolver is nil")
	}
	pinned, queueEmpty, err := p.Run(ramBudgetBytes, dbBranches, resolve, cache, logger)
	if logger != nil {
		logger.Info("[trunk-preload-parallel] complete",
			"contract_hash", fmt.Sprintf("%x", contractHash),
			"ram_budget_mb", ramBudgetBytes/(1<<20),
			"used_mb", p.UsedBytes()/(1<<20),
			"pinned", pinned,
			"db_hits_pinned", p.DbHitsPinned(),
			"max_depth_reached", p.MaxDepthReached(),
			"budget_exhausted", !queueEmpty,
			"cache_pinned_total", cache.PinnedCount())
	}
	return pinned, err
}
