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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// not goroutine-safe; pass a fresh one per Run call.
type BatchBranchResolver func(keys [][]byte) (vals [][]byte, err error)

func estimatedEntryCost(key, value []byte) int {
	return estimatedEntryOverheadBytes + len(key) + len(value)
}

// 33 = shortest HexToCompact key at depth >= 64.
const minEntryBytes = estimatedEntryOverheadBytes + 33

// 64 (account path) + 64 (keccak256(slot)).
const maxStorageTrunkDepth = 128

type pathKey struct {
	path []byte
	key  []byte
}

func toPathKey(path []byte) pathKey {
	// HexToCompact result may alias a reused buffer, so copy it.
	return pathKey{path: path, key: bytes.Clone(nibbles.HexToCompact(path))}
}

type ContractTrunkPreloadParallel struct {
	contractHash    []byte
	frontier        []pathKey
	pendingChildren []pathKey

	nextDepth       int
	pinnedPrefixes  [][]byte
	pinned          int
	usedBytes       int
	maxDepthReached int
	dbHitsPinned    int
	pinTxNum        uint64

	scratchDbHits   []pathKey
	scratchDbVals   [][]byte
	scratchFileMiss []pathKey
}

func NewContractTrunkPreloadParallel(contractHash []byte) (*ContractTrunkPreloadParallel, error) {
	if len(contractHash) != 32 {
		return nil, fmt.Errorf("NewContractTrunkPreloadParallel: contractHash must be 32 bytes, got %d", len(contractHash))
	}
	contractHashCopy := bytes.Clone(contractHash)
	return &ContractTrunkPreloadParallel{
		contractHash:    contractHashCopy,
		frontier:        []pathKey{toPathKey(ContractNibbles(contractHashCopy))},
		nextDepth:       64,
		maxDepthReached: 64,
	}, nil
}

// split out so profiler cost lands here, not Run.
func (p *ContractTrunkPreloadParallel) sortAndPartitionFrontier(dbBranches map[string][]byte) (dbHits []pathKey, dbVals [][]byte, fileMiss []pathKey, dbHitsBytes int) {
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
	// clear tail: shorter waves must not see stale entries.
	clear(dbHits[len(dbHits):cap(dbHits)])
	clear(dbVals[len(dbVals):cap(dbVals)])
	clear(fileMiss[len(fileMiss):cap(fileMiss)])
	return dbHits, dbVals, fileMiss, dbHitsBytes
}

// safe to clear: frontier already holds independent copies.
func (p *ContractTrunkPreloadParallel) releaseScratch() {
	clear(p.scratchDbHits)
	clear(p.scratchDbVals)
	clear(p.scratchFileMiss)
	p.scratchDbHits = p.scratchDbHits[:0]
	p.scratchDbVals = p.scratchDbVals[:0]
	p.scratchFileMiss = p.scratchFileMiss[:0]
}

// Partial pin set survives resolver errors for retry.
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

	// returns false once the step budget is exhausted.
	pin := func(pk pathKey, v []byte, depth int, next *[]pathKey) bool {
		cost := estimatedEntryCost(pk.key, v)
		if p.usedBytes+cost > stepCap {
			budgetHit = true
			return false
		}
		// step=0: no single source; pinTxNum keeps unwind coherent.
		cache.PinEntry(pk.key, v, 0, p.pinTxNum)
		p.pinnedPrefixes = append(p.pinnedPrefixes, bytes.Clone(pk.key))
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
			// Preserve un-pinned items; pendingChildren stays for next Run.
			rest := make([]pathKey, 0, len(dbHits)-dbHitStop+len(fileMiss)-fileMissStop+len(fileMissDeferred))
			rest = append(rest, dbHits[dbHitStop:]...)
			rest = append(rest, fileMiss[fileMissStop:]...)
			rest = append(rest, fileMissDeferred...)
			p.frontier = rest
			break
		}

		if len(fileMissDeferred) > 0 {
			// Clone so next partition can't overwrite scratch-aliased slice.
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
