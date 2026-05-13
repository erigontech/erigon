// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package commitment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// BatchBranchResolver resolves a batch of compact-encoded CommitmentDomain
// keys to their (already-dereferenced) branch-node values, reading from the
// file layer only — no MDBX cursor, hence no per-key cgo boundary crossing.
// The implementation is expected to fan the batch out across worker RoTxs;
// keys are passed in ascending key order so a contiguous-slice partition maps
// to contiguous file regions (drives page-cache readahead). Results MUST be
// returned in the same order as keys, with vals[i] == nil for keys not present
// in the file layer.
//
// Decoupled (callback form) so the commitment package needn't import db/state.
type BatchBranchResolver func(keys [][]byte) (vals [][]byte, err error)

// estimatedEntryCost is the predicted RAM cost of pinning a (key, value) branch
// entry — see estimatedEntryOverheadBytes in preload.go.
func estimatedEntryCost(key, value []byte) int {
	return estimatedEntryOverheadBytes + len(key) + len(value)
}

// minEntryBytes is a true lower bound on estimatedEntryCost for a storage-trunk
// branch (33 = shortest HexToCompact key at depth >= 64; value may be empty).
// Used to bound a wave's *file fetch*: capping at remaining/minEntryBytes+1
// never under-fetches, so the budget is guaranteed exhausted inside the wave.
const minEntryBytes = estimatedEntryOverheadBytes + 33

// maxStorageTrunkDepth is the deepest total nibble path a storage branch can
// have: 64 (account path) + 64 (keccak256(slot)) = 128.
const maxStorageTrunkDepth = 128

// pathKey is a single frontier entry: the nibble path (1 byte / nibble) and the
// HexToCompact-encoded CommitmentDomain key.
type pathKey struct {
	path []byte // nibble path (1 byte / nibble)
	key  []byte // HexToCompact(path) — the CommitmentDomain key
}

func toPathKey(path []byte) pathKey {
	k := nibbles.HexToCompact(path)
	kc := make([]byte, len(k))
	copy(kc, k) // HexToCompact result may alias a reused buffer
	return pathKey{path: path, key: kc}
}

// ContractTrunkPreloadParallel is the resumable analogue of ContractTrunkPreload
// (the serial BFS via CommitmentReader). It walks the contract's storage subtree
// breadth-first one depth-level ("wave") at a time and resolves missing branches
// through a batched, file-only BatchBranchResolver — no MDBX boundary crossings
// in the hot path. Each Run advances zero or more complete waves bounded by a
// per-call step budget; partial waves can be truncated by the file-fetch cap to
// fit the budget, but the truncated tail's siblings are skipped (BFS at the
// frontier is then resumed at depth+1 from the children of whatever was pinned).
//
// Mirrors ContractTrunkPreload's API surface (Run/PinnedTotal/UsedBytes/
// QueueRemaining/MaxDepthReached/PinnedPrefixes/ContractHash) so it slots in to
// adaptive_pin.go as a drop-in replacement without changing the controller's
// promote/extend/demote loop.
//
// Correctness: dbBranches values shadow file values for the same key (DB is
// authoritative for steps not yet flushed to files), so callers extending on a
// live node should pass the freshest MDBX-resident set per Run. With an empty
// dbBranches Run is a pure file-only preload — correct only from a cold snapshot
// or when MDBX/SD's flush-callback has already populated the cache with any
// freshly-written branches that would otherwise shadow the file values.
//
// Caller MUST NOT use the same instance from multiple goroutines. The
// BatchBranchResolver is passed PER Run rather than held on the struct so
// callers can supply a fresh tx-scoped resolver each call (e.g. adaptive
// controller building a resolver from the in-flight Flush tx every block).
//
// Resumability: when a step budget cuts a wave mid-iteration, the un-processed
// items at the current depth are preserved in `frontier`; the children of
// the items that did get pinned this wave land in `pendingChildren` at depth
// nextDepth+1. The next Run finishes the current depth first, then advances.
// Once a wave fully completes, frontier := pendingChildren and nextDepth++.
type ContractTrunkPreloadParallel struct {
	contractHash    []byte
	frontier        []pathKey // paths to process at depth = nextDepth
	pendingChildren []pathKey // accumulated children of pinned items at depth = nextDepth+1
	nextDepth       int       // depth of the next wave (starts at 64)
	pinnedPrefixes  [][]byte
	pinned          int
	usedBytes       int
	maxDepthReached int
	dbHitsPinned    int
}

// NewContractTrunkPreloadParallel seeds a resumable preload state at depth 64
// (storage subtree root) for the given contract. The resolver is passed
// per-Run (not stored on the state) so callers can hand a fresh tx-scoped
// resolver to each call.
func NewContractTrunkPreloadParallel(contractHash []byte) (*ContractTrunkPreloadParallel, error) {
	if len(contractHash) != 32 {
		return nil, fmt.Errorf("NewContractTrunkPreloadParallel: contractHash must be 32 bytes, got %d", len(contractHash))
	}
	contractHashCopy := make([]byte, len(contractHash))
	copy(contractHashCopy, contractHash)
	return &ContractTrunkPreloadParallel{
		contractHash:    contractHashCopy,
		frontier:        []pathKey{toPathKey(ContractNibbles(contractHashCopy))},
		nextDepth:       64,
		maxDepthReached: 64,
	}, nil
}

// Run advances the wave-BFS, pinning branches into cache until either the step
// budget is exhausted, the frontier is empty (preload complete), or
// maxStorageTrunkDepth is reached. Returns newlyPinned during THIS call (not
// cumulative — see PinnedTotal), whether the preload is now complete, and any
// resolver error.
//
// dbBranches is consulted per wave (MDBX-resident overlays — values shadow file
// values, freshest). Pass nil for file-only mode (correct from a cold snapshot
// or when SD.Flush's cache callback has already populated freshly-written
// branches into the cache that would otherwise need to come from dbBranches).
//
// On resolver error the partial pin set survives; the wave position is
// preserved so a retry on the next Run picks up where it left off (at the same
// p.nextDepth).
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

	stepCap := p.usedBytes + stepBudgetBytes
	chunkPinned := 0
	budgetHit := false

	// pin records the entry, advances accounting, queues the branch's
	// children. Returns false if the entry didn't fit (step budget exhausted).
	pin := func(pk pathKey, v []byte, depth int, next *[]pathKey) bool {
		cost := estimatedEntryCost(pk.key, v)
		if p.usedBytes+cost > stepCap {
			budgetHit = true
			return false
		}
		cache.PinEntry(pk.key, v, 0, "preload-trunk-parallel-resumable")
		kc := make([]byte, len(pk.key))
		copy(kc, pk.key)
		p.pinnedPrefixes = append(p.pinnedPrefixes, kc)
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
			for n := 0; n < 16; n++ {
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
		// Ascending key order so a contiguous-slice partition of the file batch
		// is contiguous-in-file (page-cache readahead). BFS append order is
		// already key-sorted (HexToCompact packing is monotone for same-length
		// paths), but sort defensively — a few k entries / wave, cheap.
		sort.Slice(p.frontier, func(i, j int) bool { return bytes.Compare(p.frontier[i].key, p.frontier[j].key) < 0 })

		// Split this wave into DB-resident hits (have the value) and file-misses
		// (need a fetch). Both stay sorted (sub-sequences of the sorted frontier).
		var dbHits []pathKey
		var dbVals [][]byte
		var fileMiss []pathKey
		dbHitsBytes := 0
		for _, pk := range p.frontier {
			if v, ok := dbBranches[string(pk.key)]; ok {
				dbHits = append(dbHits, pk)
				dbVals = append(dbVals, v)
				dbHitsBytes += estimatedEntryCost(pk.key, v)
			} else {
				fileMiss = append(fileMiss, pk)
			}
		}

		// Cap the *file fetch* at what the step budget can absorb after the
		// DB-hits (which we pin first). minEntryBytes is a true lower bound,
		// so this never under-fetches; if the cap truncates the wave, the
		// un-fetched tail is preserved in p.frontier for the next Run.
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
				// State unchanged — retry on next Run picks up at same depth.
				return chunkPinned, false, fmt.Errorf("preload at depth %d: %w", depth, err)
			}
			if len(fileVals) != len(keys) {
				return chunkPinned, false, fmt.Errorf("preload at depth %d: resolver returned %d vals for %d keys", depth, len(fileVals), len(keys))
			}
		}

		// Pin in dbHits-then-fileMiss order. Children of pinned items accumulate
		// in p.pendingChildren (depth nextDepth+1). On budget hit mid-iteration,
		// the un-processed remainder of dbHits/fileMiss + the deferred
		// fileMissDeferred all become the new frontier (still at nextDepth).
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
					continue // not in the file layer either
				}
				if !pin(pk, v, depth, &p.pendingChildren) {
					fileMissStop = i
					break
				}
			}
		}

		if budgetHit {
			// Preserve un-pinned items at the current depth for the next Run.
			// Resolved-but-not-pinned file values are discarded (callable resolver
			// will be re-invoked on the next Run for these keys).
			var rest []pathKey
			rest = append(rest, dbHits[dbHitStop:]...)
			rest = append(rest, fileMiss[fileMissStop:]...)
			rest = append(rest, fileMissDeferred...)
			p.frontier = rest
			// Don't advance nextDepth — pendingChildren stays at depth+1 for
			// when this depth is fully drained on a future Run.
			break
		}

		// Wave fully processed. Anything in fileMissDeferred (won't have any
		// since !budgetHit ⇒ no truncation) goes back to frontier; pending
		// children promote to the next wave.
		if len(fileMissDeferred) > 0 {
			// Shouldn't happen — if we got here without budgetHit, no truncation
			// occurred. Defensive: re-queue them at the current depth so we
			// don't lose data.
			p.frontier = fileMissDeferred
		} else {
			p.frontier = p.pendingChildren
			p.pendingChildren = nil
			p.nextDepth++
		}
	}

	queueEmpty = (len(p.frontier) == 0 && len(p.pendingChildren) == 0) || p.nextDepth > maxStorageTrunkDepth
	if logger != nil && (chunkPinned > 0 || queueEmpty) {
		logger.Info("[trunk-preload-parallel] step",
			"contract_hash", fmt.Sprintf("%x", p.contractHash),
			"step_budget_mb", stepBudgetBytes/(1<<20),
			"used_mb_total", p.usedBytes/(1<<20),
			"pinned_this_step", chunkPinned,
			"pinned_total", p.pinned,
			"db_hits_total", p.dbHitsPinned,
			"max_depth_reached", p.maxDepthReached,
			"queue_empty", queueEmpty,
			"next_depth", p.nextDepth,
			"frontier_size", len(p.frontier))
	}
	return chunkPinned, queueEmpty, nil
}

// PinnedTotal returns the cumulative number of branches pinned by this preload
// state across all Run calls.
func (p *ContractTrunkPreloadParallel) PinnedTotal() int { return p.pinned }

// UsedBytes returns the cumulative byte cost of this preload's pin set.
func (p *ContractTrunkPreloadParallel) UsedBytes() int { return p.usedBytes }

// QueueRemaining returns the number of paths still queued for descent
// (un-processed at the current depth + accumulated children at the next).
// Zero means the preload is complete; further Run calls are no-ops.
func (p *ContractTrunkPreloadParallel) QueueRemaining() int {
	return len(p.frontier) + len(p.pendingChildren)
}

// MaxDepthReached returns the deepest trie depth pinned so far.
func (p *ContractTrunkPreloadParallel) MaxDepthReached() int { return p.maxDepthReached }

// PinnedPrefixes returns the prefix slices this preload has added to the cache
// so far. The adaptive controller uses this to Invalidate the contract's pin
// set on demotion. The returned slice aliases internal storage; do not mutate.
func (p *ContractTrunkPreloadParallel) PinnedPrefixes() [][]byte { return p.pinnedPrefixes }

// ContractHash returns the contract this preload targets.
func (p *ContractTrunkPreloadParallel) ContractHash() []byte { return p.contractHash }

// DbHitsPinned returns the cumulative number of branches that were pinned from
// the dbBranches overlay (rather than via the file resolver). Diagnostic only.
func (p *ContractTrunkPreloadParallel) DbHitsPinned() int { return p.dbHitsPinned }

// PreloadContractTrunkParallel pins contract H's storage-trunk branches into
// cache in a single call, bounded by ramBudgetBytes. Convenience wrapper around
// NewContractTrunkPreloadParallel + Run for callers that don't need phased
// loading. The startup PIN_CONTRACT_TRUNKS hook uses this; the adaptive layer
// holds a *ContractTrunkPreloadParallel and calls Run incrementally.
//
// dbBranches is the MDBX-resident set — values shadow file values for the same
// key. Pass nil/empty for file-only mode (correct only from a cold snapshot).
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
