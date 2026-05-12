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

// PreloadContractTrunkParallel pins contract H's storage-trunk branches into
// cache by walking the subtree breadth-first, one depth level ("wave") at a
// time. Within a wave, each branch is resolved either from dbBranches (the
// MDBX-resident set — the freshest values; supplied by the caller's phase-1
// range-cursor prefetch, may be nil/empty) or, for keys absent from it, through
// a single batched, parallelisable file-only read (BatchBranchResolver).
//
// Breadth-first so a budget-truncated run still pins the shallowest, highest-
// reuse branches first; wave-batched so the file resolver can sort+partition the
// reads for locality. DB-hits are pinned before file-hits within a wave (the DB
// holds the freshest, most-reused branches of a hot contract). Bounded by
// ramBudgetBytes. Returns the number of branches pinned.
//
// Correctness: dbBranches values shadow file values for the same key (DB is
// authoritative for steps not yet flushed to files), so passing the DB-resident
// set is what makes this safe on a live node. With an empty dbBranches it's a
// pure file-only preload — correct only from a cold snapshot, where MDBX holds
// nothing for the subtree.
func PreloadContractTrunkParallel(
	contractHash []byte,
	ramBudgetBytes int,
	dbBranches map[string][]byte,
	resolve BatchBranchResolver,
	cache *BranchCache,
	logger log.Logger,
) (pinned int, err error) {
	if cache == nil {
		return 0, fmt.Errorf("PreloadContractTrunkParallel: cache is nil")
	}
	if resolve == nil {
		return 0, fmt.Errorf("PreloadContractTrunkParallel: resolver is nil")
	}
	if ramBudgetBytes <= 0 {
		return 0, fmt.Errorf("PreloadContractTrunkParallel: ramBudgetBytes must be positive, got %d", ramBudgetBytes)
	}
	if len(contractHash) != 32 {
		return 0, fmt.Errorf("PreloadContractTrunkParallel: contractHash must be 32 bytes, got %d", len(contractHash))
	}

	type pathKey struct {
		path []byte // nibble path (1 byte / nibble)
		key  []byte // HexToCompact(path) — the CommitmentDomain key
	}
	toPathKey := func(path []byte) pathKey {
		k := nibbles.HexToCompact(path)
		kc := make([]byte, len(k))
		copy(kc, k) // HexToCompact result may alias a reused buffer
		return pathKey{path: path, key: kc}
	}

	frontier := []pathKey{toPathKey(ContractNibbles(contractHash))}
	usedBytes := 0
	maxDepthReached := 64
	budgetHit := false
	var dbHitsPinned int

	// pin records the entry, advances accounting, and queues the branch's
	// children. Returns false if the entry didn't fit (budget exhausted).
	pin := func(pk pathKey, v []byte, depth int, next *[]pathKey) bool {
		cost := estimatedEntryCost(pk.key, v)
		if usedBytes+cost > ramBudgetBytes {
			budgetHit = true
			return false
		}
		cache.PinEntry(pk.key, v, 0, "preload-trunk-parallel")
		usedBytes += cost
		pinned++
		if depth > maxDepthReached {
			maxDepthReached = depth
		}
		if logger != nil && pinned%5000 == 0 {
			logger.Info("[trunk-preload-parallel] progress",
				"pinned", pinned, "depth", depth, "used_mb", usedBytes/(1<<20))
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

	for depth := 64; depth <= maxStorageTrunkDepth && len(frontier) > 0 && !budgetHit; depth++ {
		// Ascending key order so a contiguous-slice partition of the file batch
		// is contiguous-in-file (page-cache readahead). BFS append order is
		// already key-sorted (HexToCompact packing is monotone for same-length
		// paths), but sort defensively — a few k entries / wave, cheap.
		sort.Slice(frontier, func(i, j int) bool { return bytes.Compare(frontier[i].key, frontier[j].key) < 0 })

		// Split this wave into DB-resident hits (have the value) and file-misses
		// (need a fetch). Both stay sorted (sub-sequences of the sorted frontier).
		var dbHits []pathKey
		var dbVals [][]byte
		var fileMiss []pathKey
		dbHitsBytes := 0
		for _, pk := range frontier {
			if v, ok := dbBranches[string(pk.key)]; ok {
				dbHits = append(dbHits, pk)
				dbVals = append(dbVals, v)
				dbHitsBytes += estimatedEntryCost(pk.key, v)
			} else {
				fileMiss = append(fileMiss, pk)
			}
		}

		// Cap the *file fetch* at what the budget can absorb after the DB-hits
		// (which we pin first). minEntryBytes is a true lower bound, so this
		// never under-fetches; a truncated wave is guaranteed to exhaust the
		// budget, so we stop after it.
		truncatedByBudget := false
		if fileBudget := ramBudgetBytes - usedBytes - dbHitsBytes; fileBudget <= 0 {
			if len(fileMiss) > 0 {
				fileMiss = fileMiss[:0]
				truncatedByBudget = true
			}
		} else if maxFileFetch := fileBudget/minEntryBytes + 1; maxFileFetch < len(fileMiss) {
			fileMiss = fileMiss[:maxFileFetch]
			truncatedByBudget = true
		}

		var fileVals [][]byte
		if len(fileMiss) > 0 {
			keys := make([][]byte, len(fileMiss))
			for i := range fileMiss {
				keys[i] = fileMiss[i].key
			}
			fileVals, err = resolve(keys)
			if err != nil {
				return pinned, fmt.Errorf("preload at depth %d: %w", depth, err)
			}
			if len(fileVals) != len(keys) {
				return pinned, fmt.Errorf("preload at depth %d: resolver returned %d vals for %d keys", depth, len(fileVals), len(keys))
			}
		}

		var next []pathKey
		for i, pk := range dbHits {
			if !pin(pk, dbVals[i], depth, &next) {
				break
			}
			dbHitsPinned++
		}
		if !budgetHit {
			for i, pk := range fileMiss {
				v := fileVals[i]
				if v == nil {
					continue // not in the file layer either (shouldn't happen with a complete dbBranches+files)
				}
				if !pin(pk, v, depth, &next) {
					break
				}
			}
		}
		if truncatedByBudget {
			budgetHit = true
		}
		frontier = next
	}

	if logger != nil {
		logger.Info("[trunk-preload-parallel] complete",
			"contract_hash", fmt.Sprintf("%x", contractHash),
			"ram_budget_mb", ramBudgetBytes/(1<<20),
			"used_mb", usedBytes/(1<<20),
			"pinned", pinned,
			"db_hits_pinned", dbHitsPinned,
			"max_depth_reached", maxDepthReached,
			"budget_exhausted", budgetHit,
			"cache_pinned_total", cache.PinnedCount())
	}
	return pinned, nil
}

// maxStorageTrunkDepth is the deepest total nibble path a storage branch can
// have: 64 (account path) + 64 (keccak256(slot)) = 128.
const maxStorageTrunkDepth = 128
