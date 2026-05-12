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

// PreloadContractTrunkParallel pins contract H's storage-trunk branches into
// cache by walking the subtree breadth-first, one depth level ("wave") at a
// time, resolving each wave's branches through a single batched, parallelisable
// file-only read. Breadth-first so a budget-truncated run still pins the
// shallowest (highest-reuse) branches first; wave-batched so the resolver can
// sort+partition the reads for locality instead of issuing them one at a time.
//
// Bounded by ramBudgetBytes (same accounting as the BFS ContractTrunkPreload).
// Returns the number of branches pinned.
//
// File-layer-only: a branch present only in MDBX (recently written, not yet in
// a .kv file) is invisible here, and a branch present in both MDBX (fresh) and
// files (stale) resolves to the stale value. That is correct for the
// from-cold-snapshot trigger path (MDBX holds nothing for the subtree) — the
// case PIN_CONTRACT_TRUNKS targets. The MDBX-resident set is handled separately
// (a single range-cursor prefetch); see agentspecs/commitment-branch-preload-redesign.md.
func PreloadContractTrunkParallel(
	contractHash []byte,
	ramBudgetBytes int,
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

	contractNibbles := make([]byte, 64)
	for i, b := range contractHash {
		contractNibbles[2*i] = b >> 4
		contractNibbles[2*i+1] = b & 0x0f
	}

	type pathKey struct {
		path []byte // nibble path (1 byte / nibble)
		key  []byte // HexToCompact(path) — the CommitmentDomain key
	}
	toPathKey := func(path []byte) pathKey {
		k := nibbles.HexToCompact(path)
		// HexToCompact may return a slice over a reused buffer.
		kc := make([]byte, len(k))
		copy(kc, k)
		return pathKey{path: path, key: kc}
	}

	frontier := []pathKey{toPathKey(contractNibbles)}
	usedBytes := 0
	maxDepthReached := 64
	budgetHit := false

	for depth := 64; depth <= maxStorageTrunkDepth && len(frontier) > 0 && !budgetHit; depth++ {
		// Ascending key order so the resolver's contiguous partition is
		// contiguous-in-file. BFS append order is already key-sorted (the
		// HexToCompact packing is monotone for same-length paths), but sort
		// defensively — it's a few k entries / wave, cheap.
		sort.Slice(frontier, func(i, j int) bool { return bytes.Compare(frontier[i].key, frontier[j].key) < 0 })

		// Cap the wave's *fetch* at what the remaining budget can possibly
		// absorb (minEntryBytes is a true lower bound on a pinned entry's cost,
		// so this never under-fetches and the budget is guaranteed exhausted
		// inside the wave). Without it, a wide budget-truncated wave — depth 69
		// is ~1.3M keys but only ~20k fit — would issue >1M file lookups to pin
		// a tiny fraction, and that over-fetch races the next block.
		const minEntryBytes = estimatedEntryOverheadBytes + 33 // 33 = shortest compact key at depth >= 64; value >= 0 bytes
		truncatedByBudget := false
		if remaining := ramBudgetBytes - usedBytes; remaining > 0 {
			if maxFetch := remaining/minEntryBytes + 1; maxFetch < len(frontier) {
				frontier = frontier[:maxFetch]
				truncatedByBudget = true
			}
		}

		keys := make([][]byte, len(frontier))
		for i := range frontier {
			keys[i] = frontier[i].key
		}
		vals, rerr := resolve(keys)
		if rerr != nil {
			return pinned, fmt.Errorf("preload at depth %d: %w", depth, rerr)
		}
		if len(vals) != len(keys) {
			return pinned, fmt.Errorf("preload at depth %d: resolver returned %d vals for %d keys", depth, len(vals), len(keys))
		}

		var next []pathKey
		for i, pk := range frontier {
			v := vals[i]
			if v == nil {
				continue // not in the file layer
			}
			entryCost := estimatedEntryOverheadBytes + len(pk.key) + len(v)
			if usedBytes+entryCost > ramBudgetBytes {
				budgetHit = true
				break
			}
			cache.PinEntry(pk.key, v, 0, "preload-trunk-parallel")
			usedBytes += entryCost
			pinned++
			if depth > maxDepthReached {
				maxDepthReached = depth
			}
			if logger != nil && pinned%5000 == 0 {
				logger.Info("[trunk-preload-parallel] progress",
					"pinned", pinned, "depth", depth, "used_mb", usedBytes/(1<<20))
			}
			// Decode the branch header (2-byte touchMap || 2-byte afterMap ||
			// per-child data) and queue the children that exist in the trie.
			if len(v) < 4 {
				continue
			}
			bitmap := binary.BigEndian.Uint16(v[2:4])
			for n := 0; n < 16; n++ {
				if bitmap&(1<<uint(n)) == 0 {
					continue
				}
				childPath := make([]byte, len(pk.path)+1)
				copy(childPath, pk.path)
				childPath[len(pk.path)] = byte(n)
				next = append(next, toPathKey(childPath))
			}
		}
		if truncatedByBudget {
			budgetHit = true // belt-and-braces: the wave was sized to exhaust the budget
		}
		frontier = next
	}

	if logger != nil {
		logger.Info("[trunk-preload-parallel] complete",
			"contract_hash", fmt.Sprintf("%x", contractHash),
			"ram_budget_mb", ramBudgetBytes/(1<<20),
			"used_mb", usedBytes/(1<<20),
			"pinned", pinned,
			"max_depth_reached", maxDepthReached,
			"budget_exhausted", budgetHit,
			"cache_pinned_total", cache.PinnedCount())
	}
	return pinned, nil
}

// maxStorageTrunkDepth is the deepest total nibble path a storage branch can
// have: 64 (account path) + 64 (keccak256(slot)) = 128.
const maxStorageTrunkDepth = 128
