// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package commitment

import (
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// CommitmentReader is the read interface PreloadContractTrunk needs:
// GetLatest on the CommitmentDomain. Returns (value, step, found, err).
// Decoupled from any specific tx/aggregator type so callers can plug
// the reader in at preload time without dragging in db/state imports.
type CommitmentReader func(prefix []byte) (v []byte, step uint64, found bool, err error)

// estimatedEntryOverheadBytes accounts for the per-entry RAM cost
// beyond the encoded value itself: the branchCacheEntry struct
// (~80 B), maphash slot + hash (~40 B), prefix slice (~24 B header +
// content), value slice header (~24 B). Used to predict whether
// pinning the next entry would exceed the configured budget.
const estimatedEntryOverheadBytes = 168

// PreloadContractTrunk pins commitment branches for the given contract's
// storage subtree into the supplied BranchCache, breadth-first from
// depth 64 (storage subtree root) outward. The descent is bounded by
// ramBudgetBytes — pinning stops the moment the next entry would push
// estimated total RAM over the budget. The depth reached is therefore
// emergent from (trie shape, contract sparsity, budget), not configured
// directly.
//
// Why BFS: a depth-first descent under a hard branch-count cap can
// exhaust budget on one subtree's deep children before completing the
// shallower trunk for sibling branches, producing a pin set that is
// "deep narrow slice" rather than "saturated trunk". Trunk reuse is
// proportional to depth shallowness — each branch at depth N is shared
// across all of its descendant slot reads. BFS guarantees layer N is
// completely pinned before layer N+1 starts, so the budget is always
// spent on the highest-value (most-shared) branches first.
//
// contractHash is keccak256(address) — 32 bytes. The trunk lives at
// the commitment-domain prefix that corresponds to nibbles 0..63 of
// the storage path; enumeration extends from there into nibbles 64+
// until the budget is exhausted or the contract's storage trie has
// no further branches.
//
// Returns the number of pinned branches and any error from the reader.
// On error the partial pin set remains in place — those entries are
// still valid cache hits.
func PreloadContractTrunk(
	contractHash []byte,
	ramBudgetBytes int,
	reader CommitmentReader,
	cache *BranchCache,
	logger log.Logger,
) (int, error) {
	if len(contractHash) != 32 {
		return 0, fmt.Errorf("PreloadContractTrunk: contractHash must be 32 bytes, got %d", len(contractHash))
	}
	if cache == nil {
		return 0, fmt.Errorf("PreloadContractTrunk: cache is nil")
	}
	if ramBudgetBytes <= 0 {
		return 0, fmt.Errorf("PreloadContractTrunk: ramBudgetBytes must be positive, got %d", ramBudgetBytes)
	}

	// Convert contract hash bytes to 64 nibbles — the path prefix
	// that anchors all of the contract's storage subtree branches.
	contractNibbles := make([]byte, 64)
	for i, b := range contractHash {
		contractNibbles[2*i] = b >> 4
		contractNibbles[2*i+1] = b & 0x0f
	}

	type pathDepth struct {
		path  []byte
		depth int
	}

	queue := []pathDepth{{path: contractNibbles, depth: 64}}
	pinned := 0
	usedBytes := 0
	maxDepthReached := 64
	budgetExhausted := false

	for len(queue) > 0 {
		head := queue[0]
		queue = queue[1:]

		prefix := nibbles.HexToCompact(head.path)
		v, step, found, err := reader(prefix)
		if err != nil {
			return pinned, fmt.Errorf("preload at depth %d: %w", head.depth, err)
		}
		if !found {
			continue
		}

		entryCost := estimatedEntryOverheadBytes + len(prefix) + len(v)
		if usedBytes+entryCost > ramBudgetBytes {
			budgetExhausted = true
			break
		}

		cache.PinEntry(prefix, v, step, "preload-trunk")
		usedBytes += entryCost
		pinned++
		if head.depth > maxDepthReached {
			maxDepthReached = head.depth
		}
		if logger != nil && pinned%5000 == 0 {
			logger.Info("[trunk-preload] progress",
				"pinned", pinned, "depth", head.depth,
				"used_mb", usedBytes/(1<<20))
		}

		// Decode bitmap and queue children. Branch encoding:
		// 2-byte touchMap || 2-byte bitmap || per-child data. Only
		// queue children that actually exist in the trie (bit set).
		if len(v) < 4 {
			continue
		}
		bitmap := binary.BigEndian.Uint16(v[2:4])
		for n := 0; n < 16; n++ {
			if bitmap&(1<<uint(n)) == 0 {
				continue
			}
			childPath := make([]byte, len(head.path)+1)
			copy(childPath, head.path)
			childPath[len(head.path)] = byte(n)
			queue = append(queue, pathDepth{path: childPath, depth: head.depth + 1})
		}
	}

	if logger != nil {
		logger.Info("[trunk-preload] complete",
			"contract_hash", fmt.Sprintf("%x", contractHash),
			"ram_budget_mb", ramBudgetBytes/(1<<20),
			"used_mb", usedBytes/(1<<20),
			"pinned", pinned,
			"max_depth_reached", maxDepthReached,
			"budget_exhausted", budgetExhausted,
			"queue_remaining", len(queue),
			"cache_pinned_total", cache.PinnedCount())
	}
	return pinned, nil
}
