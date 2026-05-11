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

// CommitmentReader is the read interface ContractTrunkPreload needs:
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

// pathDepth pairs a nibble path with the trie depth it terminates at.
// Used as the BFS queue element.
type pathDepth struct {
	path  []byte
	depth int
}

// ContractTrunkPreload holds the resumable state of a BFS preload for
// one contract's storage subtree. Created by NewContractTrunkPreload
// and advanced incrementally via Run; this lets callers split the
// preload into an initial sync view (small budget for d=64-67) and
// per-block extensions (additional budget while the contract stays
// hot), instead of burst-loading the full budget at promotion time.
//
// Tracks its pinned prefixes so callers can Invalidate the contract's
// pin set on demotion (PinnedPrefixes returns the slice).
//
// Caller MUST NOT use the same instance from multiple goroutines.
type ContractTrunkPreload struct {
	contractHash    []byte
	contractNibbles []byte // 64 nibbles of contractHash, cached for the bulk range scan (preload_bulk.go)
	queue           []pathDepth
	pinnedPrefixes  [][]byte
	pinned          int
	usedBytes       int
	maxDepthReached int
	lastBulkHadMore bool // last LoadBulk hit the byte budget with branches left unpinned
}

// NewContractTrunkPreload constructs a preload state seeded at depth 64
// (storage subtree root) for the given contract. The contract's
// storage subtree path is keccak256(address) — 32 bytes / 64 nibbles.
func NewContractTrunkPreload(contractHash []byte) (*ContractTrunkPreload, error) {
	if len(contractHash) != 32 {
		return nil, fmt.Errorf("NewContractTrunkPreload: contractHash must be 32 bytes, got %d", len(contractHash))
	}
	contractNibbles := make([]byte, 64)
	for i, b := range contractHash {
		contractNibbles[2*i] = b >> 4
		contractNibbles[2*i+1] = b & 0x0f
	}
	return &ContractTrunkPreload{
		contractHash:    contractHash,
		contractNibbles: contractNibbles,
		queue:           []pathDepth{{path: contractNibbles, depth: 64}},
		maxDepthReached: 64,
	}, nil
}

// Run advances the BFS preload, pinning branches into cache until
// either the additional budget is exhausted or the queue is empty.
// Returns the number of entries pinned during THIS call (not the
// cumulative total — see PinnedTotal), whether the queue is now
// empty (preload fully complete), and any error from the reader.
//
// Calling Run repeatedly with small budgets implements the phased
// load: an initial Run with the InitialBudget covers depths 64-67;
// each subsequent Run extends BFS one chunk further. Per-block
// callers should size the chunk to fit within inter-block idle time.
//
// On reader error the partial pin set survives; the queue position
// is preserved so a retry on the next Run picks up where it left off.
func (p *ContractTrunkPreload) Run(
	additionalBudgetBytes int,
	reader CommitmentReader,
	cache *BranchCache,
	logger log.Logger,
) (newlyPinned int, queueEmpty bool, err error) {
	if cache == nil {
		return 0, false, fmt.Errorf("ContractTrunkPreload.Run: cache is nil")
	}
	if additionalBudgetBytes <= 0 {
		return 0, len(p.queue) == 0, nil
	}

	chunkUsedBytes := 0
	chunkPinned := 0

	for len(p.queue) > 0 {
		head := p.queue[0]
		p.queue = p.queue[1:]

		prefix := nibbles.HexToCompact(head.path)
		v, step, found, rerr := reader(prefix)
		if rerr != nil {
			return chunkPinned, false, fmt.Errorf("preload at depth %d: %w", head.depth, rerr)
		}
		if !found {
			continue
		}

		entryCost := estimatedEntryOverheadBytes + len(prefix) + len(v)
		if chunkUsedBytes+entryCost > additionalBudgetBytes {
			// Re-queue the head so a future Run picks it up.
			p.queue = append([]pathDepth{head}, p.queue...)
			break
		}

		cache.PinEntry(prefix, v, step, "preload-trunk")
		// Copy prefix because nibbles.HexToCompact may return a slice
		// over a reused buffer; we need a stable handle for later
		// Invalidate on demotion.
		prefixCopy := make([]byte, len(prefix))
		copy(prefixCopy, prefix)
		p.pinnedPrefixes = append(p.pinnedPrefixes, prefixCopy)
		chunkUsedBytes += entryCost
		chunkPinned++
		if head.depth > p.maxDepthReached {
			p.maxDepthReached = head.depth
		}
		if logger != nil && (p.pinned+chunkPinned)%5000 == 0 {
			logger.Info("[trunk-preload] progress",
				"pinned", p.pinned+chunkPinned, "depth", head.depth,
				"used_mb", (p.usedBytes+chunkUsedBytes)/(1<<20))
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
			p.queue = append(p.queue, pathDepth{path: childPath, depth: head.depth + 1})
		}
	}

	p.pinned += chunkPinned
	p.usedBytes += chunkUsedBytes
	return chunkPinned, len(p.queue) == 0, nil
}

// PinnedTotal returns the cumulative number of branches pinned by
// this preload state across all Run calls.
func (p *ContractTrunkPreload) PinnedTotal() int { return p.pinned }

// UsedBytes returns the cumulative byte cost of this preload's pin set.
func (p *ContractTrunkPreload) UsedBytes() int { return p.usedBytes }

// QueueRemaining returns the number of paths still queued for descent.
// Zero means the preload is complete; further Run calls are no-ops.
func (p *ContractTrunkPreload) QueueRemaining() int { return len(p.queue) }

// MaxDepthReached returns the deepest trie depth pinned so far.
func (p *ContractTrunkPreload) MaxDepthReached() int { return p.maxDepthReached }

// HasMore reports whether more branches remain to be pinned (BFS queue
// non-empty, or the last bulk scan was cut short by the byte budget).
func (p *ContractTrunkPreload) HasMore() bool { return len(p.queue) > 0 || p.lastBulkHadMore }

// PinnedPrefixes returns the prefix slices this preload has added to
// the cache so far. The adaptive controller uses this to Invalidate
// the contract's pin set on demotion. The returned slice aliases
// internal storage; do not mutate.
func (p *ContractTrunkPreload) PinnedPrefixes() [][]byte { return p.pinnedPrefixes }

// ContractHash returns the contract this preload targets.
func (p *ContractTrunkPreload) ContractHash() []byte { return p.contractHash }

// PreloadContractTrunk runs the full BFS preload for a contract in a
// single call, bounded by ramBudgetBytes. Convenience wrapper around
// NewContractTrunkPreload + Run for callers that don't need phased
// loading. Existing one-shot trigger paths (PIN_CONTRACT_TRUNKS env
// hook) use this; the adaptive layer holds a *ContractTrunkPreload
// and calls Run incrementally.
func PreloadContractTrunk(
	contractHash []byte,
	ramBudgetBytes int,
	reader CommitmentReader,
	cache *BranchCache,
	logger log.Logger,
) (int, error) {
	if ramBudgetBytes <= 0 {
		return 0, fmt.Errorf("PreloadContractTrunk: ramBudgetBytes must be positive, got %d", ramBudgetBytes)
	}
	p, err := NewContractTrunkPreload(contractHash)
	if err != nil {
		return 0, err
	}
	pinned, queueEmpty, err := p.Run(ramBudgetBytes, reader, cache, logger)
	if logger != nil {
		logger.Info("[trunk-preload] complete",
			"contract_hash", fmt.Sprintf("%x", contractHash),
			"ram_budget_mb", ramBudgetBytes/(1<<20),
			"used_mb", p.UsedBytes()/(1<<20),
			"pinned", pinned,
			"max_depth_reached", p.MaxDepthReached(),
			"budget_exhausted", !queueEmpty,
			"queue_remaining", p.QueueRemaining(),
			"cache_pinned_total", cache.PinnedCount())
	}
	return pinned, err
}
