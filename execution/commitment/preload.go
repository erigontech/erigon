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

// CommitmentReader does GetLatest on the CommitmentDomain. Decoupled from
// tx/aggregator types to keep this package free of db/state imports.
type CommitmentReader func(prefix []byte) (v []byte, step uint64, found bool, err error)

// estimatedEntryOverheadBytes is the per-entry RAM cost beyond the encoded
// value itself: branchCacheEntry (~80 B), maphash slot + hash (~40 B),
// prefix slice (~24 B header + content), value slice header (~24 B).
const estimatedEntryOverheadBytes = 168

type pathDepth struct {
	path  []byte
	depth int
}

// ContractTrunkPreload holds the resumable state of a BFS preload for one
// contract's storage subtree. Not goroutine-safe.
type ContractTrunkPreload struct {
	contractHash    []byte
	queue           []pathDepth
	pinnedPrefixes  [][]byte
	pinned          int
	usedBytes       int
	maxDepthReached int
}

// NewContractTrunkPreload seeds a preload state at depth 64 (storage
// subtree root: keccak256(address), 32 bytes / 64 nibbles).
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
		queue:           []pathDepth{{path: contractNibbles, depth: 64}},
		maxDepthReached: 64,
	}, nil
}

// Run advances the BFS one chunk, pinning branches until additionalBudgetBytes
// is exhausted or the queue is empty. Returns entries pinned THIS call, whether
// the preload is now complete, and any reader error. On error the partial pin
// set and queue position are preserved for a retry on the next Run.
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
			p.queue = append([]pathDepth{head}, p.queue...)
			break
		}

		// Trunk preload reads frozen .kv files only (below any unwind point),
		// so the bytes can never go stale on a reorg — stamp txNum 0 ("frozen,
		// always valid") so the tx-aware Get keeps them warm across unwinds.
		_ = step
		cache.PinEntry(prefix, v, 0, "preload-trunk")
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

		// Branch encoding: 2-byte touchMap || 2-byte bitmap || per-child data.
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

func (p *ContractTrunkPreload) PinnedTotal() int     { return p.pinned }
func (p *ContractTrunkPreload) UsedBytes() int       { return p.usedBytes }
func (p *ContractTrunkPreload) QueueRemaining() int  { return len(p.queue) }
func (p *ContractTrunkPreload) MaxDepthReached() int { return p.maxDepthReached }

// PinnedPrefixes returns slices aliasing internal storage — do not mutate.
func (p *ContractTrunkPreload) PinnedPrefixes() [][]byte { return p.pinnedPrefixes }
func (p *ContractTrunkPreload) ContractHash() []byte     { return p.contractHash }

// PreloadContractTrunk is the one-shot wrapper around NewContractTrunkPreload + Run.
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
