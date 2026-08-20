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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

type CommitmentReader func(prefix []byte) (v []byte, step uint64, found bool, err error)

// 168 = branchCacheEntry (~80B) + maphash slot/hash (~40B) + prefix/value slice headers (~24B each).
const estimatedEntryOverheadBytes = 168

type pathDepth struct {
	path  []byte
	depth int
}

// Not goroutine-safe.
type ContractTrunkPreload struct {
	contractHash    []byte
	queue           []pathDepth
	pinnedPrefixes  [][]byte
	pinned          int
	usedBytes       int
	maxDepthReached int
	pinTxNum        uint64
}

func NewContractTrunkPreload(contractHash []byte) (*ContractTrunkPreload, error) {
	if len(contractHash) != 32 {
		return nil, fmt.Errorf("NewContractTrunkPreload: contractHash must be 32 bytes, got %d", len(contractHash))
	}
	return &ContractTrunkPreload{
		contractHash:    bytes.Clone(contractHash),
		queue:           []pathDepth{{path: ContractNibbles(contractHash), depth: 64}},
		maxDepthReached: 64,
	}, nil
}

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
	cacheView := cache.View()

	chunkUsedBytes := 0
	chunkPinned := 0

	for len(p.queue) > 0 {
		head := p.queue[0]

		prefix := nibbles.HexToCompact(head.path)
		v, step, found, rerr := reader(prefix)
		if rerr != nil {
			p.pinned += chunkPinned
			p.usedBytes += chunkUsedBytes
			return chunkPinned, false, fmt.Errorf("preload at depth %d: %w", head.depth, rerr)
		}
		if !found {
			p.queue = p.queue[1:]
			continue
		}

		entryCost := estimatedEntryOverheadBytes + len(prefix) + len(v)
		if chunkUsedBytes+entryCost > additionalBudgetBytes {
			break
		}
		p.queue = p.queue[1:]

		cacheView.PinEntry(prefix, v, step, p.pinTxNum)
		p.pinnedPrefixes = append(p.pinnedPrefixes, prefix)
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

		if len(v) < 4 {
			continue
		}
		bitmap := binary.BigEndian.Uint16(v[2:4])
		for n := range 16 {
			if bitmap&(1<<uint(n)) == 0 {
				continue
			}
			if head.depth+1 > maxStorageTrunkDepth {
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
	if err != nil {
		return pinned, err
	}
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
	return pinned, nil
}
