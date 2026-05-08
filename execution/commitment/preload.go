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

// PreloadContractTrunk pins commitment branches for the given contract
// from depth 64 (storage subtree root) down to maxDepth into the supplied
// BranchCache. Walks the trie structure depth-by-depth: reads the branch,
// pins it, decodes its child bitmap, and recurses only into children that
// actually exist.
//
// contractHash is keccak256(address) — 32 bytes. The trunk lives at the
// commitment-domain prefix that corresponds to nibbles 0..63 of the
// storage path (the contract's account hash); enumeration extends from
// there into nibbles 64..maxDepth.
//
// Read amplification: total reads = number of branches actually present
// in the trunk (bounded by the contract's storage trie shape). For a
// dense storage subtree, this is ~16+256+4096 ≈ 4.4 K reads at depth 65,
// 66, 67. Sparse subtrees produce fewer. maxDepth caps the descent.
//
// Returns the number of pinned branches and any error from the reader.
// On error the partial pin set remains in place — those entries are
// still valid cache hits, they just won't see further enumeration below
// the failure point.
//
// Loading strategy: this is the simplest correct implementation —
// recursive enumerate via per-prefix reader lookups. A bulk variant
// using seg.Getter range-scan over sorted .kv would amortize disk
// seeks but requires building a prefix-range API on top of recsplit.
// Defer that optimization until per-prefix lookup is shown to bottleneck.
func PreloadContractTrunk(
	contractHash []byte,
	maxDepth int,
	reader CommitmentReader,
	cache *BranchCache,
	logger log.Logger,
) (int, error) {
	if len(contractHash) != 32 {
		return 0, fmt.Errorf("PreloadContractTrunk: contractHash must be 32 bytes, got %d", len(contractHash))
	}
	if maxDepth < 64 {
		return 0, fmt.Errorf("PreloadContractTrunk: maxDepth %d < 64 (storage subtree starts at depth 64)", maxDepth)
	}
	if cache == nil {
		return 0, fmt.Errorf("PreloadContractTrunk: cache is nil")
	}

	// Convert contract hash bytes to 64 nibbles.
	contractNibbles := make([]byte, 64)
	for i, b := range contractHash {
		contractNibbles[2*i] = b >> 4
		contractNibbles[2*i+1] = b & 0x0f
	}

	// Hard cap to bound preload work. A fully-saturated 4-level subtree
	// is 1+16+256+4096 = 4369 branches; this gives headroom but still
	// fails fast on malformed input (e.g. a contract with truly
	// pathological branching). Tune downward if this turns out to bite.
	const maxBranches = 10000

	pinned := 0
	var stopped bool
	var enumerate func(pathNibbles []byte, depth int) error
	enumerate = func(pathNibbles []byte, depth int) error {
		if stopped {
			return nil
		}
		if pinned >= maxBranches {
			stopped = true
			if logger != nil {
				logger.Warn("[trunk-preload] hit max-branches cap",
					"cap", maxBranches, "depth", depth)
			}
			return nil
		}
		prefix := nibbles.HexToCompact(pathNibbles)
		v, step, found, err := reader(prefix)
		if err != nil {
			return fmt.Errorf("preload at depth %d: %w", depth, err)
		}
		if !found {
			return nil
		}
		cache.PinEntry(prefix, v, step, "preload-trunk")
		pinned++
		// Periodic progress log so a slow preload is observable.
		if logger != nil && pinned%500 == 0 {
			logger.Info("[trunk-preload] progress", "pinned", pinned, "depth", depth)
		}
		if depth >= maxDepth {
			return nil
		}
		if len(v) < 4 {
			return nil
		}
		// Branch encoding: 2-byte touchMap || 2-byte bitmap || per-child data.
		// Only recurse into children that actually exist (bit set in bitmap).
		bitmap := binary.BigEndian.Uint16(v[2:4])
		for n := 0; n < 16; n++ {
			if bitmap&(1<<uint(n)) == 0 {
				continue
			}
			childPath := append(append([]byte{}, pathNibbles...), byte(n))
			if err := enumerate(childPath, depth+1); err != nil {
				return err
			}
		}
		return nil
	}

	if err := enumerate(contractNibbles, 64); err != nil {
		return pinned, err
	}
	if logger != nil {
		logger.Info("[trunk-preload] complete",
			"contract_hash", fmt.Sprintf("%x", contractHash),
			"max_depth", maxDepth,
			"pinned", pinned,
			"cache_pinned_total", cache.PinnedCount())
	}
	return pinned, nil
}
