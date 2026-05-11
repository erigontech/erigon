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
	"fmt"
	"sort"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// CommitmentRangeReader yields, in ascending key order, every latest
// CommitmentDomain entry whose key K satisfies fromKey <= K < toKey. yield
// returns false to stop early. step is the domain step the value came from
// (forwarded to BranchCache.PinEntry). Decoupled (callback form) so the
// commitment package doesn't import db/kv/stream.
type CommitmentRangeReader func(fromKey, toKey []byte, yield func(key, value []byte, step uint64) bool) error

// nextSubtree returns the smallest key K' such that K' is greater than every
// key having `in` as a prefix (i.e. the exclusive upper bound for a
// prefix-range scan over `in`). Returns nil if `in` is all 0xff (no such K').
// Equivalent to db/kv.NextSubtree; inlined to keep this package's import set
// minimal.
func nextSubtree(in []byte) []byte {
	r := make([]byte, len(in))
	copy(r, in)
	for i := len(r) - 1; i >= 0; i-- {
		if r[i] != 0xff {
			r[i]++
			return r[:i+1]
		}
	}
	return nil // all 0xff
}

// contractTrunkKeyRanges returns the two CommitmentDomain key ranges that
// together hold every branch node of a contract's storage subtree.
//
// The commitment domain keys branches by HexToCompact(nibblePath). A storage
// branch of contract H (32 bytes => 64 nibbles) at slot-path of k nibbles has
// nibblePath = H_nibbles ++ slotNibbles, total 64+k nibbles. HexToCompact's
// HP flag byte differs by parity of the total length, so the branches split
// into two non-adjacent byte ranges:
//
//   even total length (k even — incl. the depth-64 subtree-root branch):
//     key = 0x00 || H || <k/2 slot bytes>  ⇒ prefix 0x00||H (33 bytes)
//   odd total length (k odd):
//     key = (0x10 | H[0]>>4) || <31 bytes derived from H[1:]> ||
//           <byte: high nibble = H's last nibble, low nibble = slotNibbles[0]> || ...
//     ⇒ all such keys lie in [HexToCompact(H_nibbles||0), nextSubtree(HexToCompact(H_nibbles||15))).
//
// (HexToCompact of the 64-nibble path = 0x00||H; of a 65-nibble path = the
// 33-byte odd prefix with the final byte's low nibble = the 65th nibble.)
func contractTrunkKeyRanges(contractNibbles []byte) (evenFrom, evenTo, oddFrom, oddTo []byte) {
	evenFrom = nibbles.HexToCompact(contractNibbles) // 0x00 || H, 33 bytes
	evenTo = nextSubtree(evenFrom)

	odd0 := make([]byte, 0, 65)
	odd0 = append(append(odd0, contractNibbles...), 0)
	oddF := make([]byte, 0, 65)
	oddF = append(append(oddF, contractNibbles...), 15)
	oddFrom = nibbles.HexToCompact(odd0)
	oddTo = nextSubtree(nibbles.HexToCompact(oddF))
	return evenFrom, evenTo, oddFrom, oddTo
}

// maxStorageTrunkDepth is the deepest total nibble path a storage branch can
// have: 64 (account path) + 64 (keccak256(slot)) = 128. Used as a hard sanity
// cap on what the bulk scan considers; the byte budget is the real bound.
const maxStorageTrunkDepth = 128

type bulkBranch struct {
	key   []byte
	value []byte
	step  uint64
	depth int // len(nibblePath)
}

// LoadBulk pins this contract's storage-trunk branches into cache by scanning
// the two CommitmentDomain key ranges (one sequential file+DB-merged range scan
// per parity, via rr) instead of N per-prefix point lookups. It collects every
// branch of the contract's subtree, sorts them shallowest-first (the
// BFS/highest-reuse-first ordering — a lexicographic scan would yield
// depth-first-within-subtree order, which under a budget pins a deep narrow
// slice), and pins from the front until budgetBytes is reached.
//
// Returns the number of branches pinned by this call (cumulative total via
// PinnedTotal). Calling LoadBulk again with a larger total budget extends the
// pinned set: it re-scans (a sequential range scan is cheap), skips the
// already-pinned shallowest entries, and pins the next budget's worth — the
// resumable/phased semantics of Run, implemented by re-scan.
func (p *ContractTrunkPreload) LoadBulk(budgetBytes int, rr CommitmentRangeReader, cache *BranchCache, logger log.Logger) (newlyPinned int, err error) {
	if cache == nil {
		return 0, fmt.Errorf("ContractTrunkPreload.LoadBulk: cache is nil")
	}
	if rr == nil {
		return 0, fmt.Errorf("ContractTrunkPreload.LoadBulk: range reader is nil")
	}
	if budgetBytes <= p.usedBytes {
		return 0, nil
	}

	evenFrom, evenTo, oddFrom, oddTo := contractTrunkKeyRanges(p.contractNibbles)

	var collected []bulkBranch
	collect := func(from, to []byte) error {
		return rr(from, to, func(key, value []byte, step uint64) bool {
			path := nibbles.CompactToHex(key)
			if len(path) < 64 || len(path) > maxStorageTrunkDepth || !bytes.Equal(path[:64], p.contractNibbles) {
				return true // not a branch of this contract's subtree — skip, keep scanning
			}
			kc := make([]byte, len(key))
			copy(kc, key)
			vc := make([]byte, len(value))
			copy(vc, value)
			collected = append(collected, bulkBranch{key: kc, value: vc, step: step, depth: len(path)})
			return true
		})
	}
	if err = collect(evenFrom, evenTo); err != nil {
		return 0, fmt.Errorf("LoadBulk even-range scan: %w", err)
	}
	if err = collect(oddFrom, oddTo); err != nil {
		return 0, fmt.Errorf("LoadBulk odd-range scan: %w", err)
	}

	// Shallowest first; tie-break by path bytes for determinism.
	sort.Slice(collected, func(i, j int) bool {
		if collected[i].depth != collected[j].depth {
			return collected[i].depth < collected[j].depth
		}
		return bytes.Compare(collected[i].key, collected[j].key) < 0
	})

	alreadyPinned := p.pinned // skip the entries a previous LoadBulk already pinned (same shallowest-first order)
	chunkPinned := 0
	chunkBytes := 0
	for idx, b := range collected {
		if idx < alreadyPinned {
			continue
		}
		entryCost := estimatedEntryOverheadBytes + len(b.key) + len(b.value)
		if p.usedBytes+chunkBytes+entryCost > budgetBytes {
			break
		}
		cache.PinEntry(b.key, b.value, b.step, "preload-trunk-bulk")
		p.pinnedPrefixes = append(p.pinnedPrefixes, b.key)
		chunkBytes += entryCost
		chunkPinned++
		if b.depth > p.maxDepthReached {
			p.maxDepthReached = b.depth
		}
		if logger != nil && (p.pinned+chunkPinned)%5000 == 0 {
			logger.Info("[trunk-preload-bulk] progress",
				"pinned", p.pinned+chunkPinned, "depth", b.depth, "used_mb", (p.usedBytes+chunkBytes)/(1<<20))
		}
	}
	p.pinned += chunkPinned
	p.usedBytes += chunkBytes
	p.lastBulkHadMore = p.pinned < len(collected)
	if logger != nil {
		logger.Info("[trunk-preload-bulk] scanned", "contract_hash", fmt.Sprintf("%x", p.contractHash),
			"scanned", len(collected), "pinned_this_call", chunkPinned, "pinned_total", p.pinned,
			"used_mb", p.usedBytes/(1<<20), "max_depth_reached", p.maxDepthReached, "budget_exhausted", p.pinned < len(collected))
	}
	return chunkPinned, nil
}

// PreloadContractTrunkBulk is the bulk analogue of PreloadContractTrunk: one
// call, bounded by ramBudgetBytes, using a range scan.
func PreloadContractTrunkBulk(contractHash []byte, ramBudgetBytes int, rr CommitmentRangeReader, cache *BranchCache, logger log.Logger) (int, error) {
	if ramBudgetBytes <= 0 {
		return 0, fmt.Errorf("PreloadContractTrunkBulk: ramBudgetBytes must be positive, got %d", ramBudgetBytes)
	}
	p, err := NewContractTrunkPreload(contractHash)
	if err != nil {
		return 0, err
	}
	return p.LoadBulk(ramBudgetBytes, rr, cache, logger)
}
