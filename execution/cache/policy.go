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

package cache

// Mode selects what GenericCache does when an insert would overflow the
// capacity. The seam is the same for both modes — the in-package eviction
// pop is the only behaviour difference.
//
// ModeEvictLRU is the default and matches the policy other state caches in
// the tree use (db/state/cache.go's DomainGetFromFileCache /
// IISeekInFilesCache, execution/cache/code_cache.go's addr LRUs,
// execution/balcache).
//
// ModeNoOp preserves the historical "first writers win forever" behaviour:
// once full, every new key is silently dropped (counted via the dropped
// metric). Kept as a deliberate diagnostic baseline so the regression
// bench can compare against the pre-policy behaviour without flipping
// branches.
//
// LFU is a known follow-up. Pure LRU is scan-fragile: a flood of one-shot
// keys (cold-storage bloat workloads; mainnet's long tail of single-touch
// slots) evicts the genuinely-hot working set because every cold scan is
// "more recent" than the hot entries. W-TinyLFU keeps a frequency sketch
// so single-touch entries can't displace frequently-touched ones — which
// is why reth uses it for state caches. A ModeEvictLFU would slot in
// behind the same wrapper without touching call sites.
type Mode uint8

const (
	ModeEvictLRU Mode = iota
	ModeNoOp
)

func (m Mode) String() string {
	switch m {
	case ModeEvictLRU:
		return "evict"
	case ModeNoOp:
		return "noop"
	}
	return "unknown"
}
