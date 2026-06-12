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
// Pure LRU is scan-fragile in principle: a flood of one-shot keys (mainnet's
// long tail of single-touch slots) can evict the genuinely-hot working set
// because every cold scan is "more recent" than the hot entries. Two known
// follow-up policies sit behind this seam:
//
//   - ModeEvictFixedCache (reth's choice). Reth's execution cache is
//     `fixed-cache`: a lock-free direct-mapped / set-associative array
//     with collision-evict semantics — no LRU list, no LFU sketch. Their
//     PR #21128 (v1.11.0) quoted ~25% newPayload p50 / +33% gas/s vs the
//     prior moka / quick-cache attempts; the win came from *removing*
//     LRU/LFU bookkeeping, not adding LFU.
//   - ModeEvictLFU. W-TinyLFU (Caffeine-style) admission policy keeps a
//     small frequency sketch so single-touch entries can't displace
//     frequently-touched ones. Helps mainnet steady-state in principle
//     (+5-15pp on Zipfian-with-scan per Caffeine literature). Does NOT
//     help the cycle-2 bloat fixtures — those are pure cold scans with
//     no reuse, so admission policy has nothing to admit.
//
// See agentspecs/lfu-vs-lru-state-cache-decision-2026-05-15.md for the
// full analysis (which lib to use if we ship LFU — otter, not ristretto)
// and the decision criterion (24h mainnet replay hit-rate < 90% before
// LFU is worth a new dep).
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
