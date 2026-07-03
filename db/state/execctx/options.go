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

package execctx

import "github.com/erigontech/erigon/execution/commitment"

type sharedDomainOptions struct {
	trieCfg       commitment.TrieConfig
	noBranchCache bool
}

// SharedDomainOption configures NewSharedDomains.
type SharedDomainOption func(*sharedDomainOptions)

// WithTrieConfig replaces the trie configuration wholesale; the caller owns Variant.
func WithTrieConfig(cfg commitment.TrieConfig) SharedDomainOption {
	return func(o *sharedDomainOptions) { o.trieCfg = cfg }
}

// WithoutDeferredBranchUpdates disables deferred branch updates (read-only / one-shot domains).
func WithoutDeferredBranchUpdates() SharedDomainOption {
	return func(o *sharedDomainOptions) { o.trieCfg.DeferBranchUpdates = false }
}

// WithSequentialCommitment forces the sequential HexPatriciaHashed trie regardless
// of the experimental parallel/concurrent flags — for one-shot / empty-DB paths
// (e.g. genesis) that wire no trie-context factory for the parallel trie.
func WithSequentialCommitment() SharedDomainOption {
	return func(o *sharedDomainOptions) { o.trieCfg.Variant = commitment.VariantHexPatriciaTrie }
}

// WithoutBranchCache leaves the SharedDomains detached from the aggregator-scope
// BranchCache: commitment branch reads go straight to sd.mem/overlay/MDBX and no
// read populates the shared cache. Required for SDs that read concurrently with
// head progression (payload builds, latest-state RPC readers) — the shared cache
// can be ahead of their snapshot, and their read-fills could shadow fresher
// canonical entries. SDs serialized with head progression keep the warm cache.
func WithoutBranchCache() SharedDomainOption {
	return func(o *sharedDomainOptions) { o.noBranchCache = true }
}
