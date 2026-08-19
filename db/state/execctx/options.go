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

import (
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

type sharedDomainOptions struct {
	trieCfg              commitment.TrieConfig
	useSharedBranchCache bool
	mem                  kv.TemporalMemBatch
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

// WithoutSharedBranchCache disables the aggregator-scoped commitment branch cache
// and its adaptive pin controller. Cache entries are not view-bound, so callers
// whose reads can overlap cache writes from another transaction must pass this option.
func WithoutSharedBranchCache() SharedDomainOption {
	return func(o *sharedDomainOptions) { o.useSharedBranchCache = false }
}

// WithMemBatch supplies the in-memory domain batch instead of building the
// default one from the tx. The seam an ephemeral single-block replay uses to
// serve a flat witness (no temporal source) via SharedDomains. A general
// top-down hoist of mem-batch construction out of the tx is a follow-up.
func WithMemBatch(mem kv.TemporalMemBatch) SharedDomainOption {
	return func(o *sharedDomainOptions) { o.mem = mem }
}

// WithSequentialCommitment forces the sequential HexPatriciaHashed trie regardless
// of the experimental parallel/concurrent flags — for one-shot / empty-DB paths
// (e.g. genesis) that wire no trie-context factory for the parallel trie.
func WithSequentialCommitment() SharedDomainOption {
	return func(o *sharedDomainOptions) { o.trieCfg.Variant = commitment.VariantHexPatriciaTrie }
}
