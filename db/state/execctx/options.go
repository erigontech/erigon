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
	"errors"

	"github.com/erigontech/erigon/execution/commitment"
)

// ErrBinCommitmentUnsupported is returned by NewSharedDomains for a caller that
// declared itself hex-only (WithHexCommitmentOnly) over a bin-variant datadir.
var ErrBinCommitmentUnsupported = errors.New("this code path supports the hex commitment trie only, and the datadir uses the bin trie")

type sharedDomainOptions struct {
	trieCfg              commitment.TrieConfig
	useSharedBranchCache bool
	hexCommitmentOnly    bool
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

// WithoutSharedBranchCache keeps commitment reads within the transaction snapshot.
func WithoutSharedBranchCache() SharedDomainOption {
	return func(o *sharedDomainOptions) { o.useSharedBranchCache = false }
}

// WithoutParallelCommitment demotes the experimental parallel/streaming tries to the
// sequential HexPatriciaHashed — for one-shot / empty-DB paths (e.g. genesis) that
// wire no trie-context factory for the parallel trie. The bin variant is a persisted
// whole-datadir property and stays bin: demoting it would compute a hex root over a
// datadir the executor reads as bin.
func WithoutParallelCommitment() SharedDomainOption {
	return func(o *sharedDomainOptions) {
		if o.trieCfg.Variant != commitment.VariantBinPatriciaTrie {
			o.trieCfg.Variant = commitment.VariantHexPatriciaTrie
		}
	}
}

// WithHexCommitmentOnly is WithoutParallelCommitment for callers that can only read
// hex branch records — eth_getProof, eth_getWitness, eth_simulateV1, receipt
// regeneration, commitment integrity. Under the bin variant NewSharedDomains returns
// ErrBinCommitmentUnsupported instead of reading bit-path records as hex ones.
func WithHexCommitmentOnly() SharedDomainOption {
	return func(o *sharedDomainOptions) {
		o.hexCommitmentOnly = true
		WithoutParallelCommitment()(o)
	}
}
