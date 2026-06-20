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
	trieCfg commitment.TrieConfig
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
