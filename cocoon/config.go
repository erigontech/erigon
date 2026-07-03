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

// Package cocoon is the reference wiring for a sovereign single-chain
// embedder on Erigon's L2 extension points: a chain whose blocks are
// produced and trusted by the embedder rather than by an L2 stack's own
// consensus, and which otherwise follows the L1 fork schedule unmodified.
package cocoon

import "github.com/erigontech/erigon/execution/chain"

// Config is the chain.L2Config for a sovereign chain: it follows the L1 fork
// schedule as-is, so it has no version ladder of its own to resolve.
type Config struct {
	ChainID uint64
}

var _ chain.L2Config = Config{}

func (Config) Name() string { return "cocoon" }

func (Config) ResolveRules(l2Version, blockNum, blockTime uint64, rules *chain.Rules) {}
