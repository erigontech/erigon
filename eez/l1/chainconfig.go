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

package l1

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// ChainConfig returns the configuration of the L1 of chainID: a known
// network's, else a devnet's, every fork through Osaka from genesis.
func ChainConfig(chainID uint64) *chain.Config {
	for _, s := range []*chainspec.Spec{&chainspec.Mainnet, &chainspec.Sepolia, &chainspec.Hoodi, &chainspec.Gnosis, &chainspec.Chiado} {
		if s.Config.ChainID.IsUint64() && s.Config.ChainID.Uint64() == chainID {
			return s.Config
		}
	}
	c := *chain.TestChainOsakaConfig
	c.ChainID = uint256.NewInt(chainID)
	return &c
}
