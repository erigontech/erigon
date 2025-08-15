// Copyright 2025 The Erigon Authors
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

package evmtypes

import (
	"math/big"

	"github.com/erigontech/erigon/execution/chain"
)

// Rules ensures c's ChainID is not nil and returns a new Rules instance
func (bc *BlockContext) Rules(c *chain.Config) *chain.Rules {
	chainID := c.ChainID
	if chainID == nil {
		chainID = new(big.Int)
	}

	return &chain.Rules{
		ChainID:            new(big.Int).Set(chainID),
		IsHomestead:        c.IsHomestead(bc.BlockNumber),
		IsTangerineWhistle: c.IsTangerineWhistle(bc.BlockNumber),
		IsSpuriousDragon:   c.IsSpuriousDragon(bc.BlockNumber),
		IsByzantium:        c.IsByzantium(bc.BlockNumber),
		IsConstantinople:   c.IsConstantinople(bc.BlockNumber),
		IsPetersburg:       c.IsPetersburg(bc.BlockNumber),
		IsIstanbul:         c.IsIstanbul(bc.BlockNumber),
		IsBerlin:           c.IsBerlin(bc.BlockNumber),
		IsLondon:           c.IsLondon(bc.BlockNumber),
		IsShanghai:         c.IsShanghai(bc.Time) || c.IsAgra(bc.BlockNumber),
		IsCancun:           c.IsCancun(bc.Time),
		IsNapoli:           c.IsNapoli(bc.BlockNumber),
		IsBhilai:           c.IsBhilai(bc.BlockNumber),
		IsPrague:           c.IsPrague(bc.Time) || c.IsBhilai(bc.BlockNumber),
		IsOsaka:            c.IsOsaka(bc.Time),
		IsAura:             c.Aura != nil,
	}
}
