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
func Rules(c *chain.Config, num uint64, time uint64) *chain.Rules {
	chainID := c.ChainID
	if chainID == nil {
		chainID = new(big.Int)
	}

	return &chain.Rules{
		ChainID:            new(big.Int).Set(chainID),
		IsHomestead:        c.IsHomestead(num),
		IsTangerineWhistle: c.IsTangerineWhistle(num),
		IsSpuriousDragon:   c.IsSpuriousDragon(num),
		IsByzantium:        c.IsByzantium(num),
		IsConstantinople:   c.IsConstantinople(num),
		IsPetersburg:       c.IsPetersburg(num),
		IsIstanbul:         c.IsIstanbul(num),
		IsBerlin:           c.IsBerlin(num),
		IsLondon:           c.IsLondon(num),
		IsShanghai:         c.IsShanghai(time) || c.IsAgra(num),
		IsCancun:           c.IsCancun(time),
		IsNapoli:           c.IsNapoli(num),
		IsBhilai:           c.IsBhilai(num),
		IsPrague:           c.IsPrague(time) || c.IsBhilai(num),
		IsOsaka:            c.IsOsaka(time),
		IsAura:             c.Aura != nil,
	}
}
