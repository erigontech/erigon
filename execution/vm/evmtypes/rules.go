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
	"sync"

	"github.com/erigontech/erigon/execution/chain"
)

var rulesPool = sync.Pool{
	New: func() any {
		return &chain.Rules{
			ChainID: new(big.Int),
		}
	},
}

// Rules ensures c's ChainID is not nil and returns the cached Rules instance.
// The returned Rules pointer is valid for the lifetime of the BlockContext.
func (bc *BlockContext) Rules(c *chain.Config) *chain.Rules {
	// Return cached rules if already computed
	if bc.cachedRules != nil {
		return bc.cachedRules
	}

	chainID := c.ChainID
	if chainID == nil {
		chainID = new(big.Int)
	}

	rules := rulesPool.Get().(*chain.Rules)
	rules.ChainID.Set(chainID)
	rules.IsHomestead = c.IsHomestead(bc.BlockNumber)
	rules.IsTangerineWhistle = c.IsTangerineWhistle(bc.BlockNumber)
	rules.IsSpuriousDragon = c.IsSpuriousDragon(bc.BlockNumber)
	rules.IsByzantium = c.IsByzantium(bc.BlockNumber)
	rules.IsConstantinople = c.IsConstantinople(bc.BlockNumber)
	rules.IsPetersburg = c.IsPetersburg(bc.BlockNumber)
	rules.IsIstanbul = c.IsIstanbul(bc.BlockNumber)
	rules.IsBerlin = c.IsBerlin(bc.BlockNumber)
	rules.IsLondon = c.IsLondon(bc.BlockNumber)
	rules.IsShanghai = c.IsShanghai(bc.Time) || c.IsAgra(bc.BlockNumber)
	rules.IsCancun = c.IsCancun(bc.Time)
	rules.IsNapoli = c.IsNapoli(bc.BlockNumber)
	rules.IsBhilai = c.IsBhilai(bc.BlockNumber)
	rules.IsPrague = c.IsPrague(bc.Time) || c.IsBhilai(bc.BlockNumber)
	rules.IsOsaka = c.IsOsaka(bc.Time)
	rules.IsAmsterdam = c.IsAmsterdam(bc.Time)
	rules.IsAura = c.Aura != nil

	bc.cachedRules = rules
	return rules
}

// ReleaseRules returns the cached rules to the pool.
// Should be called when the BlockContext is no longer needed.
func (bc *BlockContext) ReleaseRules() {
	if bc.cachedRules != nil {
		rulesPool.Put(bc.cachedRules)
		bc.cachedRules = nil
	}
}
