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

package vm

import (
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var customPrecompileAddr = accounts.InternAddress(common.BytesToAddress([]byte{0x42}))

func cancunRules(cp *chain.ChainPrecompiles) *chain.Rules {
	return &chain.Rules{
		ChainID:     uint256.NewInt(1),
		IsByzantium: true,
		IsIstanbul:  true,
		IsBerlin:    true,
		IsCancun:    true,
		Precompiles: cp,
	}
}

// A nil precompile set must reproduce the built-in defaults exactly — the
// standard single-chain path is unchanged.
func TestPrecompiles_NilUsesDefaults(t *testing.T) {
	rules := cancunRules(nil)
	assert.Equal(t, PrecompiledContractsCancun, Precompiles(rules))
	assert.ElementsMatch(t, PrecompiledAddressesCancun, ActivePrecompiles(rules))
	_, hasCustom := Precompiles(rules)[customPrecompileAddr]
	assert.False(t, hasCustom)
}

// A chain's own set overlays custom precompiles on top of the fork defaults,
// and the custom address is warmed via ActivePrecompiles.
func TestChainPrecompiles_CustomOverlay(t *testing.T) {
	cp := NewChainPrecompiles(PrecompiledContracts{customPrecompileAddr: &dataCopy{}})
	rules := cancunRules(cp)

	got := Precompiles(rules)
	_, hasCustom := got[customPrecompileAddr]
	assert.True(t, hasCustom, "custom precompile must be present")
	// Base precompiles for the fork are preserved alongside the custom one.
	for addr := range PrecompiledContractsCancun {
		_, ok := got[addr]
		assert.True(t, ok, "base precompile must be preserved")
	}
	assert.True(t, slices.Contains(ActivePrecompiles(rules), customPrecompileAddr))
}

// Two chains' sets are isolated: a custom precompile registered for one never
// appears in another. This is structural — each set is its own immutable map.
func TestChainPrecompiles_Isolation(t *testing.T) {
	withCustom := cancunRules(NewChainPrecompiles(PrecompiledContracts{customPrecompileAddr: &dataCopy{}}))
	withoutCustom := cancunRules(NewChainPrecompiles(nil))

	_, inFirst := Precompiles(withCustom)[customPrecompileAddr]
	_, inSecond := Precompiles(withoutCustom)[customPrecompileAddr]
	assert.True(t, inFirst)
	assert.False(t, inSecond, "custom precompile must not leak to another chain")
}
