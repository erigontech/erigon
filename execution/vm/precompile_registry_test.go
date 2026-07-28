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

package vm

import (
	"reflect"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type stubPrecompile struct{ name string }

func (s stubPrecompile) RequiredGas([]byte) uint64        { return 0 }
func (s stubPrecompile) Run(input []byte) ([]byte, error) { return input, nil }
func (s stubPrecompile) Name() string                     { return s.name }

func rulesForChain(chainID, l2Version uint64) *chain.Rules {
	return &chain.Rules{
		ChainID:     uint256.NewInt(chainID),
		IsHomestead: true,
		IsByzantium: true,
		IsIstanbul:  true,
		IsBerlin:    true,
		IsCancun:    true,
		L2Version:   l2Version,
	}
}

func TestRegisteredProviderScopedToChainID(t *testing.T) {
	const registeredChainID = 900101
	const otherChainID = 900102
	extraAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x99}))
	ecrecoverAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x01}))

	RegisterPrecompiles(registeredChainID, func(*chain.Rules) PrecompiledContracts {
		return PrecompiledContracts{extraAddr: stubPrecompile{"EXTRA"}}
	})
	t.Cleanup(func() { UnregisterPrecompiles(registeredChainID) })

	registered := Precompiles(rulesForChain(registeredChainID, 0))
	other := Precompiles(rulesForChain(otherChainID, 0))

	_, ok := registered[extraAddr]
	require.True(t, ok, "registered chain must expose the provider's precompile")
	_, ok = other[extraAddr]
	require.False(t, ok, "other chains must not see the provider's precompile")
	_, ok = registered[ecrecoverAddr]
	require.True(t, ok, "built-ins must still be present alongside a provider")

	require.Contains(t, ActivePrecompiles(rulesForChain(registeredChainID, 0)), extraAddr)
}

func TestRegisteredProviderVersionGating(t *testing.T) {
	const chainID = 900201
	addrV30 := accounts.InternAddress(common.BytesToAddress([]byte{0x77}))

	RegisterPrecompiles(chainID, func(rules *chain.Rules) PrecompiledContracts {
		if rules.L2Version >= 30 {
			return PrecompiledContracts{addrV30: stubPrecompile{"V30"}}
		}
		return PrecompiledContracts{}
	})
	t.Cleanup(func() { UnregisterPrecompiles(chainID) })

	_, ok := Precompiles(rulesForChain(chainID, 0))[addrV30]
	require.False(t, ok, "precompile must be absent below its activation version")
	_, ok = Precompiles(rulesForChain(chainID, 30))[addrV30]
	require.True(t, ok, "precompile must be present at/above its activation version")

	first := Precompiles(rulesForChain(chainID, 30))
	second := Precompiles(rulesForChain(chainID, 30))
	require.Equal(t, reflect.ValueOf(first).Pointer(), reflect.ValueOf(second).Pointer(),
		"identical (chainID, fork, L2Version) must hit the cache and return the same map instance")
}

func TestRegisterPrecompilesPanics(t *testing.T) {
	const chainID = 900301
	RegisterPrecompiles(chainID, func(*chain.Rules) PrecompiledContracts { return nil })
	t.Cleanup(func() { UnregisterPrecompiles(chainID) })

	require.Panics(t, func() {
		RegisterPrecompiles(chainID, func(*chain.Rules) PrecompiledContracts { return nil })
	}, "duplicate chainID registration must panic")

	require.Panics(t, func() {
		RegisterPrecompiles(900302, nil)
	}, "nil PrecompilesFunc must panic")
}

func TestPrecompilesNilChainID(t *testing.T) {
	rules := &chain.Rules{IsIstanbul: true}
	require.NotPanics(t, func() {
		require.NotEmpty(t, Precompiles(rules))
		require.NotEmpty(t, ActivePrecompiles(rules))
	})
}
