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
	"fmt"
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

func registerPrecompiles(t *testing.T, chainID uint64, f PrecompilesFunc) {
	t.Helper()
	RegisterPrecompiles(uint256.NewInt(chainID), f)
	t.Cleanup(func() { UnregisterPrecompiles(uint256.NewInt(chainID)) })
}

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

	registerPrecompiles(t, registeredChainID, func(uint64) PrecompiledContracts {
		return PrecompiledContracts{extraAddr: stubPrecompile{"EXTRA"}}
	})

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

	registerPrecompiles(t, chainID, func(l2Version uint64) PrecompiledContracts {
		if l2Version >= 30 {
			return PrecompiledContracts{addrV30: stubPrecompile{"V30"}}
		}
		return PrecompiledContracts{}
	})

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
	registerPrecompiles(t, chainID, func(uint64) PrecompiledContracts { return nil })

	require.Panics(t, func() {
		RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) PrecompiledContracts { return nil })
	}, "duplicate chainID registration must panic")

	require.Panics(t, func() {
		RegisterPrecompiles(uint256.NewInt(900302), nil)
	}, "nil PrecompilesFunc must panic")

	for _, id := range []*uint256.Int{nil, new(uint256.Int)} {
		require.Panics(t, func() { RegisterPrecompiles(id, func(uint64) PrecompiledContracts { return nil }) },
			"a nil or zero chain ID must be refused on register")
		require.Panics(t, func() { UnregisterPrecompiles(id) },
			"and refused on unregister too, or the same argument has two contracts")
	}
}

func TestRegisteredProviderForkDimension(t *testing.T) {
	const chainID = 900501
	extraAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x55}))
	osakaOnly := accounts.InternAddress(common.BytesToAddress([]byte{0x01, 0x00}))

	registerPrecompiles(t, chainID, func(uint64) PrecompiledContracts {
		return PrecompiledContracts{extraAddr: stubPrecompile{"EXTRA"}}
	})

	cancun := &chain.Rules{ChainID: uint256.NewInt(chainID), IsCancun: true}
	osaka := &chain.Rules{ChainID: uint256.NewInt(chainID), IsCancun: true, IsPrague: true, IsOsaka: true}

	// Resolve Cancun first: a fork-blind key would then serve that set to Osaka too.
	_, ok := Precompiles(cancun)[osakaOnly]
	require.False(t, ok, "0x0100 is not a Cancun built-in")

	merged := Precompiles(osaka)
	_, ok = merged[osakaOnly]
	require.True(t, ok, "the Osaka base set must reach an L2 that resolved Cancun first")
	_, ok = merged[extraAddr]
	require.True(t, ok, "the overlay must still be applied at the later fork")
}

func TestRegisteredProviderWinsOnCollision(t *testing.T) {
	const chainID = 900502
	ecrecoverAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x01}))

	registerPrecompiles(t, chainID, func(uint64) PrecompiledContracts {
		return PrecompiledContracts{ecrecoverAddr: stubPrecompile{"CHAIN-ECRECOVER"}}
	})

	p, ok := Precompiles(rulesForChain(chainID, 0))[ecrecoverAddr]
	require.True(t, ok)
	require.Equal(t, "CHAIN-ECRECOVER", p.Name(), "the chain's own entry must replace the built-in")
}

func TestRegisterSweepsStaleCache(t *testing.T) {
	const chainID = 900503
	oldAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x44}))
	newAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x45}))
	rules := rulesForChain(chainID, 0)

	registerPrecompiles(t, chainID, func(uint64) PrecompiledContracts {
		return PrecompiledContracts{oldAddr: stubPrecompile{"OLD"}}
	})
	_, ok := Precompiles(rules)[oldAddr]
	require.True(t, ok, "the first provider's overlay must resolve and cache")

	UnregisterPrecompiles(uint256.NewInt(chainID))
	registerPrecompiles(t, chainID, func(uint64) PrecompiledContracts {
		return PrecompiledContracts{newAddr: stubPrecompile{"NEW"}}
	})

	merged := Precompiles(rules)
	_, ok = merged[newAddr]
	require.True(t, ok, "the newly registered provider's overlay must be served")
	_, ok = merged[oldAddr]
	require.False(t, ok, "the unregistered provider's overlay must not survive re-registration")
}

// Rejected where the sets merge, not at evm.call on a live transaction.
func TestProviderNilContractPanics(t *testing.T) {
	for _, tc := range []struct {
		name     string
		chainID  uint64
		addrByte byte
		contract PrecompiledContract
	}{
		{name: "untyped nil", chainID: 900504, addrByte: 0x46},
		{name: "typed nil", chainID: 900505, addrByte: 0x47, contract: (*stubPrecompile)(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			badAddr := accounts.InternAddress(common.BytesToAddress([]byte{tc.addrByte}))
			registerPrecompiles(t, tc.chainID, func(uint64) PrecompiledContracts {
				return PrecompiledContracts{badAddr: tc.contract}
			})

			require.PanicsWithValue(t,
				fmt.Sprintf("vm: precompile provider for chain %d returned a nil contract at %x", tc.chainID, badAddr),
				func() { Precompiles(rulesForChain(tc.chainID, 0)) })
		})
	}
}

func TestPrecompilesNilChainID(t *testing.T) {
	rules := &chain.Rules{IsIstanbul: true}
	require.NotPanics(t, func() {
		require.NotEmpty(t, Precompiles(rules))
		require.NotEmpty(t, ActivePrecompiles(rules))
	})
}

// Guards the no-provider fast path: registryMu on the rules path anti-scales with core count.
func BenchmarkActivePrecompilesParallel(b *testing.B) {
	rules := &chain.Rules{ChainID: uint256.NewInt(1), IsOsaka: true}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = ActivePrecompiles(rules)
		}
	})
}
