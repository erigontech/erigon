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

func registerPrecompiles(t *testing.T, chainID uint64, overlay PrecompiledContracts) {
	t.Helper()
	RegisterPrecompiles(uint256.NewInt(chainID), overlay)
	t.Cleanup(func() { unregisterPrecompiles(uint256.NewInt(chainID)) })
}

func rulesForChain(chainID uint64) *chain.Rules {
	return &chain.Rules{
		ChainID:     uint256.NewInt(chainID),
		IsHomestead: true,
		IsByzantium: true,
		IsIstanbul:  true,
		IsBerlin:    true,
		IsCancun:    true,
	}
}

func TestRegisteredOverlayScopedToChainID(t *testing.T) {
	const registeredChainID = 900101
	const otherChainID = 900102
	extraAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x99}))
	ecrecoverAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x01}))

	registerPrecompiles(t, registeredChainID, PrecompiledContracts{extraAddr: stubPrecompile{"EXTRA"}})

	registered := Precompiles(rulesForChain(registeredChainID))
	other := Precompiles(rulesForChain(otherChainID))

	_, ok := registered[extraAddr]
	require.True(t, ok, "registered chain must expose its overlay's precompile")
	_, ok = other[extraAddr]
	require.False(t, ok, "other chains must not see the registered chain's overlay")
	_, ok = registered[ecrecoverAddr]
	require.True(t, ok, "built-ins must still be present alongside an overlay")

	require.Contains(t, ActivePrecompiles(rulesForChain(registeredChainID)), extraAddr)
}

func TestRegisterPrecompilesPanics(t *testing.T) {
	const chainID = 900301
	registerPrecompiles(t, chainID, nil)

	require.Panics(t, func() {
		RegisterPrecompiles(uint256.NewInt(chainID), nil)
	}, "duplicate chainID registration must panic")

	for _, id := range []*uint256.Int{nil, new(uint256.Int)} {
		require.Panics(t, func() { RegisterPrecompiles(id, nil) },
			"a nil or zero chain ID must be refused on register")
	}
}

func TestRegisteredOverlayForkDimension(t *testing.T) {
	const chainID = 900501
	extraAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x55}))
	osakaOnly := accounts.InternAddress(common.BytesToAddress([]byte{0x01, 0x00}))

	registerPrecompiles(t, chainID, PrecompiledContracts{extraAddr: stubPrecompile{"EXTRA"}})

	cancun := &chain.Rules{ChainID: uint256.NewInt(chainID), IsCancun: true}
	osaka := &chain.Rules{ChainID: uint256.NewInt(chainID), IsCancun: true, IsPrague: true, IsOsaka: true}

	_, ok := Precompiles(cancun)[osakaOnly]
	require.False(t, ok, "0x0100 is not a Cancun built-in")

	merged := Precompiles(osaka)
	_, ok = merged[osakaOnly]
	require.True(t, ok, "the registered chain's Osaka tier must carry the Osaka built-in at 0x0100")
	_, ok = merged[extraAddr]
	require.True(t, ok, "the overlay must still be applied at the later fork")
}

func TestRegisteredOverlayWinsOnCollision(t *testing.T) {
	const chainID = 900502
	ecrecoverAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x01}))

	registerPrecompiles(t, chainID, PrecompiledContracts{ecrecoverAddr: stubPrecompile{"CHAIN-ECRECOVER"}})

	p, ok := Precompiles(rulesForChain(chainID))[ecrecoverAddr]
	require.True(t, ok)
	require.Equal(t, "CHAIN-ECRECOVER", p.Name(), "the chain's own entry must replace the built-in")
}

func TestRegisterNilContractPanics(t *testing.T) {
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
			require.PanicsWithValue(t,
				fmt.Sprintf("vm: RegisterPrecompiles: chain ID %d has a nil contract at %x", tc.chainID, badAddr),
				func() { RegisterPrecompiles(uint256.NewInt(tc.chainID), PrecompiledContracts{badAddr: tc.contract}) })
		})
	}
}

func TestRegisterAfterResolvePanics(t *testing.T) {
	const chainID = 900506
	_ = Precompiles(rulesForChain(chainID))
	t.Cleanup(func() { unregisterPrecompiles(uint256.NewInt(chainID)) })

	require.PanicsWithValue(t,
		fmt.Sprintf("vm: RegisterPrecompiles: chain ID %d precompiles already resolved", chainID),
		func() { RegisterPrecompiles(uint256.NewInt(chainID), PrecompiledContracts{}) })
}

func TestPrecompilesNilChainID(t *testing.T) {
	rules := &chain.Rules{IsIstanbul: true}
	require.NotPanics(t, func() {
		require.NotEmpty(t, Precompiles(rules))
		require.NotEmpty(t, ActivePrecompiles(rules))
	})
}
