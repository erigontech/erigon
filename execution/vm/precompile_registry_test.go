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
	"math"
	"reflect"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
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

// TestRegisteredProviderForkDimension pins the fork dimension of the cache
// key. Without it, an L2 crossing a fork boundary keeps being served the
// merged set built at the earlier tier — the Osaka repricings and the 0x0100
// entry would never appear.
func TestRegisteredProviderForkDimension(t *testing.T) {
	const chainID = 900501
	extraAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x55}))
	osakaOnly := accounts.InternAddress(common.BytesToAddress([]byte{0x01, 0x00}))

	registerPrecompiles(t, chainID, func(uint64) PrecompiledContracts {
		return PrecompiledContracts{extraAddr: stubPrecompile{"EXTRA"}}
	})

	cancun := &chain.Rules{ChainID: uint256.NewInt(chainID), IsCancun: true}
	osaka := &chain.Rules{ChainID: uint256.NewInt(chainID), IsCancun: true, IsPrague: true, IsOsaka: true}

	// Resolve Cancun first, so a key that ignored the fork would serve its set
	// to Osaka as well.
	_, ok := Precompiles(cancun)[osakaOnly]
	require.False(t, ok, "0x0100 is not a Cancun built-in")

	merged := Precompiles(osaka)
	_, ok = merged[osakaOnly]
	require.True(t, ok, "the Osaka base set must reach an L2 that resolved Cancun first")
	_, ok = merged[extraAddr]
	require.True(t, ok, "the overlay must still be applied at the later fork")
}

// TestRegisteredProviderWinsOnCollision pins the documented precedence: a
// provider entry replaces a built-in at the same address. Swapping the
// maps.Copy operands leaves every other test green.
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

// TestProviderNilContractPanics pins that a nil entry is rejected where it is
// merged, naming the chain and address, rather than reaching evm.call and
// nil-dereferencing inside RunPrecompiledContract on a transaction.
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

// BenchmarkActivePrecompilesParallel guards the no-provider fast path. Rules
// resolution runs a few times per transaction on every worker, so taking
// registryMu here anti-scales with core count.
func BenchmarkActivePrecompilesParallel(b *testing.B) {
	rules := &chain.Rules{ChainID: uint256.NewInt(1), IsOsaka: true}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = ActivePrecompiles(rules)
		}
	})
}

func TestChargeStateRejectsUnrepresentableAmounts(t *testing.T) {
	for _, tc := range []struct {
		name      string
		remaining mdgas.MdGas
		used      mdgas.MdGasUsage
		amount    uint64
		accepted  bool
		want      mdgas.MdGas
		wantUsed  mdgas.MdGasUsage
	}{
		{
			name:      "amount above MaxInt64 cannot reach the signed usage",
			remaining: mdgas.MdGas{Execution: 1_000, State: math.MaxUint64},
			amount:    math.MaxInt64 + 1,
		},
		{
			name:      "MaxUint64 charge would read as minus one",
			remaining: mdgas.MdGas{Execution: 1_000, State: math.MaxUint64},
			amount:    math.MaxUint64,
		},
		{
			name:      "signed usage would overflow",
			remaining: mdgas.MdGas{Execution: 1_000, State: math.MaxUint64},
			used:      mdgas.MdGasUsage{State: 2},
			amount:    math.MaxInt64,
		},
		{
			name:      "spill accumulation would wrap",
			remaining: mdgas.MdGas{Execution: math.MaxUint64, State: 0},
			used:      mdgas.MdGasUsage{StateSpill: math.MaxUint64 - 5},
			amount:    10,
		},
		{
			name:      "the exact signed boundary is representable",
			remaining: mdgas.MdGas{Execution: 1_000, State: math.MaxUint64},
			amount:    math.MaxInt64,
			accepted:  true,
			want:      mdgas.MdGas{Execution: 1_000, State: math.MaxInt64 + 1},
			wantUsed:  mdgas.MdGasUsage{State: math.MaxInt64},
		},
		{
			name:      "an ordinary charge takes reservoir gas",
			remaining: mdgas.MdGas{Execution: 1_000, State: 50},
			amount:    10,
			accepted:  true,
			want:      mdgas.MdGas{Execution: 1_000, State: 40},
			wantUsed:  mdgas.MdGasUsage{State: 10},
		},
		{
			name:      "a charge past the reservoir spills into execution gas",
			remaining: mdgas.MdGas{Execution: 1_000, State: 10},
			amount:    40,
			accepted:  true,
			want:      mdgas.MdGas{Execution: 970, State: 0},
			wantUsed:  mdgas.MdGasUsage{State: 40, StateSpill: 30},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remaining, used := tc.remaining, tc.used
			g := &PrecompileGas{remaining: &remaining, used: &used, amsterdam: true}

			require.Equal(t, tc.accepted, g.ChargeState(tc.amount))
			if !tc.accepted {
				require.Equal(t, tc.remaining, remaining, "a rejected charge must leave the reservoir alone")
				require.Equal(t, tc.used, used, "a rejected charge must leave usage alone")
				return
			}
			require.Equal(t, tc.want, remaining)
			require.Equal(t, tc.wantUsed, used)
		})
	}
}

func TestRefundStateRejectsUnrepresentableAmounts(t *testing.T) {
	for _, tc := range []struct {
		name      string
		remaining mdgas.MdGas
		used      mdgas.MdGasUsage
		amount    uint64
		accepted  bool
		want      mdgas.MdGas
		wantUsed  mdgas.MdGasUsage
	}{
		{
			name:      "amount above MaxInt64 cannot reach the signed usage",
			remaining: mdgas.MdGas{Execution: 1_000, State: 50},
			amount:    math.MaxInt64 + 1,
		},
		{
			name:      "MaxUint64 refund would read as minus one",
			remaining: mdgas.MdGas{Execution: 1_000, State: 50},
			amount:    math.MaxUint64,
		},
		{
			name:      "signed usage would underflow",
			remaining: mdgas.MdGas{Execution: 1_000, State: 50},
			used:      mdgas.MdGasUsage{State: -2},
			amount:    math.MaxInt64,
		},
		{
			name:      "reservoir addition would wrap",
			remaining: mdgas.MdGas{Execution: 1_000, State: math.MaxUint64 - 10},
			used:      mdgas.MdGasUsage{State: 10},
			amount:    math.MaxInt64,
		},
		{
			name:      "spill restore would wrap execution gas",
			remaining: mdgas.MdGas{Execution: math.MaxUint64 - 5, State: 0},
			used:      mdgas.MdGasUsage{State: 10, StateSpill: 10},
			amount:    10,
		},
		{
			name:      "the exact signed boundary is representable",
			remaining: mdgas.MdGas{Execution: 1_000},
			used:      mdgas.MdGasUsage{State: -1},
			amount:    math.MaxInt64,
			accepted:  true,
			want:      mdgas.MdGas{Execution: 1_000, State: math.MaxInt64},
			wantUsed:  mdgas.MdGasUsage{State: math.MinInt64},
		},
		{
			name:      "an ordinary refund restores the reservoir",
			remaining: mdgas.MdGas{Execution: 1_000, State: 40},
			used:      mdgas.MdGasUsage{State: 10},
			amount:    10,
			accepted:  true,
			want:      mdgas.MdGas{Execution: 1_000, State: 50},
		},
		{
			name:      "a spilled refund comes back to execution gas first",
			remaining: mdgas.MdGas{Execution: 970, State: 0},
			used:      mdgas.MdGasUsage{State: 40, StateSpill: 30},
			amount:    40,
			accepted:  true,
			want:      mdgas.MdGas{Execution: 1_000, State: 10},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remaining, used := tc.remaining, tc.used
			g := &PrecompileGas{remaining: &remaining, used: &used, amsterdam: true}

			require.Equal(t, tc.accepted, g.RefundState(tc.amount))
			if !tc.accepted {
				require.Equal(t, tc.remaining, remaining, "a rejected refund must leave the reservoir alone")
				require.Equal(t, tc.used, used, "a rejected refund must leave usage alone")
				return
			}
			require.Equal(t, tc.want, remaining)
			require.Equal(t, tc.wantUsed, used)
		})
	}
}
