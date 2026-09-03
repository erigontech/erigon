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
	"maps"
	"math"
	"reflect"
	"slices"
	"sync"
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

type ptrPrecompile struct{ name string }

func (p *ptrPrecompile) RequiredGas([]byte) uint64        { return uint64(len(p.name)) }
func (p *ptrPrecompile) Run(input []byte) ([]byte, error) { return input, nil }
func (p *ptrPrecompile) Name() string                     { return p.name }

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

	RegisterPrecompiles(uint256.NewInt(registeredChainID), func(uint64) PrecompiledContracts {
		return PrecompiledContracts{extraAddr: stubPrecompile{"EXTRA"}}
	})
	t.Cleanup(func() { UnregisterPrecompiles(uint256.NewInt(registeredChainID)) })

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

	RegisterPrecompiles(uint256.NewInt(chainID), func(l2Version uint64) PrecompiledContracts {
		if l2Version >= 30 {
			return PrecompiledContracts{addrV30: stubPrecompile{"V30"}}
		}
		return PrecompiledContracts{}
	})
	t.Cleanup(func() { UnregisterPrecompiles(uint256.NewInt(chainID)) })

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
	RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) PrecompiledContracts { return nil })
	t.Cleanup(func() { UnregisterPrecompiles(uint256.NewInt(chainID)) })

	require.Panics(t, func() {
		RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) PrecompiledContracts { return nil })
	}, "duplicate chainID registration must panic")

	require.Panics(t, func() {
		RegisterPrecompiles(uint256.NewInt(900302), nil)
	}, "nil PrecompilesFunc must panic")
}

// TestRegisteredProviderWideChainID pins that the registry keys on the whole
// 256-bit chain ID. A key truncated to 64 bits would alias 2^64+1 onto chain 1
// and hand one chain's precompiles to another in a multi-chain embed.
func TestRegisteredProviderWideChainID(t *testing.T) {
	wide := new(uint256.Int).AddUint64(new(uint256.Int).Lsh(uint256.NewInt(1), 64), 1) // 2^64 + 1
	narrow := uint256.NewInt(1)
	wideAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x66}))

	RegisterPrecompiles(wide, func(uint64) PrecompiledContracts {
		return PrecompiledContracts{wideAddr: stubPrecompile{"WIDE"}}
	})
	t.Cleanup(func() { UnregisterPrecompiles(wide) })

	wideRules := &chain.Rules{ChainID: wide, IsCancun: true}
	narrowRules := &chain.Rules{ChainID: narrow, IsCancun: true}

	_, ok := Precompiles(wideRules)[wideAddr]
	require.True(t, ok, "the registering chain must see its own precompile")
	_, ok = Precompiles(narrowRules)[wideAddr]
	require.False(t, ok, "chain 1 must not inherit the provider registered for 2^64+1")
}

// TestRegisteredProviderForkDimension pins the fork dimension of the cache
// key. Without it, an L2 crossing a fork boundary keeps being served the
// merged set built at the earlier tier — the Osaka repricings and the 0x0100
// entry would never appear.
func TestRegisteredProviderForkDimension(t *testing.T) {
	const chainID = 900501
	extraAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x55}))
	osakaOnly := accounts.InternAddress(common.BytesToAddress([]byte{0x01, 0x00}))

	RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) PrecompiledContracts {
		return PrecompiledContracts{extraAddr: stubPrecompile{"EXTRA"}}
	})
	t.Cleanup(func() { UnregisterPrecompiles(uint256.NewInt(chainID)) })

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

	RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) PrecompiledContracts {
		return PrecompiledContracts{ecrecoverAddr: stubPrecompile{"CHAIN-ECRECOVER"}}
	})
	t.Cleanup(func() { UnregisterPrecompiles(uint256.NewInt(chainID)) })

	p, ok := Precompiles(rulesForChain(chainID, 0))[ecrecoverAddr]
	require.True(t, ok)
	require.Equal(t, "CHAIN-ECRECOVER", p.Name(), "the chain's own entry must replace the built-in")
}

// TestForkSetsCoverEveryTier pins the forkTier -> built-in set binding. The
// array is sized by forkTierCount, so a tier added to forkTierFor but missed
// in init() leaves a zero forkSet: every precompile vanishes at that fork,
// with no panic and no error.
func TestForkSetsCoverEveryTier(t *testing.T) {
	for i := range int(forkTierCount) {
		tier := forkTier(i)
		require.NotEmpty(t, forkSets[tier].contracts, "forkSets[%d] has no contracts", tier)
		require.NotEmpty(t, forkSets[tier].addresses, "forkSets[%d] has no addresses", tier)
		require.Len(t, forkSets[tier].addresses, len(forkSets[tier].contracts),
			"forkSets[%d] address list and contract map disagree", tier)
	}
}

// TestRegisterSweepsStaleCache reproduces the outcome of a provider call that
// was in flight across an unregister: its merged set lands after the sweep and
// outlives the provider that built it. Registering must not serve it.
func TestRegisterSweepsStaleCache(t *testing.T) {
	const chainID = 900503
	oldAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x44}))
	newAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x45}))
	rules := rulesForChain(chainID, 0)

	RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) PrecompiledContracts {
		return PrecompiledContracts{oldAddr: stubPrecompile{"OLD"}}
	})
	_, ok := Precompiles(rules)[oldAddr]
	require.True(t, ok)
	staleKey := precompileCacheKey{chainID: *uint256.NewInt(chainID), fork: forkTierFor(rules), l2Version: 0}
	registryMu.RLock()
	stale := mergedCache[staleKey]
	registryMu.RUnlock()
	require.NotNil(t, stale, "resolving must have cached a merged set")

	UnregisterPrecompiles(uint256.NewInt(chainID))
	// The insert the racing provider call would have made after the sweep.
	registryMu.Lock()
	mergedCache[staleKey] = stale
	registryMu.Unlock()

	RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) PrecompiledContracts {
		return PrecompiledContracts{newAddr: stubPrecompile{"NEW"}}
	})
	t.Cleanup(func() { UnregisterPrecompiles(uint256.NewInt(chainID)) })

	merged := Precompiles(rules)
	_, ok = merged[newAddr]
	require.True(t, ok, "the newly registered provider's overlay must be served")
	_, ok = merged[oldAddr]
	require.False(t, ok, "the unregistered provider's overlay must not survive re-registration")
}

// TestInFlightProviderCannotCacheAcrossReRegistration drives the ordering the
// generation token exists for: the old provider's call passes the cache miss,
// blocks, and resumes only after both the unregister and the re-registration
// have swept the cache. Its insert then lands after both sweeps, and every
// later lookup is a hit on the retired provider's overlay.
func TestInFlightProviderCannotCacheAcrossReRegistration(t *testing.T) {
	const chainID = 900506
	oldAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x4a}))
	newAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x4b}))
	rules := rulesForChain(chainID, 0)

	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) PrecompiledContracts {
		once.Do(func() { close(entered) })
		<-release
		return PrecompiledContracts{oldAddr: stubPrecompile{"OLD"}}
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		Precompiles(rules)
	}()
	<-entered

	UnregisterPrecompiles(uint256.NewInt(chainID))
	RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) PrecompiledContracts {
		return PrecompiledContracts{newAddr: stubPrecompile{"NEW"}}
	})
	t.Cleanup(func() { UnregisterPrecompiles(uint256.NewInt(chainID)) })

	close(release)
	<-done

	merged := Precompiles(rules)
	_, ok := merged[newAddr]
	require.True(t, ok, "the provider registered last must be served")
	_, ok = merged[oldAddr]
	require.False(t, ok, "a provider call in flight across the swap must not cache its overlay")
}

// TestProviderNilContractPanics pins that a nil entry is rejected where it is
// merged, naming the chain and address, rather than reaching evm.call and
// nil-dereferencing inside RunPrecompiledContract on a transaction.
func TestProviderNilContractPanics(t *testing.T) {
	const chainID = 900504
	badAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x46}))

	RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) PrecompiledContracts {
		return PrecompiledContracts{badAddr: nil}
	})
	t.Cleanup(func() { UnregisterPrecompiles(uint256.NewInt(chainID)) })

	require.PanicsWithValue(t,
		"vm: precompile provider for chain 900504 returned a nil contract at 0000000000000000000000000000000000000046",
		func() { Precompiles(rulesForChain(chainID, 0)) })
}

func TestProviderTypedNilContractPanics(t *testing.T) {
	const chainID = 900505
	badAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x47}))

	RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) PrecompiledContracts {
		return PrecompiledContracts{badAddr: (*ptrPrecompile)(nil)}
	})
	t.Cleanup(func() { UnregisterPrecompiles(uint256.NewInt(chainID)) })

	require.PanicsWithValue(t,
		"vm: precompile provider for chain 900505 returned a nil contract at 0000000000000000000000000000000000000047",
		func() { Precompiles(rulesForChain(chainID, 0)) })
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

// TestDeprecatedForkAddressExportsTrackTheirSets pins the exported per-fork
// address slices to the sets they name. Chains outside this repo compile
// against them, so an empty or drifted slice is a break no in-repo grep sees.
func TestDeprecatedForkAddressExportsTrackTheirSets(t *testing.T) {
	for name, tc := range map[string]struct {
		addrs     []accounts.Address
		contracts PrecompiledContracts
	}{
		"homestead": {PrecompiledAddressesHomestead, PrecompiledContractsHomestead},
		"byzantium": {PrecompiledAddressesByzantium, PrecompiledContractsByzantium},
		"istanbul":  {PrecompiledAddressesIstanbul, PrecompiledContractsIstanbul},
		"berlin":    {PrecompiledAddressesBerlin, PrecompiledContractsBerlin},
		"cancun":    {PrecompiledAddressesCancun, PrecompiledContractsCancun},
		"prague":    {PrecompiledAddressesPrague, PrecompiledContractsPrague},
		"osaka":     {PrecompiledAddressesOsaka, PrecompiledContractsOsaka},
	} {
		require.NotEmpty(t, tc.addrs, name)
		require.ElementsMatch(t, slices.Collect(maps.Keys(tc.contracts)), tc.addrs, name)
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
