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
	"maps"
	"slices"
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// PrecompilesFunc builds a chain's precompile overlay for the given resolved
// Rules. Its output must depend only on Rules.L2Version — the merged result
// is cached per (chainID, base fork tier, L2Version), so keying on anything
// else would serve a stale set on a cache hit.
type PrecompilesFunc func(rules *chain.Rules) PrecompiledContracts

var (
	registryMu  sync.RWMutex
	providers   = map[uint64]PrecompilesFunc{}
	mergedCache = map[precompileCacheKey]*mergedPrecompileSet{}
)

// RegisterPrecompiles registers f as the precompile provider for chainID. A
// provider's entries overlay the fork-selected built-ins on that chain only,
// and win on address collision (a chain may deliberately replace a built-in).
// Panics if chainID is already registered or f is nil.
func RegisterPrecompiles(chainID uint64, f PrecompilesFunc) {
	if f == nil {
		panic("vm: RegisterPrecompiles: nil PrecompilesFunc")
	}
	if chainID == 0 {
		// Chain ID 0 is what nil-ChainID (bare/genesis/test) Rules map to.
		panic("vm: RegisterPrecompiles: chain ID 0")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := providers[chainID]; exists {
		panic(fmt.Sprintf("vm: RegisterPrecompiles: chain ID %d already registered", chainID))
	}
	providers[chainID] = f
}

// UnregisterPrecompiles removes a chain's provider and its cached merged
// sets; for tests and controlled teardown of an embedded chain.
func UnregisterPrecompiles(chainID uint64) {
	registryMu.Lock()
	defer registryMu.Unlock()
	delete(providers, chainID)
	for k := range mergedCache {
		if k.chainID == chainID {
			delete(mergedCache, k)
		}
	}
}

type precompileCacheKey struct {
	chainID   uint64
	fork      forkTier
	l2Version uint64
}

type mergedPrecompileSet struct {
	contracts PrecompiledContracts
	addresses []accounts.Address
}

// rulesChainID tolerates a nil ChainID (bare Rules values are used on
// genesis and test paths); no provider registers chain ID 0.
func rulesChainID(rules *chain.Rules) uint64 {
	if rules.ChainID == nil {
		return 0
	}
	return rules.ChainID.Uint64()
}

func lookupProvider(chainID uint64) (PrecompilesFunc, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	f, ok := providers[chainID]
	return f, ok
}

// mergedSetFor returns the cached (contracts, addresses) pair for
// (chainID, fork, rules.L2Version), building and caching it on first miss.
func mergedSetFor(rules *chain.Rules, base PrecompiledContracts, fork forkTier, chainID uint64, provider PrecompilesFunc) *mergedPrecompileSet {
	key := precompileCacheKey{chainID: chainID, fork: fork, l2Version: rules.L2Version}

	registryMu.RLock()
	if set, ok := mergedCache[key]; ok {
		registryMu.RUnlock()
		return set
	}
	registryMu.RUnlock()

	contracts := maps.Clone(base)
	maps.Copy(contracts, provider(rules))
	set := &mergedPrecompileSet{contracts: contracts, addresses: slices.Collect(maps.Keys(contracts))}

	registryMu.Lock()
	defer registryMu.Unlock()
	if existing, ok := mergedCache[key]; ok {
		return existing
	}
	mergedCache[key] = set
	return set
}

// PrecompileContext carries a stateful precompile's calling frame: Self is
// the precompile's own code address, ActingAs is the address the frame acts
// as (diverges from Self under CALLCODE/DELEGATECALL), and Caller is the
// address the callee sees as its caller.
type PrecompileContext struct {
	Self     accounts.Address
	ActingAs accounts.Address
	Caller   accounts.Address
	Value    *uint256.Int
	ReadOnly bool
	Evm      *EVM
}

// StatefulPrecompile is a PrecompiledContract that additionally receives the
// calling frame's context. The implementation owns ALL of its gas accounting
// end to end — RequiredGas is not consulted on this path — and must not
// mutate state when ctx.ReadOnly is true, since the interpreter's readOnly
// enforcement does not cover precompiles.
type StatefulPrecompile interface {
	PrecompiledContract
	RunStateful(input []byte, gas mdgas.MdGas, ctx *PrecompileContext) (ret []byte, remaining mdgas.MdGas, err error)
}
