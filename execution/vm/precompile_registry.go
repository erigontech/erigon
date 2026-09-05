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
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// PrecompilesFunc builds a chain's precompile overlay at an L2 version. It is
// handed the version and nothing else on purpose: the merged result is cached
// per (chainID, base fork tier, L2Version), so an overlay that varied with any
// other part of Rules would be served a stale set on a cache hit. L1-fork
// variation belongs to the built-in base sets, which the fork tier already
// keys.
//
// That cache has no eviction, so L2Version has to be a short upgrade ladder
// (ArbOS 30, 50, …). A chain whose L2Config resolves it from the block number
// grows the cache without bound.
type PrecompilesFunc func(l2Version uint64) PrecompiledContracts

var (
	registryMu sync.RWMutex
	providers  = map[uint256.Int]PrecompilesFunc{}
	// providerCount keeps the overwhelmingly common no-provider case off
	// registryMu: Precompiles and ActivePrecompiles run 2-3 times per
	// transaction, and an RWMutex read lock anti-scales with worker count.
	providerCount atomic.Int64
	mergedCache   = map[precompileCacheKey]*mergedPrecompileSet{}
)

// RegisterPrecompiles registers f as the precompile provider for chainID. A
// provider's entries overlay the fork-selected built-ins on that chain only,
// and win on address collision (a chain may deliberately replace a built-in).
// Panics if chainID is already registered, is nil or zero, or f is nil.
//
// Registration must complete before any EVM exists for that chain. The set is
// snapshotted per EVM but resolved live by state.Prepare, so a change made
// mid-run desyncs the EIP-2929 warm set from what dispatches, and parallel
// workers can run one block against different sets.
func RegisterPrecompiles(chainID *uint256.Int, f PrecompilesFunc) {
	if f == nil {
		panic("vm: RegisterPrecompiles: nil PrecompilesFunc")
	}
	if chainID == nil || chainID.IsZero() {
		// Chain ID 0 is what nil-ChainID (bare/genesis/test) Rules map to.
		panic("vm: RegisterPrecompiles: chain ID 0")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := providers[*chainID]; exists {
		panic(fmt.Sprintf("vm: RegisterPrecompiles: chain ID %s already registered", chainID))
	}
	providers[*chainID] = f
	providerCount.Add(1)
	dropCachedLocked(*chainID)
}

// UnregisterPrecompiles removes a chain's provider and its cached merged
// sets; for tests and controlled teardown of an embedded chain.
func UnregisterPrecompiles(chainID *uint256.Int) {
	if chainID == nil {
		return
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := providers[*chainID]; exists {
		delete(providers, *chainID)
		providerCount.Add(-1)
	}
	dropCachedLocked(*chainID)
}

// dropCachedLocked removes every merged set cached for chainID. Caller holds
// the write lock.
func dropCachedLocked(chainID uint256.Int) {
	for k := range mergedCache {
		if k.chainID == chainID {
			delete(mergedCache, k)
		}
	}
}

type precompileCacheKey struct {
	chainID   uint256.Int
	fork      forkTier
	l2Version uint64
}

type mergedPrecompileSet struct {
	contracts PrecompiledContracts
	addresses []accounts.Address
}

// rulesChainID tolerates a nil ChainID (bare Rules values are used on
// genesis and test paths); no provider registers chain ID 0.
func rulesChainID(rules *chain.Rules) uint256.Int {
	if rules.ChainID == nil {
		return uint256.Int{}
	}
	return *rules.ChainID
}

func lookupProvider(chainID uint256.Int) (f PrecompilesFunc, ok bool) {
	if providerCount.Load() == 0 {
		return nil, false
	}
	registryMu.RLock()
	defer registryMu.RUnlock()
	f, ok = providers[chainID]
	return f, ok
}

// mergedSetFor returns the cached (contracts, addresses) pair for
// (chainID, fork, rules.L2Version), building and caching it on first miss.
func mergedSetFor(rules *chain.Rules, fork forkTier, chainID uint256.Int, provider PrecompilesFunc) *mergedPrecompileSet {
	key := precompileCacheKey{chainID: chainID, fork: fork, l2Version: rules.L2Version}

	registryMu.RLock()
	if set, ok := mergedCache[key]; ok {
		registryMu.RUnlock()
		return set
	}
	registryMu.RUnlock()

	overlay := provider(rules.L2Version)
	for addr, p := range overlay {
		if isNilContract(p) {
			panic(fmt.Sprintf("vm: precompile provider for chain %s returned a nil contract at %x", &chainID, addr))
		}
	}
	contracts := maps.Clone(forkSets[fork].contracts)
	maps.Copy(contracts, overlay)
	set := &mergedPrecompileSet{contracts: contracts, addresses: slices.Collect(maps.Keys(contracts))}

	registryMu.Lock()
	defer registryMu.Unlock()
	if existing, ok := mergedCache[key]; ok {
		return existing
	}
	mergedCache[key] = set
	return set
}

func isNilContract(p PrecompiledContract) bool {
	if p == nil {
		return true
	}
	switch v := reflect.ValueOf(p); v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
		return v.IsNil()
	default:
		return false
	}
}
