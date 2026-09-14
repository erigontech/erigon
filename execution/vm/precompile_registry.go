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
)

// PrecompilesFunc builds a chain's precompile overlay at an L2 version. The
// merged result is cached per (chainID, fork tier, L2Version) with no
// eviction: an overlay that varied on anything else in Rules is served stale
// on a hit, and an L2Version derived from block number grows the cache forever.
type PrecompilesFunc func(l2Version uint64) PrecompiledContracts

var (
	registryMu  sync.Mutex
	providers   atomic.Pointer[map[uint256.Int]PrecompilesFunc]
	mergedCache sync.Map
)

func providerSnapshot() map[uint256.Int]PrecompilesFunc {
	if m := providers.Load(); m != nil {
		return *m
	}
	return nil
}

// RegisterPrecompiles registers f as chainID's precompile provider; its entries
// overlay the fork-selected built-ins and win on address collision. Must return
// before any EVM exists for the chain: the set is snapshotted per EVM but
// resolved live by state.Prepare, so a mid-run change desyncs the EIP-2929 warm
// set from what dispatches and splits parallel workers across two sets.
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
	if _, exists := providerSnapshot()[*chainID]; exists {
		panic(fmt.Sprintf("vm: RegisterPrecompiles: chain ID %s already registered", chainID))
	}
	next := maps.Clone(providerSnapshot())
	if next == nil {
		next = map[uint256.Int]PrecompilesFunc{}
	}
	next[*chainID] = f
	providers.Store(&next)
	dropCached(*chainID)
}

// UnregisterPrecompiles removes chainID's provider and its cached merged sets.
func UnregisterPrecompiles(chainID *uint256.Int) {
	if chainID == nil || chainID.IsZero() {
		panic("vm: UnregisterPrecompiles: chain ID 0")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := providerSnapshot()[*chainID]; exists {
		next := maps.Clone(providerSnapshot())
		delete(next, *chainID)
		providers.Store(&next)
	}
	dropCached(*chainID)
}

func dropCached(chainID uint256.Int) {
	mergedCache.Range(func(k, _ any) bool {
		if k.(precompileCacheKey).chainID == chainID {
			mergedCache.Delete(k)
		}
		return true
	})
}

type precompileCacheKey struct {
	chainID   uint256.Int
	fork      forkTier
	l2Version uint64
}

func rulesChainID(rules *chain.Rules) uint256.Int {
	if rules.ChainID == nil {
		return uint256.Int{}
	}
	return *rules.ChainID
}

func lookupProvider(chainID uint256.Int) (PrecompilesFunc, bool) {
	f, ok := providerSnapshot()[chainID]
	return f, ok
}

func mergedSetFor(rules *chain.Rules, fork forkTier, chainID uint256.Int, provider PrecompilesFunc) *mergedPrecompileSet {
	key := precompileCacheKey{chainID: chainID, fork: fork, l2Version: rules.L2Version}

	if set, ok := mergedCache.Load(key); ok {
		return set.(*mergedPrecompileSet)
	}

	overlay := provider(rules.L2Version)
	for addr, p := range overlay {
		if isNilContract(p) {
			panic(fmt.Sprintf("vm: precompile provider for chain %s returned a nil contract at %x", &chainID, addr))
		}
	}
	contracts := maps.Clone(forkSets[fork].contracts)
	maps.Copy(contracts, overlay)
	set := &mergedPrecompileSet{contracts: contracts, addresses: slices.Collect(maps.Keys(contracts))}

	actual, _ := mergedCache.LoadOrStore(key, set)
	return actual.(*mergedPrecompileSet)
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
