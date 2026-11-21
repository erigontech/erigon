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

var (
	registryMu sync.Mutex
	chainSets  atomic.Pointer[map[uint256.Int]*[forkTierCount]mergedPrecompileSet]
)

func chainSetsSnapshot() map[uint256.Int]*[forkTierCount]mergedPrecompileSet {
	if m := chainSets.Load(); m != nil {
		return *m
	}
	return nil
}

func storeChainSets(chainID uint256.Int, sets *[forkTierCount]mergedPrecompileSet) {
	next := maps.Clone(chainSetsSnapshot())
	if next == nil {
		next = map[uint256.Int]*[forkTierCount]mergedPrecompileSet{}
	}
	next[chainID] = sets
	chainSets.Store(&next)
}

func RegisterPrecompiles(chainID *uint256.Int, overlay PrecompiledContracts) {
	if chainID == nil || chainID.IsZero() {
		// Chain ID 0 is what nil-ChainID (bare/genesis/test) Rules map to.
		panic("vm: RegisterPrecompiles: chain ID 0")
	}
	for addr, p := range overlay {
		if isNilContract(p) {
			panic(fmt.Sprintf("vm: RegisterPrecompiles: chain ID %s has a nil contract at %x", chainID, addr))
		}
	}
	var sets [forkTierCount]mergedPrecompileSet
	for tier := range sets {
		contracts := maps.Clone(forkSets[tier].contracts)
		maps.Copy(contracts, overlay)
		sets[tier] = mergedPrecompileSet{contracts: contracts, addresses: slices.Collect(maps.Keys(contracts))}
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	switch existing, ok := chainSetsSnapshot()[*chainID]; {
	case existing == &forkSets:
		panic(fmt.Sprintf("vm: RegisterPrecompiles: chain ID %s precompiles already resolved", chainID))
	case ok:
		panic(fmt.Sprintf("vm: RegisterPrecompiles: chain ID %s already registered", chainID))
	}
	storeChainSets(*chainID, &sets)
}

func rulesChainID(rules *chain.Rules) uint256.Int {
	if rules.ChainID == nil {
		return uint256.Int{}
	}
	return *rules.ChainID
}

func resolveChainSets(chainID uint256.Int) *[forkTierCount]mergedPrecompileSet {
	if sets, ok := chainSetsSnapshot()[chainID]; ok {
		return sets
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	sets, ok := chainSetsSnapshot()[chainID]
	if !ok {
		sets = &forkSets
		storeChainSets(chainID, sets)
	}
	return sets
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
