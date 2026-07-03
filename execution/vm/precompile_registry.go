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
	"sync"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// PrecompilesFunc builds a chain's precompile overlay for the given resolved
// Rules. Its output must depend only on Rules.L2Version — the merged result
// is cached per (chainID, base fork tier, L2Version), so keying on anything
// else (including L1 fork booleans) would serve a stale set on a cache hit.
// L1-fork variation belongs to the built-in base sets; an L2 gating an entry
// on an L1 fork encodes that into its own L2Version ladder. Registration is
// init-time; the cache holds one entry per distinct key, so it stays
// upgrade-ladder-sized.
type PrecompilesFunc func(rules *chain.Rules) PrecompiledContracts

var (
	registryMu  sync.Mutex
	providers   atomic.Pointer[map[uint64]PrecompilesFunc]
	mergedMu    sync.RWMutex
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
	cur := providers.Load()
	if cur != nil {
		if _, exists := (*cur)[chainID]; exists {
			panic(fmt.Sprintf("vm: RegisterPrecompiles: chain ID %d already registered", chainID))
		}
	}
	next := map[uint64]PrecompilesFunc{}
	if cur != nil {
		maps.Copy(next, *cur)
	}
	next[chainID] = f
	providers.Store(&next)
}

// UnregisterPrecompiles removes a chain's provider and its cached merged
// sets; for tests and controlled teardown of an embedded chain.
func UnregisterPrecompiles(chainID uint64) {
	registryMu.Lock()
	defer registryMu.Unlock()
	cur := providers.Load()
	if cur == nil {
		return
	}
	next := maps.Clone(*cur)
	delete(next, chainID)
	providers.Store(&next)
	mergedMu.Lock()
	defer mergedMu.Unlock()
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
	cur := providers.Load()
	if cur == nil {
		return nil, false
	}
	f, ok := (*cur)[chainID]
	return f, ok
}

// mergedSetFor returns the cached (contracts, addresses) pair for
// (chainID, fork, rules.L2Version), building and caching it on first miss.
func mergedSetFor(rules *chain.Rules, base PrecompiledContracts, fork forkTier, chainID uint64, provider PrecompilesFunc) *mergedPrecompileSet {
	key := precompileCacheKey{chainID: chainID, fork: fork, l2Version: rules.L2Version}

	mergedMu.RLock()
	if set, ok := mergedCache[key]; ok {
		mergedMu.RUnlock()
		return set
	}
	mergedMu.RUnlock()

	contracts := maps.Clone(base)
	maps.Copy(contracts, provider(rules))
	addresses := make([]accounts.Address, 0, len(contracts))
	for addr := range contracts {
		addresses = append(addresses, addr)
	}
	set := &mergedPrecompileSet{contracts: contracts, addresses: addresses}

	mergedMu.Lock()
	defer mergedMu.Unlock()
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
