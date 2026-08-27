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
	"github.com/erigontech/erigon/execution/tracing"
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
	// registryGen advances on every provider change. A merged set built from a
	// provider read before the change must not be cached after it.
	registryGen uint64
	mergedCache = map[precompileCacheKey]*mergedPrecompileSet{}
)

// RegisterPrecompiles registers f as the precompile provider for chainID. A
// provider's entries overlay the fork-selected built-ins on that chain only,
// and win on address collision (a chain may deliberately replace a built-in).
// Panics if chainID is already registered, is nil or zero, or f is nil.
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
	registryGen++
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
	delete(providers, *chainID)
	registryGen++
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

func lookupProvider(chainID uint256.Int) (f PrecompilesFunc, gen uint64, ok bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	f, ok = providers[chainID]
	return f, registryGen, ok
}

// mergedSetFor returns the cached (contracts, addresses) pair for
// (chainID, fork, rules.L2Version), building and caching it on first miss.
// gen is the registry generation the provider was read at; a set built from a
// provider that has since been replaced is returned to its own caller but never
// cached.
func mergedSetFor(rules *chain.Rules, base PrecompiledContracts, fork forkTier, chainID uint256.Int, provider PrecompilesFunc, gen uint64) *mergedPrecompileSet {
	key := precompileCacheKey{chainID: chainID, fork: fork, l2Version: rules.L2Version}

	registryMu.RLock()
	if set, ok := mergedCache[key]; ok {
		registryMu.RUnlock()
		return set
	}
	registryMu.RUnlock()

	overlay := provider(rules.L2Version)
	for addr, p := range overlay {
		if p == nil {
			panic(fmt.Sprintf("vm: precompile provider for chain %s returned a nil contract at %x", &chainID, addr))
		}
	}
	contracts := maps.Clone(base)
	maps.Copy(contracts, overlay)
	set := &mergedPrecompileSet{contracts: contracts, addresses: slices.Collect(maps.Keys(contracts))}

	registryMu.Lock()
	defer registryMu.Unlock()
	if existing, ok := mergedCache[key]; ok {
		return existing
	}
	if registryGen == gen {
		mergedCache[key] = set
	}
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

// PrecompileGas is the frame's gas, charged through the same helpers the
// interpreter uses. Going through it is what keeps the reservoir and the
// frame's usage report agreeing: a state charge that exceeds the EIP-8037
// reservoir spills into execution gas, and the spill has to be recorded for
// handleFrameRevert to give it back on REVERT. A precompile adjusting a raw
// MdGas would have to reproduce that, and silently mis-attribute the frame
// if it got it wrong.
type PrecompileGas struct {
	remaining *mdgas.MdGas
	used      *mdgas.MdGasUsage
	tracer    *tracing.Hooks
	// Execution gas charged through this handle and not yet given back.
	chargedExecution uint64
}

// onGasChange reports a charge or refund the way the interpreter's useMdGas
// does, so a tracer sees the same event stream either way: a state charge the
// EIP-8037 reservoir covers in full is reported in the state dimension, and
// only a spill moves execution gas.
func (g *PrecompileGas) onGasChange(before mdgas.MdGas, spilled uint64, typ mdgas.MdGasType, reason tracing.GasChangeReason) {
	if g.tracer == nil || g.tracer.OnGasChange == nil {
		return
	}
	from, to := before.Execution, g.remaining.Execution
	if typ == mdgas.StateGas && spilled == 0 {
		from, to = before.State, g.remaining.State
	}
	if from != to {
		g.tracer.OnGasChange(from, to, reason)
	}
}

// Remaining reports the gas left in both dimensions.
func (g *PrecompileGas) Remaining() mdgas.MdGas { return *g.remaining }

// ChargeExecution deducts execution gas, reporting false and charging
// nothing when the frame cannot cover it.
func (g *PrecompileGas) ChargeExecution(amount uint64) bool {
	before := *g.remaining
	if !mdgas.Consume(g.remaining, g.used, amount, mdgas.ExecutionGas) {
		return false
	}
	g.chargedExecution += amount
	g.onGasChange(before, 0, mdgas.ExecutionGas, tracing.GasChangeCallPrecompiledContract)
	return true
}

// ChargeState deducts state gas, spilling into execution gas when the
// EIP-8037 reservoir is short. Reports false and charges nothing when
// neither dimension can cover it.
func (g *PrecompileGas) ChargeState(amount uint64) bool {
	before, spilledBefore := *g.remaining, g.used.StateSpill
	if !mdgas.Consume(g.remaining, g.used, amount, mdgas.StateGas) {
		return false
	}
	g.onGasChange(before, g.used.StateSpill-spilledBefore, mdgas.StateGas, tracing.GasChangeCallPrecompiledContract)
	return true
}

// RefundExecution gives execution gas back, e.g. the leftover a nested
// ctx.Evm call returned. Reports false and refunds nothing above what this
// handle charged: unlike a state refund, execution gas can only return from a
// charge this frame made, and an unbounded refill underflows used.Execution
// and hands the caller more gas than the frame was given.
func (g *PrecompileGas) RefundExecution(amount uint64) bool {
	if amount > g.chargedExecution {
		return false
	}
	before := *g.remaining
	mdgas.Refill(g.remaining, g.used, amount, mdgas.ExecutionGas)
	g.chargedExecution -= amount
	g.onGasChange(before, 0, mdgas.ExecutionGas, tracing.GasChangeCallLeftOverRefunded)
	return true
}

// RefundState reverses a state charge — clearing state the frame created, or
// forwarding a nested call's refunded reservoir. Nothing bounds the amount
// against what this frame charged, since a forwarded refund did not come from
// it, and a frame that clears more than it created legitimately ends with a
// negative net state usage.
func (g *PrecompileGas) RefundState(amount uint64) {
	before, spilledBefore := *g.remaining, g.used.StateSpill
	mdgas.Refill(g.remaining, g.used, amount, mdgas.StateGas)
	g.onGasChange(before, spilledBefore-g.used.StateSpill, mdgas.StateGas, tracing.GasChangeCallLeftOverRefunded)
}

// StatefulPrecompile is a PrecompiledContract that additionally receives the
// calling frame's context and charges its own gas. RequiredGas is not
// consulted on this path. The implementation must not mutate state when
// ctx.ReadOnly is true — the interpreter's readOnly enforcement does not
// cover precompiles.
//
// Handing the EIP-8037 state reservoir to a nested ctx.Evm call is not
// covered here: CALL moves the whole reservoir to the callee and adopts back
// whatever it leaves (opCall/restoreChildGas). An implementation that needs
// that has to follow the same convention itself.
type StatefulPrecompile interface {
	PrecompiledContract
	RunStateful(input []byte, gas *PrecompileGas, ctx *PrecompileContext) (ret []byte, err error)
}
