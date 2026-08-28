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
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

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
	// providerCount keeps the overwhelmingly common no-provider case off
	// registryMu: Precompiles and ActivePrecompiles run 2-3 times per
	// transaction, and an RWMutex read lock anti-scales with worker count.
	providerCount atomic.Int64
	// registryGen advances on every provider change. A merged set built from a
	// provider read before the change must not be cached after it.
	registryGen uint64
	mergedCache = map[precompileCacheKey]*mergedPrecompileSet{}
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
	if _, exists := providers[*chainID]; exists {
		delete(providers, *chainID)
		providerCount.Add(-1)
	}
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
	if providerCount.Load() == 0 {
		return nil, 0, false
	}
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
func mergedSetFor(rules *chain.Rules, fork forkTier, chainID uint256.Int, provider PrecompilesFunc, gen uint64) *mergedPrecompileSet {
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
	contracts := maps.Clone(forkSets[fork].contracts)
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
	// Before Amsterdam there is no state dimension to charge against.
	amsterdam bool
	// Execution gas charged through this handle and not yet given back.
	chargedExecution uint64
}

// onGasChange reports a charge or refund the way the interpreter's useMdGas
// does, so a tracer sees the same event stream either way.
func (g *PrecompileGas) onGasChange(before mdgas.MdGas, spilled uint64, typ mdgas.MdGasType, reason tracing.GasChangeReason) {
	if g.tracer == nil || g.tracer.OnGasChange == nil {
		return
	}
	from, to := gasChangeDimension(before, *g.remaining, typ, spilled)
	if from != to {
		g.tracer.OnGasChange(from, to, reason)
	}
}

// release detaches the handle when RunStateful returns. evm.call's named
// returns die with the frame, so a handle stashed past that point would
// otherwise mutate a dead copy and still report success.
func (g *PrecompileGas) release() { g.remaining, g.used = nil, nil }

func (g *PrecompileGas) live() bool { return g.remaining != nil }

// Remaining reports the gas left in both dimensions, and zero once the frame
// this handle belongs to has returned.
func (g *PrecompileGas) Remaining() mdgas.MdGas {
	if !g.live() {
		return mdgas.MdGas{}
	}
	return *g.remaining
}

// ChargeExecution deducts execution gas, reporting false and charging
// nothing when the frame cannot cover it.
func (g *PrecompileGas) ChargeExecution(amount uint64) bool {
	if !g.live() {
		return false
	}
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
//
// Before Amsterdam it charges execution gas outright. The reservoir is empty
// then, so a state charge would spill wholesale into execution gas but record
// itself in used.State — which pre-Amsterdam transaction accounting drops,
// taking the gas off the frame without it reaching the receipt or the block.
func (g *PrecompileGas) ChargeState(amount uint64) bool {
	if !g.live() {
		return false
	}
	if !g.amsterdam {
		return g.ChargeExecution(amount)
	}
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
	if !g.live() {
		return false
	}
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
	if !g.live() {
		return
	}
	if !g.amsterdam {
		// Mirrors ChargeState: the charge went to execution, so the refund has
		// to come back from it rather than invent reservoir gas that cannot
		// exist before Amsterdam.
		g.RefundExecution(amount)
		return
	}
	before, spilledBefore := *g.remaining, g.used.StateSpill
	mdgas.Refill(g.remaining, g.used, amount, mdgas.StateGas)
	g.onGasChange(before, spilledBefore-g.used.StateSpill, mdgas.StateGas, tracing.GasChangeCallLeftOverRefunded)
}

// StatefulPrecompile is a PrecompiledContract that additionally receives the
// calling frame's context and charges its own gas. RequiredGas is not
// consulted on this path.
//
// One instance serves every frame, including those on parallel-executor
// workers, so an implementation must keep no per-call mutable state on its
// receiver.
//
// The implementation must not mutate state when ctx.ReadOnly is true. Only the
// re-entrant ctx.Evm call and create paths refuse on their own; the state
// surface reached through ctx.Evm carries no readOnly awareness, so a missed
// branch corrupts state under STATICCALL rather than failing.
//
// Nested calls belong on PrecompileContext.Call, which carries the EIP-8037
// reservoir handoff a bare ctx.Evm.Call cannot.
type StatefulPrecompile interface {
	PrecompiledContract
	RunStateful(input []byte, gas *PrecompileGas, ctx *PrecompileContext) (ret []byte, err error)
}

// reenter runs one nested frame with the EIP-8037 reservoir handoff: it charges
// executionGas through the handle, moves the whole reservoir to the child and
// zeroes this frame's, then restores from the child's leftover and adopts its
// state usage. A raw ctx.Evm call cannot express this — MdGas passes by value,
// so handing it gas.Remaining() leaves the reservoir standing in this frame too
// and duplicates it once per nesting level, and the child's spill is dropped
// instead of reaching handleFrameRevert.
func (ctx *PrecompileContext) reenter(gas *PrecompileGas, executionGas uint64,
	run func(handed mdgas.MdGas) ([]byte, mdgas.MdGas, mdgas.MdGasUsage, error),
) ([]byte, error) {
	if !gas.live() {
		return nil, ErrOutOfGas
	}
	if !gas.ChargeExecution(executionGas) {
		return nil, ErrOutOfGas
	}
	handed := mdgas.MdGas{Execution: executionGas, State: gas.remaining.State}
	gas.remaining.State = 0

	ret, leftover, usage, err := run(handed)

	// The child's own revert already restored its entry reservoir into
	// leftover.State, so this is the whole reservoir back on the error path.
	gas.remaining.State = leftover.State
	if err == nil {
		gas.used.State += usage.State
		gas.used.StateSpill += usage.StateSpill
	}
	gas.RefundExecution(leftover.Execution)
	return ret, err
}

func orZero(v *uint256.Int) uint256.Int {
	if v == nil {
		return uint256.Int{}
	}
	return *v
}

// Call runs a nested CALL out of the precompile's frame.
func (ctx *PrecompileContext) Call(gas *PrecompileGas, addr accounts.Address, input []byte, executionGas uint64, value *uint256.Int) ([]byte, error) {
	return ctx.reenter(gas, executionGas, func(handed mdgas.MdGas) ([]byte, mdgas.MdGas, mdgas.MdGasUsage, error) {
		return ctx.Evm.Call(ctx.ActingAs, addr, input, handed, orZero(value), false)
	})
}

// StaticCall runs a nested STATICCALL out of the precompile's frame.
func (ctx *PrecompileContext) StaticCall(gas *PrecompileGas, addr accounts.Address, input []byte, executionGas uint64) ([]byte, error) {
	return ctx.reenter(gas, executionGas, func(handed mdgas.MdGas) ([]byte, mdgas.MdGas, mdgas.MdGasUsage, error) {
		return ctx.Evm.StaticCall(ctx.ActingAs, addr, input, handed)
	})
}

// CallCode runs a nested CALLCODE out of the precompile's frame.
func (ctx *PrecompileContext) CallCode(gas *PrecompileGas, addr accounts.Address, input []byte, executionGas uint64, value *uint256.Int) ([]byte, error) {
	return ctx.reenter(gas, executionGas, func(handed mdgas.MdGas) ([]byte, mdgas.MdGas, mdgas.MdGasUsage, error) {
		return ctx.Evm.CallCode(ctx.ActingAs, addr, input, handed, orZero(value))
	})
}

// DelegateCall runs a nested DELEGATECALL out of the precompile's frame, which
// keeps this frame's own identity, caller and value. DELEGATECALL has no value
// operand — the callee observes the calling frame's msg.value — so there is
// deliberately no value parameter here.
func (ctx *PrecompileContext) DelegateCall(gas *PrecompileGas, addr accounts.Address, input []byte, executionGas uint64) ([]byte, error) {
	return ctx.reenter(gas, executionGas, func(handed mdgas.MdGas) ([]byte, mdgas.MdGas, mdgas.MdGasUsage, error) {
		return ctx.Evm.DelegateCall(ctx.ActingAs, ctx.Caller, addr, input, orZero(ctx.Value), handed)
	})
}

// Create runs a nested CREATE out of the precompile's frame, or CREATE2 when
// salt is non-nil.
func (ctx *PrecompileContext) Create(gas *PrecompileGas, code []byte, executionGas uint64, endowment, salt *uint256.Int) (ret []byte, created accounts.Address, err error) {
	ret, err = ctx.reenter(gas, executionGas, func(handed mdgas.MdGas) ([]byte, mdgas.MdGas, mdgas.MdGasUsage, error) {
		out, addr, leftover, usage, cerr := ctx.Evm.Create(ctx.ActingAs, code, handed, orZero(endowment), salt, false)
		created = addr
		return out, leftover, usage, cerr
	})
	return ret, created, err
}

// NoStatelessRun supplies the stateless half of PrecompiledContract for a
// StatefulPrecompile. evm.call dispatches one through RunStateful, so these are
// reached only by a misroute — Run says so rather than returning an empty
// success. Embed it and supply Name.
type NoStatelessRun struct{}

func (NoStatelessRun) RequiredGas([]byte) uint64 { return 0 }

func (NoStatelessRun) Run([]byte) ([]byte, error) {
	return nil, errors.New("vm: stateful precompile reached the stateless Run path")
}
