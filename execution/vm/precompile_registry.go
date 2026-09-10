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
	"math"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
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

// PrecompileContext carries the precompile's calling frame. Self is its own
// code address; ActingAs is the identity the frame acts under, which diverges
// from Self under CALLCODE and DELEGATECALL.
type PrecompileContext struct {
	Self     accounts.Address
	ActingAs accounts.Address
	Caller   accounts.Address
	ReadOnly bool
	EVM      *EVM
	Value    uint256.Int
}

// PrecompileGas is the frame's gas handle. Charging through it is what keeps
// the reservoir and the usage report agreeing: a state charge exceeding the
// EIP-8037 reservoir spills into execution gas, and handleFrameRevert returns
// the spill on REVERT only if it was recorded.
type PrecompileGas struct {
	remaining *mdgas.MdGas
	used      *mdgas.MdGasUsage
	tracer    *tracing.Hooks
	// Before Amsterdam there is no state dimension to charge against.
	amsterdam bool
	// Execution gas charged through this handle and not yet given back.
	chargedExecution uint64
	aborted          error
}

// onGasChange mirrors useMdGas so a tracer sees the same event stream either way.
func (g *PrecompileGas) onGasChange(before mdgas.MdGas, spilled uint64, typ mdgas.MdGasType, reason tracing.GasChangeReason) {
	if g.tracer == nil || g.tracer.OnGasChange == nil {
		return
	}
	from, to := gasChangeDimension(before, *g.remaining, typ, spilled)
	if from != to {
		g.tracer.OnGasChange(from, to, reason)
	}
}

// release detaches the handle when RunStateful returns: evm.call's named
// returns die with the frame, and a stashed handle would mutate a dead copy.
func (g *PrecompileGas) release() { g.remaining, g.used = nil, nil }

func (g *PrecompileGas) abort(err error) {
	g.aborted = err
	g.release()
}

func (g *PrecompileGas) live() bool { return g.remaining != nil }

// Remaining reports the gas left, or zero once the frame has returned.
func (g *PrecompileGas) Remaining() mdgas.MdGas {
	if !g.live() {
		return mdgas.MdGas{}
	}
	return *g.remaining
}

// ChargeExecution deducts execution gas, charging nothing when it reports false.
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

// ChargeState deducts state gas, spilling into execution gas when the EIP-8037
// reservoir is short, and charging nothing when it reports false. Before
// Amsterdam it charges execution gas outright: the reservoir is empty, so the
// charge would land in used.State, which pre-Amsterdam accounting drops —
// taking gas off the frame without it reaching the receipt or the block.
func (g *PrecompileGas) ChargeState(amount uint64) bool {
	if !g.live() {
		return false
	}
	if !g.amsterdam {
		return g.ChargeExecution(amount)
	}
	if !g.stateChargeFits(amount) {
		return false
	}
	before, spilledBefore := *g.remaining, g.used.StateSpill
	if !mdgas.Consume(g.remaining, g.used, amount, mdgas.StateGas) {
		return false
	}
	g.onGasChange(before, g.used.StateSpill-spilledBefore, mdgas.StateGas, tracing.GasChangeCallPrecompiledContract)
	return true
}

// RefundExecution gives execution gas back, capped at what this handle charged:
// execution gas can only return from this frame's own charge, and an unbounded
// refill underflows used.Execution and mints gas for the caller.
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

// RefundState reverses a state charge or forwards a nested call's refunded
// reservoir. Deliberately unbounded, unlike RefundExecution: a forwarded refund
// did not originate here, and net state usage may legitimately go negative.
func (g *PrecompileGas) RefundState(amount uint64) bool {
	if !g.live() {
		return false
	}
	if !g.amsterdam {
		// Mirrors ChargeState: pre-Amsterdam the charge went to execution.
		return g.RefundExecution(amount)
	}
	if !g.stateRefundFits(amount) {
		return false
	}
	before, spilledBefore := *g.remaining, g.used.StateSpill
	mdgas.Refill(g.remaining, g.used, amount, mdgas.StateGas)
	g.onGasChange(before, spilledBefore-g.used.StateSpill, mdgas.StateGas, tracing.GasChangeCallLeftOverRefunded)
	return true
}

func (g *PrecompileGas) stateChargeFits(amount uint64) bool {
	if amount > math.MaxInt64 || g.used.State > math.MaxInt64-int64(amount) {
		return false
	}
	spill := amount - min(amount, g.remaining.State)
	return spill <= math.MaxUint64-g.used.StateSpill
}

func (g *PrecompileGas) stateRefundFits(amount uint64) bool {
	if amount > math.MaxInt64 || g.used.State < math.MinInt64+int64(amount) {
		return false
	}
	spill := min(amount, g.used.StateSpill)
	return spill <= math.MaxUint64-g.remaining.Execution &&
		amount-spill <= math.MaxUint64-g.remaining.State
}

// StatefulPrecompile receives the calling frame's context and charges its own
// gas; RequiredGas is not consulted. One instance serves every frame, including
// parallel-executor workers, so it must keep no per-call state on its receiver.
// It must not mutate state when ctx.ReadOnly is set: the state surface reached
// through ctx.EVM has no readOnly awareness, so a missed branch corrupts state
// under STATICCALL instead of failing. Nested calls go through
// PrecompileContext.Call, which carries the EIP-8037 reservoir handoff a bare
// ctx.EVM.Call drops.
type StatefulPrecompile interface {
	PrecompiledContract
	RunStateful(input []byte, gas *PrecompileGas, ctx *PrecompileContext) (ret []byte, err error)
}

// reenter runs one nested frame with the EIP-8037 reservoir handoff: hand the
// whole reservoir down, restore from the child's leftover, adopt its usage.
// MdGas passes by value, so a bare ctx.EVM call handed gas.Remaining() leaves
// the reservoir standing here too and duplicates it once per nesting level.
func (ctx *PrecompileContext) reenter(gas *PrecompileGas, executionGas uint64,
	run func(handed mdgas.MdGas) ([]byte, mdgas.MdGas, mdgas.MdGasUsage, error),
) ([]byte, error) {
	if !gas.live() {
		return nil, ErrOutOfGas
	}
	refundable := gas.chargedExecution
	if !gas.ChargeExecution(executionGas) {
		return nil, ErrOutOfGas
	}
	handed := mdgas.MdGas{Execution: executionGas, State: gas.remaining.State}
	gas.remaining.State = 0

	ret, leftover, usage, err := run(handed)

	if err == nil && !gas.adoptChildUsage(usage) {
		gas.abort(ErrGasUintOverflow)
		return nil, ErrGasUintOverflow
	}

	// The child's revert already restored its entry reservoir into leftover.State.
	gas.remaining.State = leftover.State
	gas.RefundExecution(leftover.Execution)
	gas.chargedExecution = refundable
	if err == nil {
		gas.absorbMisplacedStateGas()
	}
	return ret, err
}

func (g *PrecompileGas) absorbMisplacedStateGas() {
	misplaced := min(g.remaining.State, g.used.StateSpill)
	if misplaced == 0 {
		return
	}
	before := g.remaining.Execution
	g.remaining.State -= misplaced
	g.remaining.Execution += misplaced
	g.used.StateSpill -= misplaced
	if g.tracer != nil && g.tracer.OnGasChange != nil {
		g.tracer.OnGasChange(before, g.remaining.Execution, tracing.GasChangeCallStateGasReturned)
	}
}

func (g *PrecompileGas) adoptChildUsage(usage mdgas.MdGasUsage) bool {
	state := g.used.State + usage.State
	if (usage.State > 0 && state < g.used.State) || (usage.State < 0 && state > g.used.State) {
		return false
	}
	if usage.StateSpill > math.MaxUint64-g.used.StateSpill {
		return false
	}
	g.used.State = state
	g.used.StateSpill += usage.StateSpill
	return true
}

func (ctx *PrecompileContext) Call(gas *PrecompileGas, addr accounts.Address, input []byte, executionGas uint64, value uint256.Int) ([]byte, error) {
	return ctx.reenter(gas, executionGas, func(handed mdgas.MdGas) ([]byte, mdgas.MdGas, mdgas.MdGasUsage, error) {
		return ctx.EVM.Call(ctx.ActingAs, addr, input, handed, value, false)
	})
}

func (ctx *PrecompileContext) StaticCall(gas *PrecompileGas, addr accounts.Address, input []byte, executionGas uint64) ([]byte, error) {
	return ctx.reenter(gas, executionGas, func(handed mdgas.MdGas) ([]byte, mdgas.MdGas, mdgas.MdGasUsage, error) {
		return ctx.EVM.StaticCall(ctx.ActingAs, addr, input, handed)
	})
}

// DelegateCall runs a nested DELEGATECALL, which keeps this frame's identity,
// caller and value — hence no value parameter.
func (ctx *PrecompileContext) DelegateCall(gas *PrecompileGas, addr accounts.Address, input []byte, executionGas uint64) ([]byte, error) {
	return ctx.reenter(gas, executionGas, func(handed mdgas.MdGas) ([]byte, mdgas.MdGas, mdgas.MdGasUsage, error) {
		return ctx.EVM.DelegateCall(ctx.ActingAs, ctx.Caller, addr, input, ctx.Value, handed)
	})
}

// Create runs a nested CREATE, or CREATE2 when salt is non-nil.
func (ctx *PrecompileContext) Create(gas *PrecompileGas, code []byte, executionGas uint64, endowment uint256.Int, salt *uint256.Int) (ret []byte, created accounts.Address, err error) {
	ret, err = ctx.reenter(gas, executionGas, func(handed mdgas.MdGas) ([]byte, mdgas.MdGas, mdgas.MdGasUsage, error) {
		out, addr, leftover, usage, cerr := ctx.EVM.Create(ctx.ActingAs, code, handed, endowment, salt, false)
		created = addr
		return out, leftover, usage, cerr
	})
	return ret, created, err
}

// NoStatelessRun supplies the stateless half of PrecompiledContract: it is
// reached only by a misroute, so Run errors rather than returning empty success.
type NoStatelessRun struct{}

func (NoStatelessRun) RequiredGas([]byte) uint64 { return 0 }

func (NoStatelessRun) Run([]byte) ([]byte, error) {
	return nil, errors.New("vm: stateful precompile reached the stateless Run path")
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
