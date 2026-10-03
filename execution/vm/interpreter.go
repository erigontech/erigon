// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"slices"
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Config are the configuration options for the Interpreter
type Config struct {
	Tracer        *tracing.Hooks
	NoRecursion   bool // Disables call, callcode, delegate call and create
	NoBaseFee     bool // Skips the EIP-1559 and EIP-4844 fee cap checks (needed for 0 price calls)
	NoReceipts    bool // Do not calculate receipts
	ReadOnly      bool // Do no perform any block finalisation
	StatelessExec bool // true is certain conditions (like state trie root hash matching) need to be relaxed for stateless EVM execution
	RestoreState  bool // Revert all changes made to the state (useful for constant system calls)

	ExtraEips []int // Additional EIPS that are to be enabled
}

func (vmConfig *Config) HasEip3860(rules *chain.Rules) bool {
	return slices.Contains(vmConfig.ExtraEips, 3860) || rules.IsShanghai
}

// CallContext contains the things that are per-call, such as stack and memory,
// but not transients like pc and gas
type CallContext struct {
	gas               uint64
	stateGas          uint64
	stateGasSpill     uint64
	newAccountCharged bool
	input             []byte
	Memory            Memory

	// Opcode-scoped key/address intern cache. cacheGen is incremented once per
	// opcode dispatch in the interpreter loop; cachedKeyGen/cachedAddrGen hold
	// the generation at which the entry was populated. An entry is valid only
	// when its gen equals cacheGen, giving the gas phase and execute phase of
	// the same opcode a shared interned value without a second unique.Make call.
	// Placed before Stack so these fields stay in L1D rather than being pushed
	// out by Stack.data (32 KB).
	cacheGen      uint64
	cachedKeyGen  uint64
	cachedAddrGen uint64
	cachedKey     accounts.StorageKey
	cachedAddr    accounts.Address

	// Contract carries pointers, so it must precede the pointer-free Stack:
	// the GC scans a struct only up to its last pointer word (PtrBytes), and
	// Stack.data is 32 KB it can skip entirely.
	Contract Contract
	create   createGasPreparation
	Stack    Stack
}

// peekStorageKey returns the top-of-stack value as an interned StorageKey.
// The result is cached for the lifetime of one opcode dispatch (gas phase +
// execute phase share the same cacheGen), so the key is resolved at most
// once per opcode. Callers must invoke this before any stack mutation
// (pop/push/swap) within the same dispatch — the cache is keyed by generation
// only and will not detect a changed stack top within the same opcode.
func (ctx *CallContext) peekStorageKey(evm *EVM) accounts.StorageKey {
	if ctx.cachedKeyGen == ctx.cacheGen {
		return ctx.cachedKey
	}
	return ctx.memoStorageKey(evm)
}

// memoStorageKey is outlined from peekStorageKey, and memoAddress from
// peekAddress, to keep the two peek functions inside the inlining budget.
// Folding either back into its caller costs about 10% on the call benchmarks.
func (ctx *CallContext) memoStorageKey(evm *EVM) accounts.StorageKey {
	ctx.cachedKey = evm.internStorageKey(ctx.Stack.peek())
	ctx.cachedKeyGen = ctx.cacheGen
	return ctx.cachedKey
}

// peekAddress returns the top-of-stack value as an interned Address.
// Cached like peekStorageKey; same constraint: call before any stack mutation.
func (ctx *CallContext) peekAddress(evm *EVM) accounts.Address {
	if ctx.cachedAddrGen == ctx.cacheGen {
		return ctx.cachedAddr
	}
	return ctx.memoAddress(evm)
}

func (ctx *CallContext) memoAddress(evm *EVM) accounts.Address {
	ctx.cachedAddr = evm.internAddress(ctx.Stack.peek())
	ctx.cachedAddrGen = ctx.cacheGen
	return ctx.cachedAddr
}

var contextPool = sync.Pool{
	New: func() any {
		return &CallContext{}
	},
}

func getCallContext(contract Contract, input []byte, gas mdgas.MdGas) *CallContext {
	ctx, ok := contextPool.Get().(*CallContext)
	if !ok {
		log.Error("Type assertion failure", "err", "cannot get CallContext from contextPool")
	}

	ctx.gas = gas.Execution
	ctx.stateGas = gas.State
	ctx.stateGasSpill = 0
	ctx.newAccountCharged = false
	ctx.input = input
	ctx.Contract = contract
	return ctx
}

func (ctx *CallContext) put() {
	ctx.Memory.reset()
	ctx.Stack.Reset()
	ctx.cacheGen = 0
	ctx.stateGasSpill = 0
	ctx.newAccountCharged = false
	ctx.create = createGasPreparation{}
	// Use sentinel values so that a peek call before the first cacheGen++ is
	// always a miss rather than returning a stale handle from a prior use.
	ctx.cachedKeyGen = ^uint64(0)
	ctx.cachedAddrGen = ^uint64(0)
	// Zero the handles to release their canonMap pins while the context is
	// idle in the pool; unique.Handle values keep interned entries alive.
	ctx.cachedKey = accounts.NilKey
	ctx.cachedAddr = accounts.NilAddress
	ctx.input = nil
	ctx.Contract = Contract{}
	contextPool.Put(ctx)
}

func (ctx *CallContext) useMdGas(gas uint64, t mdgas.MdGasType, tracer *tracing.Hooks, reason tracing.GasChangeReason) (ok bool) {
	remaining, stateSpill, ok := useMdGas(ctx.Gas(), gas, t, tracer, reason)
	if ok {
		ctx.gas = remaining.Execution
		ctx.stateGas = remaining.State
		ctx.stateGasSpill += stateSpill
	}
	return ok
}

// mergeChildStateGas takes over the child's state-gas spill, then absorbs state
// gas the child left in the reservoir, up to the spill available in this frame.
func (ctx *CallContext) mergeChildStateGas(childSpill uint64, tracer *tracing.Hooks) {
	ctx.stateGasSpill += childSpill
	misplaced := min(ctx.stateGas, ctx.stateGasSpill)
	if misplaced == 0 {
		return
	}
	gasTracing := tracer.HasGasChangeHook()
	var old mdgas.MdGas
	if gasTracing {
		old = ctx.Gas()
	}
	ctx.stateGas -= misplaced
	ctx.refillStateGas(misplaced, nil, tracing.GasChangeIgnored) // capped by the spill, so LIFO returns all to gas_left
	if gasTracing {
		tracer.EmitGasChange(old, ctx.Gas(), tracing.GasChangeCallStateGasReturned)
	}
}

func (ctx *CallContext) refillStateGas(amount uint64, tracer *tracing.Hooks, reason tracing.GasChangeReason) {
	remaining := ctx.Gas()
	gasTracing := reason != tracing.GasChangeIgnored && tracer.HasGasChangeHook()
	var old mdgas.MdGas
	if gasTracing {
		old = remaining
	}
	used := mdgas.MdGasUsage{State: int64(amount), StateSpill: ctx.stateGasSpill}
	mdgas.Refill(&remaining, &used, amount, mdgas.StateGas)
	ctx.gas = remaining.Execution
	ctx.stateGas = remaining.State
	ctx.stateGasSpill = used.StateSpill
	if gasTracing {
		tracer.EmitGasChange(old, remaining, reason)
	}
}

func useMdGas(initial mdgas.MdGas, gas uint64, t mdgas.MdGasType, tracer *tracing.Hooks, reason tracing.GasChangeReason) (mdgas.MdGas, uint64, bool) {
	remaining := initial
	var used mdgas.MdGasUsage
	if !mdgas.Consume(&remaining, &used, gas, t) {
		return initial, 0, false
	}
	if reason != tracing.GasChangeIgnored && tracer.HasGasChangeHook() {
		tracer.EmitGasChange(initial, remaining, reason)
	}
	return remaining, used.StateSpill, true
}

// MemoryData returns the underlying memory slice. Callers must not modify the contents
// of the returned data.
func (ctx *CallContext) MemoryData() []byte {
	return ctx.Memory.Data()
}

// StackData returns the stack data. Callers must not modify the contents
// of the returned data.
func (ctx *CallContext) StackData() []uint256.Int {
	return ctx.Stack.data[:ctx.Stack.top]
}

// Caller returns the current caller.
func (ctx *CallContext) Caller() accounts.Address {
	return ctx.Contract.Caller()
}

// Address returns the address where this scope of execution is taking place.
func (ctx *CallContext) Address() accounts.Address {
	return ctx.Contract.Address()
}

// CallValue returns the value supplied with this call.
func (ctx *CallContext) CallValue() uint256.Int {
	return ctx.Contract.Value()
}

// CallInput returns the input/calldata with this call. Callers must not modify
// the contents of the returned data.
func (ctx *CallContext) CallInput() []byte {
	return ctx.input
}

func (ctx *CallContext) Code() []byte {
	return ctx.Contract.Code
}

func (ctx *CallContext) CodeHash() accounts.CodeHash {
	return ctx.Contract.CodeHash
}

func (ctx *CallContext) Gas() mdgas.MdGas {
	return mdgas.MdGas{
		Execution: ctx.gas,
		State:     ctx.stateGas,
	}
}

// restoreChildGas returns the child frame's leftover gas to the parent.
// On success the parent adopts the child's remaining reservoir.
// On error handleFrameRevert adds childStateConsumed back to returnGas.State
// per EIP-8037: "all state gas consumed by the child… is restored to the
// parent's reservoir." Early-exit errors (collision, depth, insufficient
// balance) preserve gasRemaining.State so the reservoir is returned intact.
func (ctx *CallContext) restoreChildGas(returnGas mdgas.MdGas, tracer *tracing.Hooks) {
	if returnGas.Execution == 0 && returnGas.State == ctx.stateGas {
		return
	}
	gasTracing := tracer.HasGasChangeHook()
	var old mdgas.MdGas
	if gasTracing {
		old = ctx.Gas()
	}
	ctx.stateGas = returnGas.State
	ctx.gas += returnGas.Execution
	if gasTracing {
		tracer.EmitGasChange(old, ctx.Gas(), tracing.GasChangeCallLeftOverRefunded)
	}
}

func (ctx *CallContext) forwardStateGas(tracer *tracing.Hooks) {
	if ctx.stateGas == 0 {
		return
	}
	if !tracer.HasGasChangeHook() {
		ctx.stateGas = 0
		return
	}
	old := ctx.Gas()
	ctx.stateGas = 0
	tracer.EmitGasChange(old, ctx.Gas(), tracing.GasChangeCallGasForwarded)
}

// callGas builds the MdGas to pass to a child CALL frame from the
// pre-computed callGasTemp (63/64 rule) and the current state reservoir.
func (ctx *CallContext) callGas(evm *EVM) mdgas.MdGas {
	return mdgas.MdGas{
		Execution: evm.CallGasTemp(),
		State:     ctx.stateGas,
	}
}

func copyJumpTable(jt *JumpTable) *JumpTable {
	copy := *jt
	return &copy
}

// LookupInstructionSet returns a copy of the jump table active under rules.
func LookupInstructionSet(rules *chain.Rules) JumpTable {
	return *jumpTable(rules, Config{})
}

func jumpTable(chainRules *chain.Rules, cfg Config) *JumpTable {
	var jt *JumpTable
	switch {
	case chainRules.IsAmsterdam:
		jt = &amsterdamInstructionSet
	case chainRules.IsOsaka:
		jt = &osakaInstructionSet
	case chainRules.IsPrague:
		jt = &pragueInstructionSet
	case chainRules.IsCancun:
		jt = &cancunInstructionSet
	case chainRules.IsShanghai:
		jt = &shanghaiInstructionSet
	case chainRules.IsLondon:
		jt = &londonInstructionSet
	case chainRules.IsBerlin:
		jt = &berlinInstructionSet
	case chainRules.IsIstanbul:
		jt = &istanbulInstructionSet
	case chainRules.IsConstantinople:
		jt = &constantinopleInstructionSet
	case chainRules.IsByzantium:
		jt = &byzantiumInstructionSet
	case chainRules.IsSpuriousDragon:
		jt = &spuriousDragonInstructionSet
	case chainRules.IsTangerineWhistle:
		jt = &tangerineWhistleInstructionSet
	case chainRules.IsHomestead:
		jt = &homesteadInstructionSet
	default:
		jt = &frontierInstructionSet
	}
	if len(cfg.ExtraEips) > 0 {
		jt = copyJumpTable(jt)
		for i, eip := range cfg.ExtraEips {
			if err := EnableEIP(eip, jt); err != nil {
				// Disable it, so caller can check if it's activated or not
				cfg.ExtraEips = append(cfg.ExtraEips[:i], cfg.ExtraEips[i+1:]...)
				log.Error("EIP activation failed", "eip", eip, "err", err)
			}
		}
	}

	return jt
}

// stackBoundsErr reconstructs which bound the failed range check violated.
func stackBoundsErr(sLen int, operation *operation) error {
	if sLen < operation.numPop {
		return &ErrStackUnderflow{stackLen: sLen, required: operation.numPop}
	}
	return &ErrStackOverflow{stackLen: sLen, limit: operation.maxStack}
}

// traceGas picks the figure the dev instruction trace should report: call
// opcodes forward gas to the callee, so their charged cost is not the
// interesting number.
func traceGas(op OpCode, callGas mdgas.MdGasCost, cost mdgas.MdGasCost) mdgas.MdGasCost {
	switch op {
	case CALL, CALLCODE, DELEGATECALL, STATICCALL:
		return callGas
	}
	return cost
}

// Run loops and evaluates the contract's code with the given input data and returns
// the return byte-slice and an error if one occurred.
//
// It's important to note that any errors returned by the interpreter should be
// considered a revert-and-consume-all-gas operation except for
// ErrExecutionReverted which means revert-and-keep-gas-left.
func (evm *EVM) Run(contract Contract, gas mdgas.MdGas, input []byte, readOnly bool) (ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	tracer := evm.config.Tracer
	debug := tracer != nil && (tracer.HasOpcodeHook() || tracer.HasGasChangeHook() || tracer.HasFaultHook())
	trace := dbg.TraceInstructions && evm.intraBlockState.Trace()
	if debug || trace || dbg.TraceDynamicGas {
		return evm.runTraced(contract, gas, input, readOnly, debug, trace)
	}
	return evm.run(contract, gas, input, readOnly, false, false)
}

// runTracing is false in run and true in runTraced.
const runTracing = false
