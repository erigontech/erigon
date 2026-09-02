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
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Config are the configuration options for the Interpreter
type Config struct {
	Tracer        *tracing.Hooks
	NoRecursion   bool // Disables call, callcode, delegate call and create
	NoBaseFee     bool // Forces the EIP-1559 baseFee to 0 (needed for 0 price calls)
	TraceJumpDest bool // Print transaction hashes where jumpdest analysis was useful
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

func (c *CallContext) put() {
	c.Memory.reset()
	c.Stack.Reset()
	c.cacheGen = 0
	c.stateGasSpill = 0
	c.newAccountCharged = false
	// Use sentinel values so that a peek call before the first cacheGen++ is
	// always a miss rather than returning a stale handle from a prior use.
	c.cachedKeyGen = ^uint64(0)
	c.cachedAddrGen = ^uint64(0)
	// Zero the handles to release their canonMap pins while the context is
	// idle in the pool; unique.Handle values keep interned entries alive.
	c.cachedKey = accounts.NilKey
	c.cachedAddr = accounts.NilAddress
	c.input = nil
	c.Contract = Contract{}
	contextPool.Put(c)
}

// UseGas attempts the use gas and subtracts it and returns true on success
// We collect the gas change reason today, future changes will add gas change(s) tracking with reason
func (c *CallContext) useGas(gas uint64, tracer *tracing.Hooks, reason tracing.GasChangeReason) (ok bool) {
	if remaining, ok := useGas(c.gas, gas, tracer, reason); ok {
		c.gas = remaining
		return true
	}
	return false
}

func (c *CallContext) useMdGas(gas uint64, t mdgas.MdGasType, tracer *tracing.Hooks, reason tracing.GasChangeReason) (ok bool) {
	remaining, stateSpill, ok := useMdGas(c.Gas(), gas, t, tracer, reason)
	if ok {
		c.gas = remaining.Execution
		c.stateGas = remaining.State
		c.stateGasSpill += stateSpill
	}
	return ok
}

func (c *CallContext) refillStateGas(amount uint64) {
	remaining := c.Gas()
	used := mdgas.MdGasUsage{State: int64(amount), StateSpill: c.stateGasSpill}
	mdgas.Refill(&remaining, &used, amount, mdgas.StateGas)
	c.gas = remaining.Execution
	c.stateGas = remaining.State
	c.stateGasSpill = used.StateSpill
}

func useGas(initial uint64, gas uint64, tracer *tracing.Hooks, reason tracing.GasChangeReason) (remaining uint64, ok bool) {
	if initial < gas {
		return initial, false
	}

	if tracer != nil && tracer.OnGasChange != nil && reason != tracing.GasChangeIgnored {
		tracer.OnGasChange(initial, initial-gas, reason)
	}

	return initial - gas, true
}

func useMdGas(initial mdgas.MdGas, gas uint64, t mdgas.MdGasType, tracer *tracing.Hooks, reason tracing.GasChangeReason) (mdgas.MdGas, uint64, bool) {
	remaining := initial
	var used mdgas.MdGasUsage
	if !mdgas.Consume(&remaining, &used, gas, t) {
		return initial, 0, false
	}
	if tracer != nil && tracer.OnGasChange != nil && reason != tracing.GasChangeIgnored {
		before, after := initial.Execution, remaining.Execution
		if t == mdgas.StateGas && used.StateSpill == 0 {
			before, after = initial.State, remaining.State
		}
		tracer.OnGasChange(before, after, reason)
	}
	return remaining, used.StateSpill, true
}

// RefundGas refunds gas to the contract
func (c *CallContext) refundGas(gas uint64, tracer *tracing.Hooks, reason tracing.GasChangeReason) {
	// We collect the gas change reason today, future changes will add gas change(s) tracking with reason
	_ = reason

	if gas == 0 {
		return
	}
	if tracer != nil && tracer.OnGasChange != nil && reason != tracing.GasChangeIgnored {
		tracer.OnGasChange(c.gas, c.gas+gas, reason)
	}
	c.gas += gas
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
	ctx.stateGas = returnGas.State
	ctx.refundGas(returnGas.Execution, tracer, tracing.GasChangeCallLeftOverRefunded)
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
func traceGas(op OpCode, callGas, cost uint64) uint64 {
	switch op {
	case CALL, CALLCODE, DELEGATECALL, STATICCALL:
		return callGas
	default:
		return cost
	}
}

// Run loops and evaluates the contract's code with the given input data and returns
// the return byte-slice and an error if one occurred.
//
// It's important to note that any errors returned by the interpreter should be
// considered a revert-and-consume-all-gas operation except for
// ErrExecutionReverted which means revert-and-keep-gas-left.
func (evm *EVM) Run(contract Contract, gas mdgas.MdGas, input []byte, readOnly bool) (ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	// Don't bother with the execution if there's no code.
	if len(contract.Code) == 0 {
		return nil, gas, mdgas.MdGasUsage{}, nil
	}

	// Reset the previous call's return data. It's unimportant to preserve the old buffer
	// as every returning call will return new data anyway.
	evm.returnData = nil

	var (
		op          OpCode // current opcode
		callContext = getCallContext(contract, input, gas)
		// For optimisation reason we're using uint64 as the program counter.
		// It's theoretically possible to go above 2^64. The YP defines the PC
		// to be uint256. Practically much less so feasible.
		pc   = uint64(0) // program counter
		cost uint64
		// copies used by tracer
		pcCopy  uint64 // needed for the deferred Tracer
		gasCopy uint64 // for Tracer to log gas remaining before execution
		callGas uint64
		logged  bool   // deferred Tracer should ignore already logged steps
		res     []byte // result of the opcode execution function
		tracer  = evm.config.Tracer
		debug   = tracer != nil && (tracer.OnOpcode != nil || tracer.OnGasChange != nil || tracer.OnFault != nil)
		trace   = dbg.TraceInstructions && evm.intraBlockState.Trace()
	)

	// Make sure the readOnly is only set if we aren't in readOnly yet.
	// This makes also sure that the readOnly flag isn't removed for child calls.
	restoreReadonly := readOnly && !evm.readOnly
	if restoreReadonly {
		evm.readOnly = true
	}
	// Increment the call depth which is restricted to 1024
	evm.depth++
	defer func() {
		// EIP-8037: snapshot the spilled portion and derive the frame's net
		// state-gas usage from the reservoir delta before callContext.put()
		// clears them. A state charge lowers stateGas (or raises stateGasSpill
		// on spill) and a refill reverses it, so the net used (signed) is
		// (initialReservoir - stateGas) + stateGasSpill. gasUsed.Execution is
		// derived uniformly by evm.call/evm.create's defer from the final
		// gasRemaining (covers precompile/no-code paths and the revert burn).
		gasUsed.StateSpill = callContext.stateGasSpill
		gasUsed.State = int64(gas.State) - int64(callContext.stateGas) + int64(callContext.stateGasSpill)
		callContext.put()
		if restoreReadonly {
			evm.readOnly = false
		}
		evm.depth--
	}()

	// Registered after the cleanup defer so LIFO runs it first: the tracer needs
	// the stacks before callContext.put() returns them to the pool.
	if debug {
		defer func() {
			if err == nil {
				return
			}
			if !logged && tracer.OnOpcode != nil {
				tracer.OnOpcode(pcCopy, byte(op), gasCopy, cost, callContext, evm.returnData, evm.depth, VMErrorFromErr(err))
			}
			if logged && tracer.OnFault != nil {
				tracer.OnFault(pcCopy, byte(op), gasCopy, cost, callContext, evm.depth, VMErrorFromErr(err))
			}
		}()
	}

	// The Interpreter main run loop (contextual). This loop runs until either an
	// explicit STOP, RETURN or SELFDESTRUCT is executed, an error occurred during
	// the execution of one of the operations or until the done flag is set by the
	// parent context.

	// Hoist to locals so the compiler sees them as loop-invariant.
	anyTrace := dbg.TraceDynamicGas || debug || trace
	stack := &callContext.Stack
	jt := evm.jt

	for {
		callContext.cacheGen++
		if debug {
			// Capture pre-execution values for tracing.
			logged, pcCopy, gasCopy = false, pc, callContext.gas
		}
		// Get the operation from the jump table and validate the stack to ensure there are
		// enough stack items available to perform the operation.
		op = contract.GetOp(pc)
		operation := &jt[op]
		cost = operation.constantGas // For tracing
		// Valid iff numPop <= sLen <= maxStack, as one unsigned range check:
		// a stack shallower than numPop wraps negative and fails the compare.
		if sLen := stack.len(); uint(sLen-operation.numPop) > uint(operation.maxStack-operation.numPop) {
			return nil, callContext.Gas(), mdgas.MdGasUsage{}, stackBoundsErr(sLen, operation)
		}
		// for tracing: this gas consumption event is emitted below in the debug section.
		if callContext.gas < cost {
			return nil, callContext.Gas(), mdgas.MdGasUsage{}, ErrOutOfGas
		} else {
			callContext.gas -= cost
		}

		// All ops with a dynamic memory usage also has a dynamic gas cost.
		var memorySize uint64
		if operation.dynamicGas != nil {
			// calculate the new memory size and expand the memory to fit
			// the operation
			// Memory check needs to be done prior to evaluating the dynamic gas portion,
			// to detect calculation overflows
			if operation.memorySize != nil {
				memSize, overflow := operation.memorySize(callContext)
				if overflow {
					return nil, callContext.Gas(), mdgas.MdGasUsage{}, ErrGasUintOverflow
				}
				// memory is expanded in words of 32 bytes. Gas
				// is also calculated in words.
				if memorySize, overflow = math.SafeMul(ToWordSize(memSize), 32); overflow {
					return nil, callContext.Gas(), mdgas.MdGasUsage{}, ErrGasUintOverflow
				}
			}
			// Reset callGasTemp so we can detect if dynamicGas sets it (CALL variants)
			evm.callGasTemp = 0
			// Consume the gas and return an error if not enough gas is available.
			// cost is explicitly set so that the capture state defer method can get the proper cost
			var dynamicCost mdgas.MdGas
			dynamicCost, err = operation.dynamicGas(evm, callContext, callContext.Gas(), memorySize)
			if err != nil {
				if !errors.Is(err, ErrOutOfGas) {
					err = fmt.Errorf("%w: %w", ErrOutOfGas, err)
				}
				return nil, callContext.Gas(), mdgas.MdGasUsage{}, err
			}
			if anyTrace {
				cost += dynamicCost.Execution
				callGas = operation.constantGas + dynamicCost.Execution - evm.CallGasTemp()
				if dbg.TraceDynamicGas && dynamicCost.Execution > 0 {
					fmt.Printf("%d (%d.%d) Dynamic Gas: %d (%s)\n", evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(), traceGas(op, callGas, cost), op)
				}
			}
			// EIP-8037: "Execution gas charge MUST be applied first. If the execution
			// gas charge triggers an out-of-gas error, the state gas charge is
			// not applied." Deduct execution gas before state gas so that any
			// state-to-execution spill operates on the already-reduced balance.
			if callContext.gas < dynamicCost.Execution {
				return nil, callContext.Gas(), mdgas.MdGasUsage{}, ErrOutOfGas
			}
			callContext.gas -= dynamicCost.Execution
			if dynamicCost.State > 0 {
				// Note: do NOT add dynamicCost.State to `cost` here.
				// `cost` is only used for tracing and is compared against `gasCopy`
				// which captures only execution gas. Adding state gas would cause
				// uint64 underflow in the OnGasChange(gasCopy, gasCopy-cost, ...) call below.
				// State gas is charged separately via useMdGas.
				ok := callContext.useMdGas(dynamicCost.State, mdgas.StateGas, nil, tracing.GasChangeIgnored)
				if !ok {
					return nil, callContext.Gas(), mdgas.MdGasUsage{}, ErrOutOfGas
				}
			}
		}

		// Do gas tracing before memory expansion
		if debug {
			if tracer.OnGasChange != nil {
				tracer.OnGasChange(gasCopy, gasCopy-cost, tracing.GasChangeCallOpCode)
			}
			if tracer.OnOpcode != nil {
				tracer.OnOpcode(pc, byte(op), gasCopy, cost, callContext, evm.returnData, evm.depth, VMErrorFromErr(err))
				logged = true
			}
		}

		if memorySize > 0 {
			callContext.Memory.Resize(memorySize)
		}

		// TODO - move this to a trace & set in the worker

		if trace {
			var opstr string
			if operation.string != nil {
				opstr = operation.string(pc, callContext)
			} else {
				opstr = op.String()
			}

			fmt.Printf("%d (%d.%d) %5d %5d %s\n", evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(), pc, traceGas(op, callGas, cost), opstr)
		}

		// execute the operation
		pc, res, err = operation.execute(pc, evm, callContext)

		if err != nil {
			break
		}
		pc++
	}

	if errors.Is(err, errStopToken) {
		err = nil // clear stop token error
	}

	return res, callContext.Gas(), mdgas.MdGasUsage{}, err
}
