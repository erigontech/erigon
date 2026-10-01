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

	// pending is the child call a call opcode staged for Run's frame loop,
	// standing in for the recursive evm.Call it used to make.
	pending pendingCall

	Stack Stack
}

type pendingCall struct {
	input              []byte
	caller             accounts.Address
	callerAddr         accounts.Address
	addr               accounts.Address
	value              uint256.Int
	gas                mdgas.MdGas
	retOffset, retSize uint64
	typ                OpCode
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
	ctx.pending = pendingCall{}
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

// flatFrame is one EVM call frame on evm.frames. pc and res are the inner
// loop's state, saved whenever the frame yields to a child.
type flatFrame struct {
	ctx             *CallContext
	res             []byte
	pc              uint64
	gasIn           mdgas.MdGas
	info            callFrame
	restoreReadonly bool
	spawned         bool // pushed by a call opcode rather than by Run itself
}

func (evm *EVM) pushFrame(contract Contract, gas mdgas.MdGas, input []byte, readOnly bool, info callFrame, spawned bool) {
	// Reset the previous call's return data. It's unimportant to preserve the old buffer
	// as every returning call will return new data anyway.
	evm.returnData = nil
	// Make sure the readOnly is only set if we aren't in readOnly yet.
	// This makes also sure that the readOnly flag isn't removed for child calls.
	restoreReadonly := readOnly && !evm.readOnly
	if restoreReadonly {
		evm.readOnly = true
	}
	// Increment the call depth which is restricted to 1024
	evm.depth++
	evm.frames = append(evm.frames, flatFrame{
		ctx:             getCallContext(contract, input, gas),
		gasIn:           gas,
		info:            info,
		restoreReadonly: restoreReadonly,
		spawned:         spawned,
	})
}

// popFrame releases the top frame and derives its net state-gas usage.
//
// EIP-8037: a state charge lowers stateGas (or raises stateGasSpill on spill)
// and a refill reverses it, so the net used (signed) is
// (initialReservoir - stateGas) + stateGasSpill. gasUsed.Execution is derived
// later by callEpilogue, which also covers the precompile and no-code paths.
func (evm *EVM) popFrame() (gasUsed mdgas.MdGasUsage, info callFrame, spawned bool) {
	f := &evm.frames[len(evm.frames)-1]
	gasUsed.StateSpill = f.ctx.stateGasSpill
	gasUsed.State = int64(f.gasIn.State) - int64(f.ctx.stateGas) + int64(f.ctx.stateGasSpill)
	info, spawned = f.info, f.spawned
	f.ctx.put()
	if f.restoreReadonly {
		evm.readOnly = false
	}
	evm.depth--
	evm.frames = evm.frames[:len(evm.frames)-1]
	return gasUsed, info, spawned
}

// Run loops and evaluates the contract's code with the given input data and returns
// the return byte-slice and an error if one occurred.
//
// Child calls do not recurse: a call opcode pushes a frame onto evm.frames and
// this loop picks it up, so one Go stack frame serves every EVM call depth.
//
// It's important to note that any errors returned by the interpreter should be
// considered a revert-and-consume-all-gas operation except for
// ErrExecutionReverted which means revert-and-keep-gas-left.
func (evm *EVM) Run(contract Contract, gas mdgas.MdGas, input []byte, readOnly bool) (ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	// Don't bother with the execution if there's no code.
	if len(contract.Code) == 0 {
		return nil, gas, mdgas.MdGasUsage{}, nil
	}

	base := len(evm.frames)
	evm.pushFrame(contract, gas, input, readOnly, callFrame{}, false)
	defer func() {
		// A panic escaping the loop must not leave frames, depth or readOnly
		// behind for the next Run on this EVM.
		for len(evm.frames) > base {
			evm.popFrame()
		}
	}()

	for {
		ret, gasRemaining, err = evm.runFrame(len(evm.frames) - 1)
		if err == errCallFrame { //nolint:errorlint // intentional bare sentinel check
			continue // a child frame was pushed; run it
		}
		gasUsed, info, spawned := evm.popFrame()
		if !spawned {
			return ret, gasRemaining, gasUsed, err
		}
		ret, gasRemaining, gasUsed, err = evm.callEpilogue(&info, ret, gasRemaining, gasUsed, err)
		parent := len(evm.frames) - 1
		evm.frames[parent].res = finishCall(evm, evm.frames[parent].ctx, ret, gasRemaining, gasUsed, err)
	}
}

// beginPendingCall resolves the call a call opcode staged. It either pushes a
// child frame (spawned) or returns the already-finished result.
func (evm *EVM) beginPendingCall(scope *CallContext) (spawned bool, ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	p := &scope.pending
	if evm.abort.Load() {
		return false, nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, nil
	}
	f, ret, gasRemaining, run, err := evm.callPrologue(p.typ, p.caller, p.callerAddr, p.addr, p.input, p.gas, p.value, false)
	if !run {
		ret, gasRemaining, gasUsed, err = evm.callEpilogue(&f, ret, gasRemaining, mdgas.MdGasUsage{}, err)
		return false, ret, gasRemaining, gasUsed, err
	}
	evm.pushFrame(f.contract, gasRemaining, p.input, f.readOnly, f, true)
	return true, nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, nil
}

// runFrame executes opcodes of frame fi until it halts, faults, or yields to a
// child frame — in which case it returns errCallFrame.
func (evm *EVM) runFrame(fi int) (ret []byte, gasRemaining mdgas.MdGas, err error) {
	var (
		op          OpCode // current opcode
		callContext = evm.frames[fi].ctx
		// For optimisation reason we're using uint64 as the program counter.
		// It's theoretically possible to go above 2^64. The YP defines the PC
		// to be uint256. Practically much less so feasible.
		pc   = evm.frames[fi].pc // program counter
		cost mdgas.MdGasCost
		// copies used by tracer
		pcCopy  uint64 // needed for the deferred Tracer
		oldGas  mdgas.MdGas
		callGas mdgas.MdGasCost
		logged  bool                 // deferred Tracer should ignore already logged steps
		res     = evm.frames[fi].res // result of the opcode execution function
		tracer  = evm.config.Tracer
		debug   = tracer != nil && (tracer.HasOpcodeHook() || tracer.HasGasChangeHook() || tracer.HasFaultHook())
		trace   = dbg.TraceInstructions && evm.intraBlockState.Trace()
	)

	// The Interpreter main run loop (contextual). This loop runs until either an
	// explicit STOP, RETURN or SELFDESTRUCT is executed, an error occurred during
	// the execution of one of the operations or until the done flag is set by the
	// parent context.

	// Hoist to locals so the compiler sees them as loop-invariant.
	anyTrace := dbg.TraceDynamicGas || debug || trace
	contract := &callContext.Contract
	stack := &callContext.Stack
	jt := evm.jt

	for {
		callContext.cacheGen++
		if debug {
			// Capture pre-execution values for tracing.
			logged = false
			pcCopy = pc
			oldGas = callContext.Gas()
		}
		// Get the operation from the jump table and validate the stack to ensure there are
		// enough stack items available to perform the operation.
		op = contract.GetOp(pc)
		operation := &jt[op]
		cost = mdgas.MdGasCost{Execution: operation.constantGas} // For tracing
		// Valid iff numPop <= sLen <= maxStack, as one unsigned range check:
		// a stack shallower than numPop wraps negative and fails the compare.
		if sLen := stack.len(); uint(sLen-operation.numPop) > uint(operation.maxStack-operation.numPop) {
			res, err = nil, stackBoundsErr(sLen, operation)
			break
		}
		// for tracing: this gas consumption event is emitted below in the debug section.
		if callContext.gas < cost.Execution {
			res, err = nil, ErrOutOfGas
			break
		} else {
			callContext.gas -= cost.Execution
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
					res, err = nil, ErrGasUintOverflow
					break
				}
				// memory is expanded in words of 32 bytes. Gas
				// is also calculated in words.
				if memorySize, overflow = math.SafeMul(ToWordSize(memSize), 32); overflow {
					res, err = nil, ErrGasUintOverflow
					break
				}
			}
			// Reset callGasTemp so we can detect if dynamicGas sets it (CALL variants)
			evm.callGasTemp = 0
			// Consume the gas and return an error if not enough gas is available.
			// cost is explicitly set so that the capture state defer method can get the proper cost
			var dynamicCost mdgas.MdGasCost
			dynamicCost, err = operation.dynamicGas(evm, callContext, callContext.Gas(), memorySize)
			if err != nil {
				if !errors.Is(err, ErrOutOfGas) {
					err = fmt.Errorf("%w: %w", ErrOutOfGas, err)
				}
				res = nil
				break
			}
			if anyTrace {
				cost = cost.Plus(dynamicCost)
				callGas = cost
				callGas.Execution -= evm.CallGasTemp()
				if dbg.TraceDynamicGas && dynamicCost != (mdgas.MdGasCost{}) {
					gasCost := traceGas(op, callGas, cost)
					fmt.Printf("%d (%d.%d) Dynamic Gas: %d %d (%s)\n", evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(), gasCost.Execution, gasCost.State, op)
				}
			}
			if callContext.gas < dynamicCost.Execution {
				res, err = nil, ErrOutOfGas
				break
			}
			callContext.gas -= dynamicCost.Execution
			if dynamicCost.State > 0 {
				ok := callContext.useMdGas(uint64(dynamicCost.State), mdgas.StateGas, nil, tracing.GasChangeIgnored)
				if !ok {
					res, err = nil, ErrOutOfGas
					break
				}
			} else if dynamicCost.State < 0 {
				callContext.refillStateGas(uint64(-dynamicCost.State), nil, tracing.GasChangeIgnored)
			}
		}

		// Do gas tracing before memory expansion
		if debug {
			if tracer.HasGasChangeHook() {
				tracer.EmitGasChange(oldGas, callContext.Gas(), tracing.GasChangeCallOpCode)
			}
			if tracer.HasOpcodeHook() && tracer.WantsOpcode(byte(op)) {
				tracer.EmitOpcode(pc, byte(op), oldGas, cost, callContext, evm.returnData, evm.depth, VMErrorFromErr(err))
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

			gasCost := traceGas(op, callGas, cost)
			fmt.Printf("%d (%d.%d) %5d %5d %5d %s\n", evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(), pc, gasCost.Execution, gasCost.State, opstr)
		}

		// execute the operation
		pc, res, err = operation.execute(pc, evm, callContext)
		if err == errCallFrame { //nolint:errorlint // intentional bare sentinel check
			// The opcode staged a child call in callContext.pending instead of
			// recursing. Resume at the next instruction once it returns.
			evm.frames[fi].pc, evm.frames[fi].res = pc+1, res
			spawned, cret, cgas, cusage, cerr := evm.beginPendingCall(callContext)
			if spawned {
				return nil, mdgas.MdGas{}, errCallFrame
			}
			res = finishCall(evm, callContext, cret, cgas, cusage, cerr)
			pc, err = evm.frames[fi].pc, nil
			continue
		}
		if err != nil {
			break
		}
		pc++
	}

	if errors.Is(err, errStopToken) {
		err = nil // clear stop token error
	}

	if debug && err != nil {
		switch {
		case !logged && tracer.HasOpcodeHook() && tracer.WantsOpcode(byte(op)):
			tracer.EmitOpcode(pcCopy, byte(op), oldGas, cost, callContext, evm.returnData, evm.depth, VMErrorFromErr(err))
		case tracer.HasOpcodeHook() && tracer.HasFaultHook():
			tracer.EmitFault(pcCopy, byte(op), oldGas, cost, callContext, evm.depth, VMErrorFromErr(err))
		}
	}

	return res, callContext.Gas(), err
}
