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
	"fmt"
	"hash"
	"slices"
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Config are the configuration options for the Interpreter
type Config struct {
	Tracer        *tracing.Hooks
	JumpDestCache *JumpDestCache
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

// Interpreter is used to run Ethereum based contracts and will utilise the
// passed environment to query external sources for state information.
// The Interpreter will run the byte code VM based on the passed
// configuration.
type Interpreter interface {
	// Run loops and evaluates the contract's code with the given input data and returns
	// the return byte-slice and an error if one occurred.
	Run(contract Contract, gas uint64, input []byte, static bool) ([]byte, uint64, error)
	Depth() int // `Depth` returns the current call stack's depth.
	IncDepth()  // Increments the current call stack's depth.
	DecDepth()  // Decrements the current call stack's depth
}

// CallContext contains the things that are per-call, such as stack and memory,
// but not transients like pc and gas
type CallContext struct {
	gas      uint64
	input    []byte
	Memory   Memory
	Stack    Stack
	Contract Contract
}

var contextPool = sync.Pool{
	New: func() any {
		return &CallContext{
			Stack: Stack{data: make([]uint256.Int, 0, 16)},
		}
	},
}

func getCallContext(contract Contract, input []byte, gas uint64) *CallContext {
	ctx, ok := contextPool.Get().(*CallContext)
	if !ok {
		log.Error("Type assertion failure", "err", "cannot get Stack pointer from stackPool")
	}

	ctx.gas = gas
	ctx.input = input
	ctx.Contract = contract
	return ctx
}

func (c *CallContext) put() {
	c.Memory.reset()
	c.Stack.Reset()
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

func useGas(initial uint64, gas uint64, tracer *tracing.Hooks, reason tracing.GasChangeReason) (remaining uint64, ok bool) {
	if initial < gas {
		return initial, false
	}

	if tracer != nil && tracer.OnGasChange != nil && reason != tracing.GasChangeIgnored {
		tracer.OnGasChange(initial, initial-gas, reason)
	}

	return initial - gas, true
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
	return ctx.Stack.data
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

func (ctx *CallContext) Gas() uint64 {
	return ctx.gas
}

// keccakState wraps sha3.state. In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

// structcheck doesn't see embedding
//
//nolint:structcheck
type VM struct {
	evm *EVM
	cfg Config

	hasher    keccakState // Keccak256 hasher instance shared across opcodes
	hasherBuf common.Hash // Keccak256 hasher result array shared across opcodes

	readOnly   bool   // Whether to throw on stateful modifications
	returnData []byte // Last CALL's return data for subsequent reuse

	// pendingCall is set by call opcodes to signal a nested call should be made.
	// The main loop reads this to create a new call frame instead of recursing.
	pendingCall *PendingCall

	// callStack is the explicit frame stack for iterative EVM execution.
	// It replaces the implicit Go call stack used in recursive execution.
	callStack *CallStack

	// buffers tracks pooled buffers allocated during tx execution for batch return
	buffers BufferTracker
}

func (vm *VM) setReadonly(outerReadonly bool) func() {
	if outerReadonly && !vm.readOnly {
		vm.readOnly = true
		return func() {
			vm.readOnly = false
		}
	}
	return func() {}
}

func (vm *VM) getReadonly() bool {
	return vm.readOnly
}

func copyJumpTable(jt *JumpTable) *JumpTable {
	var copy JumpTable
	for i, op := range jt {
		if op != nil {
			opCopy := *op
			copy[i] = &opCopy
		}
	}
	return &copy
}

// EVMInterpreter represents an EVM interpreter
type EVMInterpreter struct {
	*VM
	jt    *JumpTable // EVM instruction table
	depth int
}

// NewEVMInterpreter returns a new instance of the Interpreter.
func NewEVMInterpreter(evm *EVM, cfg Config) *EVMInterpreter {
	var jt *JumpTable
	switch {
	case evm.chainRules.IsOsaka:
		jt = &osakaInstructionSet
	case evm.ChainRules().IsBhilai:
		jt = &bhilaiInstructionSet
	case evm.ChainRules().IsPrague:
		jt = &pragueInstructionSet
	case evm.ChainRules().IsCancun:
		jt = &cancunInstructionSet
	case evm.ChainRules().IsNapoli:
		jt = &napoliInstructionSet
	case evm.ChainRules().IsShanghai:
		jt = &shanghaiInstructionSet
	case evm.ChainRules().IsLondon:
		jt = &londonInstructionSet
	case evm.ChainRules().IsBerlin:
		jt = &berlinInstructionSet
	case evm.ChainRules().IsIstanbul:
		jt = &istanbulInstructionSet
	case evm.ChainRules().IsConstantinople:
		jt = &constantinopleInstructionSet
	case evm.ChainRules().IsByzantium:
		jt = &byzantiumInstructionSet
	case evm.ChainRules().IsSpuriousDragon:
		jt = &spuriousDragonInstructionSet
	case evm.ChainRules().IsTangerineWhistle:
		jt = &tangerineWhistleInstructionSet
	case evm.ChainRules().IsHomestead:
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

	return &EVMInterpreter{
		VM: &VM{
			evm:       evm,
			cfg:       cfg,
			callStack: NewCallStack(),
		},
		jt: jt,
	}
}

// Run loops and evaluates the contract's code with the given input data and returns
// the return byte-slice and an error if one occurred.
//
// It's important to note that any errors returned by the interpreter should be
// considered a revert-and-consume-all-gas operation except for
// ErrExecutionReverted which means revert-and-keep-gas-left.
func (in *EVMInterpreter) Run(contract Contract, gas uint64, input []byte, readOnly bool) ([]byte, uint64, error) {
	// Don't bother with the execution if there's no code.
	if len(contract.Code) == 0 {
		return nil, gas, nil
	}

	// Reset the previous call's return data. It's unimportant to preserve the old buffer
	// as every returning call will return new data anyway.
	in.returnData = nil

	// Set up the initial frame
	callContext := getCallContext(contract, input, gas)

	// Make sure the readOnly is only set if we aren't in readOnly yet.
	// This makes also sure that the readOnly flag isn't removed for child calls.
	if readOnly && !in.readOnly {
		in.readOnly = true
	}

	// Create the initial frame
	initialFrame := getFrame()
	initialFrame.callContext = callContext
	initialFrame.contract = contract
	initialFrame.pc = 0
	initialFrame.callType = CALL // Entry point is like a CALL
	initialFrame.readOnly = readOnly
	initialFrame.snapshot = -1 // No snapshot for entry frame (managed by caller)
	initialFrame.isCreate = false

	// Push initial frame to stack
	in.callStack.Push(initialFrame)
	in.depth++

	// Main iterative execution loop
	ret, retGas, err := in.runLoop()

	// Cleanup
	in.callStack.Clear()
	in.depth = 0
	in.readOnly = false
	in.buffers.ReturnAll()

	return ret, retGas, err
}

// runLoop is the iterative execution loop that processes frames from the call stack.
func (in *EVMInterpreter) runLoop() ([]byte, uint64, error) {
	var (
		op      OpCode
		cost    uint64
		pcCopy  uint64
		gasCopy uint64
		callGas uint64
		logged  bool
		res     []byte
		err     error
		debug   = in.cfg.Tracer != nil && (in.cfg.Tracer.OnOpcode != nil || in.cfg.Tracer.OnGasChange != nil || in.cfg.Tracer.OnFault != nil)
		trace   = dbg.TraceInstructions && in.evm.intraBlockState.Trace()

		blockNum               uint64
		txIndex, txIncarnation int
	)

	traceGas := func(op OpCode, callGas, cost uint64) uint64 {
		switch op {
		case CALL, CALLCODE, DELEGATECALL, STATICCALL:
			return callGas
		default:
			return cost
		}
	}

	// Outer loop: iterate over frames in the call stack
	// We only re-fetch frame/callContext/contract when switching frames
frameLoop:
	for !in.callStack.IsEmpty() {
		frame := in.callStack.Peek()
		callContext := frame.callContext
		contract := &frame.contract // Use pointer to avoid struct copy
		pc := frame.pc
		code := contract.Code       // Cache code slice for fast access
		codeLen := uint64(len(code))

		// Inner loop: execute instructions within the same frame
		// This avoids re-fetching frame/callContext/contract on every instruction
		for {
			// Capture state for tracer (only when tracing is enabled)
			if debug || trace {
				logged = false
				pcCopy, gasCopy = pc, callContext.gas
				if trace {
					blockNum, txIndex, txIncarnation = in.evm.intraBlockState.BlockNumber(), in.evm.intraBlockState.TxIndex(), in.evm.intraBlockState.Incarnation()
				}
			}

			// Get the operation - inline GetOp for speed
			if pc < codeLen {
				op = OpCode(code[pc])
			} else {
				op = STOP
			}
			operation := in.jt[op]
			cost = operation.constantGas
			numPop := operation.numPop
			maxStack := operation.maxStack

			// Validate stack
			sLen := callContext.Stack.len()
			if sLen < numPop {
				err = &ErrStackUnderflow{stackLen: sLen, required: numPop}
				goto handleError
			}
			if sLen > maxStack {
				err = &ErrStackOverflow{stackLen: sLen, limit: maxStack}
				goto handleError
			}

			// Fast path for gas check - inline the common case
			if callContext.gas < cost {
				err = ErrOutOfGas
				goto handleError
			}
			callContext.gas -= cost

			// Handle dynamic gas
			if operation.dynamicGas != nil {
				var memorySize uint64
				if operation.memorySize != nil {
					memSize, overflow := operation.memorySize(callContext)
					if overflow {
						err = ErrGasUintOverflow
						goto handleError
					}
					if memorySize, overflow = math.SafeMul(ToWordSize(memSize), 32); overflow {
						err = ErrGasUintOverflow
						goto handleError
					}
				}

				var dynamicCost uint64
				dynamicCost, err = operation.dynamicGas(in.evm, callContext, callContext.gas, memorySize)
				if err != nil {
					err = fmt.Errorf("%w: %v", ErrOutOfGas, err)
					goto handleError
				}
				cost += dynamicCost
				callGas = operation.constantGas + dynamicCost - in.evm.CallGasTemp()

				if dbg.TraceDyanmicGas && dynamicCost > 0 {
					bn, ti, ti2 := in.evm.intraBlockState.BlockNumber(), in.evm.intraBlockState.TxIndex(), in.evm.intraBlockState.Incarnation()
					fmt.Printf("%d (%d.%d) Dynamic Gas: %d (%s)\n", bn, ti, ti2, traceGas(op, callGas, cost), op)
				}

				// Fast path for dynamic gas check - inline the common case
				if callContext.gas < dynamicCost {
					err = ErrOutOfGas
					goto handleError
				}
				callContext.gas -= dynamicCost

				if memorySize > 0 {
					callContext.Memory.Resize(memorySize)
				}
			}

			// Tracer hooks
			if in.cfg.Tracer != nil {
				if in.cfg.Tracer.OnGasChange != nil {
					in.cfg.Tracer.OnGasChange(gasCopy, gasCopy-cost, tracing.GasChangeCallOpCode)
				}
				if in.cfg.Tracer.OnOpcode != nil {
					in.cfg.Tracer.OnOpcode(pc, byte(op), gasCopy, cost, callContext, in.returnData, in.depth, nil)
					logged = true
				}
			}

			if trace {
				var opstr string
				if operation.string != nil {
					opstr = operation.string(pc, callContext)
				} else {
					opstr = op.String()
				}
				fmt.Printf("%d (%d.%d) %5d %5d %s\n", blockNum, txIndex, txIncarnation, pc, traceGas(op, callGas, cost), opstr)
			}

			// Execute the operation
			pc, res, err = operation.execute(pc, in, callContext)

			// Fast path: no error, just continue to next instruction (stay in inner loop)
			if err == nil {
				pc++
				continue
			}

			// Error or frame switch - save PC and break out
			frame.pc = pc
			break

		handleError:
			// Handle pre-execution errors
			if debug {
				if !logged && in.cfg.Tracer.OnOpcode != nil {
					in.cfg.Tracer.OnOpcode(pcCopy, byte(op), gasCopy, cost, callContext, in.returnData, in.depth, VMErrorFromErr(err))
				}
			}
			frame.pc = pc
			break
		}

		// Handle CALL/CALLCODE/DELEGATECALL/STATICCALL suspension
		if err == errSuspendForCall {
			pending := in.pendingCall
			in.pendingCall = nil

			// Save current frame state
			frame.pc = pc
			frame.retOffset = pending.RetOffset
			frame.retSize = pending.RetSize

			// Prepare the child call
			prepared, prepErr := in.evm.PrepareCall(
				pending.CallType,
				pending.Caller,
				pending.CallerAddr,
				pending.Addr,
				pending.Input,
				pending.Gas,
				pending.Value,
				false, // bailout
			)

			if prepErr != nil {
				// Call setup failed - push 0 to stack and refund gas
				callContext.Stack.push(uint256.Int{})
				// Refund the gas that was allocated for this call
				callContext.refundGas(pending.Gas, in.cfg.Tracer, tracing.GasChangeCallLeftOverRefunded)
				// Call tracer OnExit (PrepareCall already called captureBegin)
				if in.cfg.Tracer != nil {
					in.evm.captureEnd(in.depth, pending.CallType, pending.Gas, pending.Gas, nil, prepErr)
				}
				in.returnData = nil
				frame.pc = pc + 1
				putPendingCall(pending)
				continue frameLoop
			}

			if prepared == nil {
				// No code to execute (empty account or NoRecursion)
				// Refund the gas that was allocated for this call
				callContext.Stack.push(*uint256.NewInt(1))
				callContext.refundGas(pending.Gas, in.cfg.Tracer, tracing.GasChangeCallLeftOverRefunded)
				// Call tracer OnExit (PrepareCall already called captureBegin)
				if in.cfg.Tracer != nil {
					in.evm.captureEnd(in.depth, pending.CallType, pending.Gas, pending.Gas, nil, nil)
				}
				in.returnData = nil
				frame.pc = pc + 1
				putPendingCall(pending)
				continue frameLoop
			}

			// Handle precompiles synchronously
			if prepared.IsPrecompile {
				// Note: captureBegin was already called by PrepareCall, so we don't call it again here

				ret, returnGas, callErr := RunPrecompiledContract(prepared.Precompile, prepared.Input, prepared.Gas, in.cfg.Tracer)
				returnGas = in.evm.FinishCall(prepared.Snapshot, returnGas, callErr)

				// Call tracer OnExit for precompile
				if in.cfg.Tracer != nil {
					in.evm.captureEnd(in.depth, pending.CallType, pending.Gas, returnGas, ret, callErr)
				}

				// Push result to parent stack
				var result uint256.Int
				if callErr == nil {
					result.SetOne()
				}
				callContext.Stack.push(result)

				// Copy return data to memory (Memory.Set copies internally)
				if callErr == nil || callErr == ErrExecutionReverted {
					callContext.Memory.Set(pending.RetOffset, pending.RetSize, ret)
				}

				callContext.refundGas(returnGas, in.cfg.Tracer, tracing.GasChangeCallLeftOverRefunded)
				in.returnData = ret
				frame.pc = pc + 1
				putPreparedCall(prepared)
				putPendingCall(pending)
				continue frameLoop
			}

			// Create new frame for the call
			childFrame := getFrame()
			childFrame.callContext = getCallContext(prepared.Contract, prepared.Input, prepared.Gas)
			childFrame.contract = prepared.Contract
			childFrame.pc = 0
			childFrame.callType = pending.CallType
			childFrame.readOnly = prepared.ReadOnly || in.readOnly
			childFrame.snapshot = prepared.Snapshot
			childFrame.isCreate = false
			childFrame.startGas = prepared.Gas
			childFrame.caller = pending.Caller
			childFrame.target = pending.Addr
			childFrame.value = pending.Value

			// Set readOnly mode if needed
			if childFrame.readOnly && !in.readOnly {
				in.readOnly = true
			}

			in.callStack.Push(childFrame)
			in.depth++
			// Reset returnData when entering a new frame (matches recursive behavior where each Run() starts with returnData=nil)
			in.returnData = nil
			putPreparedCall(prepared)
			putPendingCall(pending)
			continue frameLoop
		}

		// Handle CREATE/CREATE2 suspension
		if err == errSuspendForCreate {
			pending := in.pendingCall
			in.pendingCall = nil

			// Save current frame state
			frame.pc = pc

			// Prepare the create
			var salt *uint256.Int
			if pending.IsCreate2 {
				salt = &pending.Salt
			}
			prepared, createAddr, prepErr := in.evm.PrepareCreate(
				pending.Caller,
				pending.Input,
				pending.Gas,
				pending.Value,
				salt,
				pending.IsCreate2,
			)

			if prepErr != nil {
				// Create setup failed - push 0 to stack
				callContext.Stack.push(uint256.Int{})
				// Most errors refund gas, but ErrContractAddressCollision consumes all
				var leftOverGas uint64
				if prepErr != ErrContractAddressCollision {
					// Refund the gas that was allocated for this create
					leftOverGas = pending.Gas
					callContext.refundGas(pending.Gas, in.cfg.Tracer, tracing.GasChangeCallLeftOverRefunded)
				}
				// Call tracer OnExit (PrepareCreate already called captureBegin)
				if in.cfg.Tracer != nil {
					in.evm.captureEnd(in.depth, pending.CallType, pending.Gas, leftOverGas, nil, prepErr)
				}
				in.returnData = nil
				frame.pc = pc + 1
				putPendingCall(pending)
				continue frameLoop
			}

			if prepared == nil {
				// NoRecursion case - push address to stack and refund gas
				if createAddr != (accounts.Address{}) {
					var addrVal = createAddr.Value()
					var addrInt uint256.Int
					addrInt.SetBytes(addrVal[:])
					callContext.Stack.push(addrInt)
				} else {
					callContext.Stack.push(uint256.Int{})
				}
				// Refund the gas that was allocated for this create
				callContext.refundGas(pending.Gas, in.cfg.Tracer, tracing.GasChangeCallLeftOverRefunded)
				// Call tracer OnExit (PrepareCreate already called captureBegin)
				if in.cfg.Tracer != nil {
					in.evm.captureEnd(in.depth, pending.CallType, pending.Gas, pending.Gas, nil, nil)
				}
				in.returnData = nil
				frame.pc = pc + 1
				putPendingCall(pending)
				continue frameLoop
			}

			// Create new frame for the create init code execution
			childFrame := getFrame()
			childFrame.callContext = getCallContext(prepared.Contract, nil, prepared.Gas)
			childFrame.contract = prepared.Contract
			childFrame.pc = 0
			childFrame.callType = pending.CallType
			childFrame.readOnly = false
			childFrame.snapshot = prepared.Snapshot
			childFrame.createAddr = createAddr
			childFrame.isCreate = true
			childFrame.startGas = prepared.Gas
			childFrame.caller = pending.Caller
			childFrame.target = createAddr
			childFrame.value = pending.Value

			// Note: captureBegin was already called by PrepareCreate, so we don't call it again here

			in.callStack.Push(childFrame)
			in.depth++
			// Reset returnData when entering a new frame (matches recursive behavior where each Run() starts with returnData=nil)
			in.returnData = nil
			putPreparedCreate(prepared)
			putPendingCall(pending)
			continue frameLoop
		}

		// Handle normal termination (STOP, RETURN, REVERT, error)
		// Note: At this point err is non-nil (we already handled err==nil fast path above)
		{
			// Call tracer hooks for errors (matches main branch defer behavior)
			if debug && err != errStopToken {
				if !logged && in.cfg.Tracer.OnOpcode != nil {
					in.cfg.Tracer.OnOpcode(pcCopy, byte(op), gasCopy, cost, callContext, in.returnData, in.depth, VMErrorFromErr(err))
				}
				if logged && in.cfg.Tracer.OnFault != nil {
					in.cfg.Tracer.OnFault(pcCopy, byte(op), gasCopy, cost, callContext, in.depth, VMErrorFromErr(err))
				}
			}

			if err == errStopToken {
				err = nil
			}

			// Pop current frame
			completedFrame := in.callStack.Pop()
			in.depth--
			// After decrementing, in.depth is now the PARENT's depth, which is what we
			// should pass to captureEnd (same depth that was passed to captureBegin)

			// If this was the last frame, we're done
			if in.callStack.IsEmpty() {
				retGas := completedFrame.callContext.gas
				completedFrame.callContext.put()
				putFrame(completedFrame)
				return res, retGas, err
			}

			// Return to parent frame
			parentFrame := in.callStack.Peek()
			parentCallContext := parentFrame.callContext

			// Handle CREATE/CREATE2 completion
			if completedFrame.isCreate {
				// Finish the create (code storage, EIP checks, etc.)
				// We need to reconstruct the PreparedCreate info
				prepCreate := &PreparedCreate{
					Snapshot: completedFrame.snapshot,
					Addr:     completedFrame.createAddr,
				}
				finalRet, finalGas, finalErr := in.evm.FinishCreate(prepCreate, res, completedFrame.callContext.gas, err)

				// Call tracer OnExit for CREATE/CREATE2 completion (use parent's depth, same as captureBegin)
				if in.cfg.Tracer != nil {
					in.evm.captureEnd(in.depth, completedFrame.callType, completedFrame.startGas, finalGas, finalRet, finalErr)
				}

				// Push result to parent stack
				var stackResult uint256.Int
				if finalErr != nil {
					// Error - special handling for ErrCodeStoreOutOfGas on Frontier
					if in.evm.ChainRules().IsHomestead || finalErr != ErrCodeStoreOutOfGas {
						stackResult.Clear()
					} else {
						// Frontier: ErrCodeStoreOutOfGas is success
						addrVal := completedFrame.createAddr.Value()
						stackResult.SetBytes(addrVal[:])
					}
				} else {
					addrVal := completedFrame.createAddr.Value()
					stackResult.SetBytes(addrVal[:])
				}
				parentCallContext.Stack.push(stackResult)

				// Refund gas
				parentCallContext.refundGas(finalGas, in.cfg.Tracer, tracing.GasChangeCallLeftOverRefunded)

				// Set return data
				if finalErr == ErrExecutionReverted {
					in.returnData = finalRet
				} else {
					in.returnData = nil
				}
			} else {
				// Handle CALL completion
				returnGas := completedFrame.callContext.gas
				if completedFrame.snapshot >= 0 {
					returnGas = in.evm.FinishCall(completedFrame.snapshot, returnGas, err)
				}

				// Call tracer OnExit for CALL completion (use parent's depth, same as captureBegin)
				if in.cfg.Tracer != nil {
					in.evm.captureEnd(in.depth, completedFrame.callType, completedFrame.startGas, returnGas, res, err)
				}

				// Push success/failure to parent stack
				var result uint256.Int
				if err == nil {
					result.SetOne()
				}
				parentCallContext.Stack.push(result)

				// Copy return data to memory
				if err == nil || err == ErrExecutionReverted {
					ret := common.Copy(res)
					parentCallContext.Memory.Set(parentFrame.retOffset, parentFrame.retSize, ret)
					in.returnData = ret
				} else {
					in.returnData = nil
				}

				// Refund gas
				parentCallContext.refundGas(returnGas, in.cfg.Tracer, tracing.GasChangeCallLeftOverRefunded)
			}

			// Restore readOnly state for parent
			// Note: We always set readOnly to parent's value. The readOnly flag "sticks"
			// when entering a STATICCALL (set to true) but must be restored when returning.
			in.readOnly = parentFrame.readOnly

			// Cleanup completed frame
			completedFrame.callContext.put()
			putFrame(completedFrame)

			// Advance parent PC
			parentFrame.pc++
			continue frameLoop
		}
	}

	// Should not reach here
	return nil, 0, nil
}

// Depth returns the current call stack depth.
func (in *EVMInterpreter) Depth() int { return in.depth }

// Increments the current call stack's depth.
func (in *EVMInterpreter) IncDepth() { in.depth++ }

// Decrements the current call stack's depth
func (in *EVMInterpreter) DecDepth() { in.depth-- }
