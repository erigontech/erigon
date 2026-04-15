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
	gas      uint64
	stateGas uint64
	input    []byte
	Memory   Memory
	Stack    Stack
	Contract Contract
}

var contextPool = sync.Pool{
	New: func() any {
		return &CallContext{}
	},
}

func getCallContext(contract Contract, input []byte, gas mdgas.MdGas) *CallContext {
	ctx, ok := contextPool.Get().(*CallContext)
	if !ok {
		log.Error("Type assertion failure", "err", "cannot get Stack pointer from stackPool")
	}

	ctx.gas = gas.Regular
	ctx.stateGas = gas.State
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

func (c *CallContext) useMdGas(evm *EVM, gas uint64, t mdgas.MdGasType, tracer *tracing.Hooks, reason tracing.GasChangeReason) (ok bool) {
	remaining, ok := useMdGas(evm, c.Gas(), gas, t, tracer, reason)
	if ok {
		c.gas = remaining.Regular
		c.stateGas = remaining.State
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

func useMdGas(evm *EVM, initial mdgas.MdGas, gas uint64, t mdgas.MdGasType, tracer *tracing.Hooks, reason tracing.GasChangeReason) (mdgas.MdGas, bool) {
	var ok bool
	switch t {
	case mdgas.StateGas:
		originalGas := gas
		initial.State, ok = useGas(initial.State, gas, tracer, reason)
		if ok {
			if evm != nil {
				evm.stateGasConsumed += originalGas
			}
			return initial, true
		}
		// otherwise use up all remaining state gas and try to use some from the regular gas
		gas = gas - initial.State
		initial.State = 0
		initial.Regular, ok = useGas(initial.Regular, gas, tracer, reason)
		if ok && evm != nil {
			evm.stateGasConsumed += originalGas
		}
		return initial, ok
	case mdgas.RegularGas:
		initial.Regular, ok = useGas(initial.Regular, gas, tracer, reason)
		if ok && evm != nil {
			evm.regularGasConsumed += gas
		}
		return initial, ok
	default:
		panic(fmt.Errorf("useMdGas: invalid gas type: %d", t))
	}
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
		Regular: ctx.gas,
		State:   ctx.stateGas,
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
	ctx.refundGas(returnGas.Regular, tracer, tracing.GasChangeCallLeftOverRefunded)
}

// callGas builds the MdGas to pass to a child CALL frame from the
// pre-computed callGasTemp (63/64 rule) and the current state reservoir.
func (ctx *CallContext) callGas(evm *EVM) mdgas.MdGas {
	return mdgas.MdGas{
		Regular: evm.CallGasTemp(),
		State:   ctx.stateGas,
	}
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

func jumpTable(chainRules *chain.Rules, cfg Config) *JumpTable {
	var jt *JumpTable
	switch {
	case chainRules.IsAmsterdam:
		jt = &amsterdamInstructionSet
	case chainRules.IsOsaka:
		jt = &osakaInstructionSet
	case chainRules.IsBhilai:
		jt = &bhilaiInstructionSet
	case chainRules.IsPrague:
		jt = &pragueInstructionSet
	case chainRules.IsCancun:
		jt = &cancunInstructionSet
	case chainRules.IsNapoli:
		jt = &napoliInstructionSet
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

// Run loops and evaluates the contract's code with the given input data and returns
// the return byte-slice and an error if one occurred.
//
// It's important to note that any errors returned by the interpreter should be
// considered a revert-and-consume-all-gas operation except for
// ErrExecutionReverted which means revert-and-keep-gas-left.
func (evm *EVM) Run(contract Contract, gas mdgas.MdGas, input []byte, readOnly bool) (_ []byte, _ mdgas.MdGas, err error) {
	// Don't bother with the execution if there's no code.
	if len(contract.Code) == 0 {
		return nil, gas, nil
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
		pcCopy                 uint64 // needed for the deferred Tracer
		gasCopy                uint64 // for Tracer to log gas remaining before execution
		callGas                uint64
		logged                 bool   // deferred Tracer should ignore already logged steps
		res                    []byte // result of the opcode execution function
		tracer                 = evm.config.Tracer
		debug                  = tracer != nil && (tracer.OnOpcode != nil || tracer.OnGasChange != nil || tracer.OnFault != nil)
		trace                  = dbg.TraceInstructions && evm.intraBlockState.Trace()
		blockNum               uint64
		txIndex, txIncarnation int
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
		// first: capture data/memory/state/depth/etc... then clenup them
		if debug && err != nil {
			if !logged && tracer.OnOpcode != nil {
				tracer.OnOpcode(pcCopy, byte(op), gasCopy, cost, callContext, evm.returnData, evm.depth, VMErrorFromErr(err))
			}
			if logged && tracer.OnFault != nil {
				tracer.OnFault(pcCopy, byte(op), gasCopy, cost, callContext, evm.depth, VMErrorFromErr(err))
			}
		}
		// this function must execute _after_: the `CaptureState` needs the stacks before
		callContext.put()
		if restoreReadonly {
			evm.readOnly = false
		}
		evm.depth--
	}()

	// The Interpreter main run loop (contextual). This loop runs until either an
	// explicit STOP, RETURN or SELFDESTRUCT is executed, an error occurred during
	// the execution of one of the operations or until the done flag is set by the
	// parent context.

	var traceGas = func(op OpCode, callGas, cost uint64) uint64 {
		switch op {
		case CALL, CALLCODE, DELEGATECALL, STATICCALL:
			return callGas
		default:
			return cost
		}
	}

	// Hoist to locals so the compiler sees them as loop-invariant.
	isAmsterdam := evm.chainRules.IsAmsterdam
	anyTrace := dbg.TraceDynamicGas || debug || trace

	for {
		if anyTrace {
			// Capture pre-execution values for tracing.
			logged, pcCopy, gasCopy = false, pc, callContext.gas
			blockNum, txIndex, txIncarnation = evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation()
		}
		// Get the operation from the jump table and validate the stack to ensure there are
		// enough stack items available to perform the operation.
		op = contract.GetOp(pc)
		operation := evm.jt[op]
		cost = operation.constantGas // For tracing
		// Validate stack
		if sLen := callContext.Stack.len(); sLen < operation.numPop {
			return nil, callContext.Gas(), &ErrStackUnderflow{stackLen: sLen, required: operation.numPop}
		} else if sLen > operation.maxStack {
			return nil, callContext.Gas(), &ErrStackOverflow{stackLen: sLen, limit: operation.maxStack}
		}
		// for tracing: this gas consumption event is emitted below in the debug section.
		if callContext.gas < cost {
			return nil, callContext.Gas(), ErrOutOfGas
		} else {
			callContext.gas -= cost
		}
		// EIP-8037: Track constantGas immediately after deduction for block-level accounting.
		if isAmsterdam && cost > 0 {
			evm.regularGasConsumed += cost
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
					return nil, callContext.Gas(), ErrGasUintOverflow
				}
				// memory is expanded in words of 32 bytes. Gas
				// is also calculated in words.
				if memorySize, overflow = math.SafeMul(ToWordSize(memSize), 32); overflow {
					return nil, callContext.Gas(), ErrGasUintOverflow
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
					err = fmt.Errorf("%w: %v", ErrOutOfGas, err)
				}
				return nil, callContext.Gas(), err
			}
			cost += dynamicCost.Regular // for tracing
			callGas = operation.constantGas + dynamicCost.Regular - evm.CallGasTemp()
			if dbg.TraceDynamicGas && dynamicCost.Regular > 0 {
				fmt.Printf("%d (%d.%d) Dynamic Gas: %d (%s)\n", blockNum, txIndex, txIncarnation, traceGas(op, callGas, cost), op)
			}
			// EIP-8037: "Regular gas charge MUST be applied first. If the regular
			// gas charge triggers an out-of-gas error, the state gas charge is
			// not applied." Deduct regular gas before state gas so that any
			// state-to-regular spill operates on the already-reduced balance.
			if callContext.gas < dynamicCost.Regular {
				return nil, callContext.Gas(), ErrOutOfGas
			}
			callContext.gas -= dynamicCost.Regular
			if isAmsterdam {
				// EIP-8037: Track dynamic regular gas immediately after deduction.
				// For CALL variants, callGasTemp is the gas forwarded to child (escrow),
				// so we subtract it to get parent's actual cost.
				evm.regularGasConsumed += dynamicCost.Regular - evm.CallGasTemp()
			}
			if dynamicCost.State > 0 {
				// Note: do NOT add dynamicCost.State to `cost` here.
				// `cost` is only used for tracing and is compared against `gasCopy`
				// which captures only regular gas. Adding state gas would cause
				// uint64 underflow in the OnGasChange(gasCopy, gasCopy-cost, ...) call below.
				// State gas is charged separately via useMdGas.
				ok := callContext.useMdGas(evm, dynamicCost.State, mdgas.StateGas, nil, tracing.GasChangeIgnored)
				if !ok {
					return nil, callContext.Gas(), ErrOutOfGas
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

			fmt.Printf("%d (%d.%d) %5d %5d %s\n", blockNum, txIndex, txIncarnation, pc, traceGas(op, callGas, cost), opstr)
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

	return res, callContext.Gas(), err
}
