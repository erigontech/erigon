// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"hash"
	"sync/atomic"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/vm/stack"
	"github.com/ledgerwatch/log/v3"
)

// Config are the configuration options for the Interpreter
type Config struct {
	Debug         bool   // Enables debugging
	Tracer        Tracer // Opcode logger
	NoRecursion   bool   // Disables call, callcode, delegate call and create
	NoBaseFee     bool   // Forces the EIP-1559 baseFee to 0 (needed for 0 price calls)
	SkipAnalysis  bool   // Whether we can skip jumpdest analysis based on the checked history
	TraceJumpDest bool   // Print transaction hashes where jumpdest analysis was useful
	NoReceipts    bool   // Do not calculate receipts
	ReadOnly      bool   // Do no perform any block finalisation
	EnableTEMV    bool   // true if execution with TEVM enable flag

	ExtraEips []int // Additional EIPS that are to be enabled
}

// Interpreter is used to run Ethereum based contracts and will utilise the
// passed environment to query external sources for state information.
// The Interpreter will run the byte code VM based on the passed
// configuration.
type Interpreter interface {
	// Run loops and evaluates the contract's code with the given input data and returns
	// the return byte-slice and an error if one occurred.
	Run(contract *Contract, input []byte, static bool) ([]byte, error)
}

// ScopeContext contains the things that are per-call, such as stack and memory,
// but not transients like pc and gas
type ScopeContext struct {
	Memory   *Memory
	Stack    *stack.Stack
	Contract *Contract
}

// keccakState wraps sha3.state. In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

// EVMInterpreter represents an EVM interpreter
type EVMInterpreter struct {
	*VM
	jt *JumpTable // EVM instruction table
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
}

// NewEVMInterpreter returns a new instance of the Interpreter.
func NewEVMInterpreter(evm *EVM, cfg Config) *EVMInterpreter {
	var jt *JumpTable
	switch {
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
			evm: evm,
			cfg: cfg,
		},
		jt: jt,
	}
}

func NewEVMInterpreterByVM(vm *VM) *EVMInterpreter {
	var jt *JumpTable
	switch {
	case vm.evm.ChainRules().IsLondon:
		jt = &londonInstructionSet
	case vm.evm.ChainRules().IsBerlin:
		jt = &berlinInstructionSet
	case vm.evm.ChainRules().IsIstanbul:
		jt = &istanbulInstructionSet
	case vm.evm.ChainRules().IsConstantinople:
		jt = &constantinopleInstructionSet
	case vm.evm.ChainRules().IsByzantium:
		jt = &byzantiumInstructionSet
	case vm.evm.ChainRules().IsSpuriousDragon:
		jt = &spuriousDragonInstructionSet
	case vm.evm.ChainRules().IsTangerineWhistle:
		jt = &tangerineWhistleInstructionSet
	case vm.evm.ChainRules().IsHomestead:
		jt = &homesteadInstructionSet
	default:
		jt = &frontierInstructionSet
	}
	if len(vm.cfg.ExtraEips) > 0 {
		for i, eip := range vm.cfg.ExtraEips {
			if err := EnableEIP(eip, jt); err != nil {
				// Disable it, so caller can check if it's activated or not
				vm.cfg.ExtraEips = append(vm.cfg.ExtraEips[:i], vm.cfg.ExtraEips[i+1:]...)
				log.Error("EIP activation failed", "eip", eip, "err", err)
			}
		}
	}

	return &EVMInterpreter{
		VM: vm,
		jt: jt,
	}
}

// Run loops and evaluates the contract's code with the given input data and returns
// the return byte-slice and an error if one occurred.
//
// It's important to note that any errors returned by the interpreter should be
// considered a revert-and-consume-all-gas operation except for
// ErrExecutionReverted which means revert-and-keep-gas-left.
func (in *EVMInterpreter) Run(contract *Contract, input []byte, readOnly bool) (ret []byte, err error) {
	// Increment the call depth which is restricted to 1024
	in.evm.depth++
	defer func() { in.evm.depth-- }()

	// Make sure the readOnly is only set if we aren't in readOnly yet.
	// This makes also sure that the readOnly flag isn't removed for child calls.
	callback := in.setReadonly(readOnly)
	defer func() {
		callback()
	}()

	// Reset the previous call's return data. It's unimportant to preserve the old buffer
	// as every returning call will return new data anyway.
	in.returnData = nil

	// Don't bother with the execution if there's no code.
	if len(contract.Code) == 0 {
		return nil, nil
	}

	var (
		op          OpCode        // current opcode
		mem         = NewMemory() // bound memory
		locStack    = stack.New()
		callContext = &ScopeContext{
			Memory:   mem,
			Stack:    locStack,
			Contract: contract,
		}
		// For optimisation reason we're using uint64 as the program counter.
		// It's theoretically possible to go above 2^64. The YP defines the PC
		// to be uint256. Practically much less so feasible.
		pc   = uint64(0) // program counter
		cost uint64
		// copies used by tracer
		pcCopy  uint64 // needed for the deferred Tracer
		gasCopy uint64 // for Tracer to log gas remaining before execution
		logged  bool   // deferred Tracer should ignore already logged steps
		res     []byte // result of the opcode execution function
	)
	// Don't move this deferrred function, it's placed before the capturestate-deferred method,
	// so that it get's executed _after_: the capturestate needs the stacks before
	// they are returned to the pools
	defer func() {
		stack.ReturnNormalStack(locStack)
	}()
	contract.Input = input

	if in.cfg.Debug {
		defer func() {
			if err != nil {
				if !logged {
					in.cfg.Tracer.CaptureState(in.evm, pcCopy, op, gasCopy, cost, callContext, in.returnData, in.evm.depth, err) //nolint:errcheck
				} else {
					in.cfg.Tracer.CaptureFault(in.evm, pcCopy, op, gasCopy, cost, callContext, in.evm.depth, err)
				}
			}
		}()
	}
	// The Interpreter main run loop (contextual). This loop runs until either an
	// explicit STOP, RETURN or SELFDESTRUCT is executed, an error occurred during
	// the execution of one of the operations or until the done flag is set by the
	// parent context.
	steps := 0
	for {
		steps++
		if steps%1000 == 0 && atomic.LoadInt32(&in.evm.abort) != 0 {
			break
		}
		if in.cfg.Debug {
			// Capture pre-execution values for tracing.
			logged, pcCopy, gasCopy = false, pc, contract.Gas
		}

		// Get the operation from the jump table and validate the stack to ensure there are
		// enough stack items available to perform the operation.
		op = contract.GetOp(pc)
		operation := in.jt[op]

		if operation == nil {
			return nil, &ErrInvalidOpCode{opcode: op}
		}
		// Validate stack
		if sLen := locStack.Len(); sLen < operation.minStack {
			return nil, &ErrStackUnderflow{stackLen: sLen, required: operation.minStack}
		} else if sLen > operation.maxStack {
			return nil, &ErrStackOverflow{stackLen: sLen, limit: operation.maxStack}
		}
		// If the operation is valid, enforce and write restrictions
		if in.readOnly && in.evm.ChainRules().IsByzantium {
			// If the interpreter is operating in readonly mode, make sure no
			// state-modifying operation is performed. The 3rd stack item
			// for a call operation is the value. Transferring value from one
			// account to the others means the state is modified and should also
			// return with an error.
			if operation.writes || (op == CALL && !locStack.Back(2).IsZero()) {
				return nil, ErrWriteProtection
			}
		}
		// Static portion of gas
		cost = operation.constantGas // For tracing
		if !contract.UseGas(operation.constantGas) {
			return nil, ErrOutOfGas
		}

		var memorySize uint64
		// calculate the new memory size and expand the memory to fit
		// the operation
		// Memory check needs to be done prior to evaluating the dynamic gas portion,
		// to detect calculation overflows
		if operation.memorySize != nil {
			memSize, overflow := operation.memorySize(locStack)
			if overflow {
				return nil, ErrGasUintOverflow
			}
			// memory is expanded in words of 32 bytes. Gas
			// is also calculated in words.
			if memorySize, overflow = math.SafeMul(toWordSize(memSize), 32); overflow {
				return nil, ErrGasUintOverflow
			}
		}
		// Dynamic portion of gas
		// consume the gas and return an error if not enough gas is available.
		// cost is explicitly set so that the capture state defer method can get the proper cost
		if operation.dynamicGas != nil {
			var dynamicCost uint64
			dynamicCost, err = operation.dynamicGas(in.evm, contract, locStack, mem, memorySize)
			cost += dynamicCost // total cost, for debug tracing
			if err != nil || !contract.UseGas(dynamicCost) {
				return nil, ErrOutOfGas
			}
		}
		if memorySize > 0 {
			mem.Resize(memorySize)
		}

		if in.cfg.Debug {
			in.cfg.Tracer.CaptureState(in.evm, pc, op, gasCopy, cost, callContext, in.returnData, in.evm.depth, err) //nolint:errcheck
			logged = true
		}

		// execute the operation
		res, err = operation.execute(&pc, in, callContext)
		// if the operation clears the return data (e.g. it has returning data)
		// set the last return to the result of the operation.
		if operation.returns {
			in.returnData = res
		}

		switch {
		case err != nil:
			return nil, err
		case operation.reverts:
			return res, ErrExecutionReverted
		case operation.halts:
			return res, nil
		case !operation.jumps:
			pc++
		}
	}
	return nil, nil
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
