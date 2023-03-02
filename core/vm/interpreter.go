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

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/math"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/vm/stack"
)

// Config are the configuration options for the Interpreter
type Config struct {
	Debug         bool      // Enables debugging
	Tracer        EVMLogger // Opcode logger
	NoRecursion   bool      // Disables call, callcode, delegate call and create
	NoBaseFee     bool      // Forces the EIP-1559 baseFee to 0 (needed for 0 price calls)
	SkipAnalysis  bool      // Whether we can skip jumpdest analysis based on the checked history
	TraceJumpDest bool      // Print transaction hashes where jumpdest analysis was useful
	NoReceipts    bool      // Do not calculate receipts
	ReadOnly      bool      // Do no perform any block finalisation
	StatelessExec bool      // true is certain conditions (like state trie root hash matching) need to be relaxed for stateless EVM execution
	RestoreState  bool      // Revert all changes made to the state (useful for constant system calls)

	ExtraEips []int // Additional EIPS that are to be enabled
}

func (vmConfig *Config) HasEip3860(rules *chain.Rules) bool {
	for _, eip := range vmConfig.ExtraEips {
		if eip == 3860 {
			return true
		}
	}
	return rules.IsShanghai
}

// Interpreter is used to run Ethereum based contracts and will utilise the
// passed environment to query external sources for state information.
// The Interpreter will run the byte code VM based on the passed
// configuration.
type Interpreter interface {
	// Run loops and evaluates the contract's code with the given input data and returns
	// the return byte-slice and an error if one occurred.
	Run(contract *Contract, input []byte, static bool) ([]byte, error)

	// `Depth` returns the current call stack's depth.
	Depth() int
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
	jt    *JumpTable // EVM instruction table
	depth int
}

// structcheck doesn't see embedding
//
//nolint:structcheck
type VM struct {
	evm VMInterpreter
	cfg Config

	hasher    keccakState    // Keccak256 hasher instance shared across opcodes
	hasherBuf libcommon.Hash // Keccak256 hasher result array shared across opcodes

	readOnly   bool   // Whether to throw on stateful modifications
	returnData []byte // Last CALL's return data for subsequent reuse
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

// NewEVMInterpreter returns a new instance of the Interpreter.
func NewEVMInterpreter(evm VMInterpreter, cfg Config) *EVMInterpreter {
	var jt *JumpTable
	switch {
	case evm.ChainRules().IsPrague:
		jt = &pragueInstructionSet
	case evm.ChainRules().IsCancun:
		jt = &cancunInstructionSet
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
			evm: evm,
			cfg: cfg,
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
func (in *EVMInterpreter) Run(contract *Contract, input []byte, readOnly bool) (ret []byte, err error) {
	// Increment the call depth which is restricted to 1024
	in.depth++
	defer func() { in.depth-- }()

	// Make sure the readOnly is only set if we aren't in readOnly yet.
	// This makes also sure that the readOnly flag isn't removed for child calls.
	callback := in.setReadonly(readOnly)
	defer callback()

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
		_pc  = uint64(0) // program counter
		pc   = &_pc      // program counter
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
	defer stack.ReturnNormalStack(locStack)
	contract.Input = input

	if in.cfg.Debug {
		defer func() {
			if err != nil {
				if !logged {
					in.cfg.Tracer.CaptureState(pcCopy, op, gasCopy, cost, callContext, in.returnData, in.depth, err) //nolint:errcheck
				} else {
					in.cfg.Tracer.CaptureFault(pcCopy, op, gasCopy, cost, callContext, in.depth, err)
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
		if steps%1000 == 0 && in.evm.Cancelled() {
			break
		}
		if in.cfg.Debug {
			// Capture pre-execution values for tracing.
			logged, pcCopy, gasCopy = false, _pc, contract.Gas
		}
		// Get the operation from the jump table and validate the stack to ensure there are
		// enough stack items available to perform the operation.
		op = contract.GetOp(_pc)
		operation := in.jt[op]
		cost = operation.constantGas // For tracing
		// Validate stack
		if sLen := locStack.Len(); sLen < operation.numPop {
			return nil, &ErrStackUnderflow{stackLen: sLen, required: operation.numPop}
		} else if sLen > operation.maxStack {
			return nil, &ErrStackOverflow{stackLen: sLen, limit: operation.maxStack}
		}
		if !contract.UseGas(cost) {
			return nil, ErrOutOfGas
		}
		if operation.dynamicGas != nil {
			// All ops with a dynamic memory usage also has a dynamic gas cost.
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
				if memorySize, overflow = math.SafeMul(ToWordSize(memSize), 32); overflow {
					return nil, ErrGasUintOverflow
				}
			}
			// Consume the gas and return an error if not enough gas is available.
			// cost is explicitly set so that the capture state defer method can get the proper cost
			var dynamicCost uint64
			dynamicCost, err = operation.dynamicGas(in.evm, contract, locStack, mem, memorySize)
			cost += dynamicCost // for tracing
			if err != nil || !contract.UseGas(dynamicCost) {
				return nil, ErrOutOfGas
			}
			if memorySize > 0 {
				mem.Resize(memorySize)
			}
		}
		if in.cfg.Debug {
			in.cfg.Tracer.CaptureState(_pc, op, gasCopy, cost, callContext, in.returnData, in.depth, err) //nolint:errcheck
			logged = true
		}
		// execute the operation
		res, err = operation.execute(pc, in, callContext)

		if err != nil {
			break
		}
		_pc++
	}

	if err == errStopToken {
		err = nil // clear stop token error
	}

	return res, err
}

// Depth returns the current call stack depth.
func (in *EVMInterpreter) Depth() int {
	return in.depth
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
