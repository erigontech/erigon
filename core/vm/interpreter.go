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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/execution/chain"
)

// Config are the configuration options for the Interpreter
type Config struct {
	Tracer        *tracing.Hooks
	JumpDestCache *JumpDestCache
	NoRecursion   bool // Disables call, callcode, delegate call and create
	NoBaseFee     bool // Forces the EIP-1559 baseFee to 0 (needed for 0 price calls)
	SkipAnalysis  bool // Whether we can skip jumpdest analysis based on the checked history
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
	Run(contract *Contract, input []byte, static bool) ([]byte, error)
	Depth() int // `Depth` returns the current call stack's depth.
	IncDepth()  // Increments the current call stack's depth.
	DecDepth()  // Decrements the current call stack's depth
}

// ScopeContext contains the things that are per-call, such as stack and memory,
// but not transients like pc and gas
type ScopeContext struct {
	Memory   *Memory
	Stack    *Stack
	Contract *Contract
}

// MemoryData returns the underlying memory slice. Callers must not modify the contents
// of the returned data.
func (ctx *ScopeContext) MemoryData() []byte {
	if ctx.Memory == nil {
		return nil
	}
	return ctx.Memory.Data()
}

// StackData returns the stack data. Callers must not modify the contents
// of the returned data.
func (ctx *ScopeContext) StackData() []uint256.Int {
	if ctx.Stack == nil {
		return nil
	}
	return ctx.Stack.data
}

// Caller returns the current caller.
func (ctx *ScopeContext) Caller() common.Address {
	return ctx.Contract.Caller()
}

// Address returns the address where this scope of execution is taking place.
func (ctx *ScopeContext) Address() common.Address {
	return ctx.Contract.Address()
}

// CallValue returns the value supplied with this call.
func (ctx *ScopeContext) CallValue() *uint256.Int {
	return ctx.Contract.Value()
}

// CallInput returns the input/calldata with this call. Callers must not modify
// the contents of the returned data.
func (ctx *ScopeContext) CallInput() []byte {
	return ctx.Contract.Input
}

func (ctx *ScopeContext) Code() []byte {
	return ctx.Contract.Code
}

func (ctx *ScopeContext) CodeHash() common.Hash {
	return ctx.Contract.CodeHash
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
	// Don't bother with the execution if there's no code.
	if len(contract.Code) == 0 {
		return nil, nil
	}

	// Reset the previous call's return data. It's unimportant to preserve the old buffer
	// as every returning call will return new data anyway.
	in.returnData = nil

	var (
		op          OpCode        // current opcode
		mem         = NewMemory() // bound memory
		locStack    = New()
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
		debug   = in.cfg.Tracer != nil && (in.cfg.Tracer.OnOpcode != nil || in.cfg.Tracer.OnGasChange != nil || in.cfg.Tracer.OnFault != nil)
		trace   = dbg.TraceInstructions && in.evm.intraBlockState.Trace()
	)

	contract.Input = input

	// Make sure the readOnly is only set if we aren't in readOnly yet.
	// This makes also sure that the readOnly flag isn't removed for child calls.
	restoreReadonly := readOnly && !in.readOnly
	if restoreReadonly {
		in.readOnly = true
	}
	// Increment the call depth which is restricted to 1024
	in.IncDepth()
	defer func() {
		// first: capture data/memory/state/depth/etc... then clenup them
		if debug && err != nil {
			if !logged && in.cfg.Tracer.OnOpcode != nil {
				in.cfg.Tracer.OnOpcode(pcCopy, byte(op), gasCopy, cost, callContext, in.returnData, in.Depth(), VMErrorFromErr(err))
			}
			if logged && in.cfg.Tracer.OnFault != nil {
				in.cfg.Tracer.OnFault(pcCopy, byte(op), gasCopy, cost, callContext, in.Depth(), VMErrorFromErr(err))
			}
		}
		// this function must execute _after_: the `CaptureState` needs the stacks before
		mem.free()
		ReturnNormalStack(locStack)
		if restoreReadonly {
			in.readOnly = false
		}
		in.DecDepth()
	}()

	// The Interpreter main run loop (contextual). This loop runs until either an
	// explicit STOP, RETURN or SELFDESTRUCT is executed, an error occurred during
	// the execution of one of the operations or until the done flag is set by the
	// parent context.
	steps := 0

	for {
		steps++
		if steps%5000 == 0 && in.evm.Cancelled() {
			break
		}
		if debug {
			// Capture pre-execution values for tracing.
			logged, pcCopy, gasCopy = false, _pc, contract.Gas
		}
		// Get the operation from the jump table and validate the stack to ensure there are
		// enough stack items available to perform the operation.
		op = contract.GetOp(_pc)
		operation := in.jt[op]
		cost = operation.constantGas // For tracing
		// Validate stack
		if sLen := locStack.len(); sLen < operation.numPop {
			return nil, &ErrStackUnderflow{stackLen: sLen, required: operation.numPop}
		} else if sLen > operation.maxStack {
			return nil, &ErrStackOverflow{stackLen: sLen, limit: operation.maxStack}
		}
		if !contract.UseGas(cost, in.cfg.Tracer, tracing.GasChangeIgnored) {
			return nil, ErrOutOfGas
		}

		// All ops with a dynamic memory usage also has a dynamic gas cost.
		var memorySize uint64
		if operation.dynamicGas != nil {
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
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrOutOfGas, err)
			}
			if !contract.UseGas(dynamicCost, in.cfg.Tracer, tracing.GasChangeIgnored) {
				return nil, ErrOutOfGas
			}
		}

		// Do tracing before memory expansion
		if in.cfg.Tracer != nil {
			if in.cfg.Tracer.OnGasChange != nil {
				in.cfg.Tracer.OnGasChange(gasCopy, gasCopy-cost, tracing.GasChangeCallOpCode)
			}
			if in.cfg.Tracer.OnOpcode != nil {
				in.cfg.Tracer.OnOpcode(_pc, byte(op), gasCopy, cost, callContext, in.returnData, in.Depth(), VMErrorFromErr(err))
				logged = true
			}
		}

		// TODO - move this to a trace & set in the worker
		if trace {
			var str string
			if operation.string != nil {
				str = operation.string(*pc, callContext)
			} else {
				str = op.String()
			}

			fmt.Printf("(%d.%d) %5d %5d %s\n", in.evm.intraBlockState.TxIndex(), in.evm.intraBlockState.Incarnation(), _pc, cost, str)
		}

		if memorySize > 0 {
			mem.Resize(memorySize)
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

	ret = res
	return
}

// Depth returns the current call stack depth.
func (in *EVMInterpreter) Depth() int { return in.depth }

// Increments the current call stack's depth.
func (in *EVMInterpreter) IncDepth() { in.depth++ }

// Decrements the current call stack's depth
func (in *EVMInterpreter) DecDepth() { in.depth-- }
