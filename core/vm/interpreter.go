// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License fas published by
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
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/math"
	"github.com/ledgerwatch/turbo-geth/core/vm/stack"
	"github.com/ledgerwatch/turbo-geth/log"
)

// Config are the configuration options for the Interpreter
type Config struct {
	Debug					bool	 // Enables debugging
	Tracer					Tracer	 // Opcode logger
	NoRecursion				bool	 // Disables call, callcode, delegate call and create
	EnablePreimageRecording bool	 // Enables recording of SHA3/keccak preimages
	SkipAnalysis			bool	 // Whether we can skip jumpdest analysis based on the checked history
	TraceJumpDest			bool	 // Print transaction hashes where jumpdest analysis was useful
	NoReceipts				bool	 // Do not calculate receipts
	ReadOnly				bool	 // Do no perform any block finalisation

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
	// CanRun tells if the contract, passed as an argument, can be
	// run by the current interpreter. This is meant so that the
	// caller can do something like:
	//
	// ```golang
	// for _, interpreter := range interpreters {
	//	  if interpreter.CanRun(contract.code) {
	//		 interpreter.Run(contract.code, input)
	//	  }
	// }
	// ```
	CanRun([]byte) bool
}

// callCtx contains the things that are per-call, such as stack and memory,
// but not transients like pc and gas
type callCtx struct {
	memory	    *Memory
	stack		*stack.Stack
	contract	*Contract
	interpreter *EVMInterpreter
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
	evm *EVM
	cfg Config

	jt *JumpTable			// EVM instruction table

	hasher	  keccakState	// Keccak256 hasher instance shared across opcodes
	hasherBuf common.Hash	// Keccak256 hasher result array shared across opcodes

	readOnly   bool			// Whether to throw on stateful modifications
	returnData []byte		// Last CALL's return data for subsequent reuse
}
	
// NewEVMInterpreter returns a new instance of the Interpreter.
func NewEVMInterpreter(evm *EVM, cfg Config) *EVMInterpreter {
	var jt *JumpTable
	switch {
	case evm.chainRules.IsBerlin:
		jt = &berlinInstructionSet
	case evm.chainRules.IsIstanbul:
		jt = &istanbulInstructionSet
	case evm.chainRules.IsConstantinople:
		jt = &constantinopleInstructionSet
	case evm.chainRules.IsByzantium:
		jt = &byzantiumInstructionSet
	case evm.chainRules.IsEIP158:
		jt = &spuriousDragonInstructionSet
	case evm.chainRules.IsEIP150:
		jt = &tangerineWhistleInstructionSet
	case evm.chainRules.IsHomestead:
		jt = &homesteadInstructionSet
	default:
		jt = &frontierInstructionSet
	}
	if len(cfg.ExtraEips) > 0 {
		for i, eip := range cfg.ExtraEips {
			if err := EnableEIP(eip, jt); err != nil {
				// Disable it, so caller can check if it's activated or not
				cfg.ExtraEips = append(cfg.ExtraEips[:i], cfg.ExtraEips[i+1:]...)
				log.Error("EIP activation failed", "eip", eip, "error", err)
			}
		}
	}

	return &EVMInterpreter{
		evm: evm,
		cfg: cfg,
		jt:	 jt,
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
	if readOnly && !in.readOnly {
		in.readOnly = true
		defer func() { in.readOnly = false }()
	}

	// Reset the previous call's return data. It's unimportant to preserve the old buffer
	// as every returning call will return new data anyway.
	in.returnData = nil

	// Don't bother with the execution if there's no code.
	if len(contract.Code) == 0 {
		return nil, nil
	}

	var (
		mem			= NewMemory() // bound memory
		locStack	= stack.New()
		callContext = &callCtx{
			memory:		 mem,
			stack:		 locStack,
			contract:	 contract,
	        interpreter: in,
		}

		// For optimisation reason we're using uint64 as the program counter.
		// It's theoretically possible to go above 2^64. The YP defines the PC
		// to be uint256. Practically much less so feasible.
		pc  = uint64(0) // program counter
		res []byte // result of the opcode execution function
	)
	// Don't move this deferrred function, it's placed before the capturestate-deferred method,
	// so that it get's executed _after_: the capturestate needs the stacks before
	// they are returned to the pools
	defer func() {
		stack.ReturnNormalStack(locStack)
	}()
	contract.Input = input

	// The Interpreter main run loop (contextual). This loop runs until either an
	// explicit STOP, RETURN or SELFDESTRUCT is executed, an error occurred during
	// the execution of one of the operations or until the done flag is set by the
	// parent context.

	for {
		op := contract.GetOp(pc)
		
		// none of the bare ops can resize memory or use dynamic gas
		switch op {
		case ADD:
			x, y := locStack.Pop(), locStack.Peek()
			y.Add(&x, y)
		case SUB:
			x, y := locStack.Pop(), locStack.Peek()
			y.Sub(&x, y)
		case MUL:
		x, y := locStack.Pop(), locStack.Peek()
		y.Mul(&x, y)
		case DIV:
			x, y := locStack.Pop(), locStack.Peek()
			y.Div(&x, y)
		case SDIV:
			x, y := locStack.Pop(), locStack.Peek()
			y.SDiv(&x, y)
		case MOD:
			x, y := locStack.Pop(), locStack.Peek()
			y.Mod(&x, y)
		case SMOD:
		x, y := locStack.Pop(), locStack.Peek()
		y.SMod(&x, y)
		case SIGNEXTEND:
			back, num := locStack.Pop(), locStack.Peek()
			num.ExtendSign(num, &back)
		case NOT:
			x := locStack.Peek()
			x.Not(x)
		case LT:
			x, y := locStack.Pop(), locStack.Peek()
			if x.Lt(y) {
				y.SetOne()
			} else {
				y.Clear()
			}
		case GT:
			x, y := locStack.Pop(), locStack.Peek()
			if x.Gt(y) {
				y.SetOne()
			} else {
				y.Clear()
			}
		case SLT:
			x, y := locStack.Pop(), locStack.Peek()
			if x.Slt(y) {
				y.SetOne()
			} else {
				y.Clear()
			}
		case SGT:
			x, y := locStack.Pop(), locStack.Peek()
			if x.Sgt(y) {
				y.SetOne()
			} else {
				y.Clear()
			}
		case EQ:
		x, y := locStack.Pop(), locStack.Peek()
			if x.Eq(y) {
				y.SetOne()
			} else {
				y.Clear()
			}
		case ISZERO:
			x := locStack.Peek()
			if x.IsZero() {
				x.SetOne()
			} else {
				x.Clear()
			}
		case AND:
			x, y := locStack.Pop(), locStack.Peek()
			y.And(&x, y)
		case OR:
			x, y := locStack.Pop(), locStack.Peek()
			y.Or(&x, y)
		case XOR:
			x, y := locStack.Pop(), locStack.Peek()
			y.Xor(&x, y)
		case BYTE:
			th, val := locStack.Pop(), locStack.Peek()
			val.Byte(&th)
		case ADDMOD:
			x, y, z := locStack.Pop(), locStack.Pop(), locStack.Peek()
			if z.IsZero() {
				z.Clear()
			} else {
				z.AddMod(&x, &y, z)
			}
		case MULMOD:
			x, y, z := locStack.Pop(), locStack.Pop(), locStack.Peek()
			if z.IsZero() {
				z.Clear()
			} else {
				z.MulMod(&x, &y, z)
			}
		case SHL:
			// opSHL implements Shift Left
			// The SHL instruction (shift left) pops 2 values from the stack, first arg1 and then arg2,
			// and pushes on the stack arg2 shifted to the left by arg1 number of bits.
			// Note, second operand is left in the stack; accumulate result into it, and no need to push it afterwards
			shift, value := locStack.Pop(), locStack.Peek()
			if shift.LtUint64(256) {
				value.Lsh(value, uint(shift.Uint64()))
			} else {
				value.Clear()
			}
		case SHR:
			// opSHR implements Logical Shift Right
			// The SHR instruction (logical shift right) pops 2 values from the stack, first arg1 and then arg2,
			// and pushes on the stack arg2 shifted to the right by arg1 number of bits with zero fill.
			// Note, second operand is left in the stack; accumulate result into it, and no need to push it afterwards
			shift, value := locStack.Pop(), locStack.Peek()
			if shift.LtUint64(256) {
				value.Rsh(value, uint(shift.Uint64()))
			} else {
				value.Clear()
			}
		case SAR:
			// opSAR implements Arithmetic Shift Right
			// The SAR instruction (arithmetic shift right) pops 2 values from the stack, first arg1 and then arg2,
			// and pushes on the stack arg2 shifted to the right by arg1 number of bits with sign extension.
			shift, value := locStack.Pop(), locStack.Peek()
			if shift.GtUint64(255) {
				if value.Sign() >= 0 {
					value.Clear()
				} else {
					// Max negative shift: all bits set
					value.SetAllOne()
				}
				return nil, nil
			}
			n := uint(shift.Uint64())
			value.SRsh(value, n)
		case POP:
			locStack.Pop()

		case PUSH1:
            info := callContext.contract.opsInfo[pc].(PushInfo)
            integer := info.data
            callContext.stack.Push(&integer)
		case PUSH2:
            info := callContext.contract.opsInfo[pc].(PushInfo)
            integer := info.data
            callContext.stack.Push(&integer)

		case JUMP:	
			dest := locStack.Pop()
			pc = dest.Uint64()
		case JUMPI:
			dest, cond := locStack.Pop(), locStack.Pop()
			if !cond.IsZero() {
				pc = dest.Uint64()
			} else {
			    pc++
				err = enterBlock(callContext, pc)
				if err != nil {
					return nil, err
				}
			}
		case JUMPDEST:
			err = enterBlock(callContext, pc)
			if err != nil {
				return nil, err
			}

		default:

            operation, err := op_gas_memory(in, op, contract, locStack, mem)
            if err != nil {
                return nil, err
            }

			// execute the operation
			res, err = operation.execute(&pc, in, callContext)
			// if the operation clears the return data (e.g. it has returning data)
			// set the last return to the result of the operation.
			if operation.returns {
				in.returnData = common.CopyBytes(res)
			}
			switch {
			case operation.halts:
				return res, err
			case operation.reverts:
				return res, err
			case !operation.jumps:
				pc++
			}
		}
		if err != nil {
		    return nil, err
		}
	}
	return nil, nil
}

// CanRun tells if the contract, passed as an argument, can be
// run by the current interpreter.
func (in *EVMInterpreter) CanRun(code []byte) bool {
	return true
}

// check block's stack bounds and use constant gas
func enterBlock(ctx *callCtx, pc uint64) error {

    // block info is created first time block is entered
	info, err := getBlockInfo(ctx, pc)
	if info == nil || err != nil {
		return err
	}
	// check stack, gas, and memory usage
	if sLen := ctx.stack.Len(); sLen < info.minStack {
		return &ErrStackUnderflow{stackLen: sLen, required: info.minStack}
	} else if sLen > info.maxStack {
		return &ErrStackOverflow{stackLen: sLen, limit: info.maxStack}
	} else if !ctx.contract.UseGas(info.constantGas) {
		return ErrOutOfGas
	}
	return nil
}

// resize memory and use dynamic portion of gas
func op_gas_memory(in *EVMInterpreter, op OpCode, contract *Contract, locStack *stack.Stack, mem *Memory)(*operation, error) {
	
	var (
	    operation *operation = in.jt[op]
		memorySize uint64
		err error
	)
	
	err = op_Readonly(in, op, operation, locStack)
	if err != nil {
		return nil, err
	}
	
	memorySize, err = op_memory(operation, locStack)
	if err != nil {
		return nil, err
	}
	err = op_dyn_gas(in.evm, operation, contract, locStack, mem, memorySize)
	if err != nil {
		return nil, err
	}
	if memorySize > 0 {
		mem.Resize(memorySize)
	}
	return operation, nil
}

func op_Readonly(in *EVMInterpreter, op OpCode, operation *operation, locStack *stack.Stack) (error) {

	// If the operation is valid, enforce and write restrictions
	if in.readOnly && in.evm.chainRules.IsByzantium {
		// If the interpreter is operating in readonly mode, make sure no
		// state-modifying operation is performed. The 3rd stack item
		// for a call operation is the value. Transferring value from one
		// account to the others means the state is modified and should also
		// return with an error.
		if operation.writes || (op == CALL && locStack.Back(2).Sign() != 0) {
			return ErrWriteProtection
		}
	}
	return nil
}

func op_memory(operation *operation, locStack *stack.Stack)(uint64, error) {

	// calculate the new memory size to expand the memory to fit
	// the operation
	// Memory check needs to be done prior to evaluating the dynamic gas portion,
	// to detect calculation overflows
	if operation.memorySize != nil {
		memSize, overflow := operation.memorySize(locStack)
		if overflow {
			return 0, ErrGasUintOverflow
		}
		// memory is expanded in words of 32 bytes. Gas
		// is also calculated in words.
		if memSize, overflow = math.SafeMul(toWordSize(memSize), 32); overflow {
			return 0, ErrGasUintOverflow
		}
		return memSize, nil
	}
	return 0, nil
}

func op_dyn_gas(evm *EVM, operation *operation, contract *Contract, locStack *stack.Stack, mem *Memory, memorySize uint64) (error) {

	// Dynamic portion of gas
	// consume the gas and return an error if not enough gas is available.
	// cost is explicitly set so that the capture state defer method can get the proper cost
	if operation.dynamicGas != nil {
		dynamicCost, err := operation.dynamicGas(evm, contract, locStack, mem, memorySize)
		if err != nil || !contract.UseGas(dynamicCost) {
			return ErrOutOfGas
		}
	}
	if memorySize > 0 {
		mem.Resize(memorySize)
	}
	return nil
}