package vm

import (
	"sync/atomic"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/log"
)

// saCallCtx contains the things that are per-call, such as stack and memory,
// but not transients like pc and gas
type saCallCtx struct {
	memory   *Memory
	stack    *SaStack
	contract *Contract
}

// SaInterpreter is for static analysis
type SaInterpreter struct {
	evm *EVM
	cfg Config

	jt *JumpTable // EVM instruction table

	hasher    keccakState // Keccak256 hasher instance shared across opcodes
	hasherBuf common.Hash // Keccak256 hasher result array shared across opcodes

	readOnly   bool   // Whether to throw on stateful modifications
	returnData []byte // Last CALL's return data for subsequent reuse
}

// NewEVMInterpreter returns a new instance of the Interpreter.
func NewSaInterpreter(evm *EVM, cfg Config) *SaInterpreter {
	var jt *JumpTable
	switch {
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
		jtCopy := *jt
		for i, eip := range cfg.ExtraEips {
			if err := EnableEIP(eip, &jtCopy); err != nil {
				// Disable it, so caller can check if it's activated or not
				cfg.ExtraEips = append(cfg.ExtraEips[:i], cfg.ExtraEips[i+1:]...)
				log.Error("EIP activation failed", "eip", eip, "error", err)
			}
		}
		jt = &jtCopy
	}

	return &SaInterpreter{
		evm: evm,
		cfg: cfg,
		jt:  jt,
	}
}

// Run loops and evaluates the contract's code with the given input data and returns
// the return byte-slice and an error if one occurred.
//
// It's important to note that any errors returned by the interpreter should be
// considered a revert-and-consume-all-gas operation except for
// ErrExecutionReverted which means revert-and-keep-gas-left.
func (in *SaInterpreter) Run(contract *Contract, input []byte, readOnly bool) (ret []byte, err error) {
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
		op          OpCode        // current opcode
		mem         = NewMemory() // bound memory
		stack       = newSaStack()  // local stack
		callContext = &saCallCtx{
			memory:   mem,
			stack:    stack,
			contract: contract,
		}
		// For optimisation reason we're using uint64 as the program counter.
		// It's theoretically possible to go above 2^64. The YP defines the PC
		// to be uint256. Practically much less so feasible.
		pc   = uint64(0) // program counter
		res     []byte // result of the opcode execution function
	)
	contract.Input = input

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

		// Get the operation from the jump table and validate the stack to ensure there are
		// enough stack items available to perform the operation.
		op = contract.GetOp(pc)
		operation := &in.jt[op]

		if !operation.valid {
			return nil, &ErrInvalidOpCode{opcode: op}
		}
		// Validate stack
		if sLen := stack.len(); sLen < operation.minStack {
			return nil, &ErrStackUnderflow{stackLen: sLen, required: operation.minStack}
		} else if sLen > operation.maxStack {
			return nil, &ErrStackOverflow{stackLen: sLen, limit: operation.maxStack}
		}
		// If the operation is valid, enforce and write restrictions
		if in.readOnly && in.evm.chainRules.IsByzantium {
			// If the interpreter is operating in readonly mode, make sure no
			// state-modifying operation is performed. The 3rd stack item
			// for a call operation is the value. Transferring value from one
			// account to the others means the state is modified and should also
			// return with an error.
			if operation.writes {
				return nil, ErrWriteProtection
			}
			if op == CALL {
				if v, kind := stack.Back(2); kind == Constant && v.Sign() != 0 {
					return nil, ErrWriteProtection
				}
			}
		}
		// Static portion of gas
		if !contract.UseGas(operation.constantGas) {
			return nil, ErrOutOfGas
		}

		// execute the operation
		res, err = operation.saExecute(&pc, in, callContext)

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

// CanRun tells if the contract, passed as an argument, can be
// run by the current interpreter.
func (in *SaInterpreter) CanRun(code []byte) bool {
	return true
}
