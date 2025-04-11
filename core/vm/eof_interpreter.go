package vm

import (
	"fmt"

	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm/stack"
)

func (in *EVMInterpreter) RunEOF(contract *Contract, input []byte, readOnly bool, header *EofHeader) (ret []byte, err error) {

	// Don't bother with the execution if there's no code.
	if len(contract.Code) == 0 {
		return nil, nil
	}

	if !in.cfg.Debug {
		return in.runNoDebug(contract, input, readOnly, header)
	}

	// Reset the previous call's return data. It's unimportant to preserve the old buffer
	// as every returning call will return new data anyway.
	in.returnData = nil

	var (
		op          OpCode // current opcode
		mem         = pool.Get().(*Memory)
		locStack    = stack.New()
		callContext = &ScopeContext{
			Memory:      mem,
			Stack:       locStack,
			Contract:    contract,
			EofHeader:   header,
			ReturnStack: make([][2]uint64, 0, 32),
		}
		// For optimisation reason we're using uint64 as the program counter.
		// It's theoretically possible to go above 2^64. The YP defines the PC
		// to be uint256. Practically much less so feasible.
		_pc         = uint64(header.codeOffsets[0]) // program counter
		pc          = &_pc                          // program counter
		cost        uint64
		memorySize  uint64 // memory size for the current operation
		dynamicCost uint64 // dynamicCost
		operation   *operation

		// copies used by tracer
		pcCopy  uint64 // needed for the deferred Tracer
		gasCopy uint64 // for Tracer to log gas remaining before execution
		logged  bool   // deferred Tracer should ignore already logged steps
		res     []byte // result of the opcode execution function
	)

	mem.Reset()
	contract.Input = input
	// Make sure the readOnly is only set if we aren't in readOnly yet.
	// This makes also sure that the readOnly flag isn't removed for child calls.
	restoreReadonly := readOnly && !in.readOnly
	if restoreReadonly {
		in.readOnly = true
	}
	// Increment the call depth which is restricted to 1024
	in.depth++
	defer func() {
		// first: capture data/memory/state/depth/etc... then clenup them

		if !logged {
			in.cfg.Tracer.CaptureState(pcCopy, op, gasCopy, cost, callContext, in.returnData, in.depth, err) //nolint:errcheck
		} else {
			in.cfg.Tracer.CaptureFault(pcCopy, op, gasCopy, cost, callContext, in.depth, err)
		}

		// this function must execute _after_: the `CaptureState` needs the stacks before
		pool.Put(mem)
		stack.ReturnNormalStack(locStack)
		if restoreReadonly {
			in.readOnly = false
		}
		in.depth--
	}()

	// The Interpreter main run loop (contextual). This loop runs until either an
	// explicit STOP, RETURN or SELFDESTRUCT is executed, an error occurred during
	// the execution of one of the operations or until the done flag is set by the
	// parent context.
	for {
		// EOF validation guarantees that the code is not empty and <= 49152 bytes
		// so need to check for steps
		if in.evm.Cancelled() {
			break
		}

		// Capture pre-execution values for tracing.
		logged, pcCopy, gasCopy = false, _pc, contract.Gas

		op = OpCode(contract.Code[_pc])
		operation = in.jtEOF[op]
		cost = operation.constantGas // For tracing

		// no need to check for stack overflow and underflow for EOF at the runtime

		if !contract.UseGas(operation.constantGas, tracing.GasChangeIgnored) {
			return nil, ErrOutOfGas
		}

		if operation.dynamicGas != nil {

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

			dynamicCost, err = operation.dynamicGas(in.evm, callContext, memorySize, _pc)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrOutOfGas, err)
			}
			cost += dynamicCost
			if !contract.UseGas(dynamicCost, tracing.GasChangeIgnored) {
				return nil, ErrOutOfGas
			}

			if memorySize > 0 {
				mem.Resize(memorySize)
			}
		}

		in.cfg.Tracer.CaptureState(_pc, op, gasCopy, cost, callContext, in.returnData, in.depth, err) //nolint:errcheck
		logged = true

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

	ret = append(ret, res...)
	return
}

func (in *EVMInterpreter) runNoDebug(contract *Contract, input []byte, readOnly bool, header *EofHeader) (ret []byte, err error) {
	// Reset the previous call's return data. It's unimportant to preserve the old buffer
	// as every returning call will return new data anyway.
	in.returnData = nil

	var (
		op          OpCode // current opcode
		mem         = pool.Get().(*Memory)
		locStack    = stack.New()
		callContext = &ScopeContext{
			Memory:      mem,
			Stack:       locStack,
			Contract:    contract,
			EofHeader:   header,
			ReturnStack: make([][2]uint64, 0, 32),
		}
		// For optimisation reason we're using uint64 as the program counter.
		// It's theoretically possible to go above 2^64. The YP defines the PC
		// to be uint256. Practically much less so feasible.
		_pc = uint64(header.codeOffsets[0]) // program counter
		pc  = &_pc                          // program counter

		memorySize  uint64 // memory size for the current operation
		dynamicCost uint64 // dynamicCost

		operation *operation
		res       []byte // result of the opcode execution function
	)

	mem.Reset()
	contract.Input = input
	// Make sure the readOnly is only set if we aren't in readOnly yet.
	// This makes also sure that the readOnly flag isn't removed for child calls.
	restoreReadonly := readOnly && !in.readOnly
	if restoreReadonly {
		in.readOnly = true
	}

	// Increment the call depth which is restricted to 1024
	in.depth++
	defer func() {
		pool.Put(mem)
		stack.ReturnNormalStack(locStack)
		if restoreReadonly {
			in.readOnly = false
		}
		in.depth--
	}()

	for {

		if in.evm.Cancelled() {
			break
		}
		op = OpCode(contract.Code[_pc])
		operation = in.jtEOF[op]

		// no need to validate stack for EOF at the runtime

		if !contract.UseGas(operation.constantGas, tracing.GasChangeIgnored) {
			return nil, ErrOutOfGas
		}

		if operation.dynamicGas != nil {

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

			dynamicCost, err = operation.dynamicGas(in.evm, callContext, memorySize, _pc)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrOutOfGas, err)
			}

			if !contract.UseGas(dynamicCost, tracing.GasChangeIgnored) {
				return nil, ErrOutOfGas
			}

			if memorySize > 0 {
				mem.Resize(memorySize)
			}
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

	ret = append(ret, res...)
	return
}
