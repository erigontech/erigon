package vm

import (
	"github.com/gateway-fm/cdk-erigon-lib/common/math"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/core/vm/stack"
	"github.com/ledgerwatch/log/v3"
)

type ZkConfig struct {
	Config Config

	TracerCollector  bool
	CounterCollector *CounterCollector
}

func NewZkConfig(config Config, counterCollector *CounterCollector) ZkConfig {
	return ZkConfig{
		Config:           config,
		TracerCollector:  false,
		CounterCollector: counterCollector,
	}
}

func NewTracerZkConfig(config Config, counterCollector *CounterCollector) ZkConfig {
	return ZkConfig{
		Config:           config,
		TracerCollector:  true,
		CounterCollector: counterCollector,
	}
}

func getJumpTable(cr *chain.Rules) *JumpTable {
	var jt *JumpTable
	switch {
	case cr.IsForkID8Elderberry:
		jt = &forkID8ElderberryInstructionSet
	case cr.IsForkID5Dragonfruit, cr.IsForkID6IncaBerry, cr.IsForkID7Etrog:
		jt = &forkID5DragonfruitInstructionSet
	case cr.IsBerlin:
		jt = &forkID4InstructionSet
	}

	return jt
}

// NewZKEVMInterpreter returns a new instance of the Interpreter.
func NewZKEVMInterpreter(evm VMInterpreter, cfg ZkConfig) *EVMInterpreter {
	jt := getJumpTable(evm.ChainRules())

	// here we need to copy the jump table every time as we're about to wrap it with the zk counters handling
	// if we don't take a copy of this it will be wrapped over and over again causing a deeper and deeper stack
	// and duplicating the zk counters handling
	jt = copyJumpTable(jt)

	if len(cfg.Config.ExtraEips) > 0 {
		for i, eip := range cfg.Config.ExtraEips {
			if err := EnableEIP(eip, jt); err != nil {
				// Disable it, so caller can check if it's activated or not
				cfg.Config.ExtraEips = append(cfg.Config.ExtraEips[:i], cfg.Config.ExtraEips[i+1:]...)
				log.Error("EIP activation failed", "eip", eip, "err", err)
			}
		}
	}

	// if we have an active counter collector for the call then we need
	// to wrap the jump table so that we can process counters as op codes are called within
	// the EVM
	if cfg.CounterCollector != nil {
		WrapJumpTableWithZkCounters(jt, SimpleCounterOperations(cfg.CounterCollector))
	}

	return &EVMInterpreter{
		VM: &VM{
			evm: evm,
			cfg: cfg.Config,
		},
		jt: jt,
	}
}

func (in *EVMInterpreter) RunZk(contract *Contract, input []byte, readOnly bool) (ret []byte, err error) {
	// Don't bother with the execution if there's no code.
	if len(contract.Code) == 0 {
		return nil, nil
	}

	// Increment the call depth which is restricted to 1024
	in.depth++
	defer in.decrementDepth()

	// Make sure the readOnly is only set if we aren't in readOnly yet.
	// This makes also sure that the readOnly flag isn't removed for child calls.
	if readOnly && !in.readOnly {
		in.readOnly = true
		defer func() { in.readOnly = false }()
	}

	// Reset the previous call's return data. It's unimportant to preserve the old buffer
	// as every returning call will return new data anyway.
	in.returnData = nil

	var (
		op          OpCode // current opcode
		mem         = pool.Get().(*Memory)
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
	mem.Reset()
	defer pool.Put(mem)
	defer stack.ReturnNormalStack(locStack)
	contract.Input = input

	// this defer is the only one difference between original erigon Run and RunZk
	// it should be placed before the capturestate-deferred method,
	// so that it get's executed _after_: the capturestate needs the stacks before
	defer func() {
		if ret != nil || err == nil {
			return
		}

		// execute the operation in case of SLOAD | SSTORE
		// the actual result of this operation does not matter because it will be reverted anyway, because err != nil
		// we implement it this way in order execution to be identical to tracing
		if op == SLOAD || op == SSTORE {
			// we can safely use pc here instead of pcCopy,
			// because pc and pcCopy can be different only if the main loop finishes normally without error
			// but is it finishes normally without error then "ret" != nil and the .execute below will never be invoked at all
			in.jt[op].execute(pc, in, callContext)
		}
	}()

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

	ret = append(ret, res...)
	return
}
