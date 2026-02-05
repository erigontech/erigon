// Copyright 2024 The Erigon Authors
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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var errCallOrCreate = errors.New("call/create frame scheduled")

type executor struct {
	evm          *EVM
	frames       []frame
	retPooled    bool
	retPooledIdx int
}

type frame struct {
	ctx        *CallContext
	pc         uint64
	readOnly   bool
	noRetPool  bool
	depth      int
	retInfo    returnInfo
	hasRetInfo bool
}

type returnInfo struct {
	typ          OpCode
	traceDepth   int
	startGas     uint64
	snapshot     int
	snapshotSet  bool
	retOffset    uint64
	retSize      uint64
	resultSlot   uint256.Int
	resultSlotIx int
	createAddr   accounts.Address
	traceIO      bool
	traceBlock   uint64
	traceTxIndex int
	traceInc     int
	traceAddr    accounts.Address
	captured     bool
}

func newExecutor(evm *EVM) *executor {
	return &executor{evm: evm}
}

func (ex *executor) run(contract Contract, gas uint64, input []byte, readOnly bool) (_ []byte, _ uint64, err error) {
	// Don't bother with the execution if there's no code.
	if len(contract.Code) == 0 {
		return nil, gas, nil
	}

	evm := ex.evm
	prevReadOnly := evm.readOnly
	prevDepth := evm.depth
	evm.releaseAllPooled()
	evm.clearReturnData()
	ex.retPooled = false
	ex.retPooledIdx = -1

	initialReadOnly := readOnly || evm.readOnly
	initialContract := getContract()
	*initialContract = contract
	initialFrame := frame{
		ctx:       getCallContext(initialContract, input, gas),
		pc:        0,
		readOnly:  initialReadOnly,
		noRetPool: false,
		depth:     evm.depth + 1,
	}
	ex.frames = append(ex.frames, initialFrame)

	evm.executor = ex
	defer func() {
		evm.clearReturnData()
		evm.releaseAllPooled()
		evm.executor = nil
		evm.readOnly = prevReadOnly
		evm.noRetPool = false
		evm.depth = prevDepth
	}()

	steps := 0

	traceGas := func(op OpCode, callGas, cost uint64) uint64 {
		switch op {
		case CALL, CALLCODE, DELEGATECALL, STATICCALL:
			return callGas
		default:
			return cost
		}
	}

outer:
	for len(ex.frames) > 0 {
		frameIndex := len(ex.frames) - 1
		f := &ex.frames[frameIndex]
		evm.readOnly = f.readOnly
		evm.noRetPool = f.noRetPool
		evm.depth = f.depth

		callContext := f.ctx
		pc := f.pc
		err = nil

		// Local loop variables (mirrors the old Run loop).
		var (
			op          OpCode
			operation   *operation
			cost        uint64
			pcCopy      uint64
			gasCopy     uint64
			callGas     uint64
			logged      bool
			res         []byte
			tracer      = evm.config.Tracer
			debug       = tracer != nil && (tracer.OnOpcode != nil || tracer.OnGasChange != nil || tracer.OnFault != nil)
			trace       = dbg.TraceInstructions && evm.intraBlockState.Trace()
			blockNum    uint64
			txIndex     int
			txIncarn    int
			memorySize  uint64
			dynamicCost uint64
		)

		for {
			steps++
			if steps%50_000 == 0 && evm.Cancelled() {
				res = nil
				err = errStopToken
				break
			}

			if dbg.TraceDyanmicGas || debug || trace {
				// Capture pre-execution values for tracing.
				logged, pcCopy, gasCopy = false, pc, callContext.gas
				blockNum, txIndex, txIncarn = evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation()
			}

			// Get the operation from the jump table and validate the stack to ensure there are
			// enough stack items available to perform the operation.
			op = callContext.Contract.GetOp(pc)
			operation = evm.jt[op]
			cost = operation.constantGas // For tracing

			// Validate stack
			if sLen := callContext.Stack.len(); sLen < operation.numPop {
				err = &ErrStackUnderflow{stackLen: sLen, required: operation.numPop}
				break
			} else if sLen > operation.maxStack {
				err = &ErrStackOverflow{stackLen: sLen, limit: operation.maxStack}
				break
			}

			// for tracing: this gas consumption event is emitted below in the debug section.
			if callContext.gas < cost {
				err = ErrOutOfGas
				break
			}
			callContext.gas -= cost

			// All ops with a dynamic memory usage also has a dynamic gas cost.
			memorySize = 0
			if operation.dynamicGas != nil {
				// calculate the new memory size and expand the memory to fit
				// the operation
				// Memory check needs to be done prior to evaluating the dynamic gas portion,
				// to detect calculation overflows
				if operation.memorySize != nil {
					memSize, overflow := operation.memorySize(callContext)
					if overflow {
						err = ErrGasUintOverflow
						break
					}
					// memory is expanded in words of 32 bytes. Gas
					// is also calculated in words.
					if memorySize, overflow = math.SafeMul(ToWordSize(memSize), 32); overflow {
						err = ErrGasUintOverflow
						break
					}
				}
				// Consume the gas and return an error if not enough gas is available.
				// cost is explicitly set so that the capture state defer method can get the proper cost
				dynamicCost, err = operation.dynamicGas(evm, callContext, callContext.gas, memorySize)
				if err != nil {
					if !errors.Is(err, ErrOutOfGas) {
						err = fmt.Errorf("%w: %v", ErrOutOfGas, err)
					}
					break
				}
				cost += dynamicCost // for tracing
				callGas = operation.constantGas + dynamicCost - evm.CallGasTemp()
				if dbg.TraceDyanmicGas && dynamicCost > 0 {
					fmt.Printf("%d (%d.%d) Dynamic Gas: %d (%s)\n", blockNum, txIndex, txIncarn, traceGas(op, callGas, cost), op)
				}

				// for tracing: this gas consumption event is emitted below in the debug section.
				if callContext.gas < dynamicCost {
					err = ErrOutOfGas
					break
				}
				callContext.gas -= dynamicCost
			}

			// Do gas tracing before memory expansion
			if tracer != nil {
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

			if trace {
				var opstr string
				if operation.string != nil {
					opstr = operation.string(pc, callContext)
				} else {
					opstr = op.String()
				}
				fmt.Printf("%d (%d.%d) %5d %5d %s\n", blockNum, txIndex, txIncarn, pc, traceGas(op, callGas, cost), opstr)
			}

			// execute the operation
			pc, res, err = operation.execute(pc, evm, callContext)
			if err == errCallOrCreate {
				ex.frames[frameIndex].pc = pc + 1
				continue outer
			}
			if err != nil {
				break
			}
			pc++
		}

		if err == errStopToken {
			err = nil // clear stop token error
		}

		if debug && err != nil {
			if !logged && tracer.OnOpcode != nil {
				tracer.OnOpcode(pcCopy, byte(op), gasCopy, cost, callContext, evm.returnData, evm.depth, VMErrorFromErr(err))
			}
			if logged && tracer.OnFault != nil {
				tracer.OnFault(pcCopy, byte(op), gasCopy, cost, callContext, evm.depth, VMErrorFromErr(err))
			}
		}

		// Capture remaining gas before returning the context to the pool.
		remainingGas := callContext.gas
		callContext.put()

		retPooled := ex.retPooled
		retPooledIdx := ex.retPooledIdx
		ex.retPooled = false
		ex.retPooledIdx = -1
		retInfoSet := f.hasRetInfo
		retInfo := &f.retInfo
		ex.frames = ex.frames[:len(ex.frames)-1]

		if !retInfoSet {
			if retPooled {
				// Top-level returns should not be pooled; be safe.
				res = common.Copy(res)
				evm.releasePooled(retPooledIdx)
			}
			return res, remainingGas, err
		}

		switch retInfo.typ {
		case CALL, CALLCODE, DELEGATECALL, STATICCALL:
			ex.handleCallReturn(retInfo, res, remainingGas, err, retPooled, retPooledIdx)
		case CREATE, CREATE2:
			ex.handleCreateReturn(retInfo, res, remainingGas, err, retPooled, retPooledIdx)
		default:
			return res, remainingGas, err
		}
	}

	return nil, 0, err
}

func (ex *executor) scheduleCall(typ OpCode, caller accounts.Address, callerAddress accounts.Address, addr accounts.Address, input []byte, gas uint64, value uint256.Int, retOffset uint64, retSize uint64, resultSlot uint256.Int) (scheduled bool, ret []byte, leftOverGas uint64, err error) {
	evm := ex.evm
	if evm.abort.Load() {
		return false, nil, 0, nil
	}

	depth := evm.depth

	traceIO := (dbg.TraceTransactionIO && !dbg.TraceInstructions) && (evm.intraBlockState.Trace() || dbg.TraceAccount(caller.Handle()))
	var traceBlock uint64
	var traceTxIndex int
	var traceInc int
	if traceIO {
		version := evm.intraBlockState.Version()
		traceBlock = evm.intraBlockState.BlockNumber()
		traceTxIndex = version.TxIndex
		traceInc = version.Incarnation
		fmt.Printf("%d (%d.%d) %s: %x %x\n", traceBlock, traceTxIndex, traceInc, typ, addr, input)
	}

	p, isPrecompile := evm.precompile(addr)
	var code []byte
	if !isPrecompile {
		code, err = evm.intraBlockState.ResolveCode(addr)
		if err != nil {
			wrappedErr := fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
			ex.traceCallReturn(&returnInfo{typ: typ, traceIO: traceIO, traceBlock: traceBlock, traceTxIndex: traceTxIndex, traceInc: traceInc, traceAddr: addr}, nil, 0, wrappedErr)
			return false, nil, 0, wrappedErr
		}
	}

	info := returnInfo{
		typ:          typ,
		traceDepth:   depth,
		startGas:     gas,
		retOffset:    retOffset,
		retSize:      retSize,
		resultSlot:   resultSlot,
		resultSlotIx: -1,
		traceIO:      traceIO,
		traceBlock:   traceBlock,
		traceTxIndex: traceTxIndex,
		traceInc:     traceInc,
		traceAddr:    addr,
	}

	if evm.Config().Tracer != nil {
		evm.captureBegin(depth, typ, caller, addr, isPrecompile, input, gas, value, code)
		info.captured = true
	}

	if evm.config.NoRecursion && depth > 0 {
		ret, leftOverGas, err = nil, gas, nil
		ret, leftOverGas, err = ex.finalizeCall(&info, ret, leftOverGas, err)
		return false, ret, leftOverGas, err
	}
	if depth > int(params.CallCreateDepth) {
		ret, leftOverGas, err = nil, gas, ErrDepth
		ret, leftOverGas, err = ex.finalizeCall(&info, ret, leftOverGas, err)
		return false, ret, leftOverGas, err
	}
	if (typ == CALL || typ == CALLCODE) && !value.IsZero() {
		// Fail if we're trying to transfer more than the available balance
		canTransfer, balanceErr := evm.Context.CanTransfer(evm.intraBlockState, caller, value)
		if balanceErr != nil {
			ret, leftOverGas, err = nil, 0, balanceErr
			ret, leftOverGas, err = ex.finalizeCall(&info, ret, leftOverGas, err)
			return false, ret, leftOverGas, err
		}
		if !canTransfer {
			ret, leftOverGas, err = nil, gas, ErrInsufficientBalance
			ret, leftOverGas, err = ex.finalizeCall(&info, ret, leftOverGas, err)
			return false, ret, leftOverGas, err
		}
	}

	// BAL: record address access even if call fails due to gas/call depth and to precompiles
	evm.intraBlockState.MarkAddressAccess(addr, false)

	snapshot := evm.intraBlockState.PushSnapshot()
	info.snapshot = snapshot
	info.snapshotSet = true

	if typ == CALL {
		exist, existErr := evm.intraBlockState.Exist(addr)
		if existErr != nil {
			ret, leftOverGas, err = nil, 0, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, existErr)
			ret, leftOverGas, err = ex.finalizeCall(&info, ret, leftOverGas, err)
			return false, ret, leftOverGas, err
		}
		if !exist {
			if !isPrecompile && evm.chainRules.IsSpuriousDragon && value.IsZero() {
				ret, leftOverGas, err = nil, gas, nil
				ret, leftOverGas, err = ex.finalizeCall(&info, ret, leftOverGas, err)
				return false, ret, leftOverGas, err
			}
			evm.intraBlockState.CreateAccount(addr, false)
		}
		evm.Context.Transfer(evm.intraBlockState, caller, addr, value, false, evm.chainRules)
	} else if typ == STATICCALL {
		// Touch account during static call.
		evm.intraBlockState.AddBalance(addr, u256.Num0, tracing.BalanceChangeTouchAccount)
	}

	if isPrecompile {
		ret, leftOverGas, err = RunPrecompiledContract(p, input, gas, evm.Config().Tracer)
		ret, leftOverGas, err = ex.finalizeCall(&info, ret, leftOverGas, err)
		return false, ret, leftOverGas, err
	}
	if len(code) == 0 {
		ret, leftOverGas, err = nil, gas, nil
		ret, leftOverGas, err = ex.finalizeCall(&info, ret, leftOverGas, err)
		return false, ret, leftOverGas, err
	}

	codeHash, hashErr := evm.intraBlockState.ResolveCodeHash(addr)
	if hashErr != nil {
		evm.intraBlockState.PopSnapshot(snapshot)
		ret, leftOverGas, err = nil, 0, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, hashErr)
		if info.captured {
			evm.captureEnd(info.traceDepth, info.typ, info.startGas, leftOverGas, ret, err)
		}
		ex.traceCallReturn(&info, ret, leftOverGas, err)
		return false, ret, leftOverGas, err
	}

	contract := getContract()
	switch typ {
	case CALLCODE:
		contract.caller = caller
		contract.addr = caller
		contract.value = value
		contract.Code = code
		contract.CodeHash = codeHash
	case DELEGATECALL:
		contract.caller = callerAddress
		contract.addr = caller
		contract.value = value
		contract.Code = code
		contract.CodeHash = codeHash
	default:
		contract.caller = caller
		contract.addr = addr
		contract.value = value
		contract.Code = code
		contract.CodeHash = codeHash
	}

	frameReadOnly := evm.readOnly || typ == STATICCALL
	child := frame{
		ctx:        getCallContext(contract, input, gas),
		pc:         0,
		readOnly:   frameReadOnly,
		noRetPool:  false,
		depth:      depth + 1,
		retInfo:    info,
		hasRetInfo: true,
	}
	// Each call frame starts with empty returndata (matches per-call Run behavior).
	evm.clearReturnData()
	ex.frames = append(ex.frames, child)
	return true, nil, 0, nil
}

func (ex *executor) scheduleCreate(typ OpCode, caller accounts.Address, code []byte, codeHash accounts.CodeHash, gasRemaining uint64, value uint256.Int, address accounts.Address, incrementNonce bool, resultSlot uint256.Int, resultSlotIndex int) (scheduled bool, ret []byte, leftOverGas uint64, err error) {
	evm := ex.evm
	if evm.abort.Load() {
		return false, nil, 0, nil
	}

	depth := evm.depth

	traceIO := dbg.TraceTransactionIO && (evm.intraBlockState.Trace() || dbg.TraceAccount(caller.Handle()))
	var traceBlock uint64
	var traceTxIndex int
	var traceInc int
	if traceIO {
		version := evm.intraBlockState.Version()
		traceBlock = evm.intraBlockState.BlockNumber()
		traceTxIndex = version.TxIndex
		traceInc = version.Incarnation
	}

	info := returnInfo{
		typ:          typ,
		traceDepth:   depth,
		startGas:     gasRemaining,
		createAddr:   address,
		resultSlot:   resultSlot,
		resultSlotIx: resultSlotIndex,
		traceIO:      traceIO,
		traceBlock:   traceBlock,
		traceTxIndex: traceTxIndex,
		traceInc:     traceInc,
		traceAddr:    address,
	}

	if evm.Config().Tracer != nil {
		evm.captureBegin(depth, typ, caller, address, false, code, gasRemaining, value, nil)
		info.captured = true
	}

	// Depth check execution. Fail if we're trying to execute above the limit.
	if depth > int(params.CallCreateDepth) {
		ret, leftOverGas, err = nil, gasRemaining, ErrDepth
		ret, leftOverGas, err = ex.finalizeCreate(&info, ret, leftOverGas, err)
		return false, ret, leftOverGas, err
	}
	if !value.IsZero() {
		canTransfer, balanceErr := evm.Context.CanTransfer(evm.intraBlockState, caller, value)
		if balanceErr != nil {
			ret, leftOverGas, err = nil, 0, balanceErr
			ret, leftOverGas, err = ex.finalizeCreate(&info, ret, leftOverGas, err)
			return false, ret, leftOverGas, err
		}
		if !canTransfer {
			ret, leftOverGas, err = nil, gasRemaining, ErrInsufficientBalance
			ret, leftOverGas, err = ex.finalizeCreate(&info, ret, leftOverGas, err)
			return false, ret, leftOverGas, err
		}
	}
	if incrementNonce {
		nonce, nonceErr := evm.intraBlockState.GetNonce(caller)
		if nonceErr != nil {
			ret, leftOverGas, err = nil, 0, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, nonceErr)
			ret, leftOverGas, err = ex.finalizeCreate(&info, ret, leftOverGas, err)
			return false, ret, leftOverGas, err
		}
		if nonce+1 < nonce {
			ret, leftOverGas, err = nil, gasRemaining, ErrNonceUintOverflow
			ret, leftOverGas, err = ex.finalizeCreate(&info, ret, leftOverGas, err)
			return false, ret, leftOverGas, err
		}
		evm.intraBlockState.SetNonce(caller, nonce+1)
	}

	// We add this to the access list _before_ taking a snapshot.
	if evm.chainRules.IsBerlin {
		evm.intraBlockState.AddAddressToAccessList(address)
	}
	// BAL: record target address even on failed CREATE/CREATE2 calls
	evm.intraBlockState.MarkAddressAccess(address, false)

	// Ensure there's no existing contract already at the designated address
	contractHash, hashErr := evm.intraBlockState.ResolveCodeHash(address)
	if hashErr != nil {
		ret, leftOverGas, err = nil, 0, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, hashErr)
		ret, leftOverGas, err = ex.finalizeCreate(&info, ret, leftOverGas, err)
		return false, ret, leftOverGas, err
	}
	nonce, nonceErr := evm.intraBlockState.GetNonce(address)
	if nonceErr != nil {
		ret, leftOverGas, err = nil, 0, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, nonceErr)
		ret, leftOverGas, err = ex.finalizeCreate(&info, ret, leftOverGas, err)
		return false, ret, leftOverGas, err
	}
	hasStorage, storageErr := evm.intraBlockState.HasStorage(address)
	if storageErr != nil {
		ret, leftOverGas, err = nil, 0, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, storageErr)
		ret, leftOverGas, err = ex.finalizeCreate(&info, ret, leftOverGas, err)
		return false, ret, leftOverGas, err
	}
	if nonce != 0 || !contractHash.IsEmpty() || hasStorage {
		err = ErrContractAddressCollision
		if evm.config.Tracer != nil && evm.config.Tracer.OnGasChange != nil {
			evm.Config().Tracer.OnGasChange(gasRemaining, 0, tracing.GasChangeCallFailedExecution)
		}
		ret, leftOverGas, err = nil, 0, err
		ret, leftOverGas, err = ex.finalizeCreate(&info, ret, leftOverGas, err)
		return false, ret, leftOverGas, err
	}

	snapshot := evm.intraBlockState.PushSnapshot()
	info.snapshot = snapshot
	info.snapshotSet = true

	evm.intraBlockState.CreateAccount(address, true)
	if evm.chainRules.IsSpuriousDragon {
		evm.intraBlockState.SetNonce(address, 1)
	}
	evm.Context.Transfer(evm.intraBlockState, caller, address, value, false, evm.chainRules)

	contract := getContract()
	contract.caller = caller
	contract.addr = address
	contract.value = value
	contract.Code = code
	contract.CodeHash = codeHash

	if evm.config.NoRecursion && depth > 0 {
		if info.snapshotSet {
			evm.intraBlockState.PopSnapshot(info.snapshot)
		}
		if info.captured {
			evm.captureEnd(info.traceDepth, info.typ, info.startGas, gasRemaining, nil, nil)
		}
		ex.traceCreateReturn(&info, gasRemaining, nil)
		return false, nil, gasRemaining, nil
	}

	child := frame{
		ctx:        getCallContext(contract, nil, gasRemaining),
		pc:         0,
		readOnly:   evm.readOnly,
		noRetPool:  true,
		depth:      depth + 1,
		retInfo:    info,
		hasRetInfo: true,
	}
	// Each creation frame starts with empty returndata.
	evm.clearReturnData()
	ex.frames = append(ex.frames, child)
	return true, nil, 0, nil
}

func (ex *executor) finalizeCall(info *returnInfo, ret []byte, gas uint64, err error) ([]byte, uint64, error) {
	evm := ex.evm
	if info.snapshotSet {
		if err != nil || evm.config.RestoreState {
			evm.intraBlockState.RevertToSnapshot(info.snapshot, err)
			if err != ErrExecutionReverted {
				if evm.config.Tracer != nil && evm.config.Tracer.OnGasChange != nil {
					evm.Config().Tracer.OnGasChange(gas, 0, tracing.GasChangeCallFailedExecution)
				}
				gas = 0
			}
		}
		evm.intraBlockState.PopSnapshot(info.snapshot)
	}
	if info.captured {
		evm.captureEnd(info.traceDepth, info.typ, info.startGas, gas, ret, err)
	}
	ex.traceCallReturn(info, ret, gas, err)
	return ret, gas, err
}

func (ex *executor) finalizeCreate(info *returnInfo, ret []byte, gas uint64, err error) ([]byte, uint64, error) {
	evm := ex.evm
	// EIP-170: Contract code size limit
	if err == nil && evm.chainRules.IsSpuriousDragon && len(ret) > evm.maxCodeSize() {
		if !evm.chainRules.IsAura || evm.config.HasEip3860(evm.chainRules) {
			err = ErrMaxCodeSizeExceeded
		}
	}
	// Reject code starting with 0xEF if EIP-3541 is enabled.
	if err == nil && evm.chainRules.IsLondon && len(ret) >= 1 && ret[0] == 0xEF {
		err = ErrInvalidCode
	}
	if err == nil {
		createDataGas := uint64(len(ret)) * params.CreateDataGas
		var ok bool
		if gas, ok = useGas(gas, createDataGas, evm.Config().Tracer, tracing.GasChangeCallCodeStorage); ok {
			evm.intraBlockState.SetCode(info.createAddr, ret)
		} else {
			ret = []byte{}
			if evm.chainRules.IsHomestead {
				err = ErrCodeStoreOutOfGas
			}
		}
	}
	if info.snapshotSet {
		if err != nil && (evm.chainRules.IsHomestead || err != ErrCodeStoreOutOfGas) {
			evm.intraBlockState.RevertToSnapshot(info.snapshot, nil)
			if err != ErrExecutionReverted {
				gas, _ = useGas(gas, gas, evm.Config().Tracer, tracing.GasChangeCallFailedExecution)
			}
		}
		evm.intraBlockState.PopSnapshot(info.snapshot)
	}
	if info.captured {
		evm.captureEnd(info.traceDepth, info.typ, info.startGas, gas, ret, err)
	}
	ex.traceCreateReturn(info, gas, err)
	return ret, gas, err
}

func (ex *executor) handleCallReturn(info *returnInfo, ret []byte, gas uint64, err error, retPooled bool, retPooledIdx int) {
	evm := ex.evm
	ret, gas, err = ex.finalizeCall(info, ret, gas, err)

	parent := &ex.frames[len(ex.frames)-1]
	if err != nil {
		info.resultSlot.Clear()
	} else {
		info.resultSlot.SetOne()
	}
	parent.ctx.Stack.push(info.resultSlot)
	if err == nil || err == ErrExecutionReverted {
		parent.ctx.Memory.Set(info.retOffset, info.retSize, ret)
	}
	parent.ctx.refundGas(gas, evm.config.Tracer, tracing.GasChangeCallLeftOverRefunded)
	evm.setReturnData(ret, retPooled, retPooledIdx)
}

func (ex *executor) handleCreateReturn(info *returnInfo, ret []byte, gas uint64, err error, retPooled bool, retPooledIdx int) {
	evm := ex.evm
	if retPooled && err == nil {
		retCopy := common.Copy(ret)
		evm.releasePooled(retPooledIdx)
		ret = retCopy
		retPooled = false
		retPooledIdx = -1
	}
	ret, gas, err = ex.finalizeCreate(info, ret, gas, err)

	parent := &ex.frames[len(ex.frames)-1]
	if info.typ == CREATE && info.resultSlotIx >= 0 {
		slot := &parent.ctx.Stack.data[info.resultSlotIx]
		if evm.chainRules.IsHomestead && err == ErrCodeStoreOutOfGas {
			slot.Clear()
		} else if err != nil && err != ErrCodeStoreOutOfGas {
			slot.Clear()
		} else {
			addrVal := info.createAddr.Value()
			slot.SetBytes(addrVal[:])
		}
	} else {
		if evm.chainRules.IsHomestead && err == ErrCodeStoreOutOfGas {
			info.resultSlot.Clear()
		} else if err != nil && err != ErrCodeStoreOutOfGas {
			info.resultSlot.Clear()
		} else {
			addrVal := info.createAddr.Value()
			info.resultSlot.SetBytes(addrVal[:])
		}
		parent.ctx.Stack.push(info.resultSlot)
	}
	parent.ctx.refundGas(gas, evm.config.Tracer, tracing.GasChangeCallLeftOverRefunded)

	if err == ErrExecutionReverted {
		evm.setReturnData(ret, retPooled, retPooledIdx)
	} else {
		evm.clearReturnData()
	}
}

func (ex *executor) traceCallReturn(info *returnInfo, ret []byte, gas uint64, err error) {
	if !info.traceIO {
		return
	}
	fmt.Printf("%d (%d.%d) RETURN (%s): %x: %x, %d, %v\n", info.traceBlock, info.traceTxIndex, info.traceInc, info.typ, info.traceAddr, ret, gas, err)
}

func (ex *executor) traceCreateReturn(info *returnInfo, gas uint64, err error) {
	if !info.traceIO {
		return
	}
	if err != nil {
		fmt.Printf("%d (%d.%d) Create Contract: %x, err=%s\n", info.traceBlock, info.traceTxIndex, info.traceInc, info.createAddr, err)
		return
	}
	fmt.Printf("%d (%d.%d) Create Contract: %x, gas=%d\n", info.traceBlock, info.traceTxIndex, info.traceInc, info.createAddr, gas)
}
