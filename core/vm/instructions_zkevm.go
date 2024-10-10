package vm

import (
	"math/big"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
)

func opCallDataLoad_zkevmIncompatible(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x := scope.Stack.Peek()
	if offset, overflow := x.Uint64WithOverflow(); !overflow {
		data := getData(scope.Contract.Input, offset, 32)
		if len(scope.Contract.Input) == 0 {
			data = getData(scope.Contract.Code, offset, 32)
		}
		x.SetBytes(data)
	} else {
		x.Clear()
	}
	return nil, nil
}

func opCallDataCopy_zkevmIncompatible(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		memOffset  = scope.Stack.Pop()
		dataOffset = scope.Stack.Pop()
		length     = scope.Stack.Pop()
	)
	dataOffset64, overflow := dataOffset.Uint64WithOverflow()
	if overflow {
		dataOffset64 = 0xffffffffffffffff
	}
	// These values are checked for overflow during gas cost calculation
	memOffset64 := memOffset.Uint64()
	length64 := length.Uint64()

	if len(scope.Contract.Input) == 0 {
		scope.Memory.Set(memOffset64, length64, getData(scope.Contract.Code, dataOffset64, length64))
	} else {
		scope.Memory.Set(memOffset64, length64, getData(scope.Contract.Input, dataOffset64, length64))
	}

	return nil, nil
}

func opExtCodeHash_zkevm(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	slot := scope.Stack.Peek()
	address := libcommon.Address(slot.Bytes20())
	ibs := interpreter.evm.IntraBlockState()
	if ibs.GetCodeSize(address) == 0 {
		slot.SetBytes(libcommon.Hash{}.Bytes())
	} else {
		slot.SetBytes(ibs.GetCodeHash(address).Bytes())
	}
	return nil, nil
}

func opBlockhash_zkevm(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	num := scope.Stack.Peek()

	ibs := interpreter.evm.IntraBlockState()
	num.Set(ibs.GetBlockStateRoot(num))

	return nil, nil
}

func opNumber_zkevm(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	ibs := interpreter.evm.IntraBlockState()
	num := ibs.GetBlockNumber()
	scope.Stack.Push(num)
	return nil, nil
}

func opDifficulty_zkevm(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	zeroInt := new(big.Int).SetUint64(0)
	v, _ := uint256.FromBig(zeroInt)
	scope.Stack.Push(v)
	return nil, nil
}

func opStaticCall_zkevm(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	stack := scope.Stack
	// We use it as a temporary value
	temp := stack.Pop()
	gas := interpreter.evm.CallGasTemp()
	// Pop other call parameters.
	addr, inOffset, inSize, retOffset, retSize := stack.Pop(), stack.Pop(), stack.Pop(), stack.Pop(), stack.Pop()
	toAddr := libcommon.Address(addr.Bytes20())
	// Get arguments from the memory.
	args := scope.Memory.GetPtr(int64(inOffset.Uint64()), int64(inSize.Uint64()))
	ret, returnGas, err := interpreter.evm.StaticCall(scope.Contract, toAddr, args, gas)
	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.Push(&temp)
	if err == nil || IsErrTypeRevert(err) {
		ret = libcommon.CopyBytes(ret)
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}

	scope.Contract.Gas += returnGas

	//[zkevm] do not overryde returnData if reverted
	if !IsErrTypeRevert(err) {
		interpreter.returnData = ret
	}

	return ret, nil
}

// removed the actual self destruct at the end
func opSendAll_zkevm(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	beneficiary := scope.Stack.Pop()
	callerAddr := scope.Contract.Address()
	beneficiaryAddr := libcommon.Address(beneficiary.Bytes20())
	balance := interpreter.evm.IntraBlockState().GetBalance(callerAddr)
	if interpreter.evm.Config().Debug {
		if interpreter.cfg.Debug {
			interpreter.cfg.Tracer.CaptureEnter(SELFDESTRUCT, callerAddr, beneficiaryAddr, false /* precompile */, false /* create */, []byte{}, 0, balance, nil /* code */)
			interpreter.cfg.Tracer.CaptureExit([]byte{}, 0, nil)
		}
	}

	if beneficiaryAddr != callerAddr {
		interpreter.evm.IntraBlockState().AddBalance(beneficiaryAddr, balance)
		interpreter.evm.IntraBlockState().SubBalance(callerAddr, balance)
	}
	return nil, errStopToken
}

func makeLog_zkevm_logIndexFromZero(size int) executionFunc {
	return makeLog_zkevm(size, true)
}

func makeLog_zkevm_regularLogIndexes(size int) executionFunc {
	return makeLog_zkevm(size, false)
}

// [zkEvm] log data length must be a multiple of 32, if not - fill 0 at the end until it is
func makeLog_zkevm(size int, logIndexPerTx bool) executionFunc {
	return func(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
		if interpreter.readOnly {
			return nil, ErrWriteProtection
		}
		topics := make([]libcommon.Hash, size)
		stack := scope.Stack
		mStart, mSize := stack.Pop(), stack.Pop()
		for i := 0; i < size; i++ {
			addr := stack.Pop()
			topics[i] = addr.Bytes32()
		}

		d := scope.Memory.GetCopy(int64(mStart.Uint64()), int64(mSize.Uint64()))

		log := types.Log{
			Address: scope.Contract.Address(),
			Topics:  topics,
			Data:    d,
			// This is a non-consensus field, but assigned here because
			// core/state doesn't know the current block number.
			BlockNumber: interpreter.evm.Context.BlockNumber,
		}
		if logIndexPerTx {
			interpreter.evm.IntraBlockState().AddLog_zkEvm(&log)
		} else {
			interpreter.evm.IntraBlockState().AddLog(&log)
		}

		return nil, nil
	}
}

func opCreate_zkevm(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	var (
		value  = scope.Stack.Pop()
		offset = scope.Stack.Pop()
		size   = scope.Stack.Peek()
		input  = scope.Memory.GetCopy(int64(offset.Uint64()), int64(size.Uint64()))
		gas    = scope.Contract.Gas
	)
	if interpreter.evm.ChainRules().IsTangerineWhistle {
		gas -= gas / 64
	}
	// reuse size int for stackvalue
	stackvalue := size

	scope.Contract.UseGas(gas)

	res, addr, returnGas, suberr := interpreter.evm.Create(scope.Contract, input, gas, &value, 0)

	// Push item on the stack based on the returned error. If the ruleset is
	// homestead we must check for CodeStoreOutOfGasError (homestead only
	// rule) and treat as an error, if the ruleset is frontier we must
	// ignore this error and pretend the operation was successful.
	if interpreter.evm.ChainRules().IsHomestead && suberr == ErrCodeStoreOutOfGas {
		stackvalue.Clear()
	} else if suberr != nil && suberr != ErrCodeStoreOutOfGas {
		stackvalue.Clear()
	} else {
		stackvalue.SetBytes(addr.Bytes())
	}
	scope.Contract.Gas += returnGas

	if IsErrTypeRevert(suberr) {
		interpreter.returnData = res // set REVERT data to return data buffer
		return res, nil
	}
	interpreter.returnData = nil // clear dirty return data buffer
	return nil, nil
}

func opCreate2_zkevm(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	var (
		endowment    = scope.Stack.Pop()
		offset, size = scope.Stack.Pop(), scope.Stack.Pop()
		salt         = scope.Stack.Pop()
		input        = scope.Memory.GetCopy(int64(offset.Uint64()), int64(size.Uint64()))
		gas          = scope.Contract.Gas
	)

	// Apply EIP150
	gas -= gas / 64
	scope.Contract.UseGas(gas)
	// reuse size int for stackvalue
	stackValue := size
	res, addr, returnGas, suberr := interpreter.evm.Create2(scope.Contract, input, gas, &endowment, &salt, gas)

	// Push item on the stack based on the returned error.
	if suberr != nil {
		stackValue.Clear()
	} else {
		stackValue.SetBytes(addr.Bytes())
	}

	scope.Stack.Push(&stackValue)
	scope.Contract.Gas += returnGas

	if IsErrTypeRevert(suberr) {
		interpreter.returnData = res // set REVERT data to return data buffer
		return res, nil
	}
	interpreter.returnData = nil // clear dirty return data buffer
	return nil, nil
}

func opCall_zkevm(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	stack := scope.Stack
	// Pop gas. The actual gas in interpreter.evm.callGasTemp.
	// We can use this as a temporary value
	temp := stack.Pop()
	gas := interpreter.evm.CallGasTemp()
	// Pop other call parameters.
	addr, value, inOffset, inSize, retOffset, retSize := stack.Pop(), stack.Pop(), stack.Pop(), stack.Pop(), stack.Pop(), stack.Pop()
	toAddr := libcommon.Address(addr.Bytes20())
	// Get the arguments from the memory.
	args := scope.Memory.GetPtr(int64(inOffset.Uint64()), int64(inSize.Uint64()))

	if !value.IsZero() {
		if interpreter.readOnly {
			return nil, ErrWriteProtection
		}
		gas += params.CallStipend
	}

	ret, returnGas, err := interpreter.evm.Call_zkEvm(scope.Contract, toAddr, args, gas, &value, false /* bailout */, 0, int(retSize.Uint64()))

	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.Push(&temp)
	if err == nil || IsErrTypeRevert(err) {
		ret = libcommon.CopyBytes(ret)
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}

	scope.Contract.Gas += returnGas

	interpreter.returnData = ret
	return ret, nil
}

func opCallCode_zkevm(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	stack := scope.Stack
	// We use it as a temporary value
	temp := stack.Pop()
	gas := interpreter.evm.CallGasTemp()
	// Pop other call parameters.
	addr, value, inOffset, inSize, retOffset, retSize := stack.Pop(), stack.Pop(), stack.Pop(), stack.Pop(), stack.Pop(), stack.Pop()
	toAddr := libcommon.Address(addr.Bytes20())
	// Get arguments from the memory.
	args := scope.Memory.GetPtr(int64(inOffset.Uint64()), int64(inSize.Uint64()))

	if !value.IsZero() {
		gas += params.CallStipend
	}

	ret, returnGas, err := interpreter.evm.CallCode_zkEvm(scope.Contract, toAddr, args, gas, &value, int(retSize.Uint64()))
	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.Push(&temp)
	if err == nil || IsErrTypeRevert(err) {
		ret = libcommon.CopyBytes(ret)
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}

	scope.Contract.Gas += returnGas

	interpreter.returnData = ret
	return ret, nil
}

func opDelegateCall_zkevm(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	stack := scope.Stack
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	// We use it as a temporary value
	temp := stack.Pop()
	gas := interpreter.evm.CallGasTemp()
	// Pop other call parameters.
	addr, inOffset, inSize, retOffset, retSize := stack.Pop(), stack.Pop(), stack.Pop(), stack.Pop(), stack.Pop()
	toAddr := libcommon.Address(addr.Bytes20())
	// Get arguments from the memory.
	args := scope.Memory.GetPtr(int64(inOffset.Uint64()), int64(inSize.Uint64()))

	ret, returnGas, err := interpreter.evm.DelegateCall(scope.Contract, toAddr, args, gas)
	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.Push(&temp)
	if err == nil || IsErrTypeRevert(err) {
		ret = libcommon.CopyBytes(ret)
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}

	scope.Contract.Gas += returnGas

	interpreter.returnData = ret
	return ret, nil
}

// OpCoded execution overrides that are used for executing the last opcode in case of an error
func opBlockhash_zkevm_lastOpCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	num := scope.Stack.Peek()

	ibs := interpreter.evm.IntraBlockState()
	ibs.GetBlockStateRoot(num)

	return nil, nil
}

func opCodeSize_lastOpCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	return nil, nil
}

func opExtCodeSize_lastOpCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	slot := scope.Stack.Peek()
	interpreter.evm.IntraBlockState().GetCodeSize(slot.Bytes20())
	return nil, nil
}

func opExtCodeCopy_lastOpCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		stack = scope.Stack
		a     = stack.Pop()
	)
	addr := libcommon.Address(a.Bytes20())
	interpreter.evm.IntraBlockState().GetCode(addr)
	return nil, nil
}

func opExtCodeHash_zkevm_lastOpCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	slot := scope.Stack.Peek()
	address := libcommon.Address(slot.Bytes20())
	ibs := interpreter.evm.IntraBlockState()
	ibs.GetCodeSize(address)
	ibs.GetCodeHash(address)
	return nil, nil
}

func opSelfBalance_lastOpCode(pc *uint64, interpreter *EVMInterpreter, callContext *ScopeContext) ([]byte, error) {
	interpreter.evm.IntraBlockState().GetBalance(callContext.Contract.Address())
	return nil, nil
}

func opBalance_lastOpCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	slot := scope.Stack.Peek()
	address := libcommon.Address(slot.Bytes20())
	interpreter.evm.IntraBlockState().GetBalance(address)
	return nil, nil
}

func opCreate_zkevm_lastOpCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	var (
		value = scope.Stack.Pop()
		gas   = scope.Contract.Gas
	)
	if interpreter.evm.ChainRules().IsTangerineWhistle {
		gas -= gas / 64
	}

	caller := scope.Contract
	address := crypto.CreateAddress(caller.Address(), interpreter.evm.IntraBlockState().GetNonce(caller.Address()))

	interpreter.evm.IntraBlockState().GetBalance(caller.Address())
	nonce := interpreter.evm.IntraBlockState().GetNonce(caller.Address())
	interpreter.evm.IntraBlockState().SetNonce(caller.Address(), nonce+1)
	interpreter.evm.IntraBlockState().AddAddressToAccessList(address)
	interpreter.evm.IntraBlockState().GetCodeHash(address)
	interpreter.evm.IntraBlockState().GetNonce(address)
	interpreter.evm.IntraBlockState().CreateAccount(address, true)
	interpreter.evm.IntraBlockState().SetNonce(address, 1)
	interpreter.evm.IntraBlockState().SubBalance(caller.Address(), &value)
	interpreter.evm.IntraBlockState().AddBalance(address, &value)
	interpreter.evm.IntraBlockState().SetCode(address, []byte{0})

	return nil, nil
}

func opCreate2_zkevm_lastOpCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	var (
		endowment    = scope.Stack.Pop()
		offset, size = scope.Stack.Pop(), scope.Stack.Pop()
		salt         = scope.Stack.Pop()
		input        = scope.Memory.GetCopy(int64(offset.Uint64()), int64(size.Uint64()))
	)

	caller := scope.Contract
	codeAndHash := &codeAndHash{code: input}
	address := crypto.CreateAddress2(caller.Address(), salt.Bytes32(), codeAndHash.Hash().Bytes())

	interpreter.evm.IntraBlockState().GetBalance(caller.Address())
	nonce := interpreter.evm.IntraBlockState().GetNonce(caller.Address())
	interpreter.evm.IntraBlockState().SetNonce(caller.Address(), nonce+1)
	interpreter.evm.IntraBlockState().AddAddressToAccessList(address)
	interpreter.evm.IntraBlockState().GetCodeHash(address)
	interpreter.evm.IntraBlockState().GetNonce(address)
	interpreter.evm.IntraBlockState().CreateAccount(address, true)
	interpreter.evm.IntraBlockState().SetNonce(address, 1)
	interpreter.evm.IntraBlockState().SubBalance(caller.Address(), &endowment)
	interpreter.evm.IntraBlockState().AddBalance(address, &endowment)
	interpreter.evm.IntraBlockState().SetCode(address, []byte{0})

	return nil, nil
}

func opReturn_lastOpCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	return nil, nil
}

func opUndefined_lastOpCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	return nil, nil
}

func opSload_lastOpCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	loc := scope.Stack.Peek()
	interpreter.hasherBuf = loc.Bytes32()
	interpreter.evm.IntraBlockState().GetState(scope.Contract.Address(), &interpreter.hasherBuf, loc)
	return nil, nil
}

func opSstore_lastOpCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	loc := scope.Stack.Pop()
	val := scope.Stack.Pop()
	interpreter.hasherBuf = loc.Bytes32()
	interpreter.evm.IntraBlockState().SetState(scope.Contract.Address(), &interpreter.hasherBuf, val)
	return nil, nil
}
