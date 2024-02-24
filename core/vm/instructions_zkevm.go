package vm

import (
	"math/big"

	"github.com/iden3/go-iden3-crypto/keccak256"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
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
	_, overflow := num.Uint64WithOverflow()
	if overflow {
		num.Clear()
		return nil, nil
	}

	ibs := interpreter.evm.IntraBlockState()
	saddr := libcommon.HexToAddress("0x000000000000000000000000000000005ca1ab1e")

	d1 := common.LeftPadBytes(num.Bytes(), 32)
	d2 := common.LeftPadBytes(uint256.NewInt(1).Bytes(), 32)
	mapKey := keccak256.Hash(d1, d2)
	mkh := libcommon.BytesToHash(mapKey)

	// set mapping of keccak256(txnum,1) -> smt root
	blockHash := uint256.NewInt(0)
	ibs.GetState(saddr, &mkh, blockHash)

	num.SetBytes(blockHash.Bytes())

	return nil, nil
}

func opNumber_zkevm(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	ibs := interpreter.evm.IntraBlockState()
	saddr := libcommon.HexToAddress("0x000000000000000000000000000000005ca1ab1e")
	sl0 := libcommon.HexToHash("0x0")

	txNum := uint256.NewInt(0)
	ibs.GetState(saddr, &sl0, txNum)

	scope.Stack.Push(txNum)
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
		ret = common.CopyBytes(ret)
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

// [zkEvm] log data length must be a multiple of 32, if not - fill 0 at the end until it is
func makeLog_zkevm(size int) executionFunc {
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

		// [zkEvm] fill 0 at the end
		dataLen := len(d)
		lenMod32 := dataLen & 31
		if lenMod32 != 0 {
			d = append(d, make([]byte, 32-lenMod32)...)
		}

		interpreter.evm.IntraBlockState().AddLog(&types.Log{
			Address: scope.Contract.Address(),
			Topics:  topics,
			Data:    d,
			// This is a non-consensus field, but assigned here because
			// core/state doesn't know the current block number.
			BlockNumber: interpreter.evm.Context().BlockNumber,
		})

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
	res, addr, returnGas, suberr := interpreter.evm.Create2(scope.Contract, input, gas, &endowment, &salt)

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

	ret, returnGas, err := interpreter.evm.Call(scope.Contract, toAddr, args, gas, &value, false /* bailout */, 0)

	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.Push(&temp)
	if err == nil || IsErrTypeRevert(err) {
		ret = common.CopyBytes(ret)
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

	ret, returnGas, err := interpreter.evm.CallCode(scope.Contract, toAddr, args, gas, &value)
	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.Push(&temp)
	if err == nil || IsErrTypeRevert(err) {
		ret = common.CopyBytes(ret)
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
		ret = common.CopyBytes(ret)
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}

	scope.Contract.Gas += returnGas

	interpreter.returnData = ret
	return ret, nil
}
