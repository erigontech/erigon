package vm

import (
	"math/big"

	"github.com/iden3/go-iden3-crypto/keccak256"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common"
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
	if err == nil || err == ErrExecutionReverted {
		ret = common.CopyBytes(ret)
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}

	scope.Contract.Gas += returnGas

	//[zkevm] do not overryde returnData if reverted
	if err != ErrExecutionReverted {
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
