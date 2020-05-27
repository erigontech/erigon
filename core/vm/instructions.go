// Copyright 2015 The go-ethereum Authors
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
	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/params"
)

func opAdd(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y := callContext.stack.pop(), callContext.stack.peek()
	y.Add(&x, y)
	return nil, nil
}

func opSub(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y := callContext.stack.pop(), callContext.stack.peek()
	y.Sub(&x, y)
	return nil, nil
}

func opMul(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y := callContext.stack.pop(), callContext.stack.peek()
	y.Mul(&x, y)
	return nil, nil
}

func opDiv(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y := callContext.stack.pop(), callContext.stack.peek()
	y.Div(&x, y)
	return nil, nil
}

func opSdiv(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y := callContext.stack.pop(), callContext.stack.peek()
	y.SDiv(&x, y)
	return nil, nil
}

func opMod(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y := callContext.stack.pop(), callContext.stack.peek()
	y.Mod(&x, y)
	return nil, nil
}

func opSmod(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y := callContext.stack.pop(), callContext.stack.peek()
	y.SMod(&x, y)
	return nil, nil
}

func opExp(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	base, exponent := callContext.stack.pop(), callContext.stack.peek()
	switch {
	case exponent.IsZero():
		// x ^ 0 == 1
		exponent.SetOne()
	case base.IsZero():
		// 0 ^ y, if y != 0, == 0
		exponent.Clear()
	case exponent.LtUint64(2): // exponent == 1
		// x ^ 1 == x
		exponent.Set(&base)
	case base.LtUint64(2): // base == 1
		// 1 ^ y == 1
		exponent.SetOne()
	case base.LtUint64(3): // base == 2
		if exponent.LtUint64(256) {
			n := uint(exponent.Uint64())
			exponent.SetOne()
			exponent.Lsh(exponent, n)
		} else {
			exponent.Clear()
		}
	default:
		exponent.Exp(&base, exponent)
	}
	return nil, nil
}

func opSignExtend(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	back := callContext.stack.pop()
	num := callContext.stack.peek()
	num.ExtendSign(num, &back)
	return nil, nil
}

func opNot(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x := callContext.stack.peek()
	x.Not(x)
	return nil, nil
}

func opLt(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y := callContext.stack.pop(), callContext.stack.peek()
	if x.Lt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return nil, nil
}

func opGt(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y := callContext.stack.pop(), callContext.stack.peek()
	if x.Gt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return nil, nil
}

func opSlt(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y := callContext.stack.pop(), callContext.stack.peek()
	if x.Slt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return nil, nil
}

func opSgt(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y := callContext.stack.pop(), callContext.stack.peek()
	if x.Sgt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return nil, nil
}

func opEq(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y := callContext.stack.pop(), callContext.stack.peek()
	if x.Eq(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return nil, nil
}

func opIszero(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x := callContext.stack.peek()
	if x.IsZero() {
		x.SetOne()
	} else {
		x.Clear()
	}
	return nil, nil
}

func opAnd(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y := callContext.stack.pop(), callContext.stack.peek()
	y.And(&x, y)
	return nil, nil
}

func opOr(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y := callContext.stack.pop(), callContext.stack.peek()
	y.Or(&x, y)
	return nil, nil
}

func opXor(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y := callContext.stack.pop(), callContext.stack.peek()
	y.Xor(&x, y)
	return nil, nil
}

func opByte(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	th, val := callContext.stack.pop(), callContext.stack.peek()
	val.Byte(&th)
	return nil, nil
}

func opAddmod(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y, z := callContext.stack.pop(), callContext.stack.pop(), callContext.stack.peek()
	if z.IsZero() {
		z.Clear()
	} else {
		z.AddMod(&x, &y, z)
	}
	return nil, nil
}

func opMulmod(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x, y, z := callContext.stack.pop(), callContext.stack.pop(), callContext.stack.peek()
	if z.IsZero() {
		z.Clear()
	} else {
		z.MulMod(&x, &y, z)
	}
	return nil, nil
}

// opSHL implements Shift Left
// The SHL instruction (shift left) pops 2 values from the stack, first arg1 and then arg2,
// and pushes on the stack arg2 shifted to the left by arg1 number of bits.
func opSHL(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	// Note, second operand is left in the stack; accumulate result into it, and no need to push it afterwards
	shift, value := callContext.stack.pop(), callContext.stack.peek()
	if shift.LtUint64(256) {
		value.Lsh(value, uint(shift.Uint64()))
	} else {
		value.Clear()
	}
	return nil, nil
}

// opSHR implements Logical Shift Right
// The SHR instruction (logical shift right) pops 2 values from the stack, first arg1 and then arg2,
// and pushes on the stack arg2 shifted to the right by arg1 number of bits with zero fill.
func opSHR(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	// Note, second operand is left in the stack; accumulate result into it, and no need to push it afterwards
	shift, value := callContext.stack.pop(), callContext.stack.peek()
	if shift.LtUint64(256) {
		value.Rsh(value, uint(shift.Uint64()))
	} else {
		value.Clear()
	}
	return nil, nil
}

// opSAR implements Arithmetic Shift Right
// The SAR instruction (arithmetic shift right) pops 2 values from the stack, first arg1 and then arg2,
// and pushes on the stack arg2 shifted to the right by arg1 number of bits with sign extension.
func opSAR(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	shift, value := callContext.stack.pop(), callContext.stack.peek()
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
	return nil, nil
}

func opSha3(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	offset, size := callContext.stack.pop(), callContext.stack.peek()
	data := callContext.memory.GetPtr(offset.Uint64(), size.Uint64())

	if interpreter.hasher == nil {
		interpreter.hasher = sha3.NewLegacyKeccak256().(keccakState)
	} else {
		interpreter.hasher.Reset()
	}
	interpreter.hasher.Write(data)
	interpreter.hasher.Read(interpreter.hasherBuf[:])

	evm := interpreter.evm
	if evm.vmConfig.EnablePreimageRecording {
		evm.IntraBlockState.AddPreimage(interpreter.hasherBuf, data)
	}
	size.SetBytes(interpreter.hasherBuf[:])
	return nil, nil
}

func opAddress(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	callContext.stack.push(new(uint256.Int).SetBytes(callContext.contract.Address().Bytes()))
	return nil, nil
}

func opBalance(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	slot := callContext.stack.peek()
	address := common.Address(slot.Bytes20())
	slot.Set(interpreter.evm.IntraBlockState.GetBalance(address))
	return nil, nil
}

func opOrigin(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	callContext.stack.push(new(uint256.Int).SetBytes(interpreter.evm.Origin.Bytes()))
	return nil, nil
}

func opCaller(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	callContext.stack.push(new(uint256.Int).SetBytes(callContext.contract.Caller().Bytes()))
	return nil, nil
}

func opCallValue(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	callContext.stack.push(callContext.contract.value)
	return nil, nil
}

func opCallDataLoad(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	x := callContext.stack.peek()
	data := getDataBig(callContext.contract.Input, x, 32)
	x.SetBytes(data)
	return nil, nil
}

func opCallDataSize(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	callContext.stack.push(new(uint256.Int).SetUint64(uint64(len(callContext.contract.Input))))
	return nil, nil
}

func opCallDataCopy(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	var (
		memOffset  = callContext.stack.pop()
		dataOffset = callContext.stack.pop()
		length     = callContext.stack.pop()
	)
	len64 := length.Uint64()
	callContext.memory.Set(memOffset.Uint64(), len64, getDataBig(callContext.contract.Input, &dataOffset, len64))
	return nil, nil
}

func opReturnDataSize(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	callContext.stack.push(new(uint256.Int).SetUint64(uint64(len(interpreter.returnData))))
	return nil, nil
}

func opReturnDataCopy(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	var (
		memOffset  = callContext.stack.pop()
		dataOffset = callContext.stack.pop()
		length     = callContext.stack.pop()
	)

	offset64, overflow := dataOffset.Uint64WithOverflow()
	if overflow {
		return nil, ErrReturnDataOutOfBounds
	}

	// we can reuse dataOffset now (aliasing it for clarity)
	end := dataOffset
	overflow = end.AddOverflow(&dataOffset, &length)
	if overflow {
		return nil, ErrReturnDataOutOfBounds
	}

	end64, overflow := end.Uint64WithOverflow()
	if overflow || uint64(len(interpreter.returnData)) < end64 {
		return nil, ErrReturnDataOutOfBounds
	}
	callContext.memory.Set(memOffset.Uint64(), length.Uint64(), interpreter.returnData[offset64:end64])

	return nil, nil
}

func opExtCodeSize(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	slot := callContext.stack.peek()
	slot.SetUint64(uint64(interpreter.evm.IntraBlockState.GetCodeSize(common.Address(slot.Bytes20()))))

	return nil, nil
}

func opCodeSize(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	l := new(uint256.Int)
	l.SetUint64(uint64(len(callContext.contract.Code)))
	callContext.stack.push(l)

	return nil, nil
}

func opCodeCopy(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	var (
		memOffset  = callContext.stack.pop()
		codeOffset = callContext.stack.pop()
		length     = callContext.stack.pop()
	)
	len64 := length.Uint64()
	codeCopy := getDataBig(callContext.contract.Code, &codeOffset, len64)
	callContext.memory.Set(memOffset.Uint64(), len64, codeCopy)
	return nil, nil
}

func opExtCodeCopy(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	var (
		a          = callContext.stack.pop()
		memOffset  = callContext.stack.pop()
		codeOffset = callContext.stack.pop()
		length     = callContext.stack.pop()
	)
	addr := common.Address(a.Bytes20())
	len64 := length.Uint64()
	codeCopy := getDataBig(interpreter.evm.IntraBlockState.GetCode(addr), &codeOffset, len64)
	callContext.memory.Set(memOffset.Uint64(), len64, codeCopy)
	return nil, nil
}

// opExtCodeHash returns the code hash of a specified account.
// There are several cases when the function is called, while we can relay everything
// to `state.GetCodeHash` function to ensure the correctness.
//   (1) Caller tries to get the code hash of a normal contract account, state
// should return the relative code hash and set it as the result.
//
//   (2) Caller tries to get the code hash of a non-existent account, state should
// return common.Hash{} and zero will be set as the result.
//
//   (3) Caller tries to get the code hash for an account without contract code,
// state should return emptyCodeHash(0xc5d246...) as the result.
//
//   (4) Caller tries to get the code hash of a precompiled account, the result
// should be zero or emptyCodeHash.
//
// It is worth noting that in order to avoid unnecessary create and clean,
// all precompile accounts on mainnet have been transferred 1 wei, so the return
// here should be emptyCodeHash.
// If the precompile account is not transferred any amount on a private or
// customized chain, the return value will be zero.
//
//   (5) Caller tries to get the code hash for an account which is marked as suicided
// in the current transaction, the code hash of this account should be returned.
//
//   (6) Caller tries to get the code hash for an account which is marked as deleted,
// this account should be regarded as a non-existent account and zero should be returned.
func opExtCodeHash(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	slot := callContext.stack.peek()
	address := common.Address(slot.Bytes20())
	if interpreter.evm.IntraBlockState.Empty(address) {
		slot.Clear()
	} else {
		slot.SetBytes(interpreter.evm.IntraBlockState.GetCodeHash(address).Bytes())
	}
	return nil, nil
}

func opGasprice(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	v, _ := uint256.FromBig(interpreter.evm.GasPrice)
	callContext.stack.push(v)
	return nil, nil
}

func opBlockhash(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	num := callContext.stack.peek()
	num64, overflow := num.Uint64WithOverflow()
	if overflow {
		num.Clear()
		return nil, nil
	}
	upper := interpreter.evm.BlockNumber.Uint64()
	lower := uint64(0)
	if upper > 256 {
		lower = upper - 256
	}
	if num64 >= lower && num64 < upper {
		num.SetBytes(interpreter.evm.GetHash(num64).Bytes())
	} else {
		num.Clear()
	}
	return nil, nil
}

func opCoinbase(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	callContext.stack.push(new(uint256.Int).SetBytes(interpreter.evm.Coinbase.Bytes()))
	return nil, nil
}

func opTimestamp(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	v, _ := uint256.FromBig(interpreter.evm.Time)
	callContext.stack.push(v)
	return nil, nil
}

func opNumber(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	v, _ := uint256.FromBig(interpreter.evm.BlockNumber)
	callContext.stack.push(v)
	return nil, nil
}

func opDifficulty(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	v, _ := uint256.FromBig(interpreter.evm.Difficulty)
	callContext.stack.push(v)
	return nil, nil
}

func opGasLimit(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	callContext.stack.push(new(uint256.Int).SetUint64(interpreter.evm.GasLimit))
	return nil, nil
}

func opPop(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	callContext.stack.pop()
	return nil, nil
}

func opMload(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	v := callContext.stack.peek()
	offset := v.Uint64()
	v.SetBytes(callContext.memory.GetPtr(offset, 32))
	return nil, nil
}

func opMstore(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	mStart, val := callContext.stack.pop(), callContext.stack.pop()
	callContext.memory.Set32(mStart.Uint64(), &val)
	return nil, nil
}

func opMstore8(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	off, val := callContext.stack.pop(), callContext.stack.pop()
	callContext.memory.store[off.Uint64()] = byte(val.Uint64() & 0xff)
	return nil, nil
}

func opSload(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	loc := callContext.stack.peek()
	interpreter.hasherBuf = loc.Bytes32()
	interpreter.evm.IntraBlockState.GetState(callContext.contract.Address(), &interpreter.hasherBuf, loc)
	return nil, nil
}

func opSstore(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	loc := callContext.stack.pop()
	val := callContext.stack.pop()
	interpreter.hasherBuf = loc.Bytes32()
	interpreter.evm.IntraBlockState.SetState(callContext.contract.Address(), &interpreter.hasherBuf, val)
	return nil, nil
}

func opJump(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	pos := callContext.stack.pop()
	if !callContext.contract.validJumpdest(&pos) {
		return nil, ErrInvalidJump
	}
	*pc = pos.Uint64()

	return nil, nil
}

func opJumpi(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	pos, cond := callContext.stack.pop(), callContext.stack.pop()
	if !cond.IsZero() {
		if !callContext.contract.validJumpdest(&pos) {
			return nil, ErrInvalidJump
		}
		*pc = pos.Uint64()
	} else {
		*pc++
	}

	return nil, nil
}

func opJumpdest(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	return nil, nil
}

func opPc(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	callContext.stack.push(new(uint256.Int).SetUint64(*pc))
	return nil, nil
}

func opMsize(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	callContext.stack.push(new(uint256.Int).SetUint64(uint64(callContext.memory.Len())))
	return nil, nil
}

func opGas(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	callContext.stack.push(new(uint256.Int).SetUint64(callContext.contract.Gas))
	return nil, nil
}

func opCreate(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	var (
		value  = callContext.stack.pop()
		offset = callContext.stack.pop()
		size   = callContext.stack.peek()
		input  = callContext.memory.GetCopy(offset.Uint64(), size.Uint64())
		gas    = callContext.contract.Gas
	)
	if interpreter.evm.chainRules.IsEIP150 {
		gas -= gas / 64
	}

	callContext.contract.UseGas(gas)
	res, addr, returnGas, suberr := interpreter.evm.Create(callContext.contract, input, gas, &value)

	stackValue := size

	// Push item on the stack based on the returned error. If the ruleset is
	// homestead we must check for CodeStoreOutOfGasError (homestead only
	// rule) and treat as an error, if the ruleset is frontier we must
	// ignore this error and pretend the operation was successful.
	if interpreter.evm.chainRules.IsHomestead && suberr == ErrCodeStoreOutOfGas {
		stackValue.Clear()
	} else if suberr != nil && suberr != ErrCodeStoreOutOfGas {
		stackValue.Clear()
	} else {
		stackValue.SetBytes(addr.Bytes())
	}
	callContext.contract.Gas += returnGas

	if suberr == ErrExecutionReverted {
		return res, nil
	}
	return nil, nil
}

func opCreate2(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	var (
		endowment    = callContext.stack.pop()
		offset, size = callContext.stack.pop(), callContext.stack.pop()
		salt         = callContext.stack.peek()
		input        = callContext.memory.GetCopy(offset.Uint64(), size.Uint64())
		gas          = callContext.contract.Gas
	)

	// Apply EIP150
	gas -= gas / 64
	callContext.contract.UseGas(gas)
	res, addr, returnGas, suberr := interpreter.evm.Create2(callContext.contract, input, gas, &endowment, salt.ToBig())

	stackValue := salt

	// Push item on the stack based on the returned error.
	if suberr != nil {
		stackValue.Clear()
	} else {
		stackValue.SetBytes(addr.Bytes())
	}
	callContext.contract.Gas += returnGas

	if suberr == ErrExecutionReverted {
		return res, nil
	}
	return nil, nil
}

func opCall(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	// Pop gas. The actual gas in interpreter.evm.callGasTemp.
	callContext.stack.pop()
	gas := interpreter.evm.callGasTemp
	// Pop other call parameters.
	addr, value, inOffset, inSize, retOffset := callContext.stack.pop(), callContext.stack.pop(), callContext.stack.pop(), callContext.stack.pop(), callContext.stack.pop()
	retSize := callContext.stack.peek()
	toAddr := common.Address(addr.Bytes20())
	// Get the arguments from the memory.
	args := callContext.memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	if !value.IsZero() {
		gas += params.CallStipend
	}
	ret, returnGas, err := interpreter.evm.Call(callContext.contract, toAddr, args, gas, &value)
	if err == nil || err == ErrExecutionReverted {
		callContext.memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}
	if err != nil {
		retSize.Clear()
	} else {
		retSize.SetOne()
	}
	callContext.contract.Gas += returnGas

	return ret, nil
}

func opCallCode(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	callContext.stack.pop()
	gas := interpreter.evm.callGasTemp
	// Pop other call parameters.
	addr, value, inOffset, inSize, retOffset := callContext.stack.pop(), callContext.stack.pop(), callContext.stack.pop(), callContext.stack.pop(), callContext.stack.pop()
	retSize := callContext.stack.peek()
	toAddr := common.Address(addr.Bytes20())
	// Get arguments from the memory.
	args := callContext.memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	if value.Sign() != 0 {
		gas += params.CallStipend
	}
	ret, returnGas, err := interpreter.evm.CallCode(callContext.contract, toAddr, args, gas, &value)
	if err == nil || err == ErrExecutionReverted {
		callContext.memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}
	if err != nil {
		retSize.Clear()
	} else {
		retSize.SetOne()
	}
	callContext.contract.Gas += returnGas

	return ret, nil
}

func opDelegateCall(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	callContext.stack.pop()
	gas := interpreter.evm.callGasTemp
	// Pop other call parameters.
	addr, inOffset, inSize, retOffset := callContext.stack.pop(), callContext.stack.pop(), callContext.stack.pop(), callContext.stack.pop()
	retSize := callContext.stack.peek()
	toAddr := common.Address(addr.Bytes20())
	// Get arguments from the memory.
	args := callContext.memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	ret, returnGas, err := interpreter.evm.DelegateCall(callContext.contract, toAddr, args, gas)
	if err == nil || err == ErrExecutionReverted {
		callContext.memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}
	if err != nil {
		retSize.Clear()
	} else {
		retSize.SetOne()
	}
	callContext.contract.Gas += returnGas

	return ret, nil
}

func opStaticCall(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	callContext.stack.pop()
	gas := interpreter.evm.callGasTemp
	// Pop other call parameters.
	addr, inOffset, inSize, retOffset := callContext.stack.pop(), callContext.stack.pop(), callContext.stack.pop(), callContext.stack.pop()
	retSize := callContext.stack.peek()
	toAddr := common.Address(addr.Bytes20())
	// Get arguments from the memory.
	args := callContext.memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	ret, returnGas, err := interpreter.evm.StaticCall(callContext.contract, toAddr, args, gas)
	if err == nil || err == ErrExecutionReverted {
		callContext.memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}
	if err != nil {
		retSize.Clear()
	} else {
		retSize.SetOne()
	}
	callContext.contract.Gas += returnGas

	return ret, nil
}

func opReturn(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	offset, size := callContext.stack.pop(), callContext.stack.pop()
	ret := callContext.memory.GetPtr(offset.Uint64(), size.Uint64())
	return ret, nil
}

func opRevert(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	offset, size := callContext.stack.pop(), callContext.stack.pop()
	ret := callContext.memory.GetPtr(offset.Uint64(), size.Uint64())
	return ret, nil
}

func opStop(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	return nil, nil
}

func opSuicide(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	beneficiary := callContext.stack.pop()
	balance := interpreter.evm.IntraBlockState.GetBalance(callContext.contract.Address())
	interpreter.evm.IntraBlockState.AddBalance(common.Address(beneficiary.Bytes20()), balance)

	interpreter.evm.IntraBlockState.Suicide(callContext.contract.Address())
	return nil, nil
}

// following functions are used by the instruction jump  table

// make log instruction function
func makeLog(size int) executionFunc {
	return func(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
		topics := make([]common.Hash, size)
		mStart, mSize := callContext.stack.pop(), callContext.stack.pop()
		for i := 0; i < size; i++ {
			topic := callContext.stack.pop()
			topics[i] = common.Hash(topic.Bytes32())
		}

		d := callContext.memory.GetCopy(mStart.Uint64(), mSize.Uint64())
		interpreter.evm.IntraBlockState.AddLog(&types.Log{
			Address: callContext.contract.Address(),
			Topics:  topics,
			Data:    d,
			// This is a non-consensus field, but assigned here because
			// core/state doesn't know the current block number.
			BlockNumber: interpreter.evm.BlockNumber.Uint64(),
		})

		return nil, nil
	}
}

// opPush1 is a specialized version of pushN
func opPush1(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	var (
		codeLen = uint64(len(callContext.contract.Code))
		integer = new(uint256.Int)
	)
	*pc++
	if *pc < codeLen {
		callContext.stack.push(integer.SetUint64(uint64(callContext.contract.Code[*pc])))
	} else {
		callContext.stack.push(integer)
	}
	return nil, nil
}

// make push instruction function
func makePush(size uint64, pushByteSize int) executionFunc {
	return func(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
		codeLen := len(callContext.contract.Code)

		startMin := codeLen
		if int(*pc+1) < startMin {
			startMin = int(*pc + 1)
		}

		endMin := codeLen
		if startMin+pushByteSize < endMin {
			endMin = startMin + pushByteSize
		}

		integer := new(uint256.Int)
		callContext.stack.push(integer.SetBytes(common.RightPadBytes(callContext.contract.Code[startMin:endMin], pushByteSize)))

		*pc += size
		return nil, nil
	}
}

// make dup instruction function
func makeDup(size int64) executionFunc {
	return func(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
		callContext.stack.dup(int(size))
		return nil, nil
	}
}

// make swap instruction function
func makeSwap(size int64) executionFunc {
	// switch n + 1 otherwise n would be swapped with n
	size++
	return func(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
		callContext.stack.swap(int(size))
		return nil, nil
	}
}
