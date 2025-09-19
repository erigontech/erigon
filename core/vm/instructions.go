// Copyright 2015 The go-ethereum Authors
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
	"errors"
	"fmt"
	"math"

	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/types"
)

func opAdd(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.Add(&x, y)
	return nil, nil
}

func stAdd(_ uint64, scope *ScopeContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", ADD, &x, &y)
}

func opSub(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.Sub(&x, y)
	return nil, nil
}

func stSub(_ uint64, scope *ScopeContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", SUB, &x, &y)
}

func opMul(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.Mul(&x, y)
	return nil, nil
}

func stMul(_ uint64, scope *ScopeContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", MUL, &x, &y)
}

func opDiv(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.Div(&x, y)
	return nil, nil
}

func stDiv(_ uint64, scope *ScopeContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", DIV, &x, &y)
}

func opSdiv(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.SDiv(&x, y)
	return nil, nil
}

func stSdiv(_ uint64, scope *ScopeContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", SDIV, &x, &y)
}

func opMod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.Mod(&x, y)
	return nil, nil
}

func stMod(_ uint64, scope *ScopeContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", MOD, &x, &y)
}

func opSmod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.SMod(&x, y)
	return nil, nil
}

func stSmod(_ uint64, scope *ScopeContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", SMOD, &x, &y)
}

func opExp(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	base, exponent := scope.Stack.pop(), scope.Stack.peek()
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

func opSignExtend(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	back, num := scope.Stack.pop(), scope.Stack.peek()
	num.ExtendSign(num, &back)
	return nil, nil
}

func opNot(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x := scope.Stack.peek()
	x.Not(x)
	return nil, nil
}

func stNot(_ uint64, scope *ScopeContext) string {
	x := scope.Stack.peek()
	return fmt.Sprintf("%s %d", NOT.String(), x)
}

func opLt(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	if x.Lt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return nil, nil
}

func stLt(_ uint64, scope *ScopeContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", LT, &x, &y)
}

func opGt(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	if x.Gt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return nil, nil
}

func stGt(_ uint64, scope *ScopeContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", GT, &x, &y)
}

func opSlt(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	if x.Slt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return nil, nil
}

func stSlt(_ uint64, scope *ScopeContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", SLT, &x, &y)
}

func opSgt(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	if x.Sgt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return nil, nil
}

func stSgt(_ uint64, scope *ScopeContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", SLT, &x, &y)
}

func opEq(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	if x.Eq(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return nil, nil
}

func stEq(_ uint64, scope *ScopeContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", EQ, &x, &y)
}

func opIszero(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x := scope.Stack.peek()
	if x.IsZero() {
		x.SetOne()
	} else {
		x.Clear()
	}
	return nil, nil
}

func stIsZero(_ uint64, scope *ScopeContext) string {
	x := scope.Stack.data[len(scope.Stack.data)-1]
	return fmt.Sprintf("%s %d", ISZERO, &x)
}

func opAnd(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.And(&x, y)
	return nil, nil
}

func stAnd(_ uint64, scope *ScopeContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", AND, &x, &y)
}

func opOr(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.Or(&x, y)
	return nil, nil
}

func stOr(_ uint64, scope *ScopeContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", OR, &x, &y)
}

func opXor(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.Xor(&x, y)
	return nil, nil
}

func stXor(_ uint64, scope *ScopeContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", XOR, &x, &y)
}

func opByte(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	th, val := scope.Stack.pop(), scope.Stack.peek()
	val.Byte(&th)
	return nil, nil
}

func opAddmod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y, z := scope.Stack.pop(), scope.Stack.pop(), scope.Stack.peek()
	z.AddMod(&x, &y, z)
	return nil, nil
}

func stAddmod(_ uint64, scope *ScopeContext) string {
	x, y, z := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2], scope.Stack.data[len(scope.Stack.data)-3]
	return fmt.Sprintf("%s %d %d %d", ADDMOD, &x, &y, &z)
}

func opMulmod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y, z := scope.Stack.pop(), scope.Stack.pop(), scope.Stack.peek()
	z.MulMod(&x, &y, z)
	return nil, nil
}

func stMulmod(_ uint64, scope *ScopeContext) string {
	x, y, z := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2], scope.Stack.data[len(scope.Stack.data)-3]
	return fmt.Sprintf("%s %d %d %d", MULMOD, &x, &y, &z)
}

// opSHL implements Shift Left
// The SHL instruction (shift left) pops 2 values from the stack, first arg1 and then arg2,
// and pushes on the stack arg2 shifted to the left by arg1 number of bits.
func opSHL(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	// Note, second operand is left in the stack; accumulate result into it, and no need to push it afterwards
	shift, value := scope.Stack.pop(), scope.Stack.peek()
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
func opSHR(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	// Note, second operand is left in the stack; accumulate result into it, and no need to push it afterwards
	shift, value := scope.Stack.pop(), scope.Stack.peek()
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
func opSAR(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	shift, value := scope.Stack.pop(), scope.Stack.peek()
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

func opKeccak256(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	offset, size := scope.Stack.pop(), scope.Stack.peek()
	data := scope.Memory.GetPtr(offset.Uint64(), size.Uint64())

	if interpreter.hasher == nil {
		interpreter.hasher = sha3.NewLegacyKeccak256().(keccakState)
	} else {
		interpreter.hasher.Reset()
	}
	interpreter.hasher.Write(data)
	if _, err := interpreter.hasher.Read(interpreter.hasherBuf[:]); err != nil {
		panic(err)
	}

	size.SetBytes(interpreter.hasherBuf[:])
	return nil, nil
}
func opAddress(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetBytes(scope.Contract.Address().Bytes()))
	return nil, nil
}

func opBalance(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	slot := scope.Stack.peek()
	address := common.Address(slot.Bytes20())
	balance, err := interpreter.evm.IntraBlockState().GetBalance(address)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	slot.Set(&balance)
	return nil, nil
}

func opOrigin(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetBytes(interpreter.evm.Origin[:]))
	return nil, nil
}
func opCaller(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	caller := scope.Contract.Caller()
	scope.Stack.push(new(uint256.Int).SetBytes(caller[:]))
	return nil, nil
}

func opCallValue(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(scope.Contract.value)
	return nil, nil
}

func opCallDataLoad(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x := scope.Stack.peek()
	if offset, overflow := x.Uint64WithOverflow(); !overflow {
		data := getData(scope.Contract.Input, offset, 32)
		x.SetBytes(data)
	} else {
		x.Clear()
	}
	return nil, nil
}

func stCallDataLoad(_ uint64, scope *ScopeContext) string {
	x := *scope.Stack.peek()
	var data []byte
	if offset, overflow := x.Uint64WithOverflow(); !overflow {
		data = getData(scope.Contract.Input, offset, 32)
	}

	return fmt.Sprintf("%s %d (%x)", CALLDATALOAD, &x, data)
}

func opCallDataSize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetUint64(uint64(len(scope.Contract.Input))))
	return nil, nil
}

func stCallDataSize(_ uint64, scope *ScopeContext) string {
	return fmt.Sprintf("%s (%d)", CALLDATASIZE, new(uint256.Int).SetUint64(uint64(len(scope.Contract.Input))))
}

func opCallDataCopy(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		memOffset  = scope.Stack.pop()
		dataOffset = scope.Stack.pop()
		length     = scope.Stack.pop()
	)
	dataOffset64, overflow := dataOffset.Uint64WithOverflow()
	if overflow {
		dataOffset64 = math.MaxUint64
	}
	// These values are checked for overflow during gas cost calculation
	memOffset64 := memOffset.Uint64()
	length64 := length.Uint64()
	scope.Memory.Set(memOffset64, length64, getData(scope.Contract.Input, dataOffset64, length64))
	return nil, nil
}

func stCallDataCopy(_ uint64, scope *ScopeContext) string {
	var (
		memOffset  = scope.Stack.data[len(scope.Stack.data)-1]
		dataOffset = scope.Stack.data[len(scope.Stack.data)-2]
		length     = scope.Stack.data[len(scope.Stack.data)-3]
	)
	dataOffset64, overflow := dataOffset.Uint64WithOverflow()
	if overflow {
		dataOffset64 = math.MaxUint64
	}
	return fmt.Sprintf("%s %d %d %d (%x)", CALLDATACOPY, memOffset.Uint64(), dataOffset64, length.Uint64(), getData(scope.Contract.Input, dataOffset64, length.Uint64()))
}

func opReturnDataSize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetUint64(uint64(len(interpreter.returnData))))
	return nil, nil
}

func opReturnDataCopy(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		memOffset  = scope.Stack.pop()
		dataOffset = scope.Stack.pop()
		length     = scope.Stack.pop()
	)

	offset64, overflow := dataOffset.Uint64WithOverflow()
	if overflow {
		return nil, ErrReturnDataOutOfBounds
	}
	// we can reuse dataOffset now (aliasing it for clarity)
	end := dataOffset
	_, overflow = end.AddOverflow(&dataOffset, &length)
	if overflow {
		return nil, ErrReturnDataOutOfBounds
	}

	end64, overflow := end.Uint64WithOverflow()
	if overflow || uint64(len(interpreter.returnData)) < end64 {
		return nil, ErrReturnDataOutOfBounds
	}
	scope.Memory.Set(memOffset.Uint64(), length.Uint64(), interpreter.returnData[offset64:end64])
	return nil, nil
}

func stReturnDataCopy(_ uint64, scope *ScopeContext) string {
	var (
		memOffset  = scope.Stack.data[len(scope.Stack.data)-1]
		dataOffset = scope.Stack.data[len(scope.Stack.data)-2]
		length     = scope.Stack.data[len(scope.Stack.data)-3]
	)

	offset64, overflow := dataOffset.Uint64WithOverflow()
	if overflow {
		return fmt.Sprintf("%s %d %d %d (%s)", RETURNDATACOPY, memOffset.Uint64(), offset64, length.Uint64(), ErrReturnDataOutOfBounds)
	}
	// we can reuse dataOffset now (aliasing it for clarity)
	end := dataOffset
	_, overflow = end.AddOverflow(&dataOffset, &length)
	if overflow {
		return fmt.Sprintf("%s %d %d %d (%s)", RETURNDATACOPY, memOffset.Uint64(), offset64, length.Uint64(), ErrReturnDataOutOfBounds)
	}

	//end64, overflow := end.Uint64WithOverflow()
	//if overflow || uint64(len(interpreter.returnData)) < end64 {
	//	return fmt.Sprintf("%s %d %d %d (%s)", RETURNDATACOPY, memOffset.Uint64(), offset64, length.Uint64(), ErrReturnDataOutOfBounds)
	//}

	// return fmt.Sprintf("%s %d %d %d (%x)", RETURNDATACOPY, memOffset.Uint64(), offset64, length.Uint64(), interpreter.returnData[offset64:end64])
	return fmt.Sprintf("%s %d %d %d", RETURNDATACOPY, memOffset.Uint64(), offset64, length.Uint64())
}

func opExtCodeSize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	slot := scope.Stack.peek()
	addr := slot.Bytes20()
	codeSize, err := interpreter.evm.IntraBlockState().GetCodeSize(addr)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	slot.SetUint64(uint64(codeSize))
	return nil, nil
}

func opCodeSize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	l := new(uint256.Int)
	l.SetUint64(uint64(len(scope.Contract.Code)))
	scope.Stack.push(l)
	return nil, nil
}

func opCodeCopy(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		memOffset  = scope.Stack.pop()
		codeOffset = scope.Stack.pop()
		length     = scope.Stack.pop()
	)
	uint64CodeOffset, overflow := codeOffset.Uint64WithOverflow()
	if overflow {
		uint64CodeOffset = math.MaxUint64
	}
	codeCopy := getData(scope.Contract.Code, uint64CodeOffset, length.Uint64())
	scope.Memory.Set(memOffset.Uint64(), length.Uint64(), codeCopy)
	return nil, nil
}

func opExtCodeCopy(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		stack      = scope.Stack
		a          = stack.pop()
		memOffset  = stack.pop()
		codeOffset = stack.pop()
		length     = stack.pop()
	)
	addr := common.Address(a.Bytes20())
	len64 := length.Uint64()

	code, err := interpreter.evm.IntraBlockState().GetCode(addr)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}

	codeCopy := getDataBig(code, &codeOffset, len64)
	scope.Memory.Set(memOffset.Uint64(), len64, codeCopy)
	return nil, nil
}

// opExtCodeHash returns the code hash of a specified account.
// There are several cases when the function is called, while we can relay everything
// to `state.ResolveCodeHash` function to ensure the correctness.
//
//	(1) Caller tries to get the code hash of a normal contract account, state
//
// should return the relative code hash and set it as the result.
//
//	(2) Caller tries to get the code hash of a non-existent account, state should
//
// return common.Hash{} and zero will be set as the result.
//
//	(3) Caller tries to get the code hash for an account without contract code,
//
// state should return emptyCodeHash(0xc5d246...) as the result.
//
//	(4) Caller tries to get the code hash of a precompiled account, the result
//
// should be zero or emptyCodeHash.
//
// It is worth noting that in order to avoid unnecessary create and clean,
// all precompile accounts on mainnet have been transferred 1 wei, so the return
// here should be emptyCodeHash.
// If the precompile account is not transferred any amount on a private or
// customized chain, the return value will be zero.
//
//	(5) Caller tries to get the code hash for an account which is marked as suicided
//
// in the current transaction, the code hash of this account should be returned.
//
//	(6) Caller tries to get the code hash for an account which is marked as deleted,
//
// this account should be regarded as a non-existent account and zero should be returned.
//
//	(7) Caller tries to get the code hash of a delegated account, the result should be
//
// equal the result of calling extcodehash on the account directly.
func opExtCodeHash(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	slot := scope.Stack.peek()
	address := common.Address(slot.Bytes20())

	empty, err := interpreter.evm.IntraBlockState().Empty(address)
	if err != nil {
		return nil, err
	}
	if empty {
		slot.Clear()
	} else {
		var codeHash common.Hash
		codeHash, err = interpreter.evm.IntraBlockState().GetCodeHash(address)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		slot.SetBytes(codeHash.Bytes())
	}
	return nil, nil
}

func opGasprice(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(interpreter.evm.GasPrice)
	return nil, nil
}

// opBlockhash executes the BLOCKHASH opcode
func opBlockhash(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	arg := scope.Stack.peek()
	arg64, overflow := arg.Uint64WithOverflow()
	if overflow {
		arg.Clear()
		return nil, nil
	}
	var upper, lower uint64
	upper = interpreter.evm.Context.BlockNumber
	if upper <= params.BlockHashOldWindow {
		lower = 0
	} else {
		lower = upper - params.BlockHashOldWindow
	}
	if arg64 >= lower && arg64 < upper {
		hash, err := interpreter.evm.Context.GetHash(arg64)
		if err != nil {
			arg.Clear()
			return nil, err
		}
		arg.SetBytes(hash.Bytes())
	} else {
		arg.Clear()
	}

	return nil, nil
}

func stBlockhash(_ uint64, scope *ScopeContext) string {
	x := *scope.Stack.peek()
	return fmt.Sprintf("%s %d", BLOCKHASH, &x)
}

func opCoinbase(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetBytes(interpreter.evm.Context.Coinbase.Bytes()))
	return nil, nil
}

func opTimestamp(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	v := new(uint256.Int).SetUint64(interpreter.evm.Context.Time)
	scope.Stack.push(v)
	return nil, nil
}

func opNumber(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	v := new(uint256.Int).SetUint64(interpreter.evm.Context.BlockNumber)
	scope.Stack.push(v)
	return nil, nil
}

func opDifficulty(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var v *uint256.Int
	if interpreter.evm.Context.PrevRanDao != nil {
		// EIP-4399: Supplant DIFFICULTY opcode with PREVRANDAO
		v = new(uint256.Int).SetBytes(interpreter.evm.Context.PrevRanDao.Bytes())
	} else {
		var overflow bool
		v, overflow = uint256.FromBig(interpreter.evm.Context.Difficulty)
		if overflow {
			return nil, errors.New("interpreter.evm.Context.Difficulty higher than 2^256-1")
		}
	}
	scope.Stack.push(v)
	return nil, nil
}

func opGasLimit(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.evm.Context.MaxGasLimit {
		scope.Stack.push(new(uint256.Int).SetAllOne())
	} else {
		scope.Stack.push(new(uint256.Int).SetUint64(interpreter.evm.Context.GasLimit))
	}
	return nil, nil
}

func opPop(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.pop()
	return nil, nil
}

func opMload(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	v := scope.Stack.peek()
	offset := v.Uint64()
	v.SetBytes(scope.Memory.GetPtr(offset, 32))
	return nil, nil
}

func stMload(_ uint64, scope *ScopeContext) string {
	v := scope.Stack.peek()
	offset := v.Uint64()
	return fmt.Sprintf("%s %d (%d)", MLOAD, offset, (&uint256.Int{}).SetBytes(scope.Memory.GetPtr(offset, 32)))
}

func opMstore(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	mStart, val := scope.Stack.pop(), scope.Stack.pop()
	scope.Memory.Set32(mStart.Uint64(), &val)
	return nil, nil
}

func stMstore(_ uint64, scope *ScopeContext) string {
	mStart, val := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", MSTORE, mStart.Uint64(), &val)
}

func opMstore8(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	off, val := scope.Stack.pop(), scope.Stack.pop()
	scope.Memory.store[off.Uint64()] = byte(val.Uint64())
	return nil, nil
}

func opSload(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	loc := scope.Stack.peek()
	err := interpreter.evm.IntraBlockState().GetState(scope.Contract.Address(), loc.Bytes32(), loc)
	return nil, err
}

func stSload(_ uint64, scope *ScopeContext) string {
	loc := scope.Stack.peek()
	return fmt.Sprintf("%s %d", SLOAD, loc)
}

func opSstore(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	loc := scope.Stack.pop()
	val := scope.Stack.pop()
	return nil, interpreter.evm.IntraBlockState().SetState(scope.Contract.Address(), loc.Bytes32(), val)
}

func stSstore(_ uint64, scope *ScopeContext) string {
	loc, val := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", SSTORE, &loc, &val)
}

func opJump(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	pos := scope.Stack.pop()
	if valid, usedBitmap := scope.Contract.validJumpdest(&pos); !valid {
		if usedBitmap {
			if interpreter.cfg.TraceJumpDest {
				log.Debug("Code Bitmap used for detecting invalid jump",
					"tx", fmt.Sprintf("0x%x", interpreter.evm.TxHash),
					"block_num", interpreter.evm.Context.BlockNumber,
				)
			} else {
				// This is "cheaper" version because it does not require calculation of txHash for each transaction
				log.Debug("Code Bitmap used for detecting invalid jump",
					"block_num", interpreter.evm.Context.BlockNumber,
				)
			}
		}
		return nil, ErrInvalidJump
	}
	*pc = pos.Uint64() - 1 // pc will be increased by the interpreter loop
	return nil, nil
}

func stJump(_ uint64, scope *ScopeContext) string {
	pos := scope.Stack.peek()
	return fmt.Sprintf("%s %d", JUMP, pos)
}

func opJumpi(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	pos, cond := scope.Stack.pop(), scope.Stack.pop()
	if !cond.IsZero() {
		if valid, usedBitmap := scope.Contract.validJumpdest(&pos); !valid {
			if usedBitmap {
				if interpreter.cfg.TraceJumpDest {
					log.Warn("Code Bitmap used for detecting invalid jump",
						"tx", fmt.Sprintf("0x%x", interpreter.evm.TxHash),
						"block_num", interpreter.evm.Context.BlockNumber,
					)
				} else {
					// This is "cheaper" version because it does not require calculation of txHash for each transaction
					log.Warn("Code Bitmap used for detecting invalid jump",
						"block_num", interpreter.evm.Context.BlockNumber,
					)
				}
			}
			return nil, ErrInvalidJump
		}
		*pc = pos.Uint64() - 1 // pc will be increased by the interpreter loop
	}
	return nil, nil
}

func stJumpi(_ uint64, scope *ScopeContext) string {
	pos, cond := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %v %d", JUMPI, !cond.IsZero(), &pos)
}

func opJumpdest(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	return nil, nil
}

func opPc(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetUint64(*pc))
	return nil, nil
}

func stPc(pc uint64, scope *ScopeContext) string {
	return fmt.Sprintf("%s %d", PC, pc)
}

func opMsize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetUint64(uint64(scope.Memory.Len())))
	return nil, nil
}

func opGas(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetUint64(scope.Contract.Gas))
	return nil, nil
}

func opSwap1(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap1()
	return nil, nil
}

func opSwap2(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap2()
	return nil, nil
}

func opSwap3(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap3()
	return nil, nil
}

func opSwap4(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap4()
	return nil, nil
}

func opSwap5(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap5()
	return nil, nil
}

func opSwap6(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap6()
	return nil, nil
}

func opSwap7(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap7()
	return nil, nil
}

func opSwap8(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap8()
	return nil, nil
}

func opSwap9(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap9()
	return nil, nil
}

func opSwap10(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap10()
	return nil, nil
}

func opSwap11(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap11()
	return nil, nil
}

func opSwap12(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap12()
	return nil, nil
}

func opSwap13(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap13()
	return nil, nil
}

func opSwap14(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap14()
	return nil, nil
}

func opSwap15(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap15()
	return nil, nil
}

func opSwap16(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.swap16()
	return nil, nil
}

func opCreate(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	var (
		value  = scope.Stack.pop()
		offset = scope.Stack.pop()
		size   = scope.Stack.peek()
		input  = scope.Memory.GetCopy(offset.Uint64(), size.Uint64())
		gas    = scope.Contract.Gas
	)
	if interpreter.evm.ChainRules().IsTangerineWhistle {
		gas -= gas / 64
	}
	// reuse size int for stackvalue
	stackvalue := size

	scope.Contract.UseGas(gas, interpreter.evm.Config().Tracer, tracing.GasChangeCallContractCreation)

	res, addr, returnGas, suberr := interpreter.evm.Create(scope.Contract, input, gas, &value, false)

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

	scope.Contract.RefundGas(returnGas, interpreter.evm.config.Tracer, tracing.GasChangeCallLeftOverRefunded)

	if suberr == ErrExecutionReverted {
		interpreter.returnData = res // set REVERT data to return data buffer
		return res, nil
	}
	interpreter.returnData = nil // clear dirty return data buffer
	return nil, nil
}

func opCreate2(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	var (
		endowment    = scope.Stack.pop()
		offset, size = scope.Stack.pop(), scope.Stack.pop()
		salt         = scope.Stack.pop()
		input        = scope.Memory.GetCopy(offset.Uint64(), size.Uint64())
		gas          = scope.Contract.Gas
	)

	// Apply EIP150
	gas -= gas / 64
	scope.Contract.UseGas(gas, interpreter.evm.Config().Tracer, tracing.GasChangeCallContractCreation2)
	// reuse size int for stackvalue
	stackValue := size
	res, addr, returnGas, suberr := interpreter.evm.Create2(scope.Contract, input, gas, &endowment, &salt, false)

	// Push item on the stack based on the returned error.
	if suberr != nil {
		stackValue.Clear()
	} else {
		stackValue.SetBytes(addr.Bytes())
	}

	scope.Stack.push(&stackValue)
	scope.Contract.RefundGas(returnGas, interpreter.evm.config.Tracer, tracing.GasChangeCallLeftOverRefunded)

	if suberr == ErrExecutionReverted {
		interpreter.returnData = res // set REVERT data to return data buffer
		return res, nil
	}
	interpreter.returnData = nil // clear dirty return data buffer
	return nil, nil
}

func opCall(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	stack := scope.Stack
	// Pop gas. The actual gas in interpreter.evm.callGasTemp.
	// We can use this as a temporary value
	temp := stack.pop()
	gas := interpreter.evm.CallGasTemp()
	// Pop other call parameters.
	addr, value, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	toAddr := common.Address(addr.Bytes20())
	// Get the arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	if !value.IsZero() {
		if interpreter.readOnly {
			return nil, ErrWriteProtection
		}
		gas += params.CallStipend
	}

	ret, returnGas, err := interpreter.evm.Call(scope.Contract, toAddr, args, gas, &value, false /* bailout */)

	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.push(&temp)
	if err == nil || err == ErrExecutionReverted {
		ret = common.CopyBytes(ret)
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}

	scope.Contract.RefundGas(returnGas, interpreter.evm.config.Tracer, tracing.GasChangeCallLeftOverRefunded)

	interpreter.returnData = ret
	return ret, nil
}

func stCall(_ uint64, scope *ScopeContext) string {
	stack := scope.Stack
	addr, _, inOffset, inSize := stack.data[len(stack.data)-2], stack.data[len(stack.data)-3], stack.data[len(stack.data)-4], stack.data[len(stack.data)-5]
	toAddr := common.Address(addr.Bytes20())
	// Get the arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	return fmt.Sprintf("%s %x %x", CALL.String(), toAddr, args)
}

func opCallCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	stack := scope.Stack
	// We use it as a temporary value
	temp := stack.pop()
	gas := interpreter.evm.CallGasTemp()
	// Pop other call parameters.
	addr, value, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	toAddr := common.Address(addr.Bytes20())
	// Get arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	if !value.IsZero() {
		gas += params.CallStipend
	}

	ret, returnGas, err := interpreter.evm.CallCode(scope.Contract, toAddr, args, gas, &value)
	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.push(&temp)
	if err == nil || err == ErrExecutionReverted {
		ret = common.CopyBytes(ret)
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}

	scope.Contract.RefundGas(returnGas, interpreter.evm.config.Tracer, tracing.GasChangeCallLeftOverRefunded)

	interpreter.returnData = ret
	return ret, nil
}

func stCallCode(_ uint64, scope *ScopeContext) string {
	stack := scope.Stack
	addr, _, inOffset, inSize := stack.data[len(stack.data)-2], stack.data[len(stack.data)-3], stack.data[len(stack.data)-4], stack.data[len(stack.data)-5]
	toAddr := common.Address(addr.Bytes20())
	// Get the arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	return fmt.Sprintf("%s %x %x", CALLCODE.String(), toAddr, args)
}

func opDelegateCall(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	stack := scope.Stack
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	// We use it as a temporary value
	temp := stack.pop()
	gas := interpreter.evm.CallGasTemp()
	// Pop other call parameters.
	addr, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	toAddr := common.Address(addr.Bytes20())
	// Get arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	ret, returnGas, err := interpreter.evm.DelegateCall(scope.Contract, toAddr, args, gas)
	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.push(&temp)
	if err == nil || err == ErrExecutionReverted {
		ret = common.CopyBytes(ret)
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}

	scope.Contract.RefundGas(returnGas, interpreter.evm.config.Tracer, tracing.GasChangeCallLeftOverRefunded)

	interpreter.returnData = ret
	return ret, nil
}

func stDelegateCall(_ uint64, scope *ScopeContext) string {
	stack := scope.Stack
	addr, _, inOffset, inSize := stack.data[len(stack.data)-2], stack.data[len(stack.data)-3], stack.data[len(stack.data)-4], stack.data[len(stack.data)-5]
	toAddr := common.Address(addr.Bytes20())
	// Get the arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	return fmt.Sprintf("%s %x %x", DELEGATECALL.String(), toAddr, args)
}

func opStaticCall(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	stack := scope.Stack
	// We use it as a temporary value
	temp := stack.pop()
	gas := interpreter.evm.CallGasTemp()
	// Pop other call parameters.
	addr, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	toAddr := common.Address(addr.Bytes20())
	// Get arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	ret, returnGas, err := interpreter.evm.StaticCall(scope.Contract, toAddr, args, gas)
	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.push(&temp)
	if err == nil || err == ErrExecutionReverted {
		ret = common.CopyBytes(ret)
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}

	scope.Contract.RefundGas(returnGas, interpreter.evm.config.Tracer, tracing.GasChangeCallLeftOverRefunded)

	interpreter.returnData = ret
	return ret, nil
}

func stStaticCall(_ uint64, scope *ScopeContext) string {
	stack := scope.Stack
	addr, inOffset, inSize := stack.data[len(stack.data)-2], stack.data[len(stack.data)-3], stack.data[len(stack.data)-4]
	toAddr := common.Address(addr.Bytes20())
	// Get arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	return fmt.Sprintf("%s %x %x", STATICCALL.String(), toAddr, args)
}

func opReturn(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	offset, size := scope.Stack.pop(), scope.Stack.pop()
	ret := scope.Memory.GetCopy(offset.Uint64(), size.Uint64())
	return ret, errStopToken
}

func opRevert(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	offset, size := scope.Stack.pop(), scope.Stack.pop()
	ret := scope.Memory.GetCopy(offset.Uint64(), size.Uint64())
	interpreter.returnData = ret
	return ret, ErrExecutionReverted
}

func opUndefined(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	return nil, &ErrInvalidOpCode{opcode: OpCode(scope.Contract.Code[*pc])}
}

func opStop(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	return nil, errStopToken
}

func opSelfdestruct(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	beneficiary := scope.Stack.pop()
	callerAddr := scope.Contract.Address()
	beneficiaryAddr := common.Address(beneficiary.Bytes20())
	balance, err := interpreter.evm.IntraBlockState().GetBalance(callerAddr)
	if err != nil {
		return nil, err
	}

	interpreter.evm.IntraBlockState().AddBalance(beneficiaryAddr, balance, tracing.BalanceIncreaseSelfdestruct)
	interpreter.evm.IntraBlockState().Selfdestruct(callerAddr)
	if interpreter.evm.Config().Tracer != nil && interpreter.evm.Config().Tracer.OnEnter != nil {
		interpreter.evm.Config().Tracer.OnEnter(interpreter.Depth(), byte(SELFDESTRUCT), scope.Contract.Address(), beneficiary.Bytes20(), false, []byte{}, 0, &balance, nil)
	}
	if interpreter.evm.Config().Tracer != nil && interpreter.evm.Config().Tracer.OnExit != nil {
		interpreter.evm.Config().Tracer.OnExit(interpreter.Depth(), []byte{}, 0, nil, false)
	}
	return nil, errStopToken
}

func opSelfdestruct6780(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	beneficiary := scope.Stack.pop()
	callerAddr := scope.Contract.Address()
	beneficiaryAddr := common.Address(beneficiary.Bytes20())
	balance, err := interpreter.evm.IntraBlockState().GetBalance(callerAddr)
	if err != nil {
		return nil, err
	}
	interpreter.evm.IntraBlockState().SubBalance(callerAddr, balance, tracing.BalanceDecreaseSelfdestruct)
	interpreter.evm.IntraBlockState().AddBalance(beneficiaryAddr, balance, tracing.BalanceIncreaseSelfdestruct)
	interpreter.evm.IntraBlockState().Selfdestruct6780(callerAddr)
	if interpreter.evm.Config().Tracer != nil && interpreter.evm.Config().Tracer.OnEnter != nil {
		interpreter.cfg.Tracer.OnEnter(interpreter.Depth(), byte(SELFDESTRUCT), scope.Contract.Address(), beneficiary.Bytes20(), false, []byte{}, 0, &balance, nil)
	}
	if interpreter.evm.Config().Tracer != nil && interpreter.evm.Config().Tracer.OnExit != nil {
		interpreter.cfg.Tracer.OnExit(interpreter.Depth(), []byte{}, 0, nil, false)
	}
	return nil, errStopToken
}

// following functions are used by the instruction jump  table

// make log instruction function
func makeLog(size int) executionFunc {
	return func(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
		if interpreter.readOnly {
			return nil, ErrWriteProtection
		}
		topics := make([]common.Hash, size)
		stack := scope.Stack
		mStart, mSize := stack.pop(), stack.pop()
		for i := 0; i < size; i++ {
			addr := stack.pop()
			topics[i] = addr.Bytes32()
		}

		d := scope.Memory.GetCopy(mStart.Uint64(), mSize.Uint64())
		interpreter.evm.IntraBlockState().AddLog(&types.Log{
			Address: scope.Contract.Address(),
			Topics:  topics,
			Data:    d,
			// This is a non-consensus field, but assigned here because
			// core/state doesn't know the current block number.
			BlockNumber: interpreter.evm.Context.BlockNumber,
		})

		return nil, nil
	}
}

// opPush1 is a specialized version of pushN
func opPush1(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		codeLen = uint64(len(scope.Contract.Code))
		integer = new(uint256.Int)
	)
	*pc++
	if *pc < codeLen {
		scope.Stack.push(integer.SetUint64(uint64(scope.Contract.Code[*pc])))
	} else {
		scope.Stack.push(integer.Clear())
	}
	return nil, nil
}

func stPush1(pc uint64, scope *ScopeContext) string {
	var (
		codeLen = uint64(len(scope.Contract.Code))
		integer = new(uint256.Int)
	)
	pc++
	if pc < codeLen {
		return fmt.Sprintf("%s %d", PUSH1.String(), integer.SetUint64(uint64(scope.Contract.Code[pc])))
	}

	return fmt.Sprintf("%s %d", PUSH1.String(), integer.Clear())
}

// opPush2 is a specialized version of pushN
func opPush2(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		codeLen = uint64(len(scope.Contract.Code))
		integer = new(uint256.Int)
	)

	if *pc+2 < codeLen {
		scope.Stack.push(integer.SetBytes2(scope.Contract.Code[*pc+1 : *pc+3]))
	} else if *pc+1 < codeLen {
		scope.Stack.push(integer.SetUint64(uint64(scope.Contract.Code[*pc+1]) << 8))
	} else {
		scope.Stack.push(integer.Clear())
	}
	*pc += 2
	return nil, nil
}

// make push instruction function
func makePush(size uint64, pushByteSize int) executionFunc {
	return func(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
		codeLen := len(scope.Contract.Code)

		startMin := int(*pc + 1)
		if startMin >= codeLen {
			startMin = codeLen
		}
		endMin := startMin + pushByteSize
		if startMin+pushByteSize >= codeLen {
			endMin = codeLen
		}

		integer := new(uint256.Int)
		scope.Stack.push(integer.SetBytes(common.RightPadBytes(
			// So it doesn't matter what we push onto the stack.
			scope.Contract.Code[startMin:endMin], pushByteSize)))

		*pc += size
		return nil, nil
	}
}

func makePushStringer(size uint64, pushByteSize int) stringer {
	return func(pc uint64, scope *ScopeContext) string {
		codeLen := len(scope.Contract.Code)

		startMin := int(pc + 1)
		if startMin >= codeLen {
			startMin = codeLen
		}
		endMin := startMin + pushByteSize
		if startMin+pushByteSize >= codeLen {
			endMin = codeLen
		}

		integer := new(uint256.Int)
		integer.SetBytes(common.RightPadBytes(scope.Contract.Code[startMin:endMin], pushByteSize))
		return fmt.Sprintf("%s%d %d", "PUSH", size, integer)
	}
}

// make dup instruction function
func makeDup(size int64) executionFunc {
	return func(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
		scope.Stack.dup(int(size))
		return nil, nil
	}
}

func makeDupStringer(n int) stringer {
	return func(pc uint64, scope *ScopeContext) string {
		return fmt.Sprintf("DUP%d (%d)", n, &scope.Stack.data[len(scope.Stack.data)-n])
	}
}

func makeSwapStringer(n int) stringer {
	return func(pc uint64, scope *ScopeContext) string {
		return fmt.Sprintf("SWAP%d (%d %d)", n, &scope.Stack.data[len(scope.Stack.data)-1], &scope.Stack.data[len(scope.Stack.data)-(n+1)])
	}
}
