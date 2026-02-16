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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/keccak"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func opAdd(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.Add(&x, y)
	return pc, nil, nil
}

func stAdd(_ uint64, scope *CallContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", ADD, &x, &y)
}

func opSub(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.Sub(&x, y)
	return pc, nil, nil
}

func stSub(_ uint64, scope *CallContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", SUB, &x, &y)
}

func opMul(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.Mul(&x, y)
	return pc, nil, nil
}

func stMul(_ uint64, scope *CallContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", MUL, &x, &y)
}

func opDiv(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.Div(&x, y)
	return pc, nil, nil
}

func stDiv(_ uint64, scope *CallContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", DIV, &x, &y)
}

func opSdiv(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.SDiv(&x, y)
	return pc, nil, nil
}

func stSdiv(_ uint64, scope *CallContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", SDIV, &x, &y)
}

func opMod(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.Mod(&x, y)
	return pc, nil, nil
}

func stMod(_ uint64, scope *CallContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", MOD, &x, &y)
}

func opSmod(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.SMod(&x, y)
	return pc, nil, nil
}

func stSmod(_ uint64, scope *CallContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", SMOD, &x, &y)
}

func opExp(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
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
	return pc, nil, nil
}

func opSignExtend(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	back, num := scope.Stack.pop(), scope.Stack.peek()
	num.ExtendSign(num, &back)
	return pc, nil, nil
}

func opNot(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x := scope.Stack.peek()
	x.Not(x)
	return pc, nil, nil
}

func stNot(_ uint64, scope *CallContext) string {
	x := scope.Stack.peek()
	return fmt.Sprintf("%s %d", NOT.String(), x)
}

func opLt(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	if x.Lt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return pc, nil, nil
}

func stLt(_ uint64, scope *CallContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", LT, &x, &y)
}

func opGt(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	if x.Gt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return pc, nil, nil
}

func stGt(_ uint64, scope *CallContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", GT, &x, &y)
}

func opSlt(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	if x.Slt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return pc, nil, nil
}

func stSlt(_ uint64, scope *CallContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", SLT, &x, &y)
}

func opSgt(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	if x.Sgt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return pc, nil, nil
}

func stSgt(_ uint64, scope *CallContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", SGT, &x, &y)
}

func opEq(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	if x.Eq(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return pc, nil, nil
}

func stEq(_ uint64, scope *CallContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", EQ, &x, &y)
}

func opIszero(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x := scope.Stack.peek()
	if x.IsZero() {
		x.SetOne()
	} else {
		x.Clear()
	}
	return pc, nil, nil
}

func stIsZero(_ uint64, scope *CallContext) string {
	x := scope.Stack.data[len(scope.Stack.data)-1]
	return fmt.Sprintf("%s %d", ISZERO, &x)
}

func opAnd(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.And(&x, y)
	return pc, nil, nil
}

func stAnd(_ uint64, scope *CallContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", AND, &x, &y)
}

func opOr(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.Or(&x, y)
	return pc, nil, nil
}

func stOr(_ uint64, scope *CallContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", OR, &x, &y)
}

func opXor(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	y.Xor(&x, y)
	return pc, nil, nil
}

func stXor(_ uint64, scope *CallContext) string {
	x, y := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", XOR, &x, &y)
}

func opByte(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	th, val := scope.Stack.pop(), scope.Stack.peek()
	val.Byte(&th)
	return pc, nil, nil
}

func opAddmod(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y, z := scope.Stack.pop(), scope.Stack.pop(), scope.Stack.peek()
	z.AddMod(&x, &y, z)
	return pc, nil, nil
}

func stAddmod(_ uint64, scope *CallContext) string {
	x, y, z := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2], scope.Stack.data[len(scope.Stack.data)-3]
	return fmt.Sprintf("%s %d %d %d", ADDMOD, &x, &y, &z)
}

func opMulmod(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x, y, z := scope.Stack.pop(), scope.Stack.pop(), scope.Stack.peek()
	z.MulMod(&x, &y, z)
	return pc, nil, nil
}

func stMulmod(_ uint64, scope *CallContext) string {
	x, y, z := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2], scope.Stack.data[len(scope.Stack.data)-3]
	return fmt.Sprintf("%s %d %d %d", MULMOD, &x, &y, &z)
}

// opSHL implements Shift Left
// The SHL instruction (shift left) pops 2 values from the stack, first arg1 and then arg2,
// and pushes on the stack arg2 shifted to the left by arg1 number of bits.
func opSHL(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	// Note, second operand is left in the stack; accumulate result into it, and no need to push it afterwards
	shift, value := scope.Stack.pop(), scope.Stack.peek()
	if shift.LtUint64(256) {
		value.Lsh(value, uint(shift.Uint64()))
	} else {
		value.Clear()
	}
	return pc, nil, nil
}

// opSHR implements Logical Shift Right
// The SHR instruction (logical shift right) pops 2 values from the stack, first arg1 and then arg2,
// and pushes on the stack arg2 shifted to the right by arg1 number of bits with zero fill.
func opSHR(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	// Note, second operand is left in the stack; accumulate result into it, and no need to push it afterwards
	shift, value := scope.Stack.pop(), scope.Stack.peek()
	if shift.LtUint64(256) {
		value.Rsh(value, uint(shift.Uint64()))
	} else {
		value.Clear()
	}
	return pc, nil, nil
}

// opSAR implements Arithmetic Shift Right
// The SAR instruction (arithmetic shift right) pops 2 values from the stack, first arg1 and then arg2,
// and pushes on the stack arg2 shifted to the right by arg1 number of bits with sign extension.
func opSAR(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	shift, value := scope.Stack.pop(), scope.Stack.peek()
	if shift.GtUint64(255) {
		if value.Sign() >= 0 {
			value.Clear()
		} else {
			// Max negative shift: all bits set
			value.SetAllOne()
		}
		return pc, nil, nil
	}
	n := uint(shift.Uint64())
	value.SRsh(value, n)
	return pc, nil, nil
}

func opKeccak256(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	offset, size := scope.Stack.pop(), scope.Stack.peek()
	data := scope.Memory.GetPtr(offset.Uint64(), size.Uint64())

	if evm.hasher == nil {
		evm.hasher = keccak.NewLegacyKeccak256().(keccakState)
	} else {
		evm.hasher.Reset()
	}
	evm.hasher.Write(data)
	if _, err := evm.hasher.Read(evm.hasherBuf[:]); err != nil {
		panic(err)
	}

	size.SetBytes(evm.hasherBuf[:])
	return pc, nil, nil
}

func opAddress(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	addrVal := scope.Contract.Address().Value()
	scope.Stack.push(*new(uint256.Int).SetBytes(addrVal[:]))
	return pc, nil, nil
}

func opBalance(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	slot := scope.Stack.peek()
	address := accounts.InternAddress(slot.Bytes20())
	balance, err := evm.IntraBlockState().GetBalance(address)
	if err != nil {
		return pc, nil, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	slot.Set(&balance)
	return pc, nil, nil
}

func opOrigin(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	if origin := evm.Origin; origin.IsNil() {
		scope.Stack.push(uint256.Int{})
	} else {
		originVal := origin.Value()
		scope.Stack.push(*new(uint256.Int).SetBytes(originVal[:]))
	}
	return pc, nil, nil
}
func opCaller(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	if caller := scope.Contract.Caller(); caller.IsNil() {
		scope.Stack.push(uint256.Int{})
	} else {
		callerValue := caller.Value()
		scope.Stack.push(*new(uint256.Int).SetBytes(callerValue[:]))
	}
	return pc, nil, nil
}

func stCaller(_ uint64, scope *CallContext) string {
	caller := scope.Contract.Caller().Value()
	return fmt.Sprintf("%s (%d)", CALLER, new(uint256.Int).SetBytes(caller[:]))
}

func opCallValue(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.push(scope.Contract.value)
	return pc, nil, nil
}

func opCallDataLoad(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	x := scope.Stack.peek()
	if offset, overflow := x.Uint64WithOverflow(); !overflow {
		data := getData(scope.input, offset, 32)
		x.SetBytes(data)
	} else {
		x.Clear()
	}
	return pc, nil, nil
}

func stCallDataLoad(_ uint64, scope *CallContext) string {
	x := *scope.Stack.peek()
	var data []byte
	if offset, overflow := x.Uint64WithOverflow(); !overflow {
		data = getData(scope.input, offset, 32)
	}

	return fmt.Sprintf("%s %d (%x)", CALLDATALOAD, &x, data)
}

func opCallDataSize(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.push(*new(uint256.Int).SetUint64(uint64(len(scope.input))))
	return pc, nil, nil
}

func stCallDataSize(_ uint64, scope *CallContext) string {
	return fmt.Sprintf("%s (%d)", CALLDATASIZE, new(uint256.Int).SetUint64(uint64(len(scope.input))))
}

func opCallDataCopy(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
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
	scope.Memory.Set(memOffset64, length64, getData(scope.input, dataOffset64, length64))
	return pc, nil, nil
}

func stCallDataCopy(_ uint64, scope *CallContext) string {
	var (
		memOffset  = scope.Stack.data[len(scope.Stack.data)-1]
		dataOffset = scope.Stack.data[len(scope.Stack.data)-2]
		length     = scope.Stack.data[len(scope.Stack.data)-3]
	)
	dataOffset64, overflow := dataOffset.Uint64WithOverflow()
	if overflow {
		dataOffset64 = math.MaxUint64
	}
	return fmt.Sprintf("%s %d %d %d (%x)", CALLDATACOPY, memOffset.Uint64(), dataOffset64, length.Uint64(), getData(scope.input, dataOffset64, length.Uint64()))
}

func opReturnDataSize(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.push(*new(uint256.Int).SetUint64(uint64(len(evm.returnData))))
	return pc, nil, nil
}

func opReturnDataCopy(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	var (
		memOffset  = scope.Stack.pop()
		dataOffset = scope.Stack.pop()
		length     = scope.Stack.pop()
	)

	offset64, overflow := dataOffset.Uint64WithOverflow()
	if overflow {
		return pc, nil, ErrReturnDataOutOfBounds
	}
	// we can reuse dataOffset now (aliasing it for clarity)
	end := dataOffset
	_, overflow = end.AddOverflow(&dataOffset, &length)
	if overflow {
		return pc, nil, ErrReturnDataOutOfBounds
	}

	end64, overflow := end.Uint64WithOverflow()
	if overflow || uint64(len(evm.returnData)) < end64 {
		return pc, nil, ErrReturnDataOutOfBounds
	}
	scope.Memory.Set(memOffset.Uint64(), length.Uint64(), evm.returnData[offset64:end64])
	return pc, nil, nil
}

func stReturnDataCopy(_ uint64, scope *CallContext) string {
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

func opExtCodeSize(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	slot := scope.Stack.peek()
	addr := accounts.InternAddress(slot.Bytes20())
	codeSize, err := evm.IntraBlockState().GetCodeSize(addr)
	if err != nil {
		return pc, nil, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	slot.SetUint64(uint64(codeSize))
	return pc, nil, nil
}

func opCodeSize(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	l := new(uint256.Int)
	l.SetUint64(uint64(len(scope.Contract.Code)))
	scope.Stack.push(*l)
	return pc, nil, nil
}

func opCodeCopy(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
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
	return pc, nil, nil
}

func opExtCodeCopy(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	var (
		stack      = &scope.Stack
		a          = stack.pop()
		memOffset  = stack.pop()
		codeOffset = stack.pop()
		length     = stack.pop()
	)
	addr := accounts.InternAddress(a.Bytes20())
	len64 := length.Uint64()

	code, err := evm.IntraBlockState().GetCode(addr)
	if err != nil {
		return pc, nil, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}

	codeCopy := getDataBig(code, &codeOffset, len64)
	scope.Memory.Set(memOffset.Uint64(), len64, codeCopy)
	return pc, nil, nil
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
func opExtCodeHash(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	slot := scope.Stack.peek()
	address := accounts.InternAddress(slot.Bytes20())

	empty, err := evm.IntraBlockState().Empty(address)
	if err != nil {
		return pc, nil, err
	}
	if empty {
		slot.Clear()
	} else {
		var codeHash accounts.CodeHash
		codeHash, err = evm.IntraBlockState().GetCodeHash(address)
		if err != nil {
			return pc, nil, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		codeHashValue := codeHash.Value()
		slot.SetBytes(codeHashValue[:])
	}
	return pc, nil, nil
}

func opGasprice(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.push(evm.GasPrice)
	return pc, nil, nil
}

// opBlockhash executes the BLOCKHASH opcode
func opBlockhash(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	arg := scope.Stack.peek()
	arg64, overflow := arg.Uint64WithOverflow()
	if overflow {
		arg.Clear()
		return pc, nil, nil
	}
	var upper, lower uint64
	upper = evm.Context.BlockNumber
	if upper <= params.BlockHashOldWindow {
		lower = 0
	} else {
		lower = upper - params.BlockHashOldWindow
	}
	if arg64 >= lower && arg64 < upper {
		hash, err := evm.Context.GetHash(arg64)
		if err != nil {
			arg.Clear()
			return pc, nil, err
		}
		arg.SetBytes(hash.Bytes())
	} else {
		arg.Clear()
	}

	return pc, nil, nil
}

func stBlockhash(_ uint64, scope *CallContext) string {
	x := *scope.Stack.peek()
	return fmt.Sprintf("%s %d", BLOCKHASH, &x)
}

func opCoinbase(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	if coinbase := evm.Context.Coinbase; coinbase.IsNil() {
		scope.Stack.push(uint256.Int{})
	} else {
		coinbaseValue := coinbase.Value()
		scope.Stack.push(*new(uint256.Int).SetBytes(coinbaseValue[:]))
	}
	return pc, nil, nil
}

func opTimestamp(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	v := new(uint256.Int).SetUint64(evm.Context.Time)
	scope.Stack.push(*v)
	return pc, nil, nil
}

func opNumber(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	v := new(uint256.Int).SetUint64(evm.Context.BlockNumber)
	scope.Stack.push(*v)
	return pc, nil, nil
}

func opSlotNum(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	v := new(uint256.Int).SetUint64(evm.Context.SlotNumber)
	scope.Stack.push(*v)
	return pc, nil, nil
}

func opDifficulty(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	var v *uint256.Int
	if evm.Context.PrevRanDao != nil {
		// EIP-4399: Supplant DIFFICULTY opcode with PREVRANDAO
		v = new(uint256.Int).SetBytes(evm.Context.PrevRanDao.Bytes())
	} else {
		var overflow bool
		v, overflow = uint256.FromBig(evm.Context.Difficulty)
		if overflow {
			return pc, nil, errors.New("evm.Context.Difficulty higher than 2^256-1")
		}
	}
	scope.Stack.push(*v)
	return pc, nil, nil
}

func opGasLimit(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	if evm.Context.MaxGasLimit {
		scope.Stack.push(*new(uint256.Int).SetAllOne())
	} else {
		scope.Stack.push(*new(uint256.Int).SetUint64(evm.Context.GasLimit))
	}
	return pc, nil, nil
}

func opPop(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.pop()
	return pc, nil, nil
}

func opMload(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	v := scope.Stack.peek()
	offset := v.Uint64()
	v.SetBytes(scope.Memory.GetPtr(offset, 32))
	return pc, nil, nil
}

func stMload(_ uint64, scope *CallContext) string {
	v := scope.Stack.peek()
	offset := v.Uint64()
	return fmt.Sprintf("%s %d (%d)", MLOAD, offset, (&uint256.Int{}).SetBytes(scope.Memory.GetPtr(offset, 32)))
}

func opMstore(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	mStart, val := scope.Stack.pop(), scope.Stack.pop()
	scope.Memory.Set32(mStart.Uint64(), &val)
	return pc, nil, nil
}

func stMstore(_ uint64, scope *CallContext) string {
	mStart, val := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %d %d", MSTORE, mStart.Uint64(), &val)
}

func opMstore8(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	off, val := scope.Stack.pop(), scope.Stack.pop()
	scope.Memory.store[off.Uint64()] = byte(val.Uint64())
	return pc, nil, nil
}

func opSload(pc uint64, evm *EVM, scope *CallContext) (_ uint64, _ []byte, err error) {
	loc := scope.Stack.peek()
	*loc, err = evm.IntraBlockState().GetState(scope.Contract.Address(), accounts.InternKey(loc.Bytes32()))
	return pc, nil, err
}

func stSload(_ uint64, scope *CallContext) string {
	loc := scope.Stack.peek()
	return fmt.Sprintf("%s %x", SLOAD, loc)
}

func opSstore(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	if evm.readOnly {
		return pc, nil, ErrWriteProtection
	}
	loc := scope.Stack.pop()
	val := scope.Stack.pop()
	return pc, nil, evm.IntraBlockState().SetState(scope.Contract.Address(), accounts.InternKey(loc.Bytes32()), val)
}

func stSstore(_ uint64, scope *CallContext) string {
	loc, val := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %x %d", SSTORE, loc.Bytes32(), &val)
}

func opJump(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	pos := scope.Stack.pop()
	if valid, usedBitmap := scope.Contract.validJumpdest(pos); !valid {
		if usedBitmap {
			if evm.config.TraceJumpDest {
				log.Debug("Code Bitmap used for detecting invalid jump",
					"tx", fmt.Sprintf("0x%x", evm.TxHash),
					"block_num", evm.Context.BlockNumber,
				)
			} else {
				// This is "cheaper" version because it does not require calculation of txHash for each transaction
				log.Debug("Code Bitmap used for detecting invalid jump",
					"block_num", evm.Context.BlockNumber,
				)
			}
		}
		return pc, nil, ErrInvalidJump
	}
	// pc will be increased by the interpreter loop
	return pos.Uint64() - 1, nil, nil
}

func stJump(_ uint64, scope *CallContext) string {
	pos := scope.Stack.peek()
	return fmt.Sprintf("%s %d", JUMP, pos)
}

func opJumpi(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	pos, cond := scope.Stack.pop(), scope.Stack.pop()
	if !cond.IsZero() {
		if valid, usedBitmap := scope.Contract.validJumpdest(pos); !valid {
			if usedBitmap {
				if evm.config.TraceJumpDest {
					log.Warn("Code Bitmap used for detecting invalid jump",
						"tx", fmt.Sprintf("0x%x", evm.TxHash),
						"block_num", evm.Context.BlockNumber,
					)
				} else {
					// This is "cheaper" version because it does not require calculation of txHash for each transaction
					log.Warn("Code Bitmap used for detecting invalid jump",
						"block_num", evm.Context.BlockNumber,
					)
				}
			}
			return pc, nil, ErrInvalidJump
		}
		pc = pos.Uint64() - 1 // pc will be increased by the interpreter loop
	}
	return pc, nil, nil
}

func stJumpi(_ uint64, scope *CallContext) string {
	pos, cond := scope.Stack.data[len(scope.Stack.data)-1], scope.Stack.data[len(scope.Stack.data)-2]
	return fmt.Sprintf("%s %v %d", JUMPI, !cond.IsZero(), &pos)
}

func opJumpdest(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pc, nil, nil
}

func opPc(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.push(*new(uint256.Int).SetUint64(pc))
	return pc, nil, nil
}

func stPc(pc uint64, scope *CallContext) string {
	return fmt.Sprintf("%s %d", PC, pc)
}

func opMsize(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.push(*new(uint256.Int).SetUint64(uint64(scope.Memory.Len())))
	return pc, nil, nil
}

func opGas(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.push(*new(uint256.Int).SetUint64(scope.gas))
	return pc, nil, nil
}

func stGas(pc uint64, scope *CallContext) string {
	return fmt.Sprintf("%s %d", GAS, scope.gas)
}

func opSwap1(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(1)
	return pc, nil, nil
}

func opSwap2(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(2)
	return pc, nil, nil
}

func opSwap3(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(3)
	return pc, nil, nil
}

func opSwap4(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(4)
	return pc, nil, nil
}

func opSwap5(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(5)
	return pc, nil, nil
}

func opSwap6(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(6)
	return pc, nil, nil
}

func opSwap7(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(7)
	return pc, nil, nil
}

func opSwap8(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(8)
	return pc, nil, nil
}

func opSwap9(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(9)
	return pc, nil, nil
}

func opSwap10(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(10)
	return pc, nil, nil
}

func opSwap11(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(11)
	return pc, nil, nil
}

func opSwap12(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(12)
	return pc, nil, nil
}

func opSwap13(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(13)
	return pc, nil, nil
}

func opSwap14(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(14)
	return pc, nil, nil
}

func opSwap15(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(15)
	return pc, nil, nil
}

func opSwap16(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.swap(16)
	return pc, nil, nil
}

func opCreate(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	if evm.readOnly {
		return pc, nil, ErrWriteProtection
	}
	var (
		value  = scope.Stack.pop()
		offset = scope.Stack.pop()
		size   = scope.Stack.peek()
		input  = scope.Memory.GetCopy(offset.Uint64(), size.Uint64())
		gas    = scope.gas
	)
	if evm.ChainRules().IsTangerineWhistle {
		gas -= gas / 64
	}
	// reuse size int for stackvalue
	stackvalue := size

	scope.useGas(gas, evm.Config().Tracer, tracing.GasChangeCallContractCreation)

	res, addr, returnGas, suberr := evm.Create(scope.Contract.Address(), input, gas, value, false)

	// Push item on the stack based on the returned error. If the ruleset is
	// homestead we must check for CodeStoreOutOfGasError (homestead only
	// rule) and treat as an error, if the ruleset is frontier we must
	// ignore this error and pretend the operation was successful.
	if evm.ChainRules().IsHomestead && suberr == ErrCodeStoreOutOfGas {
		stackvalue.Clear()
	} else if suberr != nil && suberr != ErrCodeStoreOutOfGas {
		stackvalue.Clear()
	} else {
		addrVal := addr.Value()
		stackvalue.SetBytes(addrVal[:])
	}

	scope.refundGas(returnGas, evm.config.Tracer, tracing.GasChangeCallLeftOverRefunded)

	if suberr == ErrExecutionReverted {
		evm.returnData = res // set REVERT data to return data buffer
		return pc, res, nil
	}
	evm.returnData = nil // clear dirty return data buffer
	return pc, nil, nil
}

func stCreate(_ uint64, scope *CallContext) string {
	stack := &scope.Stack
	var (
		value  = stack.data[len(stack.data)-1]
		offset = stack.data[len(stack.data)-2]
		size   = stack.data[len(stack.data)-3]
		input  = scope.Memory.GetCopy(offset.Uint64(), size.Uint64())
	)

	return fmt.Sprintf("%s %d %x %d", CREATE.String(), &value, input, &scope.gas)
}

func opCreate2(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	if evm.readOnly {
		return pc, nil, ErrWriteProtection
	}
	var (
		endowment    = scope.Stack.pop()
		offset, size = scope.Stack.pop(), scope.Stack.pop()
		salt         = scope.Stack.pop()
		input        = scope.Memory.GetCopy(offset.Uint64(), size.Uint64())
		gas          = scope.gas
	)

	// Apply EIP150
	gas -= gas / 64
	scope.useGas(gas, evm.Config().Tracer, tracing.GasChangeCallContractCreation2)
	// reuse size int for stackvalue
	stackValue := size
	res, addr, returnGas, suberr := evm.Create2(scope.Contract.Address(), input, gas, endowment, &salt, false)

	// Push item on the stack based on the returned error.
	if suberr != nil {
		stackValue.Clear()
	} else {
		addrVal := addr.Value()
		stackValue.SetBytes(addrVal[:])
	}

	scope.Stack.push(stackValue)
	scope.refundGas(returnGas, evm.config.Tracer, tracing.GasChangeCallLeftOverRefunded)

	if suberr == ErrExecutionReverted {
		evm.returnData = res // set REVERT data to return data buffer
		return pc, res, nil
	}
	evm.returnData = nil // clear dirty return data buffer
	return pc, nil, nil
}

func stCreate2(_ uint64, scope *CallContext) string {
	stack := &scope.Stack
	var (
		endowment    = stack.data[len(stack.data)-1]
		offset, size = stack.data[len(stack.data)-2], stack.data[len(stack.data)-3]
		salt         = stack.data[len(stack.data)-4]
		input        = scope.Memory.GetCopy(offset.Uint64(), size.Uint64())
	)

	return fmt.Sprintf("%s %d %d %x %d", CREATE2.String(), &endowment, &salt, input, &scope.gas)
}

func opCall(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	stack := &scope.Stack
	// Pop gas. The actual gas in evm.callGasTemp.
	// We can use this as a temporary value
	temp := stack.pop()
	gas := evm.CallGasTemp()
	// Pop other call parameters.
	addr, value, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	toAddr := accounts.InternAddress(addr.Bytes20())
	// Get the arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	if !value.IsZero() {
		if evm.readOnly {
			return pc, nil, ErrWriteProtection
		}
		gas += params.CallStipend
	}

	ret, returnGas, err := evm.Call(scope.Contract.Address(), toAddr, args, gas, value, false /* bailout */)

	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.push(temp)
	if err == nil || err == ErrExecutionReverted {
		ret = common.Copy(ret)
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}

	scope.refundGas(returnGas, evm.config.Tracer, tracing.GasChangeCallLeftOverRefunded)

	evm.returnData = ret
	return pc, ret, nil
}

func stCall(_ uint64, scope *CallContext) string {
	stack := &scope.Stack
	addr, _, inOffset, inSize := stack.data[len(stack.data)-2], stack.data[len(stack.data)-3], stack.data[len(stack.data)-4], stack.data[len(stack.data)-5]
	toAddr := common.Address(addr.Bytes20())
	// Get the arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	return fmt.Sprintf("%s %x %x", CALL.String(), toAddr, args)
}

func opCallCode(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	// Pop gas. The actual gas is in evm.callGasTemp.
	stack := &scope.Stack
	// We use it as a temporary value
	temp := stack.pop()
	gas := evm.CallGasTemp()
	// Pop other call parameters.
	addr, value, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	toAddr := accounts.InternAddress(addr.Bytes20())
	// Get arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	if !value.IsZero() {
		gas += params.CallStipend
	}

	ret, returnGas, err := evm.CallCode(scope.Contract.Address(), toAddr, args, gas, value)
	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.push(temp)
	if err == nil || err == ErrExecutionReverted {
		ret = common.Copy(ret)
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}

	scope.refundGas(returnGas, evm.config.Tracer, tracing.GasChangeCallLeftOverRefunded)

	evm.returnData = ret
	return pc, ret, nil
}

func stCallCode(_ uint64, scope *CallContext) string {
	stack := &scope.Stack
	addr, _, inOffset, inSize := stack.data[len(stack.data)-2], stack.data[len(stack.data)-3], stack.data[len(stack.data)-4], stack.data[len(stack.data)-5]
	toAddr := common.Address(addr.Bytes20())
	// Get the arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	return fmt.Sprintf("%s %x %x", CALLCODE.String(), toAddr, args)
}

func opDelegateCall(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	stack := &scope.Stack
	// Pop gas. The actual gas is in evm.callGasTemp.
	// We use it as a temporary value
	temp := stack.pop()
	gas := evm.CallGasTemp()
	// Pop other call parameters.
	addr, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	toAddr := accounts.InternAddress(addr.Bytes20())
	// Get arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	ret, returnGas, err := evm.DelegateCall(scope.Contract.addr, scope.Contract.caller, toAddr, args, scope.Contract.value, gas)
	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.push(temp)
	if err == nil || err == ErrExecutionReverted {
		ret = common.Copy(ret)
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}

	scope.refundGas(returnGas, evm.config.Tracer, tracing.GasChangeCallLeftOverRefunded)

	evm.returnData = ret
	return pc, ret, nil
}

func stDelegateCall(_ uint64, scope *CallContext) string {
	stack := &scope.Stack
	addr, inOffset, inSize := stack.data[len(stack.data)-2], stack.data[len(stack.data)-3], stack.data[len(stack.data)-4]
	toAddr := common.Address(addr.Bytes20())
	// Get the arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	return fmt.Sprintf("%s %x %x", DELEGATECALL.String(), toAddr, args)
}

func opStaticCall(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	// Pop gas. The actual gas is in evm.callGasTemp.
	stack := &scope.Stack
	// We use it as a temporary value
	temp := stack.pop()
	gas := evm.CallGasTemp()
	// Pop other call parameters.
	addr, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	toAddr := accounts.InternAddress(addr.Bytes20())
	// Get arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	ret, returnGas, err := evm.StaticCall(scope.Contract.Address(), toAddr, args, gas)
	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.push(temp)
	if err == nil || err == ErrExecutionReverted {
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}

	scope.refundGas(returnGas, evm.config.Tracer, tracing.GasChangeCallLeftOverRefunded)

	evm.returnData = ret
	return pc, ret, nil
}

func stStaticCall(_ uint64, scope *CallContext) string {
	stack := &scope.Stack
	addr, inOffset, inSize := stack.data[len(stack.data)-2], stack.data[len(stack.data)-3], stack.data[len(stack.data)-4]
	toAddr := common.Address(addr.Bytes20())
	// Get arguments from the memory.
	args := scope.Memory.GetPtr(inOffset.Uint64(), inSize.Uint64())

	return fmt.Sprintf("%s %x %x", STATICCALL.String(), toAddr, args)
}

func opReturn(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	offset, size := scope.Stack.pop(), scope.Stack.pop()
	ret := scope.Memory.GetCopy(offset.Uint64(), size.Uint64())
	return pc, ret, errStopToken
}

func opRevert(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	offset, size := scope.Stack.pop(), scope.Stack.pop()
	ret := scope.Memory.GetCopy(offset.Uint64(), size.Uint64())
	evm.returnData = ret
	return pc, ret, ErrExecutionReverted
}

func opUndefined(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pc, nil, &ErrInvalidOpCode{opcode: OpCode(scope.Contract.Code[pc])}
}

func opStop(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pc, nil, errStopToken
}

func opSelfdestruct(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	if evm.readOnly {
		return pc, nil, ErrWriteProtection
	}
	beneficiary := scope.Stack.pop()
	self := scope.Contract.Address()
	beneficiaryAddr := accounts.InternAddress(beneficiary.Bytes20())
	ibs := evm.IntraBlockState()
	balance, err := ibs.GetBalance(self)
	if err != nil {
		return pc, nil, err
	}

	ibs.AddBalance(beneficiaryAddr, balance, tracing.BalanceIncreaseSelfdestruct)
	ibs.Selfdestruct(self)
	tracer := evm.Config().Tracer
	if tracer != nil && tracer.OnEnter != nil {
		tracer.OnEnter(evm.depth, byte(SELFDESTRUCT), scope.Contract.Address(), beneficiaryAddr, false, []byte{}, 0, balance, nil)
	}
	if tracer != nil && tracer.OnExit != nil {
		tracer.OnExit(evm.depth, []byte{}, 0, nil, false)
	}
	return pc, nil, errStopToken
}

func opSelfdestruct6780(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	if evm.readOnly {
		return pc, nil, ErrWriteProtection
	}
	beneficiary := scope.Stack.pop()
	self := scope.Contract.Address()
	beneficiaryAddr := accounts.InternAddress(beneficiary.Bytes20())
	ibs := evm.IntraBlockState()
	balance, err := ibs.GetBalance(self)
	if err != nil {
		return pc, nil, err
	}
	newContract, err := ibs.IsNewContract(self)
	if err != nil {
		return pc, nil, err
	}
	if newContract { // Contract is new and will actually be deleted.
		ibs.SubBalance(self, balance, tracing.BalanceDecreaseSelfdestruct)
		if self != beneficiaryAddr {
			ibs.AddBalance(beneficiaryAddr, balance, tracing.BalanceIncreaseSelfdestruct)
		}
		_, err = ibs.Selfdestruct(self)
		if err != nil {
			return pc, nil, err
		}
	} else if self != beneficiaryAddr { // Contract already exists, only do transfer if beneficiary is not self.
		ibs.SubBalance(self, balance, tracing.BalanceDecreaseSelfdestruct)
		ibs.AddBalance(beneficiaryAddr, balance, tracing.BalanceIncreaseSelfdestruct)
	}
	if evm.ChainRules().IsAmsterdam && !balance.IsZero() { // EIP-7708
		if self != beneficiaryAddr {
			ibs.AddLog(misc.EthTransferLog(self.Value(), beneficiaryAddr.Value(), balance))
		} else if newContract {
			ibs.AddLog(misc.EthSelfDestructLog(self.Value(), balance))
		}
	}
	tracer := evm.Config().Tracer
	if tracer != nil && tracer.OnEnter != nil {
		tracer.OnEnter(evm.depth, byte(SELFDESTRUCT), scope.Contract.Address(), beneficiaryAddr, false, []byte{}, 0, balance, nil)
	}
	if tracer != nil && tracer.OnExit != nil {
		tracer.OnExit(evm.depth, []byte{}, 0, nil, false)
	}
	return pc, nil, errStopToken
}

func decodeSingle(x byte) int {
	if x <= 90 {
		return int(x) + 17
	}
	return int(x) - 20
}

func decodePair(x byte) (int, int) {
	var k int
	if x <= 79 {
		k = int(x)
	} else {
		k = int(x) - 48
	}
	q, r := k/16, k%16
	if q < r {
		return q + 1, r + 1
	}
	return r + 1, 29 - q
}

func opDupN(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	code := scope.Contract.Code
	pc++
	x := byte(0) // see https://github.com/ethereum/EIPs/pull/11085
	if pc < uint64(len(code)) {
		x = code[pc]
	}

	// This range is excluded to preserve compatibility with existing opcodes.
	if x > 90 && x < 128 {
		return pc, nil, &ErrInvalidOpCode{opcode: OpCode(x)}
	}
	n := decodeSingle(x)

	// DUPN duplicates the n'th stack item, so the stack must contain at least n elements.
	if scope.Stack.len() < n {
		return pc, nil, &ErrStackUnderflow{stackLen: scope.Stack.len(), required: n}
	}

	//The n‘th stack item is duplicated at the top of the stack.
	scope.Stack.dup(n)
	return pc, nil, nil
}

func opSwapN(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	code := scope.Contract.Code
	pc++
	x := byte(0) // see https://github.com/ethereum/EIPs/pull/11085
	if pc < uint64(len(code)) {
		x = code[pc]
	}

	// This range is excluded to preserve compatibility with existing opcodes.
	if x > 90 && x < 128 {
		return pc, nil, &ErrInvalidOpCode{opcode: OpCode(x)}
	}
	n := decodeSingle(x)

	// SWAPN operates on the top and n+1 stack items, so the stack must contain at least n+1 elements.
	if scope.Stack.len() < n+1 {
		return pc, nil, &ErrStackUnderflow{stackLen: scope.Stack.len(), required: n + 1}
	}

	// The (n+1)‘th stack item is swapped with the top of the stack.
	scope.Stack.swap(n)
	return pc, nil, nil
}

func opExchange(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	code := scope.Contract.Code
	pc++
	x := byte(0) // see https://github.com/ethereum/EIPs/pull/11085
	if pc < uint64(len(code)) {
		x = code[pc]
	}

	// This range is excluded both to preserve compatibility with existing opcodes
	// and to keep decode_pair’s 16-aligned arithmetic mapping valid (0–79, 128–255).
	if x > 79 && x < 128 {
		return pc, nil, &ErrInvalidOpCode{opcode: OpCode(x)}
	}
	n, m := decodePair(x)
	need := max(n, m) + 1

	// EXCHANGE operates on the (n+1)'th and (m+1)'th stack items,
	// so the stack must contain at least max(n, m)+1 elements.
	if scope.Stack.len() < need {
		return pc, nil, &ErrStackUnderflow{stackLen: scope.Stack.len(), required: need}
	}

	// The (n+1)‘th stack item is swapped with the (m+1)‘th stack item.
	indexN := scope.Stack.len() - 1 - n
	indexM := scope.Stack.len() - 1 - m
	scope.Stack.data[indexN], scope.Stack.data[indexM] = scope.Stack.data[indexM], scope.Stack.data[indexN]
	return pc, nil, nil
}

// following functions are used by the instruction jump  table

// make log instruction function
func makeLog(size int) executionFunc {
	return func(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
		if evm.readOnly {
			return pc, nil, ErrWriteProtection
		}
		topics := make([]common.Hash, size)
		stack := &scope.Stack
		mStart, mSize := stack.pop(), stack.pop()
		for i := 0; i < size; i++ {
			addr := stack.pop()
			topics[i] = addr.Bytes32()
		}

		d := scope.Memory.GetCopy(mStart.Uint64(), mSize.Uint64())
		evm.IntraBlockState().AddLog(&types.Log{
			Address: scope.Contract.Address().Value(),
			Topics:  topics,
			Data:    d,
			// This is a non-consensus field, but assigned here because
			// execution/state doesn't know the current block number.
			BlockNumber: evm.Context.BlockNumber,
		})

		return pc, nil, nil
	}
}

// opPush1 is a specialized version of pushN
func opPush1(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	var (
		codeLen = uint64(len(scope.Contract.Code))
		integer = new(uint256.Int)
	)
	pc++
	if pc < codeLen {
		scope.Stack.push(*integer.SetUint64(uint64(scope.Contract.Code[pc])))
	} else {
		scope.Stack.push(uint256.Int{})
	}
	return pc, nil, nil
}

func stPush1(pc uint64, scope *CallContext) string {
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
func opPush2(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	var (
		codeLen = uint64(len(scope.Contract.Code))
		integer = new(uint256.Int)
	)

	if pc+2 < codeLen {
		integer.SetBytes2(scope.Contract.Code[pc+1 : pc+3])
	} else if pc+1 < codeLen {
		integer.SetUint64(uint64(scope.Contract.Code[pc+1]) << 8)
	}
	scope.Stack.push(*integer)
	pc += 2
	return pc, nil, nil
}

// make push instruction function
func makePush(size uint64, pushByteSize int) executionFunc {
	return func(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
		codeLen := len(scope.Contract.Code)

		startMin := min(int(pc+1), codeLen)
		endMin := min(startMin+pushByteSize, codeLen)

		integer := new(uint256.Int).SetBytes(scope.Contract.Code[startMin:endMin])
		// Missing bytes: pushByteSize - len(pushData)
		if missing := pushByteSize - (endMin - startMin); missing > 0 {
			integer.Lsh(integer, uint(8*missing))
		}
		scope.Stack.push(*integer)

		pc += size
		return pc, nil, nil
	}
}

func makePushStringer(size uint64, pushByteSize int) stringer {
	return func(pc uint64, scope *CallContext) string {
		codeLen := len(scope.Contract.Code)

		startMin := min(int(pc+1), codeLen)
		endMin := min(startMin+pushByteSize, codeLen)

		integer := new(uint256.Int)
		integer.SetBytes(common.RightPadBytes(scope.Contract.Code[startMin:endMin], pushByteSize))
		return fmt.Sprintf("%s%d %d", "PUSH", size, integer)
	}
}

// make dup instruction function
func makeDup(size int) executionFunc {
	return func(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
		scope.Stack.dup(size)
		return pc, nil, nil
	}
}

func makeDupStringer(n int) stringer {
	return func(pc uint64, scope *CallContext) string {
		return fmt.Sprintf("DUP%d (%d)", n, &scope.Stack.data[len(scope.Stack.data)-n])
	}
}

func makeSwapStringer(n int) stringer {
	return func(pc uint64, scope *CallContext) string {
		return fmt.Sprintf("SWAP%d (%d %d)", n, &scope.Stack.data[len(scope.Stack.data)-1], &scope.Stack.data[len(scope.Stack.data)-(n+1)])
	}
}
