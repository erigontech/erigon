// Copyright 2017 The go-ethereum Authors
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

//nolint:errcheck
package vm

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func BenchmarkOpAdd64(b *testing.B) {
	x := "ffffffff"
	y := "fd37f3e2bba2c4f"

	opBenchmark(b, opAdd, x, y)
}

func BenchmarkOpAdd128(b *testing.B) {
	x := "ffffffffffffffff"
	y := "f5470b43c6549b016288e9a65629687"

	opBenchmark(b, opAdd, x, y)
}

func BenchmarkOpAdd256(b *testing.B) {
	x := "0802431afcbce1fc194c9eaa417b2fb67dc75a95db0bc7ec6b1c8af11df6a1da9"
	y := "a1f5aac137876480252e5dcac62c354ec0d42b76b0642b6181ed099849ea1d57"

	opBenchmark(b, opAdd, x, y)
}

func BenchmarkOpSub64(b *testing.B) {
	x := "51022b6317003a9d"
	y := "a20456c62e00753a"

	opBenchmark(b, opSub, x, y)
}

func BenchmarkOpSub128(b *testing.B) {
	x := "4dde30faaacdc14d00327aac314e915d"
	y := "9bbc61f5559b829a0064f558629d22ba"

	opBenchmark(b, opSub, x, y)
}

func BenchmarkOpSub256(b *testing.B) {
	x := "4bfcd8bb2ac462735b48a17580690283980aa2d679f091c64364594df113ea37"
	y := "97f9b1765588c4e6b69142eb00d20507301545acf3e1238c86c8b29be227d46e"

	opBenchmark(b, opSub, x, y)
}

func BenchmarkOpMul(b *testing.B) {
	x := opTestArg
	y := opTestArg

	opBenchmark(b, opMul, x, y)
}

func BenchmarkOpDiv256(b *testing.B) {
	x := "ff3f9014f20db29ae04af2c2d265de17"
	y := "fe7fb0d1f59dfe9492ffbf73683fd1e870eec79504c60144cc7f5fc2bad1e611"
	opBenchmark(b, opDiv, x, y)
}

func BenchmarkOpDiv128(b *testing.B) {
	x := "fdedc7f10142ff97"
	y := "fbdfda0e2ce356173d1993d5f70a2b11"
	opBenchmark(b, opDiv, x, y)
}

func BenchmarkOpDiv64(b *testing.B) {
	x := "fcb34eb3"
	y := "f97180878e839129"
	opBenchmark(b, opDiv, x, y)
}

func BenchmarkOpSdiv(b *testing.B) {
	x := "ff3f9014f20db29ae04af2c2d265de17"
	y := "fe7fb0d1f59dfe9492ffbf73683fd1e870eec79504c60144cc7f5fc2bad1e611"

	opBenchmark(b, opSdiv, x, y)
}

func BenchmarkOpMod(b *testing.B) {
	x := opTestArg
	y := opTestArg

	opBenchmark(b, opMod, x, y)
}

func BenchmarkOpSmod(b *testing.B) {
	x := opTestArg
	y := opTestArg

	opBenchmark(b, opSmod, x, y)
}

func BenchmarkOpExp(b *testing.B) {
	x := opTestArg
	y := opTestArg

	opBenchmark(b, opExp, x, y)
}

func BenchmarkOpSignExtend(b *testing.B) {
	x := opTestArg
	y := opTestArg

	opBenchmark(b, opSignExtend, x, y)
}

func BenchmarkOpLt(b *testing.B) {
	x := opTestArg
	y := opTestArg

	opBenchmark(b, opLt, x, y)
}

func BenchmarkOpGt(b *testing.B) {
	x := opTestArg
	y := opTestArg

	opBenchmark(b, opGt, x, y)
}

func BenchmarkOpSlt(b *testing.B) {
	x := opTestArg
	y := opTestArg

	opBenchmark(b, opSlt, x, y)
}

func BenchmarkOpSgt(b *testing.B) {
	x := opTestArg
	y := opTestArg

	opBenchmark(b, opSgt, x, y)
}

func BenchmarkOpEq(b *testing.B) {
	x := opTestArg
	y := opTestArg

	opBenchmark(b, opEq, x, y)
}

func BenchmarkOpEq2(b *testing.B) {
	x := "FBCDEF090807060504030201ffffffffFBCDEF090807060504030201ffffffff"
	y := "FBCDEF090807060504030201ffffffffFBCDEF090807060504030201fffffffe"
	opBenchmark(b, opEq, x, y)
}

func BenchmarkOpAnd(b *testing.B) {
	x := opTestArg
	y := opTestArg

	opBenchmark(b, opAnd, x, y)
}

func BenchmarkOpOr(b *testing.B) {
	x := opTestArg
	y := opTestArg

	opBenchmark(b, opOr, x, y)
}

func BenchmarkOpXor(b *testing.B) {
	x := opTestArg
	y := opTestArg

	opBenchmark(b, opXor, x, y)
}

func BenchmarkOpByte(b *testing.B) {
	x := opTestArg
	y := opTestArg

	opBenchmark(b, opByte, x, y)
}

func BenchmarkOpAddmod(b *testing.B) {
	x := opTestArg
	y := opTestArg
	z := opTestArg

	opBenchmark(b, opAddmod, x, y, z)
}

func BenchmarkOpMulmod(b *testing.B) {
	x := opTestArg
	y := opTestArg
	z := opTestArg

	opBenchmark(b, opMulmod, x, y, z)
}

func BenchmarkOpSHL(b *testing.B) {
	x := "FBCDEF090807060504030201ffffffffFBCDEF090807060504030201ffffffff"
	y := "ff"

	opBenchmark(b, opSHL, x, y)
}

func BenchmarkOpSHR(b *testing.B) {
	x := "FBCDEF090807060504030201ffffffffFBCDEF090807060504030201ffffffff"
	y := "ff"

	opBenchmark(b, opSHR, x, y)
}

func BenchmarkOpSAR(b *testing.B) {
	x := "FBCDEF090807060504030201ffffffffFBCDEF090807060504030201ffffffff"
	y := "ff"

	opBenchmark(b, opSAR, x, y)
}

func BenchmarkOpIsZero(b *testing.B) {
	x := "FBCDEF090807060504030201ffffffffFBCDEF090807060504030201ffffffff"
	opBenchmark(b, opIszero, x)
}

func BenchmarkOpMstore(bench *testing.B) {
	var (
		evm         = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.AllProtocolChanges, Config{})
		callContext = &CallContext{}
	)

	callContext.Memory.Resize(64)
	pc := uint64(0)
	memStart := uint256.Int{}
	value := *uint256.NewInt(0x1337)

	for bench.Loop() {
		callContext.Stack.push(value)
		callContext.Stack.push(memStart)
		opMstore(pc, evm, callContext)
	}
}

func BenchmarkOpMstore8(bench *testing.B) {
	var (
		evm         = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.AllProtocolChanges, Config{})
		callContext = &CallContext{}
	)

	callContext.Memory.Resize(64)
	pc := uint64(0)
	memStart := uint256.Int{}
	value := *uint256.NewInt(0x1337)

	for bench.Loop() {
		callContext.Stack.push(value)
		callContext.Stack.push(memStart)
		opMstore8(pc, evm, callContext)
	}
}

func BenchmarkOpReturn(bench *testing.B) {
	var (
		evm         = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.AllProtocolChanges, Config{})
		callContext = &CallContext{}
	)

	callContext.Memory.Resize(64)
	pc := uint64(0)
	size := *uint256.NewInt(32)
	offset := uint256.Int{}

	for bench.Loop() {
		callContext.Stack.push(size)
		callContext.Stack.push(offset)
		_, retSink, _ = opReturn(pc, evm, callContext)
	}
}

func BenchmarkOpPush1(bench *testing.B) {
	benchPush(bench, PUSH1, bytes.Repeat([]byte{0x60, 0x42}, 16))
}

func BenchmarkOpPush2(bench *testing.B) {
	benchPush(bench, PUSH2, bytes.Repeat([]byte{0xab}, 8))
}

func BenchmarkOpPush32(bench *testing.B) {
	benchPush(bench, PUSH32, bytes.Repeat([]byte{0xab}, 40))
}

func BenchmarkOpKeccak256(bench *testing.B) {
	var (
		evm         = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.AllProtocolChanges, Config{})
		callContext = &CallContext{}
	)
	callContext.Memory.Resize(32)
	pc := uint64(0)
	start := uint256.Int{}

	for bench.Loop() {
		callContext.Stack.push(*uint256.NewInt(32))
		callContext.Stack.push(start)
		opKeccak256(pc, evm, callContext)
		callContext.Stack.pop()
	}
}

func opBenchmark(b *testing.B, op executionFunc, args ...string) {
	var (
		evm         = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.AllProtocolChanges, Config{})
		callContext = &CallContext{}
	)

	// convert args
	byteArgs := make([][]byte, len(args))
	for i, arg := range args {
		byteArgs[i] = common.Hex2Bytes(arg)
	}
	pc := uint64(0)
	for b.Loop() {
		for _, arg := range byteArgs {
			a := *new(uint256.Int).SetBytes(arg)
			callContext.Stack.push(a)
		}
		op(pc, evm, callContext)
		callContext.Stack.popCopy()
	}
}

// benchPush dispatches through the jump table (indirect call, as the real
// interpreter does) so the op is not inlined into the loop, and rebalances with
// a bare top-- to isolate the push cost.
func benchPush(bench *testing.B, op OpCode, code []byte) {
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.AllProtocolChanges, Config{})
	callContext := &CallContext{}
	callContext.Contract.Code = code
	execute := newAmsterdamInstructionSet()[op].execute
	pc := uint64(0)
	for bench.Loop() {
		execute(pc, evm, callContext)
		callContext.Stack.top--
	}
}
