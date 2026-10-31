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
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
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

func BenchmarkOpDivPow2(b *testing.B) {
	x := "80000000000000000000000000000000"
	y := "87ffb8b26394face67d77fae3608d0891ff3f9014f20db29ae04af2c2d265de1"
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

func BenchmarkLog3(b *testing.B) {
	for _, size := range []int{32, 1024, 1024 * 1024} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
			scope := &CallContext{Contract: *NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})}
			scope.Memory.Resize(uint64(size + 6))
			op := makeLog(3)
			var offset uint64
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				ibs.SetTxContext(1, 0)
				for range 3 {
					scope.Stack.push(uint256.Int{})
				}
				scope.Stack.push(*uint256.NewInt(uint64(size)))
				scope.Stack.push(*uint256.NewInt(offset))
				if _, _, err := op(0, evm, scope); err != nil {
					b.Fatal(err)
				}
				ibs.Reset()
				offset = (offset + 1) % 7
			}
		})
	}
}

func BenchmarkLog3Interpreter(b *testing.B) {
	for _, size := range []int{32, 1024, 1024 * 1024} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.TestChainOsakaConfig, Config{})
			contract := NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})
			contract.Code = []byte{
				byte(PUSH3), byte(size >> 16), byte(size >> 8), byte(size), byte(PUSH0),
				byte(DUP1), byte(DUP1), byte(DUP1), byte(DUP5),
				byte(PUSH1), 7, byte(GAS), byte(MOD), byte(LOG3), byte(STOP),
			}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				ibs.SetTxContext(1, 0)
				if _, _, _, err := evm.Run(*contract, mdgas.MdGas{Execution: 16_777_216}, nil, false); err != nil {
					b.Fatal(err)
				}
				ibs.Reset()
			}
		})
	}
}

func BenchmarkExpSmallOperands(b *testing.B) {
	for _, base := range []uint64{3, 5, 7, 11, 13, 136279841} {
		for _, exponent := range []uint64{3, 5, 7, 11, 13} {
			b.Run(fmt.Sprintf("%d^%d", base, exponent), func(b *testing.B) {
				opBenchmark(b, opExp, fmt.Sprintf("%016x", exponent), fmt.Sprintf("%016x", base))
			})
		}
	}
}

func BenchmarkExpTower(b *testing.B) {
	// Fixed points x = base**x mod 2**256: the operands of every EXP in EEST exp_bench_arithmetic.
	for _, tc := range []struct{ base, exponent string }{
		{"03", "8a5e0b27221309b932be85af9c8a042a0b3ad21d2543c3646504dbb73cb57b3b"},
		{"07", "12770d0214a04265a8b18f64aebe54b60d92f96e13c9a6244f5768f4377a1e77"},
		{"081f7721", "f7bcb1106bd6d3afaa5c95433be3b1bfc6f620b89ed0a439a7edafc1008cfb21"},
	} {
		b.Run(tc.base, func(b *testing.B) {
			opBenchmark(b, opExp, tc.exponent, tc.base)
		})
	}
}

func BenchmarkExpInterpreter(b *testing.B) {
	for _, base := range []uint64{3, 13, 136279841} {
		b.Run(fmt.Sprint(base), func(b *testing.B) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.TestChainOsakaConfig, Config{})
			contract := NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})
			for range 256 {
				contract.Code = append(contract.Code, byte(PUSH1), 13, byte(PUSH4), byte(base>>24), byte(base>>16), byte(base>>8), byte(base), byte(EXP), byte(POP))
			}
			contract.Code = append(contract.Code, byte(STOP))
			b.ReportAllocs()
			for b.Loop() {
				if _, _, _, err := evm.Run(*contract, mdgas.MdGas{Execution: 1_000_000}, nil, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkExpNegativeOne(b *testing.B) {
	for _, tc := range []struct {
		name           string
		base, exponent uint256.Int
	}{
		{"negative_one/odd", *new(uint256.Int).SetAllOne(), *new(uint256.Int).SetAllOne()},
		{"negative_one/even", *new(uint256.Int).SetAllOne(), uint256.Int{0, 0, 0, 1}},
		{"small_base", uint256.Int{3}, uint256.Int{13}},
		{"other_large_base", uint256.Int{3, 4, 5, 6}, uint256.Int{13}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			scope := &CallContext{}
			b.ReportAllocs()
			for b.Loop() {
				scope.Stack.Reset()
				scope.Stack.push(tc.exponent)
				scope.Stack.push(tc.base)
				if _, _, err := opExp(0, nil, scope); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkOpTransientStorage(b *testing.B) {
	const txOps = 20 * 1024
	cases := []struct {
		name               string
		tstore, fixedValue bool
		opsPerKey          uint64
	}{
		{"tload/fixed_key", false, false, txOps},
		{"tload/key_per_1024_ops", false, false, 1024},
		{"tload/fresh_key", false, false, 1},
		{"tstore/fixed_key", true, false, txOps},
		{"tstore/fixed_key/fixed_value", true, true, txOps},
		{"tstore/key_per_1024_ops", true, false, 1024},
		{"tstore/key_per_1024_ops/fixed_value", true, true, 1024},
		{"tstore/fresh_key", true, false, 1},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
			addr := accounts.InternAddress(common.Address{1})
			scope := &CallContext{Contract: *NewContract(addr, addr, addr, uint256.Int{})}
			tstore := func(key, val uint256.Int) {
				scope.Stack.push(val)
				scope.Stack.push(key)
				opTstore(0, evm, scope)
			}
			one := *uint256.NewInt(1)
			var key, val uint256.Int
			for i := uint64(0); b.Loop(); i++ {
				if i%txOps == 0 {
					b.StopTimer()
					ibs.Reset()
					tstore(uint256.Int{^uint64(0)}, one)
					b.StartTimer()
				}
				if i%c.opsPerKey == 0 {
					key.SetUint64(i % txOps / c.opsPerKey)
					if !c.tstore && c.opsPerKey > 1 {
						tstore(key, one)
					}
				}
				switch {
				case !c.tstore:
					scope.Stack.push(key)
					opTload(0, evm, scope)
					scope.Stack.drop()
				case c.fixedValue:
					val.AddUint64(&key, 1)
					tstore(key, val)
				default:
					val.SetUint64(i + 2)
					tstore(key, val)
				}
			}
		})
	}
}

func BenchmarkContextAddress(b *testing.B) {
	address := accounts.InternAddress(common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"))
	for _, tc := range []struct {
		name string
		op   executionFunc
	}{{"address", opAddress}, {"caller", opCaller}, {"origin", opOrigin}, {"coinbase", opCoinbase}} {
		b.Run(tc.name, func(b *testing.B) {
			evm := &EVM{TxContext: evmtypes.TxContext{Origin: address}, Context: evmtypes.BlockContext{Coinbase: address}}
			scope := evm.getCallContext(*NewContract(address, address, address, uint256.Int{}), nil, mdgas.MdGas{})
			defer evm.putCallContext(scope)
			b.ReportAllocs()
			for b.Loop() {
				if _, _, err := tc.op(0, evm, scope); err != nil {
					b.Fatal(err)
				}
				scope.Stack.Reset()
			}
		})
	}
}

// BenchmarkCopyLoop runs 64 KB of one zero-length copy after the other, as the
// codecopy benchmark fixture does, and ADD for comparison.
func BenchmarkCopyLoop(b *testing.B) {
	for _, tc := range []struct {
		name string
		iter []byte
	}{
		{"CODECOPY", []byte{byte(PUSH1), 0, byte(PUSH0), byte(PUSH0), byte(CODECOPY)}},
		{"CALLDATACOPY", []byte{byte(PUSH1), 0, byte(PUSH0), byte(PUSH0), byte(CALLDATACOPY)}},
		{"RETURNDATACOPY", []byte{byte(PUSH1), 0, byte(PUSH0), byte(PUSH0), byte(RETURNDATACOPY)}},
		{"ADD", []byte{byte(PUSH1), 1, byte(PUSH1), 1, byte(ADD), byte(POP)}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.TestChainOsakaConfig, Config{})
			contract := NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})
			n := 64 * 1024 / len(tc.iter)
			contract.Code = append([]byte{byte(PUSH1), 0xff, byte(PUSH1), 0x20, byte(MSTORE8), byte(JUMPDEST)}, bytes.Repeat(tc.iter, n)...)
			b.ReportAllocs()
			for b.Loop() {
				if _, _, _, err := evm.Run(*contract, mdgas.MdGas{Execution: 1_000_000}, nil, false); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/iter")
		})
	}
}
