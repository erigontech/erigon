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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

const opTestArg = "ABCDEF090807060504030201ffffffffffffffffffffffffffffffffffffffff"

type TwoOperandTestcase struct {
	X        string
	Y        string
	Expected string
}

type twoOperandParams struct {
	x string
	y string
}

var commonParams []*twoOperandParams
var twoOpMethods map[string]executionFunc

type contractRef struct {
	addr common.Address
}

func (c contractRef) Address() common.Address {
	return c.addr
}

func init() {

	// Params is a list of common edgecases that should be used for some common tests
	params := []string{
		"0000000000000000000000000000000000000000000000000000000000000000", // 0
		"0000000000000000000000000000000000000000000000000000000000000001", // +1
		"0000000000000000000000000000000000000000000000000000000000000005", // +5
		"7ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe", // + max -1
		"7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", // + max
		"8000000000000000000000000000000000000000000000000000000000000000", // - max
		"8000000000000000000000000000000000000000000000000000000000000001", // - max+1
		"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffb", // - 5
		"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", // - 1
	}
	// Params are combined so each param is used on each 'side'
	commonParams = make([]*twoOperandParams, len(params)*len(params))
	for i, x := range params {
		for j, y := range params {
			commonParams[i*len(params)+j] = &twoOperandParams{x, y}
		}
	}
	twoOpMethods = map[string]executionFunc{"add": opAdd,
		"sub":     opSub,
		"mul":     opMul,
		"div":     opDiv,
		"sdiv":    opSdiv,
		"mod":     opMod,
		"smod":    opSmod,
		"exp":     opExp,
		"signext": opSignExtend,
		"lt":      opLt,
		"gt":      opGt,
		"slt":     opSlt,
		"sgt":     opSgt,
		"eq":      opEq,
		"and":     opAnd,
		"or":      opOr,
		"xor":     opXor,
		"byte":    opByte,
		"shl":     opSHL,
		"shr":     opSHR,
		"sar":     opSAR,
	}
}

func testTwoOperandOp(t *testing.T, tests []TwoOperandTestcase, opFn executionFunc, name string) {
	var (
		evm         = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.TestChainConfig, Config{})
		callContext = &CallContext{}
		pc          = uint64(0)
	)

	for i, test := range tests {
		x := *uint256.NewInt(0).SetBytes(common.Hex2Bytes(test.X))
		y := *uint256.NewInt(0).SetBytes(common.Hex2Bytes(test.Y))
		expected := new(uint256.Int).SetBytes(common.Hex2Bytes(test.Expected))
		callContext.Stack.push(x)
		callContext.Stack.push(y)
		opFn(pc, evm, callContext)
		if len(callContext.Stack.data) != 1 {
			t.Errorf("Expected one item on stack after %v, got %d: ", name, len(callContext.Stack.data))
		}
		actual := callContext.Stack.pop()

		if actual.Cmp(expected) != 0 {
			t.Errorf("Testcase %v %d, %v(%x, %x): expected  %x, got %x", name, i, name, x, y, expected, &actual)
		}
	}
}

func TestByteOp(t *testing.T) {
	t.Parallel()
	tests := []TwoOperandTestcase{
		{"ABCDEF0908070605040302010000000000000000000000000000000000000000", "00", "AB"},
		{"ABCDEF0908070605040302010000000000000000000000000000000000000000", "01", "CD"},
		{"00CDEF090807060504030201ffffffffffffffffffffffffffffffffffffffff", "00", "00"},
		{"00CDEF090807060504030201ffffffffffffffffffffffffffffffffffffffff", "01", "CD"},
		{"0000000000000000000000000000000000000000000000000000000000102030", "1F", "30"},
		{"0000000000000000000000000000000000000000000000000000000000102030", "1E", "20"},
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "20", "00"},
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "FFFFFFFFFFFFFFFF", "00"},
	}
	testTwoOperandOp(t, tests, opByte, "byte")
}

func TestSHL(t *testing.T) {
	t.Parallel()
	// Testcases from https://github.com/ethereum/EIPs/blob/master/EIPS/eip-145.md#shl-shift-left
	tests := []TwoOperandTestcase{
		{"0000000000000000000000000000000000000000000000000000000000000001", "01", "0000000000000000000000000000000000000000000000000000000000000002"},
		{"0000000000000000000000000000000000000000000000000000000000000001", "ff", "8000000000000000000000000000000000000000000000000000000000000000"},
		{"0000000000000000000000000000000000000000000000000000000000000001", "0100", "0000000000000000000000000000000000000000000000000000000000000000"},
		{"0000000000000000000000000000000000000000000000000000000000000001", "0101", "0000000000000000000000000000000000000000000000000000000000000000"},
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "00", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "01", "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe"},
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "ff", "8000000000000000000000000000000000000000000000000000000000000000"},
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "0100", "0000000000000000000000000000000000000000000000000000000000000000"},
		{"0000000000000000000000000000000000000000000000000000000000000000", "01", "0000000000000000000000000000000000000000000000000000000000000000"},
		{"7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "01", "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe"},
	}
	testTwoOperandOp(t, tests, opSHL, "shl")
}

func TestSHR(t *testing.T) {
	t.Parallel()
	// Testcases from https://github.com/ethereum/EIPs/blob/master/EIPS/eip-145.md#shr-logical-shift-right
	tests := []TwoOperandTestcase{
		{"0000000000000000000000000000000000000000000000000000000000000001", "00", "0000000000000000000000000000000000000000000000000000000000000001"},
		{"0000000000000000000000000000000000000000000000000000000000000001", "01", "0000000000000000000000000000000000000000000000000000000000000000"},
		{"8000000000000000000000000000000000000000000000000000000000000000", "01", "4000000000000000000000000000000000000000000000000000000000000000"},
		{"8000000000000000000000000000000000000000000000000000000000000000", "ff", "0000000000000000000000000000000000000000000000000000000000000001"},
		{"8000000000000000000000000000000000000000000000000000000000000000", "0100", "0000000000000000000000000000000000000000000000000000000000000000"},
		{"8000000000000000000000000000000000000000000000000000000000000000", "0101", "0000000000000000000000000000000000000000000000000000000000000000"},
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "00", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "01", "7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "ff", "0000000000000000000000000000000000000000000000000000000000000001"},
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "0100", "0000000000000000000000000000000000000000000000000000000000000000"},
		{"0000000000000000000000000000000000000000000000000000000000000000", "01", "0000000000000000000000000000000000000000000000000000000000000000"},
	}
	testTwoOperandOp(t, tests, opSHR, "shr")
}

func TestSAR(t *testing.T) {
	t.Parallel()
	// Testcases from https://github.com/ethereum/EIPs/blob/master/EIPS/eip-145.md#sar-arithmetic-shift-right
	tests := []TwoOperandTestcase{
		{"0000000000000000000000000000000000000000000000000000000000000001", "00", "0000000000000000000000000000000000000000000000000000000000000001"},
		{"0000000000000000000000000000000000000000000000000000000000000001", "01", "0000000000000000000000000000000000000000000000000000000000000000"},
		{"8000000000000000000000000000000000000000000000000000000000000000", "01", "c000000000000000000000000000000000000000000000000000000000000000"},
		{"8000000000000000000000000000000000000000000000000000000000000000", "ff", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{"8000000000000000000000000000000000000000000000000000000000000000", "0100", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{"8000000000000000000000000000000000000000000000000000000000000000", "0101", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "00", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "01", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "ff", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "0100", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{"0000000000000000000000000000000000000000000000000000000000000000", "01", "0000000000000000000000000000000000000000000000000000000000000000"},
		{"4000000000000000000000000000000000000000000000000000000000000000", "fe", "0000000000000000000000000000000000000000000000000000000000000001"},
		{"7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "f8", "000000000000000000000000000000000000000000000000000000000000007f"},
		{"7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "fe", "0000000000000000000000000000000000000000000000000000000000000001"},
		{"7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "ff", "0000000000000000000000000000000000000000000000000000000000000000"},
		{"7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "0100", "0000000000000000000000000000000000000000000000000000000000000000"},
	}

	testTwoOperandOp(t, tests, opSAR, "sar")
}

func TestAddMod(t *testing.T) {
	t.Parallel()
	var (
		evm         = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.TestChainConfig, Config{})
		callContext = &CallContext{}
		pc          = uint64(0)
	)
	tests := []struct {
		x        string
		y        string
		z        string
		expected string
	}{
		{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe",
			"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe",
		},
	}
	// x + y = 0x1fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffd
	// in 256 bit repr, fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffd

	for i, test := range tests {
		x := *new(uint256.Int).SetBytes(common.Hex2Bytes(test.x))
		y := *new(uint256.Int).SetBytes(common.Hex2Bytes(test.y))
		z := *new(uint256.Int).SetBytes(common.Hex2Bytes(test.z))
		expected := new(uint256.Int).SetBytes(common.Hex2Bytes(test.expected))
		callContext.Stack.push(z)
		callContext.Stack.push(y)
		callContext.Stack.push(x)
		opAddmod(pc, evm, callContext)
		actual := callContext.Stack.pop()
		if actual.Cmp(expected) != 0 {
			t.Errorf("Testcase %d, expected  %x, got %x", i, expected, actual)
		}
	}
}

// getResult is a convenience function to generate the expected values
// func getResult(args []*twoOperandParams, opFn executionFunc) []TwoOperandTestcase {
// 	var (
// 		evm         = NewEVM(BlockContext{}, TxContext{}, nil, chain.TestChainConfig, Config{})
// 		stack       = stack.New()
// 		pc          = uint64(0)
// 	)
// 	result := make([]TwoOperandTestcase, len(args))
// 	for i, param := range args {
// 		x := new(uint256.Int).SetBytes(common.Hex2Bytes(param.x))
// 		y := new(uint256.Int).SetBytes(common.Hex2Bytes(param.y))
// 		stack.push(x)
// 		stack.push(y)
// 		opFn(&pc, evm, &callCtx{nil, stack, nil})
// 		actual := stack.pop()
// 		result[i] = TwoOperandTestcase{param.x, param.y, fmt.Sprintf("%064x", actual)}
// 	}
// 	return result
// }

// utility function to fill the json-file with testcases
// Enable this test to generate the 'testcases_xx.json' files
// func TestWriteExpectedValues(t *testing.T) {
// 	for name, method := range twoOpMethods {
// 		data, err := json.Marshal(getResult(commonParams, method))
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		_ = os.WriteFile(fmt.Sprintf("testdata/testcases_%v.json", name), data, 0644)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 	}
// }

// TestJsonTestcases runs through all the testcases defined as json-files
func TestJsonTestcases(t *testing.T) {
	t.Parallel()
	for name := range twoOpMethods {
		data, err := os.ReadFile(fmt.Sprintf("testdata/testcases_%v.json", name))
		if err != nil {
			t.Fatal("Failed to read file", err)
		}
		var testcases []TwoOperandTestcase
		json.Unmarshal(data, &testcases)
		testTwoOperandOp(t, testcases, twoOpMethods[name], name)
	}
}

func opBenchmark(b *testing.B, op executionFunc, args ...string) {
	var (
		evm         = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.TestChainConfig, Config{})
		callContext = &CallContext{}
	)

	// convert args
	byteArgs := make([][]byte, len(args))
	for i, arg := range args {
		byteArgs[i] = common.Hex2Bytes(arg)
	}
	pc := uint64(0)
	b.ResetTimer()
	for b.Loop() {
		for _, arg := range byteArgs {
			a := *new(uint256.Int).SetBytes(arg)
			callContext.Stack.push(a)
		}
		op(pc, evm, callContext)
		callContext.Stack.pop()
	}
}

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

func TestOpMstore(t *testing.T) {
	t.Parallel()
	var (
		evm         = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.TestChainConfig, Config{})
		callContext = &CallContext{}
	)

	callContext.Memory.Resize(64)
	pc := uint64(0)
	v := "abcdef00000000000000abba000000000deaf000000c0de00100000000133700"
	callContext.Stack.push(*new(uint256.Int).SetBytes(common.Hex2Bytes(v)))
	callContext.Stack.push(uint256.Int{})
	opMstore(pc, evm, callContext)
	if got := common.Bytes2Hex(callContext.Memory.GetCopy(0, 32)); got != v {
		t.Fatalf("Mstore fail, got %v, expected %v", got, v)
	}
	callContext.Stack.push(*new(uint256.Int).SetOne())
	callContext.Stack.push(uint256.Int{})
	opMstore(pc, evm, callContext)
	if common.Bytes2Hex(callContext.Memory.GetCopy(0, 32)) != "0000000000000000000000000000000000000000000000000000000000000001" {
		t.Fatalf("Mstore failed to overwrite previous value")
	}
}

func BenchmarkOpMstore(bench *testing.B) {
	var (
		evm         = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.TestChainConfig, Config{})
		callContext = &CallContext{}
	)

	callContext.Memory.Resize(64)
	pc := uint64(0)
	memStart := uint256.Int{}
	value := *new(uint256.Int).SetUint64(0x1337)

	for bench.Loop() {
		callContext.Stack.push(value)
		callContext.Stack.push(memStart)
		opMstore(pc, evm, callContext)
	}
}

func TestOpTstore(t *testing.T) {
	t.Parallel()
	var (
		state       = state.New(nil)
		evm         = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, state, chain.TestChainConfig, Config{})
		caller      = accounts.ZeroAddress
		to          = accounts.InternAddress(common.Address{1})
		callContext = &CallContext{Contract: *NewContract(caller, caller, to, uint256.Int{})}
		value       = common.Hex2Bytes("abcdef00000000000000abba000000000deaf000000c0de00100000000133700")
	)

	pc := uint64(0)
	// push the value to the stack
	callContext.Stack.push(*new(uint256.Int).SetBytes(value))
	// push the location to the stack
	callContext.Stack.push(uint256.Int{})
	opTstore(pc, evm, callContext)
	// there should be no elements on the stack after TSTORE
	if callContext.Stack.len() != 0 {
		t.Fatal("stack wrong size")
	}
	// push the location to the stack
	callContext.Stack.push(uint256.Int{})
	opTload(pc, evm, callContext)
	// there should be one element on the stack after TLOAD
	if callContext.Stack.len() != 1 {
		t.Fatal("stack wrong size")
	}
	val := callContext.Stack.peek()
	if !bytes.Equal(val.Bytes(), value) {
		t.Fatal("incorrect element read from transient storage")
	}
}

func BenchmarkOpKeccak256(bench *testing.B) {
	var (
		evm         = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.TestChainConfig, Config{})
		callContext = &CallContext{}
	)
	callContext.Memory.Resize(32)
	pc := uint64(0)
	start := uint256.Int{}

	for bench.Loop() {
		callContext.Stack.push(*uint256.NewInt(32))
		callContext.Stack.push(start)
		opKeccak256(pc, evm, callContext)
	}
}

func TestCreate2Addreses(t *testing.T) {
	t.Parallel()
	type testcase struct {
		origin   string
		salt     string
		code     string
		expected string
	}

	for i, tt := range []testcase{
		{
			origin:   "0x0000000000000000000000000000000000000000",
			salt:     "0x0000000000000000000000000000000000000000",
			code:     "0x00",
			expected: "0x4d1a2e2bb4f88f0250f26ffff098b0b30b26bf38",
		},
		{
			origin:   "0xdeadbeef00000000000000000000000000000000",
			salt:     "0x0000000000000000000000000000000000000000",
			code:     "0x00",
			expected: "0xB928f69Bb1D91Cd65274e3c79d8986362984fDA3",
		},
		{
			origin:   "0xdeadbeef00000000000000000000000000000000",
			salt:     "0xfeed000000000000000000000000000000000000",
			code:     "0x00",
			expected: "0xD04116cDd17beBE565EB2422F2497E06cC1C9833",
		},
		{
			origin:   "0x0000000000000000000000000000000000000000",
			salt:     "0x0000000000000000000000000000000000000000",
			code:     "0xdeadbeef",
			expected: "0x70f2b2914A2a4b783FaEFb75f459A580616Fcb5e",
		},
		{
			origin:   "0x00000000000000000000000000000000deadbeef",
			salt:     "0xcafebabe",
			code:     "0xdeadbeef",
			expected: "0x60f3f640a8508fC6a86d45DF051962668E1e8AC7",
		},
		{
			origin:   "0x00000000000000000000000000000000deadbeef",
			salt:     "0xcafebabe",
			code:     "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
			expected: "0x1d8bfDC5D46DC4f61D6b6115972536eBE6A8854C",
		},
		{
			origin:   "0x0000000000000000000000000000000000000000",
			salt:     "0x0000000000000000000000000000000000000000",
			code:     "0x",
			expected: "0xE33C0C7F7df4809055C3ebA6c09CFe4BaF1BD9e0",
		},
	} {

		origin := common.BytesToAddress(common.FromHex(tt.origin))
		salt := common.BytesToHash(common.FromHex(tt.salt))
		code := common.FromHex(tt.code)
		codeHash := accounts.InternCodeHash(common.Hash(crypto.Keccak256(code)))
		address := types.CreateAddress2(origin, salt, codeHash)
		/*
			stack          := newstack()
			// salt, but we don't need that for this test
			stack.push(big.NewInt(int64(len(code)))) //size
			stack.push(big.NewInt(0)) // memstart
			stack.push(big.NewInt(0)) // value
			gas, _ := gasCreate2(params.GasTable{}, nil, nil, stack, nil, 0)
			fmt.Printf("Example %d\n* address `0x%x`\n* salt `0x%x`\n* init_code `0x%x`\n* gas (assuming no mem expansion): `%v`\n* result: `%s`\n\n", i,origin, salt, code, gas, address.String())
		*/
		expected := common.BytesToAddress(common.FromHex(tt.expected))
		if !bytes.Equal(expected.Bytes(), address.Bytes()) {
			t.Errorf("test %d: expected %s, got %s", i, expected.String(), address.String())
		}
	}
}

func TestOpMCopy(t *testing.T) {
	t.Parallel()
	// Test cases from https://eips.ethereum.org/EIPS/eip-5656#test-cases
	for i, tc := range []struct {
		dst, src, len string
		pre           string
		want          string
		wantGas       uint64
	}{
		{ // MCOPY 0 32 32 - copy 32 bytes from offset 32 to offset 0.
			dst: "0x0", src: "0x20", len: "0x20",
			pre:     "0000000000000000000000000000000000000000000000000000000000000000 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			want:    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			wantGas: 6,
		},

		{ // MCOPY 0 0 32 - copy 32 bytes from offset 0 to offset 0.
			dst: "0x0", src: "0x0", len: "0x20",
			pre:     "0101010101010101010101010101010101010101010101010101010101010101",
			want:    "0101010101010101010101010101010101010101010101010101010101010101",
			wantGas: 6,
		},
		{ // MCOPY 0 1 8 - copy 8 bytes from offset 1 to offset 0 (overlapping).
			dst: "0x0", src: "0x1", len: "0x8",
			pre:     "000102030405060708 000000000000000000000000000000000000000000000000",
			want:    "010203040506070808 000000000000000000000000000000000000000000000000",
			wantGas: 6,
		},
		{ // MCOPY 1 0 8 - copy 8 bytes from offset 0 to offset 1 (overlapping).
			dst: "0x1", src: "0x0", len: "0x8",
			pre:     "000102030405060708 000000000000000000000000000000000000000000000000",
			want:    "000001020304050607 000000000000000000000000000000000000000000000000",
			wantGas: 6,
		},
		// Tests below are not in the EIP, but maybe should be added
		{ // MCOPY 0xFFFFFFFFFFFF 0xFFFFFFFFFFFF 0 - copy zero bytes from out-of-bounds index(overlapping).
			dst: "0xFFFFFFFFFFFF", src: "0xFFFFFFFFFFFF", len: "0x0",
			pre:     "11",
			want:    "11",
			wantGas: 3,
		},
		{ // MCOPY 0xFFFFFFFFFFFF 0 0 - copy zero bytes from start of mem to out-of-bounds.
			dst: "0xFFFFFFFFFFFF", src: "0x0", len: "0x0",
			pre:     "11",
			want:    "11",
			wantGas: 3,
		},
		{ // MCOPY 0 0xFFFFFFFFFFFF 0 - copy zero bytes from out-of-bounds to start of mem
			dst: "0x0", src: "0xFFFFFFFFFFFF", len: "0x0",
			pre:     "11",
			want:    "11",
			wantGas: 3,
		},
		{ // MCOPY - copy 1 from space outside of uint64  space
			dst: "0x0", src: "0x10000000000000000", len: "0x1",
			pre: "0",
		},
		{ // MCOPY - copy 1 from 0 to space outside of uint64
			dst: "0x10000000000000000", src: "0x0", len: "0x1",
			pre: "0",
		},
		{ // MCOPY - copy nothing from 0 to space outside of uint64
			dst: "0x10000000000000000", src: "0x0", len: "0x0",
			pre:     "",
			want:    "",
			wantGas: 3,
		},
		{ // MCOPY - copy 1 from 0x20 to 0x10, with no prior allocated mem
			dst: "0x10", src: "0x20", len: "0x1",
			pre: "",
			// 64 bytes
			want:    "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			wantGas: 12,
		},
		{ // MCOPY - copy 1 from 0x19 to 0x10, with no prior allocated mem
			dst: "0x10", src: "0x19", len: "0x1",
			pre: "",
			// 32 bytes
			want:    "0x0000000000000000000000000000000000000000000000000000000000000000",
			wantGas: 9,
		},
	} {
		var (
			evm         = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.TestChainConfig, Config{})
			callContext = &CallContext{}
			pc          = uint64(0)
		)
		data := common.FromHex(strings.ReplaceAll(tc.pre, " ", ""))
		// Set pre
		callContext.Memory.Resize(uint64(len(data)))
		callContext.Memory.Set(0, uint64(len(data)), data)
		// Push stack args
		len, _ := uint256.FromHex(tc.len)
		src, _ := uint256.FromHex(tc.src)
		dst, _ := uint256.FromHex(tc.dst)

		callContext.Stack.push(*len)
		callContext.Stack.push(*src)
		callContext.Stack.push(*dst)
		wantErr := (tc.wantGas == 0)
		// Calc mem expansion
		var memorySize uint64
		if memSize, overflow := memoryMcopy(callContext); overflow {
			if wantErr {
				continue
			}
			t.Errorf("overflow")
		} else {
			var overflow bool
			if memorySize, overflow = math.SafeMul(ToWordSize(memSize), 32); overflow {
				t.Error(ErrGasUintOverflow)
			}
		}
		// and the dynamic cost
		var haveGas uint64
		if dynamicCost, err := gasMcopy(evm, callContext, 0, memorySize); err != nil {
			t.Error(err)
		} else {
			haveGas = GasFastestStep + dynamicCost
		}
		// Expand mem
		if memorySize > 0 {
			callContext.Memory.Resize(memorySize)
		}
		// Do the copy
		opMcopy(pc, evm, callContext)
		want := common.FromHex(strings.ReplaceAll(tc.want, " ", ""))
		if have := callContext.Memory.store; !bytes.Equal(want, have) {
			t.Errorf("case %d: \nwant: %#x\nhave: %#x\n", i, want, have)
		}
		wantGas := tc.wantGas
		if haveGas != wantGas {
			t.Errorf("case %d: gas wrong, want %d have %d\n", i, wantGas, haveGas)
		}
	}
}

func TestOpCLZ(t *testing.T) {
	tests := []struct {
		name     string
		inputHex string // hexadecimal input for clarity
		want     uint64 // expected CLZ result
	}{
		{"zero", "0x0", 256},
		{"one", "0x1", 255},
		{"all-ones (256 bits)", "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 0},
		{"low-10-bytes ones", "0xffffffffff", 216}, // 10 bytes = 80 bits, so 256-80=176? Actually input is 0xffffffffff = 40 bits so 256-40=216
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				evm         = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.TestChainConfig, Config{})
				callContext = &CallContext{}
			)

			pc := uint64(0)

			// parse input
			val := new(uint256.Int)
			if _, err := fmt.Sscan(tc.inputHex, val); err != nil {
				// fallback: try hex
				val.SetFromHex(tc.inputHex)
			}

			callContext.Stack.push(*val)
			opCLZ(pc, evm, callContext)

			if gotLen := callContext.Stack.len(); gotLen != 1 {
				t.Fatalf("stack length = %d; want 1", gotLen)
			}
			result := callContext.Stack.pop()

			if got := result.Uint64(); got != tc.want {
				t.Fatalf("clz(%q) = %d; want %d", tc.inputHex, got, tc.want)
			}
		})
	}
}

// TestPush sanity-checks how code with immediates are handled when the code size is
// smaller than the size of the immediate.
func TestPush(t *testing.T) {
	code := common.FromHex("0011223344556677889900aabbccddeeff0102030405060708090a0b0c0d0e0ff1e1d1c1b1a19181716151413121")
	push32 := makePush(32, 32)

	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.TestChainConfig, Config{})
	callContext := &CallContext{}
	callContext.Contract.Code = code

	for i, want := range []string{
		"0x11223344556677889900aabbccddeeff0102030405060708090a0b0c0d0e0ff1",
		"0x223344556677889900aabbccddeeff0102030405060708090a0b0c0d0e0ff1e1",
		"0x3344556677889900aabbccddeeff0102030405060708090a0b0c0d0e0ff1e1d1",
		"0x44556677889900aabbccddeeff0102030405060708090a0b0c0d0e0ff1e1d1c1",
		"0x556677889900aabbccddeeff0102030405060708090a0b0c0d0e0ff1e1d1c1b1",
		"0x6677889900aabbccddeeff0102030405060708090a0b0c0d0e0ff1e1d1c1b1a1",
		"0x77889900aabbccddeeff0102030405060708090a0b0c0d0e0ff1e1d1c1b1a191",
		"0x889900aabbccddeeff0102030405060708090a0b0c0d0e0ff1e1d1c1b1a19181",
		"0x9900aabbccddeeff0102030405060708090a0b0c0d0e0ff1e1d1c1b1a1918171",
		"0xaabbccddeeff0102030405060708090a0b0c0d0e0ff1e1d1c1b1a191817161",
		"0xaabbccddeeff0102030405060708090a0b0c0d0e0ff1e1d1c1b1a19181716151",
		"0xbbccddeeff0102030405060708090a0b0c0d0e0ff1e1d1c1b1a1918171615141",
		"0xccddeeff0102030405060708090a0b0c0d0e0ff1e1d1c1b1a191817161514131",
		"0xddeeff0102030405060708090a0b0c0d0e0ff1e1d1c1b1a19181716151413121",
		"0xeeff0102030405060708090a0b0c0d0e0ff1e1d1c1b1a1918171615141312100",
		"0xff0102030405060708090a0b0c0d0e0ff1e1d1c1b1a191817161514131210000",
		"0x102030405060708090a0b0c0d0e0ff1e1d1c1b1a19181716151413121000000",
		"0x2030405060708090a0b0c0d0e0ff1e1d1c1b1a1918171615141312100000000",
		"0x30405060708090a0b0c0d0e0ff1e1d1c1b1a191817161514131210000000000",
		"0x405060708090a0b0c0d0e0ff1e1d1c1b1a19181716151413121000000000000",
		"0x5060708090a0b0c0d0e0ff1e1d1c1b1a1918171615141312100000000000000",
		"0x60708090a0b0c0d0e0ff1e1d1c1b1a191817161514131210000000000000000",
		"0x708090a0b0c0d0e0ff1e1d1c1b1a19181716151413121000000000000000000",
		"0x8090a0b0c0d0e0ff1e1d1c1b1a1918171615141312100000000000000000000",
		"0x90a0b0c0d0e0ff1e1d1c1b1a191817161514131210000000000000000000000",
		"0xa0b0c0d0e0ff1e1d1c1b1a19181716151413121000000000000000000000000",
		"0xb0c0d0e0ff1e1d1c1b1a1918171615141312100000000000000000000000000",
		"0xc0d0e0ff1e1d1c1b1a191817161514131210000000000000000000000000000",
		"0xd0e0ff1e1d1c1b1a19181716151413121000000000000000000000000000000",
		"0xe0ff1e1d1c1b1a1918171615141312100000000000000000000000000000000",
		"0xff1e1d1c1b1a191817161514131210000000000000000000000000000000000",
		"0xf1e1d1c1b1a19181716151413121000000000000000000000000000000000000",
		"0xe1d1c1b1a1918171615141312100000000000000000000000000000000000000",
		"0xd1c1b1a191817161514131210000000000000000000000000000000000000000",
		"0xc1b1a19181716151413121000000000000000000000000000000000000000000",
		"0xb1a1918171615141312100000000000000000000000000000000000000000000",
		"0xa191817161514131210000000000000000000000000000000000000000000000",
		"0x9181716151413121000000000000000000000000000000000000000000000000",
		"0x8171615141312100000000000000000000000000000000000000000000000000",
		"0x7161514131210000000000000000000000000000000000000000000000000000",
		"0x6151413121000000000000000000000000000000000000000000000000000000",
		"0x5141312100000000000000000000000000000000000000000000000000000000",
		"0x4131210000000000000000000000000000000000000000000000000000000000",
		"0x3121000000000000000000000000000000000000000000000000000000000000",
		"0x2100000000000000000000000000000000000000000000000000000000000000",
		"0x0",
	} {
		pc := uint64(i)
		push32(pc, evm, callContext)
		res := callContext.Stack.pop()
		if have := res.Hex(); have != want {
			t.Fatalf("case %d, have %v want %v", i, have, want)
		}
	}
}

func TestEIP8024_Execution(t *testing.T) {
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.TestChainConfig, Config{})

	tests := []struct {
		name       string
		codeHex    string
		wantErr    error
		wantOpcode OpCode
		wantVals   []uint64
	}{
		{
			name:    "DUPN",
			codeHex: "60016000808080808080808080808080808080e600",
			wantVals: []uint64{
				1,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				1,
			},
		},
		{
			name:    "DUPN_MISSING_IMMEDIATE",
			codeHex: "60016000808080808080808080808080808080e6",
			wantVals: []uint64{
				1,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				1,
			},
		},
		{
			name:    "SWAPN",
			codeHex: "600160008080808080808080808080808080806002e700",
			wantVals: []uint64{
				1,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				2,
			},
		},
		{
			name:    "SWAPN_MISSING_IMMEDIATE",
			codeHex: "600160008080808080808080808080808080806002e7",
			wantVals: []uint64{
				1,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				2,
			},
		},
		{
			name:     "EXCHANGE",
			codeHex:  "600060016002e801",
			wantVals: []uint64{2, 0, 1},
		},
		{
			name:    "EXCHANGE_MISSING_IMMEDIATE",
			codeHex: "600060006000600060006000600060006000600060006000600060006000600060006000600060006000600060006000600060006000600060016002e8",
			wantVals: []uint64{
				2,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				1,
			},
		},
		{
			name:       "INVALID_SWAPN_LOW",
			codeHex:    "e75b",
			wantErr:    &ErrInvalidOpCode{},
			wantOpcode: SWAPN,
		},
		{
			name:    "JUMP over INVALID_DUPN",
			codeHex: "600456e65b",
			wantErr: nil,
		},
		{
			name:       "UNDERFLOW_DUPN_1",
			codeHex:    "6000808080808080808080808080808080e600",
			wantErr:    &ErrStackUnderflow{},
			wantOpcode: DUPN,
		},
		// Additional test cases
		{
			name:       "INVALID_DUPN_LOW",
			codeHex:    "e65b",
			wantErr:    &ErrInvalidOpCode{},
			wantOpcode: DUPN,
		},
		{
			name:       "INVALID_EXCHANGE_LOW",
			codeHex:    "e850",
			wantErr:    &ErrInvalidOpCode{},
			wantOpcode: EXCHANGE,
		},
		{
			name:       "INVALID_DUPN_HIGH",
			codeHex:    "e67f",
			wantErr:    &ErrInvalidOpCode{},
			wantOpcode: DUPN,
		},
		{
			name:       "INVALID_SWAPN_HIGH",
			codeHex:    "e77f",
			wantErr:    &ErrInvalidOpCode{},
			wantOpcode: SWAPN,
		},
		{
			name:       "INVALID_EXCHANGE_HIGH",
			codeHex:    "e87f",
			wantErr:    &ErrInvalidOpCode{},
			wantOpcode: EXCHANGE,
		},
		{
			name:       "UNDERFLOW_DUPN_2",
			codeHex:    "5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5fe600", // (n=17, need 17 items, have 16)
			wantErr:    &ErrStackUnderflow{},
			wantOpcode: DUPN,
		},
		{
			name:       "UNDERFLOW_SWAPN",
			codeHex:    "5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5fe700", // (n=17, need 18 items, have 17)
			wantErr:    &ErrStackUnderflow{},
			wantOpcode: SWAPN,
		},
		{
			name:       "UNDERFLOW_EXCHANGE",
			codeHex:    "60016002e801", // (n,m)=(1,2), need 3 items, have 2
			wantErr:    &ErrStackUnderflow{},
			wantOpcode: EXCHANGE,
		},
		{
			name:     "PC_INCREMENT",
			codeHex:  "600060006000e80115",
			wantVals: []uint64{1, 0, 0},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code := common.FromHex(tc.codeHex)
			pc := uint64(0)
			callContext := new(CallContext)
			callContext.Contract.Code = code
			var err error
			var errOp OpCode
			for pc < uint64(len(code)) && err == nil {
				op := code[pc]
				switch OpCode(op) {
				case STOP:
					return
				case PUSH1:
					pc, _, err = opPush1(pc, evm, callContext)
				case DUP1:
					dup1 := makeDup(1)
					pc, _, err = dup1(pc, evm, callContext)
				case JUMP:
					pc, _, err = opJump(pc, evm, callContext)
				case JUMPDEST:
					pc, _, err = opJumpdest(pc, evm, callContext)
				case DUPN:
					pc, _, err = opDupN(pc, evm, callContext)
				case ISZERO:
					pc, _, err = opIszero(pc, evm, callContext)
				case PUSH0:
					pc, _, err = opPush0(pc, evm, callContext)
				case SWAPN:
					pc, _, err = opSwapN(pc, evm, callContext)
				case EXCHANGE:
					pc, _, err = opExchange(pc, evm, callContext)
				default:
					t.Fatalf("unexpected opcode %s at pc=%d", OpCode(op), pc)
				}
				if err != nil {
					errOp = OpCode(op)
				}
				pc++
			}
			if tc.wantErr != nil {
				// Fail because we wanted an error, but didn't get one.
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				// Fail if the wrong opcode threw an error.
				if errOp != tc.wantOpcode {
					t.Fatalf("expected error from opcode %s, got %s", tc.wantOpcode, errOp)
				}
				// Fail if we don't get the error we expect.
				switch tc.wantErr.(type) {
				case *ErrInvalidOpCode:
					var want *ErrInvalidOpCode
					if !errors.As(err, &want) {
						t.Fatalf("expected ErrInvalidOpCode, got %v", err)
					}
				case *ErrStackUnderflow:
					var want *ErrStackUnderflow
					if !errors.As(err, &want) {
						t.Fatalf("expected ErrStackUnderflow, got %v", err)
					}
				default:
					t.Fatalf("unsupported wantErr type %T", tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			stack := callContext.Stack
			got := make([]uint64, 0, stack.len())
			for i := stack.len() - 1; i >= 0; i-- {
				got = append(got, stack.data[i].Uint64())
			}
			if len(got) != len(tc.wantVals) {
				t.Fatalf("stack len=%d; want %d", len(got), len(tc.wantVals))
			}
			for i := range got {
				if got[i] != tc.wantVals[i] {
					t.Fatalf("[%s] stack[%d]=%d; want %d\nstack=%v",
						tc.name, i, got[i], tc.wantVals[i], got)
				}
			}
		})
	}
}
