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

package vm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/math"
)

// precompiledTest defines the input/output pairs for precompiled contract tests.
type precompiledTest struct {
	Input, Expected string
	Gas             uint64
	Name            string
	NoBenchmark     bool // Benchmark primarily the worst-cases
}

// precompiledFailureTest defines the input/error pairs for precompiled
// contract failure tests.
type precompiledFailureTest struct {
	Input         string
	ExpectedError string
	Name          string
}

// allPrecompiles does not map to the actual set of precompiles, as it also contains
// repriced versions of precompiles at certain slots
var allPrecompiles = map[common.Address]PrecompiledContract{
	common.BytesToAddress([]byte{0x01}):       &ecrecover{},
	common.BytesToAddress([]byte{0x02}):       &sha256hash{},
	common.BytesToAddress([]byte{0x03}):       &ripemd160hash{},
	common.BytesToAddress([]byte{0x04}):       &dataCopy{},
	common.BytesToAddress([]byte{0x05}):       &bigModExp{eip2565: false},
	common.BytesToAddress([]byte{0xa5}):       &bigModExp{eip2565: true},
	common.BytesToAddress([]byte{0xb5}):       &bigModExp{osaka: true},
	common.BytesToAddress([]byte{0x06}):       &bn254AddIstanbul{},
	common.BytesToAddress([]byte{0x07}):       &bn254ScalarMulIstanbul{},
	common.BytesToAddress([]byte{0x08}):       &bn254PairingIstanbul{},
	common.BytesToAddress([]byte{0x09}):       &blake2F{},
	common.BytesToAddress([]byte{0x0a}):       &pointEvaluation{},
	common.BytesToAddress([]byte{0x0b}):       &bls12381G1Add{},
	common.BytesToAddress([]byte{0x0c}):       &bls12381G1MultiExp{},
	common.BytesToAddress([]byte{0x0d}):       &bls12381G2Add{},
	common.BytesToAddress([]byte{0x0e}):       &bls12381G2MultiExp{},
	common.BytesToAddress([]byte{0x0f}):       &bls12381Pairing{},
	common.BytesToAddress([]byte{0x10}):       &bls12381MapFpToG1{},
	common.BytesToAddress([]byte{0x11}):       &bls12381MapFp2ToG2{},
	common.BytesToAddress([]byte{0x01, 0x00}): &p256Verify{},
	common.BytesToAddress([]byte{0xa1, 0x00}): &p256Verify{eip7951: true},
}

// EIP-152 test vectors
var blake2FMalformedInputTests = []precompiledFailureTest{
	{
		Input:         "",
		ExpectedError: errBlake2FInvalidInputLength.Error(),
		Name:          "vector 0: empty input",
	},
	{
		Input:         "00000c48c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f6c3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b61626300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000300000000000000000000000000000001",
		ExpectedError: errBlake2FInvalidInputLength.Error(),
		Name:          "vector 1: less than 213 bytes input",
	},
	{
		Input:         "000000000c48c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f6c3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b61626300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000300000000000000000000000000000001",
		ExpectedError: errBlake2FInvalidInputLength.Error(),
		Name:          "vector 2: more than 213 bytes input",
	},
	{
		Input:         "0000000c48c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f6c3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b61626300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000300000000000000000000000000000002",
		ExpectedError: errBlake2FInvalidFinalFlag.Error(),
		Name:          "vector 3: malformed final block indicator flag",
	},
}

func testPrecompiled(t *testing.T, addr string, test precompiledTest) {
	p := allPrecompiles[common.HexToAddress(addr)]
	in := common.Hex2Bytes(test.Input)
	gas := p.RequiredGas(in)
	t.Run(fmt.Sprintf("%s-Gas=%d", test.Name, gas), func(t *testing.T) {
		t.Parallel()
		if res, _, err := RunPrecompiledContract(p, in, gas, nil); err != nil {
			t.Error(err)
		} else if common.Bytes2Hex(res) != test.Expected {
			t.Errorf("Expected %v, got %v", test.Expected, common.Bytes2Hex(res))
		}
		if expGas := test.Gas; expGas != gas {
			t.Errorf("%v: gas wrong, expected %d, got %d", test.Name, expGas, gas)
		}
		// Verify that the precompile did not touch the input buffer
		exp := common.Hex2Bytes(test.Input)
		if !bytes.Equal(in, exp) {
			t.Errorf("Precompiled %v modified input data", addr)
		}
	})
}

func testPrecompiledOOG(t *testing.T, addr string, test precompiledTest) {
	p := allPrecompiles[common.HexToAddress(addr)]
	in := common.Hex2Bytes(test.Input)
	gas := p.RequiredGas(in) - 1

	t.Run(fmt.Sprintf("%s-Gas=%d", test.Name, gas), func(t *testing.T) {
		t.Parallel()
		_, _, err := RunPrecompiledContract(p, in, gas, nil)
		if err.Error() != "out of gas" {
			t.Errorf("Expected error [out of gas], got [%v]", err)
		}
		// Verify that the precompile did not touch the input buffer
		exp := common.Hex2Bytes(test.Input)
		if !bytes.Equal(in, exp) {
			t.Errorf("Precompiled %v modified input data", addr)
		}
	})
}

func testPrecompiledFailure(addr string, test precompiledFailureTest, t *testing.T) {
	p := allPrecompiles[common.HexToAddress(addr)]
	in := common.Hex2Bytes(test.Input)
	gas := p.RequiredGas(in)
	t.Run(test.Name, func(t *testing.T) {
		t.Parallel()
		_, _, err := RunPrecompiledContract(p, in, gas, nil)
		if err == nil || err.Error() != test.ExpectedError {
			t.Errorf("Expected error [%v], got [%v]", test.ExpectedError, err)
		}
		// Verify that the precompile did not touch the input buffer
		exp := common.Hex2Bytes(test.Input)
		if !bytes.Equal(in, exp) {
			t.Errorf("Precompiled %v modified input data", addr)
		}
	})
}

// Tests the sample inputs from the ModExp EIP 198.
func TestPrecompiledModExp(t *testing.T)            { testJson("modexp", "05", t) }
func TestPrecompiledModExpEip2565(t *testing.T)     { testJson("modexp_eip2565", "a5", t) }
func TestPrecompiledModExpEip7883(t *testing.T)     { testJson("modexp_eip7883", "b5", t) }
func TestPrecompiledModExpEip7823Fail(t *testing.T) { testJsonFail("modexp-eip7823", "b5", t) }

// Tests the sample inputs from the elliptic curve addition EIP 213.
func TestPrecompiledBn254Add(t *testing.T) { testJson("bn254Add", "06", t) }

// Tests OOG
func TestPrecompiledModExpOOG(t *testing.T) {
	t.Parallel()
	modexpTests, err := loadJson("modexp")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range modexpTests {
		testPrecompiledOOG(t, "05", test)
	}
}

func TestPrecompiledModExpPotentialOutOfRange(t *testing.T) {
	modExpContract := allPrecompiles[common.BytesToAddress([]byte{0xa5})]
	hexString := "0x0000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000ffffffffffffffff0000000000000000000000000000000000000000000000000000000000000000ee"
	input := hexutil.MustDecode(hexString)
	maxGas := uint64(math.MaxUint64)
	_, _, err := RunPrecompiledContract(modExpContract, input, maxGas, nil)
	require.NoError(t, err)
}

func TestPrecompiledModExpInputEip7823(t *testing.T) {
	pragueModExp := allPrecompiles[common.BytesToAddress([]byte{0xa5})]
	osakaModExp := allPrecompiles[common.BytesToAddress([]byte{0xb5})]

	// length_of_EXPONENT = 1024; everything else is zero
	in := common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000000")
	gas := pragueModExp.RequiredGas(in)
	res, _, err := RunPrecompiledContract(pragueModExp, in, gas, nil)
	require.NoError(t, err)
	assert.Equal(t, "", common.Bytes2Hex(res))
	gas = osakaModExp.RequiredGas(in)
	_, _, err = RunPrecompiledContract(osakaModExp, in, gas, nil)
	require.NoError(t, err)
	assert.Equal(t, "", common.Bytes2Hex(res))

	// length_of_EXPONENT = 1025; everything else is zero
	in = common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004010000000000000000000000000000000000000000000000000000000000000000")
	gas = pragueModExp.RequiredGas(in)
	res, _, err = RunPrecompiledContract(pragueModExp, in, gas, nil)
	require.NoError(t, err)
	assert.Equal(t, "", common.Bytes2Hex(res))
	gas = osakaModExp.RequiredGas(in)
	_, _, err = RunPrecompiledContract(osakaModExp, in, gas, nil)
	assert.ErrorIs(t, err, errModExpExponentLengthTooLarge)

	// length_of_EXPONENT = 2048; everything else is zero
	in = common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008000000000000000000000000000000000000000000000000000000000000000000")
	gas = pragueModExp.RequiredGas(in)
	res, _, err = RunPrecompiledContract(pragueModExp, in, gas, nil)
	require.NoError(t, err)
	assert.Equal(t, "", common.Bytes2Hex(res))
	gas = osakaModExp.RequiredGas(in)
	_, _, err = RunPrecompiledContract(osakaModExp, in, gas, nil)
	assert.ErrorIs(t, err, errModExpExponentLengthTooLarge)

	// length_of_EXPONENT = 2^32; everything else is zero
	in = common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000")
	gas = pragueModExp.RequiredGas(in)
	res, _, err = RunPrecompiledContract(pragueModExp, in, gas, nil)
	require.NoError(t, err)
	assert.Equal(t, "", common.Bytes2Hex(res))
	gas = osakaModExp.RequiredGas(in)
	_, _, err = RunPrecompiledContract(osakaModExp, in, gas, nil)
	assert.ErrorIs(t, err, errModExpExponentLengthTooLarge)

	// length_of_EXPONENT = 2^64; everything else is zero
	in = common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000000000000000000")
	gas = pragueModExp.RequiredGas(in)
	res, _, err = RunPrecompiledContract(pragueModExp, in, gas, nil)
	require.NoError(t, err)
	assert.Equal(t, "", common.Bytes2Hex(res))
	gas = osakaModExp.RequiredGas(in)
	_, _, err = RunPrecompiledContract(osakaModExp, in, gas, nil)
	assert.ErrorIs(t, err, errModExpExponentLengthTooLarge)
}

// Tests the sample inputs from the elliptic curve scalar multiplication EIP 213.
func TestPrecompiledBn254ScalarMul(t *testing.T) { testJson("bn254ScalarMul", "07", t) }
func TestPrecompiledBn254ScalarMulFail(t *testing.T) {
	testJsonFail("bn254ScalarMul", "07", t)
}

// Tests the sample inputs from the elliptic curve pairing check EIP 197.
func TestPrecompiledBn254Pairing(t *testing.T) { testJson("bn254Pairing", "08", t) }
func TestPrecompiledBlake2F(t *testing.T)      { testJson("blake2F", "09", t) }
func TestPrecompileBlake2FMalformedInput(t *testing.T) {
	t.Parallel()
	for _, test := range blake2FMalformedInputTests {
		testPrecompiledFailure("09", test, t)
	}
}

func TestPrecompiledEcrecover(t *testing.T) { testJson("ecRecover", "01", t) }
func testJson(name, addr string, t *testing.T) {
	tests, err := loadJson(name)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		testPrecompiled(t, addr, test)
	}
}

func testJsonFail(name, addr string, t *testing.T) {
	tests, err := loadJsonFail(name)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		testPrecompiledFailure(addr, test, t)
	}
}

func TestPrecompiledBLS12381G1Add(t *testing.T)      { testJson("blsG1Add", "0b", t) }
func TestPrecompiledBLS12381G1MultiExp(t *testing.T) { testJson("blsG1MultiExp", "0c", t) }
func TestPrecompiledBLS12381G2Add(t *testing.T)      { testJson("blsG2Add", "0d", t) }
func TestPrecompiledBLS12381G2MultiExp(t *testing.T) { testJson("blsG2MultiExp", "0e", t) }
func TestPrecompiledBLS12381Pairing(t *testing.T)    { testJson("blsPairing", "0f", t) }
func TestPrecompiledBLS12381MapG1(t *testing.T)      { testJson("blsMapG1", "10", t) }
func TestPrecompiledBLS12381MapG2(t *testing.T)      { testJson("blsMapG2", "11", t) }
func TestPrecompiledPointEvaluation(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	testJson("pointEvaluation", "0a", t)
}

// Failure tests
func TestPrecompiledBLS12381G1AddFail(t *testing.T)      { testJsonFail("blsG1Add", "0b", t) }
func TestPrecompiledBLS12381G1MultiExpFail(t *testing.T) { testJsonFail("blsG1MultiExp", "0c", t) }
func TestPrecompiledBLS12381G2AddFail(t *testing.T)      { testJsonFail("blsG2Add", "0d", t) }
func TestPrecompiledBLS12381G2MultiExpFail(t *testing.T) { testJsonFail("blsG2MultiExp", "0e", t) }
func TestPrecompiledBLS12381PairingFail(t *testing.T)    { testJsonFail("blsPairing", "0f", t) }
func TestPrecompiledBLS12381MapG1Fail(t *testing.T)      { testJsonFail("blsMapG1", "10", t) }
func TestPrecompiledBLS12381MapG2Fail(t *testing.T)      { testJsonFail("blsMapG2", "11", t) }

func loadJson(name string) ([]precompiledTest, error) {
	data, err := os.ReadFile(fmt.Sprintf("testdata/precompiles/%v.json", name))
	if err != nil {
		return nil, err
	}
	var testcases []precompiledTest
	err = json.Unmarshal(data, &testcases)
	return testcases, err
}

func loadJsonFail(name string) ([]precompiledFailureTest, error) {
	data, err := os.ReadFile(fmt.Sprintf("testdata/precompiles/fail-%v.json", name))
	if err != nil {
		return nil, err
	}
	var testcases []precompiledFailureTest
	err = json.Unmarshal(data, &testcases)
	return testcases, err
}

func TestPrecompiledP256Verify(t *testing.T) {
	t.Parallel()
	testJson("p256Verify", "100", t)
	testJson("p256Verify-EIP-7951", "a100", t)
}

// precompileSuccessVectors names, per precompile address, a fixture holding at
// least one input that precompile accepts. 0x02, 0x03 and 0x04 accept anything
// and have no fixture.
var precompileSuccessVectors = map[string]string{
	"01": "ecRecover", "05": "modexp", "a5": "modexp_eip2565", "b5": "modexp_eip7883",
	"06": "bn254Add", "07": "bn254ScalarMul", "08": "bn254Pairing", "09": "blake2F",
	"0a": "pointEvaluation", "0b": "blsG1Add", "0c": "blsG1MultiExp", "0d": "blsG2Add",
	"0e": "blsG2MultiExp", "0f": "blsPairing", "10": "blsMapG1", "11": "blsMapG2",
	"100": "p256Verify", "a100": "p256Verify-EIP-7951",
}

// precompiledContractSets is every fork's live precompile set.
var precompiledContractSets = []PrecompiledContracts{
	PrecompiledContractsHomestead, PrecompiledContractsByzantium, PrecompiledContractsIstanbul,
	PrecompiledContractsBerlin, PrecompiledContractsCancun, PrecompiledContractsPrague,
	PrecompiledContractsOsaka,
}

// checkNoAlias runs p against rawInput from a backing array with a spare tail,
// mirroring the shorter-than-capacity slice Memory.GetPtr hands a precompile at
// runtime, then flips every backing byte -- including the tail, which an output
// could alias without ever being written to. A first, throwaway run sizes that
// tail: an output appended into the input's spare capacity only lands there when
// the tail can hold it, so a fixed bound would pass silently for any output
// wider than it. Reports whether the call produced the non-empty success a
// coverage requirement needs.
func checkNoAlias(t *testing.T, p PrecompiledContract, rawInput []byte) bool {
	t.Helper()
	probe, err := p.Run(bytes.Clone(rawInput))
	if err != nil || len(probe) == 0 {
		return false
	}

	backing := make([]byte, len(rawInput)+len(probe))
	copy(backing, rawInput)
	input := backing[:len(rawInput)]

	out, err := p.Run(input)
	require.NoError(t, err, "precompile %s", p.Name())
	want := bytes.Clone(out)
	for i := range backing {
		backing[i] ^= 0xff
	}
	require.Equal(t, want, out, "precompile %s output aliases its input", p.Name())
	return true
}

// TestPrecompileOutputDoesNotAliasInput pins the invariant the CALL opcodes
// rely on: a precompile's output never shares memory with its input, which is
// what lets the caller keep it as return data without copying it first.
func TestPrecompileOutputDoesNotAliasInput(t *testing.T) {
	t.Parallel()

	covered := map[common.Address]bool{}
	for hexAddr, fixture := range precompileSuccessVectors {
		addr := common.HexToAddress(hexAddr)
		p := allPrecompiles[addr]
		require.NotNil(t, p, "no precompile at %s", hexAddr)
		tests, err := loadJson(fixture)
		require.NoError(t, err)
		for _, test := range tests {
			if checkNoAlias(t, p, common.Hex2Bytes(test.Input)) {
				covered[addr] = true
			}
		}
	}
	for _, hexAddr := range []string{"02", "03", "04"} {
		addr := common.HexToAddress(hexAddr)
		if checkNoAlias(t, allPrecompiles[addr], bytes.Repeat([]byte{0xa5}, 128)) {
			covered[addr] = true
		}
	}
	for addr, p := range allPrecompiles {
		require.True(t, covered[addr], "precompile %s at %x produced no non-empty output, so it was never checked", p.Name(), addr)
	}

	// allPrecompiles is hand-maintained and can omit a fork-only implementation,
	// such as the Byzantium bn254 wrappers that later forks superseded at the
	// same address. Cover every concrete type each live fork set actually runs.
	fixtureByAddr := make(map[common.Address]string, len(precompileSuccessVectors))
	for hexAddr, fixture := range precompileSuccessVectors {
		fixtureByAddr[common.HexToAddress(hexAddr)] = fixture
	}
	seenTypes := map[reflect.Type]bool{}
	for _, p := range allPrecompiles {
		seenTypes[reflect.TypeOf(p)] = true
	}
	for _, set := range precompiledContractSets {
		for addr, p := range set {
			typ := reflect.TypeOf(p)
			if seenTypes[typ] {
				continue
			}
			seenTypes[typ] = true

			a := addr.Value()
			fixture, ok := fixtureByAddr[a]
			require.True(t, ok, "no success-vector fixture for precompile %s at %s", p.Name(), a)
			tests, err := loadJson(fixture)
			require.NoError(t, err)
			ran := false
			for _, test := range tests {
				if checkNoAlias(t, p, common.Hex2Bytes(test.Input)) {
					ran = true
				}
			}
			require.True(t, ran, "precompile %s produced no non-empty output, so it was never checked", p.Name())
		}
	}
}
