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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/maphash"
	"maps"
	"math/big"
	"os"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	bls "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types/accounts"
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
		if res, _, err := RunPrecompiledContract(p, in, mdgas.MdGas{Execution: gas}, nil); err != nil {
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
		suppliedGas := mdgas.MdGas{Execution: gas, State: 50_000}
		_, remaining, err := RunPrecompiledContract(p, in, suppliedGas, nil)
		require.ErrorIs(t, err, ErrOutOfGas)
		require.Equal(t, suppliedGas, remaining)
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
		_, _, err := RunPrecompiledContract(p, in, mdgas.MdGas{Execution: gas}, nil)
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
	_, _, err := RunPrecompiledContract(modExpContract, input, mdgas.MdGas{Execution: maxGas}, nil)
	require.NoError(t, err)
}

func TestPrecompiledModExpInputEip7823(t *testing.T) {
	pragueModExp := allPrecompiles[common.BytesToAddress([]byte{0xa5})]
	osakaModExp := allPrecompiles[common.BytesToAddress([]byte{0xb5})]

	// length_of_EXPONENT = 1024; everything else is zero
	in := common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000000")
	gas := pragueModExp.RequiredGas(in)
	res, _, err := RunPrecompiledContract(pragueModExp, in, mdgas.MdGas{Execution: gas}, nil)
	require.NoError(t, err)
	assert.Equal(t, "", common.Bytes2Hex(res))
	gas = osakaModExp.RequiredGas(in)
	_, _, err = RunPrecompiledContract(osakaModExp, in, mdgas.MdGas{Execution: gas}, nil)
	require.NoError(t, err)
	assert.Equal(t, "", common.Bytes2Hex(res))

	// length_of_EXPONENT = 1025; everything else is zero
	in = common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004010000000000000000000000000000000000000000000000000000000000000000")
	gas = pragueModExp.RequiredGas(in)
	res, _, err = RunPrecompiledContract(pragueModExp, in, mdgas.MdGas{Execution: gas}, nil)
	require.NoError(t, err)
	assert.Equal(t, "", common.Bytes2Hex(res))
	gas = osakaModExp.RequiredGas(in)
	_, _, err = RunPrecompiledContract(osakaModExp, in, mdgas.MdGas{Execution: gas}, nil)
	assert.ErrorIs(t, err, errModExpExponentLengthTooLarge)

	// length_of_EXPONENT = 2048; everything else is zero
	in = common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008000000000000000000000000000000000000000000000000000000000000000000")
	gas = pragueModExp.RequiredGas(in)
	res, _, err = RunPrecompiledContract(pragueModExp, in, mdgas.MdGas{Execution: gas}, nil)
	require.NoError(t, err)
	assert.Equal(t, "", common.Bytes2Hex(res))
	gas = osakaModExp.RequiredGas(in)
	_, _, err = RunPrecompiledContract(osakaModExp, in, mdgas.MdGas{Execution: gas}, nil)
	assert.ErrorIs(t, err, errModExpExponentLengthTooLarge)

	// length_of_EXPONENT = 2^32; everything else is zero
	in = common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000")
	gas = pragueModExp.RequiredGas(in)
	res, _, err = RunPrecompiledContract(pragueModExp, in, mdgas.MdGas{Execution: gas}, nil)
	require.NoError(t, err)
	assert.Equal(t, "", common.Bytes2Hex(res))
	gas = osakaModExp.RequiredGas(in)
	_, _, err = RunPrecompiledContract(osakaModExp, in, mdgas.MdGas{Execution: gas}, nil)
	assert.ErrorIs(t, err, errModExpExponentLengthTooLarge)

	// length_of_EXPONENT = 2^64; everything else is zero
	in = common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000000000000000000")
	gas = pragueModExp.RequiredGas(in)
	res, _, err = RunPrecompiledContract(pragueModExp, in, mdgas.MdGas{Execution: gas}, nil)
	require.NoError(t, err)
	assert.Equal(t, "", common.Bytes2Hex(res))
	gas = osakaModExp.RequiredGas(in)
	_, _, err = RunPrecompiledContract(osakaModExp, in, mdgas.MdGas{Execution: gas}, nil)
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
			if cached, ok := p.(*cachedPrecompile); ok {
				p = cached.PrecompiledContract
			}
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

func TestForkSetsCoverEveryTier(t *testing.T) {
	for i := range int(forkTierCount) {
		tier := forkTier(i)
		require.NotEmpty(t, forkSets[tier].contracts, "forkSets[%d] has no contracts", tier)
		require.NotEmpty(t, forkSets[tier].addresses, "forkSets[%d] has no addresses", tier)
		require.Len(t, forkSets[tier].addresses, len(forkSets[tier].contracts),
			"forkSets[%d] address list and contract map disagree", tier)
	}
}

// TestDeprecatedForkAddressExportsTrackTheirSets pins the exported per-fork
// address slices to the sets they name. Chains outside this repo compile
// against them, so an empty or drifted slice is a break no in-repo grep sees.
func TestDeprecatedForkAddressExportsTrackTheirSets(t *testing.T) {
	for name, tc := range map[string]struct {
		addrs     []accounts.Address
		contracts PrecompiledContracts
	}{
		"homestead": {PrecompiledAddressesHomestead, PrecompiledContractsHomestead},
		"byzantium": {PrecompiledAddressesByzantium, PrecompiledContractsByzantium},
		"istanbul":  {PrecompiledAddressesIstanbul, PrecompiledContractsIstanbul},
		"berlin":    {PrecompiledAddressesBerlin, PrecompiledContractsBerlin},
		"cancun":    {PrecompiledAddressesCancun, PrecompiledContractsCancun},
		"prague":    {PrecompiledAddressesPrague, PrecompiledContractsPrague},
		"osaka":     {PrecompiledAddressesOsaka, PrecompiledContractsOsaka},
	} {
		require.NotEmpty(t, tc.addrs, name)
		require.ElementsMatch(t, slices.Collect(maps.Keys(tc.contracts)), tc.addrs, name)
	}
}

var cacheTestID atomic.Uint64

func wholeInputKey(input []byte) ([]byte, bool) { return input, true }

type countingPrecompile struct {
	calls  int
	gas    uint64
	output []byte
	err    error
}

func (p *countingPrecompile) Run([]byte) ([]byte, error) { p.calls++; return p.output, p.err }
func (p *countingPrecompile) RequiredGas([]byte) uint64 {
	if p.gas != 0 {
		return p.gas
	}
	return 3000
}
func (p *countingPrecompile) Name() string { return "counting" }

func TestCachedPrecompileOwnershipAndGas(t *testing.T) {
	p := &countingPrecompile{output: []byte{1, 2, 3}}
	c := newCachedPrecompile(p, wholeInputKey, [21]byte{201})
	input := fmt.Appendf(nil, "%s/%d", t.Name(), cacheTestID.Add(1))
	out, gas, err := RunPrecompiledContract(c, input, mdgas.MdGas{Execution: 3050}, nil)
	if err != nil || gas.Execution != 50 {
		t.Fatalf("gas=%v err=%v", gas, err)
	}
	out[0] = 9
	out, gas, err = RunPrecompiledContract(c, input, mdgas.MdGas{Execution: 3050}, nil)
	if err != nil || gas.Execution != 50 || !bytes.Equal(out, []byte{1, 2, 3}) || p.calls != 1 {
		t.Fatalf("output=%x gas=%v calls=%d err=%v", out, gas, p.calls, err)
	}
	out[1] = 9
	out, _, err = RunPrecompiledContract(c, input, mdgas.MdGas{Execution: 3000}, nil)
	if err != nil || !bytes.Equal(out, []byte{1, 2, 3}) {
		t.Fatal("cache output was mutated")
	}
	_, _, err = RunPrecompiledContract(c, input, mdgas.MdGas{Execution: 2999}, nil)
	if !errors.Is(err, ErrOutOfGas) || p.calls != 1 {
		t.Fatal("cache hit bypassed gas check")
	}
}

// A cheap call is cached on its second miss, so an input seen once does not pay the insert; a call
// whose gas is below its normalized key length is not cached.
func TestCachedPrecompileAdmission(t *testing.T) {
	run := func(n int, p *countingPrecompile, input []byte, normalize func([]byte) ([]byte, bool)) int {
		c := newCachedPrecompile(p, normalize, [21]byte{209})
		for range n {
			_, err := c.Run(input)
			require.NoError(t, err)
		}
		return p.calls
	}
	input := func(n int) []byte {
		in := make([]byte, n)
		copy(in, fmt.Appendf(nil, "%s/%d", t.Name(), cacheTestID.Add(1)))
		return in
	}
	require.Equal(t, 2, run(2, &countingPrecompile{output: []byte{1}, gas: 200}, input(171), wholeInputKey), "cheap call cached on its first miss")
	require.Equal(t, 2, run(3, &countingPrecompile{output: []byte{1}, gas: 200}, input(171), wholeInputKey), "cheap call not cached")
	require.Equal(t, 2, run(3, &countingPrecompile{output: []byte{1}, gas: 171}, input(171), wholeInputKey), "gas equal to key length not cached")
	require.Equal(t, 3, run(3, &countingPrecompile{output: []byte{1}, gas: 170}, input(171), wholeInputKey), "gas below key length cached")
	require.Equal(t, 2, run(3, &countingPrecompile{output: []byte{1}, gas: 200}, input(300), zeroPaddedKey(128)), "gas above the normalized key length not cached")
	require.Equal(t, 1, run(2, &countingPrecompile{output: make([]byte, 1024)}, input(64), wholeInputKey), "1024-byte output not cached")
	require.Equal(t, 2, run(2, &countingPrecompile{output: make([]byte, 1025)}, input(64), wholeInputKey), "1025-byte output cached")
	require.Equal(t, 1, run(2, &countingPrecompile{output: []byte{1}, gas: 6000}, make([]byte, 96), zeroPaddedKey(96)), "all-zero input not cached")
}

func TestCachedPrecompileDomainAndErrors(t *testing.T) {
	input := fmt.Appendf(nil, "%s/%d", t.Name(), cacheTestID.Add(1))
	p := &countingPrecompile{output: []byte{7}}
	a := newCachedPrecompile(p, wholeInputKey, [21]byte{202})
	b := newCachedPrecompile(p, wholeInputKey, [21]byte{203})
	for _, c := range []*cachedPrecompile{a, b, a, b} {
		if _, err := c.Run(input); err != nil {
			t.Fatal(err)
		}
	}
	if p.calls != 2 {
		t.Fatalf("different domains shared a result: %d calls", p.calls)
	}
	p = &countingPrecompile{err: errors.New("invalid")}
	c := newCachedPrecompile(p, wholeInputKey, [21]byte{204})
	for range 2 {
		if _, err := c.Run(input); err == nil {
			t.Fatal("lost error")
		}
	}
	if p.calls != 2 {
		t.Fatal("cached an error")
	}
	p = &countingPrecompile{output: []byte{7}, gas: 1 << 20}
	c = newCachedPrecompile(p, wholeInputKey, [21]byte{205})
	for range 2 {
		if _, err := c.Run(make([]byte, 64*1024+1)); err != nil {
			t.Fatal(err)
		}
	}
	if p.calls != 2 {
		t.Fatal("cached oversized input")
	}
	if cachePrecompile(p, forkOsaka, common.Address{}) != p {
		t.Fatal("wrapped custom contract")
	}
	p = &countingPrecompile{output: []byte{7}}
	c = newCachedPrecompile(p, wholeInputKey, [21]byte{207})
	precompileResults.Add(maphash.Bytes(precompileSeed, input)^c.domainHash, precompileResult{domain: c.domain, check: maphash.Bytes(precompileCheckSeed, []byte("other")), output: []byte{9}})
	if out, err := c.Run(input); err != nil || !bytes.Equal(out, []byte{7}) || p.calls != 1 {
		t.Fatalf("hash collision returned another input's result: %x", out)
	}
	p = &countingPrecompile{output: []byte{7}}
	c = newCachedPrecompile(p, wholeInputKey, [21]byte{208})
	for range 2 {
		if _, err := c.Run(make([]byte, 3001)); err != nil {
			t.Fatal(err)
		}
	}
	if p.calls != 2 {
		t.Fatal("hashed an input longer than the call's gas")
	}
}

func TestCachedPrecompileParallel(t *testing.T) {
	c := newCachedPrecompile(&bn254PairingIstanbul{}, wholeInputKey, [21]byte{206})
	input := make([]byte, 2*192)
	want, _ := c.PrecompiledContract.Run(input)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 100 {
				output, err := c.Run(input)
				if err != nil || !bytes.Equal(output, want) {
					t.Error("concurrent cache output mismatch")
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestBLSRepeatedPointScalars(t *testing.T) {
	_, _, g1, g2 := bls.Generators()
	for _, tc := range []struct {
		name  string
		point []byte
		p     PrecompiledContract
		want  []byte
	}{
		{"G1", encodePointG1(&g1), &bls12381G1MultiExp{}, encodePointG1(new(bls.G1Affine).ScalarMultiplication(&g1, big.NewInt(136)))},
		{"G2", encodePointG2(&g2), &bls12381G2MultiExp{}, encodePointG2(new(bls.G2Affine).ScalarMultiplication(&g2, big.NewInt(136)))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := make([]byte, 0, 16*(len(tc.point)+32))
			for i := 1; i <= 16; i++ {
				input = append(input, tc.point...)
				var scalar [32]byte
				scalar[31] = byte(i)
				input = append(input, scalar[:]...)
			}
			got, err := tc.p.Run(input)
			if err != nil || !bytes.Equal(got, tc.want) {
				t.Fatalf("got %x, %v; want %x", got, err, tc.want)
			}
			input[len(tc.point)+32] = 1
			if _, err := tc.p.Run(input); err == nil {
				t.Fatal("accepted an invalid point after a repeated point")
			}
		})
	}
}

// A point addition computes in microseconds while a hit costs tens of nanoseconds.
func TestPointAdditionsAreCached(t *testing.T) {
	for _, p := range []PrecompiledContract{&bn254AddIstanbul{}, &bn254AddByzantium{}, &bls12381G1Add{}, &bls12381G2Add{}} {
		require.NotNil(t, precompileCacheKey(p), p.Name())
	}
}

func TestPrecompileCacheKeyNormalizes(t *testing.T) {
	_, ok := precompileCacheKey(&bn254AddIstanbul{})(make([]byte, 128))
	require.False(t, ok, "an addition of two points at infinity is cheaper than a lookup")

	key, ok := zeroPaddedKey(128)(append([]byte{1, 2}, make([]byte, 300)...))
	require.True(t, ok)
	require.Equal(t, []byte{1, 2}, key, "trailing zeros and bytes past the read prefix must not split entries")

	header := make([]byte, 96)
	header[31], header[63], header[95] = 1, 1, 1
	declared := slices.Concat(header, []byte{2, 3, 5})
	key, ok = modExpKey(append(bytes.Clone(declared), 7, 7, 7))
	require.True(t, ok)
	require.Equal(t, declared, key, "bytes past the declared operands must not split entries")
}

func TestCachedPrecompileMatchesNativeVectors(t *testing.T) {
	for hexAddr, fixture := range precompileSuccessVectors {
		addr := common.HexToAddress(hexAddr)
		p := allPrecompiles[addr]
		normalize := precompileCacheKey(p)
		if normalize == nil {
			continue
		}
		// A tier no fork uses, so these entries never serve a real fork's lookups.
		c := newCachedPrecompile(p, normalize, [21]byte{byte(forkTierCount), byte(addr[19])})
		tests, err := loadJson(fixture)
		require.NoError(t, err)
		hits := 0
		for _, test := range tests {
			in := common.Hex2Bytes(test.Input)
			// The padded input is the same call for precompiles that ignore a suffix and a rejected length for the rest.
			for _, input := range [][]byte{in, slices.Concat(in, make([]byte, 64))} {
				want, wantErr := p.Run(input)
				for i := range 3 { // a cheap call is cached on its second miss
					if i == 2 && cachedPrecompileHit(c, input) {
						hits++
					}
					got, err := c.Run(input)
					require.Equal(t, wantErr == nil, err == nil, "%s %s", p.Name(), test.Name)
					if wantErr != nil {
						require.Equal(t, wantErr.Error(), err.Error(), "%s %s", p.Name(), test.Name)
					}
					require.Equal(t, want, got, "%s %s", p.Name(), test.Name)
				}
			}
		}
		require.Positive(t, hits, "%s: no vector was served from the cache", p.Name())
	}
}

func cachedPrecompileHit(c *cachedPrecompile, input []byte) bool {
	in, _ := c.normalize(input)
	r, ok := precompileResults.Get(maphash.Bytes(precompileSeed, in) ^ c.domainHash)
	return ok && r.domain == c.domain && r.check == maphash.Bytes(precompileCheckSeed, in)
}

// Every byte a key drops must be one the native precompile ignores. Dropped bytes come in
// contiguous runs (past a prefix, past the declared operands, trailing zeros), so three
// samples per run are enough to catch a key that drops a byte the result depends on.
func TestPrecompileCacheKeyKeepsEveryDecidingByte(t *testing.T) {
	for hexAddr, fixture := range precompileSuccessVectors {
		p := allPrecompiles[common.HexToAddress(hexAddr)]
		normalize := precompileCacheKey(p)
		if normalize == nil {
			continue
		}
		tests, err := loadJson(fixture)
		require.NoError(t, err)
		for _, test := range tests {
			in := common.Hex2Bytes(test.Input)
			key, ok := normalize(in)
			if !ok {
				continue
			}
			key = bytes.Clone(key)
			var runs [][2]int
			for i := range in {
				in[i] ^= 0xff
				k, ok := normalize(in)
				same := ok && len(k) == len(key) && (i >= len(k) || (k[i] == key[i] && bytes.Equal(k, key)))
				in[i] ^= 0xff
				if !same {
					continue
				}
				if n := len(runs); n > 0 && runs[n-1][1] == i-1 {
					runs[n-1][1] = i
				} else {
					runs = append(runs, [2]int{i, i})
				}
			}
			if len(runs) == 0 {
				continue
			}
			want, wantErr := p.Run(in)
			for _, r := range runs {
				for _, i := range []int{r[0], (r[0] + r[1]) / 2, r[1]} {
					flipped := bytes.Clone(in)
					flipped[i] ^= 0xff
					got, err := p.Run(flipped)
					require.Equal(t, wantErr == nil, err == nil, "%s %s byte %d", p.Name(), test.Name, i)
					require.Equal(t, want, got, "%s %s: byte %d is dropped from the key but changes the result", p.Name(), test.Name, i)
				}
			}
		}
	}
}

// A result cached under one fork must not serve a fork whose rules differ for the same input.
func TestPrecompileCacheSeparatesForks(t *testing.T) {
	in := make([]byte, 96+1025+1+1)
	binary.BigEndian.PutUint64(in[24:32], 1025)
	in[63], in[95] = 1, 1
	in[len(in)-1] = 3
	addr := accounts.InternAddress(common.BytesToAddress([]byte{5}))
	prague, osaka := forkSets[forkPrague].contracts[addr], forkSets[forkOsaka].contracts[addr]
	require.GreaterOrEqual(t, prague.RequiredGas(in), uint64(len(in)))
	for range 2 {
		_, err := prague.Run(in)
		require.NoError(t, err)
		if cached, ok := prague.(*cachedPrecompile); ok {
			require.True(t, cachedPrecompileHit(cached, in), "the Prague result was not cached")
		}
		_, err = osaka.Run(in)
		require.ErrorIs(t, err, errModExpBaseLengthTooLarge)
	}
}

// A zero modulus length answers from the header faster than a cache lookup, so it is never cached.
func TestModExpKeySkipsZeroModulus(t *testing.T) {
	header := func(baseLen, expLen, modLen uint64) []byte {
		in := make([]byte, modExpHeaderLength)
		binary.BigEndian.PutUint64(in[24:32], baseLen)
		binary.BigEndian.PutUint64(in[56:64], expLen)
		binary.BigEndian.PutUint64(in[88:96], modLen)
		return in
	}
	for _, in := range [][]byte{header(32, 0, 0), header(0, 0, 0), append(header(32, 32, 0), make([]byte, 64)...), header(32, 0, 0)[:64]} {
		_, ok := modExpKey(in)
		require.False(t, ok, "%x", in)
	}
	_, ok := modExpKey(append(header(1, 1, 1), 2, 3, 5))
	require.True(t, ok)
}
