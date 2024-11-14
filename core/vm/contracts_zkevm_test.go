package vm

import (
	"bytes"
	"fmt"
	"math/big"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common"
)

var (
	big0    = big.NewInt(0)
	big10   = big.NewInt(10)
	big8194 = big.NewInt(0).Lsh(big.NewInt(1), 8194)
)

func uint64To32Bytes(input int) []byte {
	bigInt := new(big.Int).SetUint64(uint64(input))
	bytes := bigInt.Bytes()
	result := make([]byte, 32)
	copy(result[32-len(bytes):], bytes)
	return result
}

func uint64ToDeterminedBytes(input *big.Int, length int) []byte {
	bytes := input.Bytes()
	result := make([]byte, length)
	copy(result[length-len(bytes):], bytes)
	return result
}

// This relies on PrecompiledContractsForkID13Durian.
func testPrecompiledZkevm(t *testing.T, addr string, test precompiledTest) {
	p := PrecompiledContractsForkID13Durian[libcommon.HexToAddress(addr)]
	in := libcommon.Hex2Bytes(test.Input)
	gas := p.RequiredGas(in)
	t.Run(fmt.Sprintf("%s-Gas=%d", test.Name, gas), func(t *testing.T) {
		t.Parallel()
		if res, _, err := RunPrecompiledContract(p, in, gas); err != nil {
			t.Error(err)
		} else if common.Bytes2Hex(res) != test.Expected {
			t.Errorf("Expected %v, got %v", test.Expected, common.Bytes2Hex(res))
		}
		if expGas := test.Gas; expGas != gas {
			t.Errorf("%v: gas wrong, expected %d, got %d", test.Name, expGas, gas)
		}
		// Verify that the precompile did not touch the input buffer
		exp := libcommon.Hex2Bytes(test.Input)
		if !bytes.Equal(in, exp) {
			t.Errorf("Precompiled %v modified input data", addr)
		}
	})
}

func testJsonZkevm(name, addr string, t *testing.T) {
	tests, err := loadJson(name)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		testPrecompiledZkevm(t, addr, test)
	}
}

func Test_ModExpZkevm_Gas(t *testing.T) {
	modExp := bigModExp_zkevm{enabled: true, eip2565: true}

	cases := map[string]struct {
		base     *big.Int
		exp      *big.Int
		mod      *big.Int
		expected uint64
	}{
		"simple test":                      {big10, big10, big10, 200},
		"0 mod - normal gas":               {big10, big10, big0, 200},
		"base 0 - mod < 8192 - normal gas": {big0, big10, big10, 200},
		"base 0 - mod > 8192 - 0 gas":      {big0, big10, big8194, 0},
		"base over 8192 - 0 gas":           {big8194, big10, big10, 0},
		"exp over 8192 - 0 gas":            {big10, big8194, big10, 0},
		"mod over 8192 - 0 gas":            {big10, big10, big8194, 0},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			input := make([]byte, 0)

			base := len(test.base.Bytes())
			exp := len(test.exp.Bytes())
			mod := len(test.mod.Bytes())

			input = append(input, uint64To32Bytes(base)...)
			input = append(input, uint64To32Bytes(exp)...)
			input = append(input, uint64To32Bytes(mod)...)
			input = append(input, uint64ToDeterminedBytes(test.base, base)...)
			input = append(input, uint64ToDeterminedBytes(test.exp, exp)...)
			input = append(input, uint64ToDeterminedBytes(test.mod, mod)...)

			gas := modExp.RequiredGas(input)

			if gas != test.expected {
				t.Errorf("Expected %d, got %d", test.expected, gas)
			}
		})
	}
}

func TestP256VerifyZkevm(t *testing.T) {
	testJsonZkevm("p256Verify", "0x0000000000000000000000000000000000000100", t)
}

// EIP2565 is enabled by default.
func TestBigModExpZkevm(t *testing.T) {
	testJsonZkevm("modexp_eip2565", "0x0000000000000000000000000000000000000005", t)
}
