package smtv2

import (
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/smt/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestLexographhicalCheckPaths(t *testing.T) {
	// check a series of binary paths for sorting correctness
	tests := []struct {
		path1    []int
		path2    []int
		expected int
	}{
		{[]int{1, 0, 0, 1}, []int{1, 0, 1, 0}, -1},
		{[]int{1, 0, 0, 1}, []int{1, 0, 0, 1}, 0},
		{[]int{1, 0, 0, 1}, []int{1, 0, 0, 0}, 1},
		{[]int{1, 0, 0, 1}, []int{0, 1, 0, 0}, 1},
		{[]int{1, 0, 0, 1}, []int{1, 1, 0, 0}, -1},
		{[]int{1, 0, 0, 1}, []int{1, 0, 1, 1}, -1},
		{[]int{1, 0, 0, 1}, []int{1, 0, 0, 1}, 0},
	}

	for _, test := range tests {
		result := lexographhicalCheckPaths(test.path1, test.path2)
		if result != test.expected {
			t.Errorf("LexographhicalCheckPaths(%v, %v) = %d; expected %d", test.path1, test.path2, result, test.expected)
		}
	}
}

func TestCanFastForward(t *testing.T) {
	tests := []struct {
		path               []int
		expectedShouldStop bool
		expectedPath       []int
	}{
		{[]int{0, 1}, false, []int{1, 0}},
		{[]int{0, 1, 0, 1, 1}, false, []int{0, 1, 1, 0, 0}},
		{[]int{1, 1}, true, []int{0, 0}},
	}

	for _, test := range tests {
		resultPath, shouldStop := NextFastForwardPath(test.path)
		if shouldStop != test.expectedShouldStop {
			t.Errorf("CanFastForward(%v) = %v; expected %v", test.path, shouldStop, test.expectedShouldStop)
		}
		if resultPath != nil && !reflect.DeepEqual(resultPath, test.expectedPath) {
			t.Errorf("CanFastForward(%v) = %v; expected %v", test.path, resultPath, test.expectedPath)
		}
	}
}

func Test_Key(t *testing.T) {
	tests := []struct {
		ethAddr string
		keyType int
	}{
		{"0x0", 0},
		{"0x0", 1},
		{"0x9aeCf44E36f20DC407d1A580630c9a2419912dcB", 2},
	}

	for _, test := range tests {
		new := Key(test.ethAddr, test.keyType)
		old := utils.Key(test.ethAddr, test.keyType)
		for i := 0; i < 4; i++ {
			if new[i] != old[i] {
				t.Errorf("Key(%v, %v) = %v; expected %v", test.ethAddr, test.keyType, new, old)
			}
		}
	}
}

func Test_OldMappingToNodeValue8(t *testing.T) {
	tests := []struct {
		hex string
	}{
		{"0x1"},
		{"0x2"},
		{"0x98765432112345678"},
		{"0xffffffffffffffffffffffffffffffffffffffff"},
		{"0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
	}

	for _, test := range tests {
		hex := strings.TrimPrefix(test.hex, "0x")
		biggy, ok := new(big.Int).SetString(hex, 16)
		if !ok {
			t.Errorf("Failed to convert hex to big.Int for %v", test.hex)
			continue
		}
		oldValue, err := utils.NodeValue8FromBigIntArray(utils.ScalarToArrayBig(biggy))
		if err != nil {
			t.Errorf("Failed to convert big.Int to NodeValue8 for %v", test.hex)
			continue
		}

		newValue := ScalarToSmtValue8FromBytes(biggy)

		for i := 0; i < 8; i++ {
			if newValue[i] != oldValue[i] {
				t.Errorf("ScalarToSmtValue8(%v) = %v; expected %v", test.hex, newValue, oldValue)
			}
		}
	}
}

func Test_BytesToSmtValue8_Comparison(t *testing.T) {
	tests := []struct {
		bytes []byte
	}{
		{common.Hex2Bytes("0x1")},
		{common.Hex2Bytes("0x2")},
		{common.Hex2Bytes("0x98765432112345678")},
		{common.Hex2Bytes("0xffffffffffffffffffffffffffffffffffffffff")},
		{common.Hex2Bytes("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")},
	}

	for _, test := range tests {
		paddedValue := fmt.Sprintf("%032x", common.BytesToHash(test.bytes))
		biggy, ok := new(big.Int).SetString(paddedValue, 16)
		if !ok {
			t.Errorf("Failed to convert hex to big.Int for %v", paddedValue)
			continue
		}

		fromBig := ScalarToSmtValue8FromBits(biggy)
		fromBytes := BytesToSmtValue8(test.bytes)

		for i := 0; i < 8; i++ {
			if fromBig[i] != fromBytes[i] {
				t.Errorf("BytesToSmtValue8(%v) = %v; expected %v", test.bytes, fromBytes, fromBig)
			}
		}
	}
}

func Test_GetCodeAndLength(t *testing.T) {
	tests := []struct {
		code []byte
	}{
		{[]byte{}},
		{common.Hex2Bytes("01")},
		{common.Hex2Bytes("9999")},
		{common.Hex2Bytes("1233459845676739438575623497394545968734592354837456948576498567948576")},
	}

	for _, test := range tests {
		oldCode, oldLength, err := GetCodeAndLength(test.code)
		if err != nil {
			t.Errorf("GetCodeAndLength(%v) = %v; expected %v", test.code, err, nil)
			continue
		}

		newCode, newLength, err := GetCodeAndLengthNoBig(test.code)
		if err != nil {
			t.Errorf("GetCodeAndLength(%v) = %v; expected %v", test.code, err, nil)
			continue
		}

		for i := 0; i < 8; i++ {
			if oldCode[i] != newCode[i] {
				t.Errorf("GetCodeAndLength(%v) = %v; expected %v", test.code, newCode, oldCode)
			}
		}

		for i := 0; i < 8; i++ {
			if oldLength[i] != newLength[i] {
				t.Errorf("GetCodeAndLength(%v) = %v; expected %v", test.code, newLength, oldLength)
			}
		}
	}
}

func Test_GetAllSharedPaths(t *testing.T) {
	tests := []struct {
		path     []int
		expected [][]int
	}{
		{
			path: []int{1, 0, 1, 1, 0, 5},
			expected: [][]int{
				{1, 0, 1, 1, 0, 5},
				{1, 0, 1, 1, 0, 4},
				{1, 0, 1, 0, 0, 3},
				{1, 0, 0, 0, 0, 2},
				{1, 0, 0, 0, 0, 1},
			},
		},
	}

	for _, test := range tests {
		result := GetAllSharedPaths(test.path)

		for i, v := range result {
			assert.Equal(t, len(v), len(test.expected[i]))
			assert.Equal(t, v, test.expected[i])
		}
	}
}

func Test_PathToKeyBytes_Reversal(t *testing.T) {
	tests := []struct {
		path []int
	}{
		{[]int{1, 0, 1, 1, 0}},
		{[]int{0}},
		{[]int{1, 1, 1, 1}},
		{[]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0}},
		{[]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 1, 1, 1, 1, 1, 0, 0}},
		{[]int{1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 0, 1, 1, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 0, 1,
			0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 1, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0,
			1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1}},
	}

	for _, test := range tests {
		// pad out the test sample
		padded := make([]int, 257)
		copy(padded, test.path)
		padded[256] = len(test.path)

		key := PathToKeyBytes(test.path, len(test.path))
		resultPath, length, err := KeyBytesToPath(key)
		assert.NoError(t, err)
		assert.Equal(t, key[32], byte(len(test.path)))
		assert.Equal(t, length, len(test.path))
		assert.Equal(t, resultPath, padded)
	}
}

func TestKeyVsKeyWithoutBig(t *testing.T) {
	addr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	key := AddrKeyNonce(addr)
	keyWithoutBig := utils.KeyWithoutBig(addr, KEY_NONCE)

	for i := 0; i < 4; i++ {
		if key[i] != keyWithoutBig[i] {
			t.Errorf("key doesn't match, expected: %v, got: %v", key, keyWithoutBig)
		}
	}

	key = AddrKeyBalance(addr)
	keyWithoutBig = utils.KeyWithoutBig(addr, KEY_BALANCE)

	for i := 0; i < 4; i++ {
		if key[i] != keyWithoutBig[i] {
			t.Errorf("key doesn't match, expected: %v, got: %v", key, keyWithoutBig)
		}
	}

	key = AddrKeyCode(addr)
	keyWithoutBig = utils.KeyWithoutBig(addr, SC_CODE)

	for i := 0; i < 4; i++ {
		if key[i] != keyWithoutBig[i] {
			t.Errorf("key doesn't match, expected: %v, got: %v", key, keyWithoutBig)
		}
	}

	key = AddrKeyLength(addr)
	keyWithoutBig = utils.KeyWithoutBig(addr, SC_LENGTH)

	for i := 0; i < 4; i++ {
		if key[i] != keyWithoutBig[i] {
			t.Errorf("key doesn't match, expected: %v, got: %v", key, keyWithoutBig)
		}
	}
}
