package merkletree

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testVectorKey struct {
	EthAddr         string `json:"ethAddr"`
	StoragePosition string `json:"storagePosition"`
	ExpectedKey     string `json:"expectedKey"`
}

type bytecodeTest struct {
	Bytecode     string `json:"bytecode"`
	ExpectedHash string `json:"expectedHash"`
}

func init() {
	// Change dir to project root
	// This is important because we have relative paths to files containing test vectors
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "../")
	err := os.Chdir(dir)
	if err != nil {
		panic(err)
	}
}

func Test_CommonKeys(t *testing.T) {
	tcs := []struct {
		description    string
		testVectorFile string
		keyFunc        func(common.Address) ([]byte, error)
	}{
		{
			description:    "keyEthAddressBalance",
			testVectorFile: "test/vectors/src/merkle-tree/smt-key-eth-balance.json",
			keyFunc:        KeyEthAddrBalance,
		},
		{
			description:    "keyEthAddressNonce",
			testVectorFile: "test/vectors/src/merkle-tree/smt-key-eth-nonce.json",
			keyFunc:        KeyEthAddrNonce,
		},
		{
			description:    "keyContractCode",
			testVectorFile: "test/vectors/src/merkle-tree/smt-key-contract-code.json",
			keyFunc:        KeyContractCode,
		},
		{
			description:    "keyCodeLength",
			testVectorFile: "test/vectors/src/merkle-tree/smt-key-contract-length.json",
			keyFunc:        KeyCodeLength,
		},
	}
	for _, tc := range tcs {
		tc := tc

		data, err := os.ReadFile(tc.testVectorFile)
		require.NoError(t, err)

		var testVectors []testVectorKey
		err = json.Unmarshal(data, &testVectors)
		require.NoError(t, err)

		for ti, testVector := range testVectors {
			t.Run(fmt.Sprintf("%s, test vector %d", tc.description, ti), func(t *testing.T) {
				key, err := tc.keyFunc(common.HexToAddress(testVector.EthAddr))
				require.NoError(t, err)
				require.Equal(t, len(key), maxBigIntLen)

				expected, _ := new(big.Int).SetString(testVector.ExpectedKey, 10)
				assert.Equal(t, hex.EncodeToString(expected.Bytes()), hex.EncodeToString(key))
			})
		}
	}
}

func Test_KeyContractStorage(t *testing.T) {
	data, err := os.ReadFile("test/vectors/src/merkle-tree/smt-key-contract-storage.json")
	require.NoError(t, err)

	var testVectors []testVectorKey
	err = json.Unmarshal(data, &testVectors)
	require.NoError(t, err)

	for ti, testVector := range testVectors {
		t.Run(fmt.Sprintf("Test vector %d", ti), func(t *testing.T) {
			storagePosition, ok := new(big.Int).SetString(testVector.StoragePosition, 10)
			require.True(t, ok)
			key, err := KeyContractStorage(common.HexToAddress(testVector.EthAddr), storagePosition.Bytes())
			require.NoError(t, err)
			require.Equal(t, len(key), maxBigIntLen)

			expected, _ := new(big.Int).SetString(testVector.ExpectedKey, 10)
			assert.Equal(t, hex.EncodeToString(expected.Bytes()), hex.EncodeToString(key))
		})
	}
}

func Test_byteCodeHash(t *testing.T) {
	data, err := os.ReadFile("test/vectors/src/merkle-tree/smt-hash-bytecode.json")
	require.NoError(t, err)

	var testVectors []bytecodeTest
	err = json.Unmarshal(data, &testVectors)
	require.NoError(t, err)

	for ti, testVector := range testVectors {
		t.Run(fmt.Sprintf("Test vector %d", ti), func(t *testing.T) {
			hash, err := hashContractBytecode(common.Hex2Bytes(testVector.Bytecode))
			require.NoError(t, err)
			assert.Equal(t, common.HexToHash(testVector.ExpectedHash), common.HexToHash(H4ToString(hash)))
		})
	}
}
