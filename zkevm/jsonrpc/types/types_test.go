package types

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/zkevm/hex"
	"github.com/ledgerwatch/erigon/zkevm/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArgHashUnmarshalFromShortString(t *testing.T) {
	type testCase struct {
		name           string
		input          string
		expectedResult string
		expectedError  error
	}
	testCases := []testCase{
		{
			name:           "valid hex value starting with 0x",
			input:          "0x1",
			expectedResult: "0x0000000000000000000000000000000000000000000000000000000000000001",
			expectedError:  nil,
		},
		{
			name:           "valid hex value starting without 0x",
			input:          "1",
			expectedResult: "0x0000000000000000000000000000000000000000000000000000000000000001",
			expectedError:  nil,
		},
		{
			name:           "valid full hash value",
			input:          "0x05b21ee5f65c28a0af8e71290fc33625a1279a8b3d6357ce3ca60f22dbf59e63",
			expectedResult: "0x05b21ee5f65c28a0af8e71290fc33625a1279a8b3d6357ce3ca60f22dbf59e63",
			expectedError:  nil,
		},
		{
			name:           "invalid hex value starting with 0x",
			input:          "0xG",
			expectedResult: "0x0000000000000000000000000000000000000000000000000000000000000000",
			expectedError:  fmt.Errorf("invalid hash, it needs to be a hexadecimal value"),
		},
		{
			name:           "invalid hex value starting without 0x",
			input:          "G",
			expectedResult: "0x0000000000000000000000000000000000000000000000000000000000000000",
			expectedError:  fmt.Errorf("invalid hash, it needs to be a hexadecimal value"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			arg := ArgHash{}
			err := arg.UnmarshalText([]byte(testCase.input))
			require.Equal(t, testCase.expectedError, err)
			assert.Equal(t, testCase.expectedResult, arg.Hash().String())
		})
	}
}

func TestArgAddressUnmarshalFromShortString(t *testing.T) {
	type testCase struct {
		name           string
		input          string
		expectedResult string
		expectedError  error
	}
	testCases := []testCase{
		{
			name:           "valid hex value starting with 0x",
			input:          "0x1",
			expectedResult: "0x0000000000000000000000000000000000000001",
			expectedError:  nil,
		},
		{
			name:           "valid hex value starting without 0x",
			input:          "1",
			expectedResult: "0x0000000000000000000000000000000000000001",
			expectedError:  nil,
		},
		{
			name:           "valid full address value",
			input:          "0x748964F22eFd023eB78A246A7AC2506e84CC4545",
			expectedResult: "0x748964F22eFd023eB78A246A7AC2506e84CC4545",
			expectedError:  nil,
		},
		{
			name:           "invalid hex value starting with 0x",
			input:          "0xG",
			expectedResult: "0x0000000000000000000000000000000000000000",
			expectedError:  fmt.Errorf("invalid address, it needs to be a hexadecimal value"),
		},
		{
			name:           "invalid hex value starting without 0x",
			input:          "G",
			expectedResult: "0x0000000000000000000000000000000000000000",
			expectedError:  fmt.Errorf("invalid address, it needs to be a hexadecimal value"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			arg := ArgAddress{}
			err := arg.UnmarshalText([]byte(testCase.input))
			require.Equal(t, testCase.expectedError, err)
			assert.Equal(t, testCase.expectedResult, arg.Address().String())
		})
	}
}

func TestBatchUnmarshal(t *testing.T) {
	// json: `{"number":"0x1","coinbase":"0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266","stateRoot":"0x49e7b7eb6bb34b07a1063cd2c7a9cac88845c1867e8a026d69fc00b862c2ca72","globalExitRoot":"0x0000000000000000000000000000000000000000000000000000000000000001","localExitRoot":"0x0000000000000000000000000000000000000000000000000000000000000002","accInputHash":"0x0000000000000000000000000000000000000000000000000000000000000003","timestamp":"0x64133495","sendSequencesTxHash":"0x0000000000000000000000000000000000000000000000000000000000000004","verifyBatchTxHash":"0x0000000000000000000000000000000000000000000000000000000000000005","transactions":[{"nonce":"0x8","gasPrice":"0x3b9aca00","gas":"0x5208","to":"0xb48ca794d49eec406a5dd2c547717e37b5952a83","value":"0xde0b6b3a7640000","input":"0x","v":"0x7f5","r":"0x27d94abdecca8324d23221cec81f0a3398d7eee2dc831f698fe12447695897d5","s":"0x1b9f1d7cabbb69d309f9e6ffe10b3e205ad86af1058f4dbacdd06a8db03a5669","hash":"0xd0433908a0b56ec6d90758abfe5ae11185e13bedb3d70e8ab7c0d7e3f0e395b5","from":"0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266","blockHash":"0x7e8efeb2b5bb9aaef68a9b2f5b6c0a14900745380a68f72f9c15f978546109cc","blockNumber":"0x1","transactionIndex":"0x0","chainId":"0x3e9","type":"0x0"}]}`
	type testCase struct {
		name     string
		json     string
		expected Batch
	}
	testCases := []testCase{
		{
			name: "with out txs",
			json: `{"number":"0x1","coinbase":"0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266","stateRoot":"0x49e7b7eb6bb34b07a1063cd2c7a9cac88845c1867e8a026d69fc00b862c2ca72","globalExitRoot":"0x0000000000000000000000000000000000000000000000000000000000000001","localExitRoot":"0x0000000000000000000000000000000000000000000000000000000000000002","accInputHash":"0x0000000000000000000000000000000000000000000000000000000000000003","timestamp":"0x64133495","sendSequencesTxHash":"0x0000000000000000000000000000000000000000000000000000000000000004","verifyBatchTxHash":"0x0000000000000000000000000000000000000000000000000000000000000005"}`,
			expected: Batch{
				Number:              1,
				Coinbase:            common.HexToAddress("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"),
				StateRoot:           common.HexToHash("0x49e7b7eb6bb34b07a1063cd2c7a9cac88845c1867e8a026d69fc00b862c2ca72"),
				GlobalExitRoot:      common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001"),
				LocalExitRoot:       common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002"),
				AccInputHash:        common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000003"),
				Timestamp:           ArgUint64(1678980245),
				SendSequencesTxHash: state.HexToHashPtr("0x0000000000000000000000000000000000000000000000000000000000000004"),
				VerifyBatchTxHash:   state.HexToHashPtr("0x0000000000000000000000000000000000000000000000000000000000000005"),
			},
		},
		{
			name: "with txs",
			json: `{"number":"0x1","coinbase":"0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266","stateRoot":"0x49e7b7eb6bb34b07a1063cd2c7a9cac88845c1867e8a026d69fc00b862c2ca72","globalExitRoot":"0x0000000000000000000000000000000000000000000000000000000000000001","localExitRoot":"0x0000000000000000000000000000000000000000000000000000000000000002","accInputHash":"0x0000000000000000000000000000000000000000000000000000000000000003","timestamp":"0x64133495","sendSequencesTxHash":"0x0000000000000000000000000000000000000000000000000000000000000004","verifyBatchTxHash":"0x0000000000000000000000000000000000000000000000000000000000000005","transactions":[{"nonce":"0x8","gasPrice":"0x3b9aca00","gas":"0x5208","to":"0xb48ca794d49eec406a5dd2c547717e37b5952a83","value":"0xde0b6b3a7640000","input":"0x","v":"0x7f5","r":"0x27d94abdecca8324d23221cec81f0a3398d7eee2dc831f698fe12447695897d5","s":"0x1b9f1d7cabbb69d309f9e6ffe10b3e205ad86af1058f4dbacdd06a8db03a5669","hash":"0xd0433908a0b56ec6d90758abfe5ae11185e13bedb3d70e8ab7c0d7e3f0e395b5","from":"0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266","blockHash":"0x7e8efeb2b5bb9aaef68a9b2f5b6c0a14900745380a68f72f9c15f978546109cc","blockNumber":"0x1","transactionIndex":"0x0","chainId":"0x3e9","type":"0x0"}]}`,
			expected: Batch{
				Number:              1,
				Coinbase:            common.HexToAddress("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"),
				StateRoot:           common.HexToHash("0x49e7b7eb6bb34b07a1063cd2c7a9cac88845c1867e8a026d69fc00b862c2ca72"),
				GlobalExitRoot:      common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001"),
				LocalExitRoot:       common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002"),
				AccInputHash:        common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000003"),
				Timestamp:           ArgUint64(hex.DecodeUint64("0x64133495")),
				SendSequencesTxHash: state.HexToHashPtr("0x0000000000000000000000000000000000000000000000000000000000000004"),
				VerifyBatchTxHash:   state.HexToHashPtr("0x0000000000000000000000000000000000000000000000000000000000000005"),
				Transactions: []TransactionOrHash{
					{
						Tx: &Transaction{
							Nonce:       ArgUint64(hex.DecodeUint64("0x8")),
							GasPrice:    ArgBig(*hex.DecodeBig("0x3b9aca00")),
							Gas:         ArgUint64(hex.DecodeUint64("0x5208")),
							To:          state.HexToAddressPtr("0xb48ca794d49eec406a5dd2c547717e37b5952a83"),
							Value:       ArgBig(*hex.DecodeBig("0xde0b6b3a7640000")),
							Input:       ArgBytes{},
							V:           ArgBig(*hex.DecodeBig("0x7f5")),
							R:           ArgBig(*hex.DecodeBig("0x27d94abdecca8324d23221cec81f0a3398d7eee2dc831f698fe12447695897d5")),
							S:           ArgBig(*hex.DecodeBig("0x1b9f1d7cabbb69d309f9e6ffe10b3e205ad86af1058f4dbacdd06a8db03a5669")),
							Hash:        common.HexToHash("0xd0433908a0b56ec6d90758abfe5ae11185e13bedb3d70e8ab7c0d7e3f0e395b5"),
							From:        common.HexToAddress("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"),
							BlockHash:   state.HexToHashPtr("0x7e8efeb2b5bb9aaef68a9b2f5b6c0a14900745380a68f72f9c15f978546109cc"),
							BlockNumber: ArgUint64Ptr(ArgUint64(hex.DecodeUint64("0x1"))),
							TxIndex:     ArgUint64Ptr(ArgUint64(hex.DecodeUint64("0x0"))),
							ChainID:     ArgBig(*hex.DecodeBig("0x3e9")),
							Type:        ArgUint64(hex.DecodeUint64("0x0")),
						},
					},
				},
			},
		},
		{
			name: "with tx hashes",
			json: `{"number":"0x1","coinbase":"0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266","stateRoot":"0x49e7b7eb6bb34b07a1063cd2c7a9cac88845c1867e8a026d69fc00b862c2ca72","globalExitRoot":"0x0000000000000000000000000000000000000000000000000000000000000001","localExitRoot":"0x0000000000000000000000000000000000000000000000000000000000000002","accInputHash":"0x0000000000000000000000000000000000000000000000000000000000000003","timestamp":"0x64133495","sendSequencesTxHash":"0x0000000000000000000000000000000000000000000000000000000000000004","verifyBatchTxHash":"0x0000000000000000000000000000000000000000000000000000000000000005","transactions":["0x0000000000000000000000000000000000000000000000000000000000000001","0x0000000000000000000000000000000000000000000000000000000000000002"]}`,
			expected: Batch{
				Number:              1,
				Coinbase:            common.HexToAddress("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"),
				StateRoot:           common.HexToHash("0x49e7b7eb6bb34b07a1063cd2c7a9cac88845c1867e8a026d69fc00b862c2ca72"),
				GlobalExitRoot:      common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001"),
				LocalExitRoot:       common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002"),
				AccInputHash:        common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000003"),
				Timestamp:           ArgUint64(1678980245),
				SendSequencesTxHash: state.HexToHashPtr("0x0000000000000000000000000000000000000000000000000000000000000004"),
				VerifyBatchTxHash:   state.HexToHashPtr("0x0000000000000000000000000000000000000000000000000000000000000005"),
				Transactions: []TransactionOrHash{
					{Hash: state.HexToHashPtr("0x0000000000000000000000000000000000000000000000000000000000000001")},
					{Hash: state.HexToHashPtr("0x0000000000000000000000000000000000000000000000000000000000000002")},
				},
			},
		},
	}

	for _, testCase := range testCases {
		b := []byte(testCase.json)
		var result *Batch
		err := json.Unmarshal(b, &result)
		require.NoError(t, err)

		assert.Equal(t, testCase.expected.Number, result.Number)
		assert.Equal(t, testCase.expected.Coinbase, result.Coinbase)
		assert.Equal(t, testCase.expected.StateRoot, result.StateRoot)
		assert.Equal(t, testCase.expected.GlobalExitRoot, result.GlobalExitRoot)
		assert.Equal(t, testCase.expected.LocalExitRoot, result.LocalExitRoot)
		assert.Equal(t, testCase.expected.AccInputHash, result.AccInputHash)
		assert.Equal(t, testCase.expected.Timestamp, result.Timestamp)
		assert.Equal(t, testCase.expected.SendSequencesTxHash, result.SendSequencesTxHash)
		assert.Equal(t, testCase.expected.VerifyBatchTxHash, result.VerifyBatchTxHash)
		assert.Equal(t, len(testCase.expected.Transactions), len(result.Transactions))

		for i := 0; i < len(testCase.expected.Transactions); i++ {
			txOrHashExpected := testCase.expected.Transactions[i]
			txOrHashResult := result.Transactions[i]

			if txOrHashExpected.Hash != nil {
				assert.Equal(t, txOrHashExpected.Hash.String(), txOrHashResult.Hash.String())
			} else {
				assert.Equal(t, txOrHashExpected.Tx.Nonce, txOrHashResult.Tx.Nonce)
				assert.Equal(t, txOrHashExpected.Tx.GasPrice, txOrHashResult.Tx.GasPrice)
				assert.Equal(t, txOrHashExpected.Tx.Gas, txOrHashResult.Tx.Gas)
				assert.Equal(t, txOrHashExpected.Tx.To, txOrHashResult.Tx.To)
				assert.Equal(t, txOrHashExpected.Tx.Value, txOrHashResult.Tx.Value)
				assert.Equal(t, txOrHashExpected.Tx.Input, txOrHashResult.Tx.Input)
				assert.Equal(t, txOrHashExpected.Tx.V, txOrHashResult.Tx.V)
				assert.Equal(t, txOrHashExpected.Tx.R, txOrHashResult.Tx.R)
				assert.Equal(t, txOrHashExpected.Tx.S, txOrHashResult.Tx.S)
				assert.Equal(t, txOrHashExpected.Tx.Hash, txOrHashResult.Tx.Hash)
				assert.Equal(t, txOrHashExpected.Tx.From, txOrHashResult.Tx.From)
				assert.Equal(t, txOrHashExpected.Tx.BlockHash, txOrHashResult.Tx.BlockHash)
				assert.Equal(t, txOrHashExpected.Tx.BlockNumber, txOrHashResult.Tx.BlockNumber)
				assert.Equal(t, txOrHashExpected.Tx.TxIndex, txOrHashResult.Tx.TxIndex)
				assert.Equal(t, txOrHashExpected.Tx.ChainID, txOrHashResult.Tx.ChainID)
				assert.Equal(t, txOrHashExpected.Tx.Type, txOrHashResult.Tx.Type)
			}
		}
	}
}
