package types

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"
	"gotest.tools/v3/assert"
)

func TestStartL2BlockDecode(t *testing.T) {
	type testCase struct {
		name           string
		input          []byte
		expectedResult StartL2Block
		expectedError  error
	}
	testCases := []testCase{
		{
			name: "Happy path",
			input: []byte{
				101, 0, 0, 0, 0, 0, 0, 0, // batch no
				12, 0, 0, 0, 0, 0, 0, 0, // l2 block no
				128, 0, 0, 0, 0, 0, 0, 0, // ts
				128, 0, 0, 0, // delta ts
				1, 0, 0, 0, // info tree index
				10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, // l1 block hash
				10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, // global exit root
				20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, // coinbase
				10, 0, // fork id
				0, 0, 0, 0, // chain id
			},
			expectedResult: StartL2Block{
				BatchNumber:     101,
				L2BlockNumber:   12,
				Timestamp:       128,
				DeltaTimestamp:  128,
				L1InfoTreeIndex: 1,
				L1BlockHash:     [32]byte{10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17},
				GlobalExitRoot:  [32]byte{10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17},
				Coinbase:        [20]byte{20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24},
				ForkId:          10,
				ChainId:         0,
			},
			expectedError: nil,
		},
		{
			name:           "Invalid byte array length",
			input:          []byte{20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24},
			expectedResult: StartL2Block{},
			expectedError:  fmt.Errorf("expected data length: 122, got: 20"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			decodedL2Block, err := DecodeStartL2Block(testCase.input)
			require.Equal(t, testCase.expectedError, err)
			assert.Equal(t, testCase.expectedResult, *decodedL2Block)
		})
	}
}

func TestEndL2BlockDecode(t *testing.T) {
	type testCase struct {
		name           string
		input          []byte
		expectedResult EndL2Block
		expectedError  error
	}
	testCases := []testCase{
		{
			name: "Happy path",
			input: []byte{
				10, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32,
			},
			expectedResult: EndL2Block{
				L2BlockNumber: 10,
				L2Blockhash:   common.BigToHash(big.NewInt(10)),
				StateRoot:     common.BigToHash(big.NewInt(32)),
			},
			expectedError: nil,
		},
		{
			name:           "Invalid byte array length",
			input:          []byte{20, 2, 3},
			expectedResult: EndL2Block{},
			expectedError:  fmt.Errorf("expected data length: 72, got: 3"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			decodedL2Block, err := DecodeEndL2Block(testCase.input)
			require.Equal(t, testCase.expectedError, err)
			assert.Equal(t, testCase.expectedResult, *decodedL2Block)
		})
	}
}
