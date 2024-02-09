package types

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gotest.tools/v3/assert"
)

func TestDecodeGerUpdate(t *testing.T) {
	type testCase struct {
		name           string
		input          []byte
		expectedResult GerUpdate
		expectedError  error
	}
	testCases := []testCase{
		{
			name: "Happy path",
			input: []byte{
				101, 0, 0, 0, 0, 0, 0, 0,
				128, 0, 0, 0, 0, 0, 0, 0,
				10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17,
				20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24,
				10, 0,
				10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17,
			},
			expectedResult: GerUpdate{
				BatchNumber:    101,
				Timestamp:      128,
				GlobalExitRoot: [32]byte{10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17},
				Coinbase:       [20]byte{20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24},
				ForkId:         10,
				StateRoot:      [32]byte{10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17},
			},
			expectedError: nil,
		},
		{
			name:           "Invalid byte array length",
			input:          []byte{20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24},
			expectedResult: GerUpdate{},
			expectedError:  fmt.Errorf("expected data length: 106, got: 20"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			decodedL2Block, err := DecodeGerUpdate(testCase.input)
			require.Equal(t, testCase.expectedError, err)
			assert.Equal(t, testCase.expectedResult, *decodedL2Block)
		})
	}
}
