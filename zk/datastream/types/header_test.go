package types

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gotest.tools/v3/assert"
)

func TestHeaderEntryDecode(t *testing.T) {
	type testCase struct {
		name           string
		input          []byte
		expectedResult HeaderEntry
		expectedError  error
	}
	testCases := []testCase{
		{
			name:  "Happy path",
			input: []byte{101, 0, 0, 0, 38, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			expectedResult: HeaderEntry{
				PacketType:   101,
				HeadLength:   38,
				Version:      1,
				SystemId:     0,
				StreamType:   StreamType(1),
				TotalLength:  24,
				TotalEntries: 64,
			},
			expectedError: nil,
		},
		{
			name:           "Invalid byte array length",
			input:          []byte{20, 21, 22, 23, 24, 20},
			expectedResult: HeaderEntry{},
			expectedError:  fmt.Errorf("invalid header entry binary size. Expected: 38, got: 6"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			decodeHeaderEntry, err := DecodeHeaderEntry(testCase.input)
			require.Equal(t, testCase.expectedError, err)
			assert.Equal(t, testCase.expectedResult, *decodeHeaderEntry)
		})
	}
}
