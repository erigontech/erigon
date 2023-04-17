package types

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gotest.tools/v3/assert"
)

func TestFileEntryDecode(t *testing.T) {
	type testCase struct {
		name           string
		input          []byte
		expectedResult FileEntry
		expectedError  error
	}
	testCases := []testCase{
		{
			name:  "Happy path",
			input: []byte{2, 0, 0, 0, 29, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			expectedResult: FileEntry{
				PacketType: 2,
				Length:     29,
				EntryType:  EntryType(1),
				EntryNum:   45,
				Data:       []byte{0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			},
			expectedError: nil,
		}, {
			name:  "Happy path - no data",
			input: []byte{2, 0, 0, 0, 17, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45},
			expectedResult: FileEntry{
				PacketType: 2,
				Length:     17,
				EntryType:  EntryType(1),
				EntryNum:   45,
				Data:       []byte{},
			},
			expectedError: nil,
		},
		{
			name:           "Invalid byte array length",
			input:          []byte{20, 21, 22, 23, 24, 20},
			expectedResult: FileEntry{},
			expectedError:  fmt.Errorf("invalid FileEntry binary size. Expected: >=17, got: 6"),
		}, {
			name:           "Invalid data length",
			input:          []byte{2, 0, 0, 0, 31, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			expectedResult: FileEntry{},
			expectedError:  fmt.Errorf("invalid FileEntry binary size. Expected: 31, got: 29"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			decodeFileEntry, err := DecodeFileEntry(testCase.input)
			require.Equal(t, testCase.expectedError, err)
			assert.DeepEqual(t, testCase.expectedResult, *decodeFileEntry)
		})
	}
}
