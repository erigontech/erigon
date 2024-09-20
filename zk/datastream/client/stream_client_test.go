package client

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/stretchr/testify/require"
	"gotest.tools/v3/assert"
)

func Test_readHeaderEntry(t *testing.T) {
	type testCase struct {
		name           string
		input          []byte
		expectedResult *types.HeaderEntry
		expectedError  error
	}
	testCases := []testCase{
		{
			name:  "Happy path",
			input: []byte{101, 0, 0, 0, 29, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			expectedResult: &types.HeaderEntry{
				PacketType:   101,
				HeadLength:   29,
				StreamType:   types.StreamType(1),
				TotalLength:  24,
				TotalEntries: 64,
			},
			expectedError: nil,
		},
		{
			name:           "Invalid byte array length",
			input:          []byte{20, 21, 22, 23, 24, 20},
			expectedResult: nil,
			expectedError:  fmt.Errorf("failed to read header bytes reading from server: unexpected EOF"),
		},
	}

	for _, testCase := range testCases {
		c := NewClient(context.Background(), "", 0, 0, 0)
		server, conn := net.Pipe()
		defer server.Close()
		defer c.Stop()

		c.conn = conn
		t.Run(testCase.name, func(t *testing.T) {
			go func() {
				server.Write(testCase.input)
				server.Close()
			}()

			header, err := c.readHeaderEntry()
			require.Equal(t, testCase.expectedError, err)
			assert.DeepEqual(t, testCase.expectedResult, header)
		})
	}
}

func Test_readResultEntry(t *testing.T) {
	type testCase struct {
		name           string
		input          []byte
		expectedResult *types.ResultEntry
		expectedError  error
	}
	testCases := []testCase{
		{
			name:  "Happy path",
			input: []byte{0, 0, 0, 9, 0, 0, 0, 0},
			expectedResult: &types.ResultEntry{
				PacketType: 1,
				Length:     9,
				ErrorNum:   0,
				ErrorStr:   []byte{},
			},
			expectedError: nil,
		},
		{
			name:  "Happy path - error str length",
			input: []byte{0, 0, 0, 19, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expectedResult: &types.ResultEntry{
				PacketType: 1,
				Length:     19,
				ErrorNum:   0,
				ErrorStr:   []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
			expectedError: nil,
		},
		{
			name:           "Invalid byte array length",
			input:          []byte{20, 21, 22, 23, 24, 20},
			expectedResult: nil,
			expectedError:  fmt.Errorf("failed to read main result bytes reading from server: unexpected EOF"),
		},
		{
			name:           "Invalid error length",
			input:          []byte{0, 0, 0, 12, 0, 0, 0, 0, 20, 21},
			expectedResult: nil,
			expectedError:  fmt.Errorf("failed to read result errStr bytes reading from server: unexpected EOF"),
		},
	}

	for _, testCase := range testCases {
		c := NewClient(context.Background(), "", 0, 0, 0)
		server, conn := net.Pipe()
		defer server.Close()
		defer c.Stop()

		c.conn = conn
		t.Run(testCase.name, func(t *testing.T) {
			go func() {
				server.Write(testCase.input)
				server.Close()
			}()

			result, err := c.readResultEntry([]byte{1})
			require.Equal(t, testCase.expectedError, err)
			assert.DeepEqual(t, testCase.expectedResult, result)
		})
	}
}

func Test_readFileEntry(t *testing.T) {
	type testCase struct {
		name           string
		input          []byte
		expectedResult *types.FileEntry
		expectedError  error
	}
	testCases := []testCase{
		{
			name:  "Happy path",
			input: []byte{2, 0, 0, 0, 29, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			expectedResult: &types.FileEntry{
				PacketType: 2,
				Length:     29,
				EntryType:  types.EntryType(1),
				EntryNum:   45,
				Data:       []byte{0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			},
			expectedError: nil,
		}, {
			name:  "Happy path - no data",
			input: []byte{2, 0, 0, 0, 17, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45},
			expectedResult: &types.FileEntry{
				PacketType: 2,
				Length:     17,
				EntryType:  types.EntryType(1),
				EntryNum:   45,
				Data:       []byte{},
			},
			expectedError: nil,
		},
		{
			name:           "Invalid packet type",
			input:          []byte{5, 0, 0, 0, 17, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45},
			expectedResult: nil,
			expectedError:  fmt.Errorf("error expecting data packet type 2 and received 5"),
		},
		{
			name:           "Invalid byte array length",
			input:          []byte{2, 21, 22, 23, 24, 20},
			expectedResult: nil,
			expectedError:  fmt.Errorf("error reading file bytes: reading from server: unexpected EOF"),
		}, {
			name:           "Invalid data length",
			input:          []byte{2, 0, 0, 0, 31, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			expectedResult: nil,
			expectedError:  fmt.Errorf("error reading file data bytes: reading from server: unexpected EOF"),
		},
	}
	for _, testCase := range testCases {
		c := NewClient(context.Background(), "", 0, 0, 0)
		server, conn := net.Pipe()
		defer c.Stop()
		defer server.Close()

		c.conn = conn
		t.Run(testCase.name, func(t *testing.T) {
			go func() {
				server.Write(testCase.input)
				server.Close()
			}()

			result, err := c.NextFileEntry()
			require.Equal(t, testCase.expectedError, err)
			assert.DeepEqual(t, testCase.expectedResult, result)
		})
	}
}
