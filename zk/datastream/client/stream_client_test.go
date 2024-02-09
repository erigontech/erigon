package client

import (
	"errors"
	"fmt"
	"math/big"
	"net"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/stretchr/testify/require"
	"gotest.tools/v3/assert"
)

func Test_readHeaderEntry(t *testing.T) {
	type testCase struct {
		name           string
		input          []byte
		expectedResult types.HeaderEntry
		expectedError  error
	}
	testCases := []testCase{
		{
			name:  "Happy path",
			input: []byte{101, 0, 0, 0, 29, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			expectedResult: types.HeaderEntry{
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
			expectedResult: types.HeaderEntry{},
			expectedError:  fmt.Errorf("failed to read header bytes error reading from server: unexpected EOF"),
		},
	}

	for _, testCase := range testCases {
		c := NewClient("", 0)
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
			assert.DeepEqual(t, testCase.expectedResult, *header)
		})
	}
}

func Test_readResultEntry(t *testing.T) {
	type testCase struct {
		name           string
		input          []byte
		expectedResult types.ResultEntry
		expectedError  error
	}
	testCases := []testCase{
		{
			name:  "Happy path",
			input: []byte{0, 0, 0, 9, 0, 0, 0, 0},
			expectedResult: types.ResultEntry{
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
			expectedResult: types.ResultEntry{
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
			expectedResult: types.ResultEntry{},
			expectedError:  fmt.Errorf("failed to read main result bytes error reading from server: unexpected EOF"),
		},
		{
			name:           "Invalid error length",
			input:          []byte{0, 0, 0, 12, 0, 0, 0, 0, 20, 21},
			expectedResult: types.ResultEntry{},
			expectedError:  fmt.Errorf("failed to read result errStr bytes error reading from server: unexpected EOF"),
		},
	}

	for _, testCase := range testCases {
		c := NewClient("", 0)
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
			assert.DeepEqual(t, testCase.expectedResult, *result)
		})
	}
}

func Test_readFileEntry(t *testing.T) {
	type testCase struct {
		name           string
		input          []byte
		expectedResult types.FileEntry
		expectedError  error
	}
	testCases := []testCase{
		{
			name:  "Happy path",
			input: []byte{2, 0, 0, 0, 29, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			expectedResult: types.FileEntry{
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
			expectedResult: types.FileEntry{
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
			expectedResult: types.FileEntry{},
			expectedError:  fmt.Errorf("error expecting data packet type 2 and received 5"),
		},
		{
			name:           "Invalid byte array length",
			input:          []byte{2, 21, 22, 23, 24, 20},
			expectedResult: types.FileEntry{},
			expectedError:  fmt.Errorf("error reading file bytes: error reading from server: unexpected EOF"),
		}, {
			name:           "Invalid data length",
			input:          []byte{2, 0, 0, 0, 31, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			expectedResult: types.FileEntry{},
			expectedError:  fmt.Errorf("error reading file data bytes: error reading from server: unexpected EOF"),
		},
	}
	for _, testCase := range testCases {
		c := NewClient("", 0)
		server, conn := net.Pipe()
		defer server.Close()
		defer c.Stop()

		c.conn = conn
		t.Run(testCase.name, func(t *testing.T) {
			go func() {
				server.Write(testCase.input)
				server.Close()
			}()

			result, err := c.readFileEntry()
			require.Equal(t, testCase.expectedError, err)
			assert.DeepEqual(t, testCase.expectedResult, *result)
		})
	}
}

func Test_readFullL2Blocks(t *testing.T) {
	type testCase struct {
		name                string
		inputAmount         int
		inputBytes          []byte
		expectedResult      []types.FullL2Block
		expectedEntriesRead uint64
		expectedError       error
	}
	testCases := []testCase{
		{
			name:        "Happy path",
			inputAmount: 1,
			inputBytes: []byte{
				2, 0, 0, 0, 17, 0, 0, 0, 176, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 176 - bookmark
				2, 0, 0, 0, 95, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 1 - startL2Block
				// startL2Block
				101, 0, 0, 0, 0, 0, 0, 0,
				12, 0, 0, 0, 0, 0, 0, 0,
				128, 0, 0, 0, 0, 0, 0, 0,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24,
				10, 0,
				2, 0, 0, 0, 60, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 2 - l2Transaction
				128, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 5, 1, 2, 3, 4, 5, // L2Transaction
				2, 0, 0, 0, 89, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 3 - endL2Block
				// endL2Block
				0, 0, 0, 0, 0, 0, 0, 12,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32,
			},
			expectedResult: []types.FullL2Block{
				{
					BatchNumber:    101,
					L2BlockNumber:  12,
					Timestamp:      128,
					GlobalExitRoot: [32]byte{10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17},
					Coinbase:       [20]byte{20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24},
					ForkId:         10,
					L2Blockhash:    common.BigToHash(big.NewInt(10)),
					StateRoot:      common.BigToHash(big.NewInt(32)),
					L2Txs: []types.L2Transaction{
						{
							EffectiveGasPricePercentage: 128,
							IsValid:                     1,
							EncodedLength:               5,
							Encoded:                     []byte{1, 2, 3, 4, 5},
							StateRoot:                   common.Hash{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10},
						},
					},
				},
			},
			expectedEntriesRead: 4,
			expectedError:       nil,
		},
		{
			name:        "Happy path: Available to read less than asked",
			inputAmount: 2,
			inputBytes: []byte{
				2, 0, 0, 0, 17, 0, 0, 0, 176, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 176 - bookmark
				2, 0, 0, 0, 95, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 1 - startL2Block
				// startL2Block
				101, 0, 0, 0, 0, 0, 0, 0,
				12, 0, 0, 0, 0, 0, 0, 0,
				128, 0, 0, 0, 0, 0, 0, 0,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24,
				10, 0,
				2, 0, 0, 0, 60, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 2 - l2Transaction
				128, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 5, 1, 2, 3, 4, 5, // L2Transaction
				2, 0, 0, 0, 89, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 3 - endL2Block
				// endL2Block
				0, 0, 0, 0, 0, 0, 0, 12,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32,
			},
			expectedResult: []types.FullL2Block{
				{
					BatchNumber:    101,
					L2BlockNumber:  12,
					Timestamp:      128,
					GlobalExitRoot: [32]byte{10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17},
					Coinbase:       [20]byte{20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24},
					ForkId:         10,
					L2Blockhash:    common.BigToHash(big.NewInt(10)),
					StateRoot:      common.BigToHash(big.NewInt(32)),
					L2Txs: []types.L2Transaction{
						{
							EffectiveGasPricePercentage: 128,
							IsValid:                     1,
							EncodedLength:               5,
							Encoded:                     []byte{1, 2, 3, 4, 5},
							StateRoot:                   common.Hash{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10},
						},
					},
				},
			},
			expectedEntriesRead: 4,
			expectedError:       nil,
		},
		{
			name:        "Not matching start block number and end block number",
			inputAmount: 1,
			inputBytes: []byte{
				2, 0, 0, 0, 17, 0, 0, 0, 176, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 176 - bookmark
				2, 0, 0, 0, 95, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 1 - startL2Block
				// startL2Block
				101, 0, 0, 0, 0, 0, 0, 0,
				12, 0, 0, 0, 0, 0, 0, 0,
				128, 0, 0, 0, 0, 0, 0, 0,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24,
				10, 0,
				2, 0, 0, 0, 60, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 2 - l2Transaction
				128, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 5, 1, 2, 3, 4, 5, // L2Transaction
				2, 0, 0, 0, 89, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 3 - endL2Block
				// endL2Block
				0, 0, 0, 0, 0, 0, 0, 10,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32,
			},
			expectedResult:      nil,
			expectedEntriesRead: 0,
			expectedError:       errors.New("failed to read full block: start block block number different than endBlock block number. StartBlock: 12, EndBlock: 10"),
		},
		{
			name:        "Not starting with a startL2Block",
			inputAmount: 1,
			inputBytes: []byte{
				2, 0, 0, 0, 17, 0, 0, 0, 176, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 176 - bookmark
				2, 0, 0, 0, 28, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 1 - startL2Block
				// startL2Block
				128, 1, 5, 0, 0, 0, 1, 2, 3, 4, 5, // L2Transaction
				2, 0, 0, 0, 25, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 3 - endL2Block
				// endL2Block
				10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			},
			expectedResult:      nil,
			expectedEntriesRead: 0,
			expectedError:       errors.New("failed to read full block: expected GerUpdate or Bookmark type, got type: 2"),
		},
		{
			name:        "Unexpected startL1Block in the middle of previous block",
			inputAmount: 1,
			inputBytes: []byte{
				2, 0, 0, 0, 17, 0, 0, 0, 176, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 176 - bookmark
				2, 0, 0, 0, 95, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 1 - startL2Block
				// startL2Block
				101, 0, 0, 0, 0, 0, 0, 0,
				12, 0, 0, 0, 0, 0, 0, 0,
				128, 0, 0, 0, 0, 0, 0, 0,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24,
				10, 0,
				2, 0, 0, 0, 60, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 2 - l2Transaction
				128, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 5, 1, 2, 3, 4, 5, // L2Transaction
				2, 0, 0, 0, 95, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 1 - startL2Block
				// startL2Block
				101, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 10, 0,
			},
			expectedResult:      nil,
			expectedEntriesRead: 0,
			expectedError:       errors.New("failed to read full block: expected EndL2Block or L2Transaction type, got type: 1"),
		},
	}
	for _, testCase := range testCases {
		c := NewClient("", BigEndianVersion)
		c.Header.TotalEntries = 3
		server, conn := net.Pipe()
		defer server.Close()
		defer c.Stop()

		c.conn = conn
		t.Run(testCase.name, func(t *testing.T) {
			go func() {
				server.Write(testCase.inputBytes)
				server.Close()
			}()

			result, _, _, entriesRead, err := c.readFullL2Blocks(testCase.inputAmount)
			require.Equal(t, testCase.expectedError, err)
			require.Equal(t, testCase.expectedEntriesRead, entriesRead)
			if testCase.expectedResult == nil {
				require.Nil(t, result)
			} else {
				assert.DeepEqual(t, testCase.expectedResult, *result)
			}
		})
	}
}

func Test_readFullBlock(t *testing.T) {
	type testCase struct {
		name                string
		inputAmount         int
		inputBytes          []byte
		expectedResult      types.FullL2Block
		expectedEntriesRead uint64
		expectedError       error
	}
	testCases := []testCase{
		{
			name:        "Happy path",
			inputAmount: 1,
			inputBytes: []byte{
				2, 0, 0, 0, 17, 0, 0, 0, 176, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 176 - bookmark
				2, 0, 0, 0, 95, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 1 - startL2Block
				// startL2Block
				101, 0, 0, 0, 0, 0, 0, 0,
				12, 0, 0, 0, 0, 0, 0, 0,
				128, 0, 0, 0, 0, 0, 0, 0,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24,
				10, 0,
				2, 0, 0, 0, 60, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 2 - l2Transaction
				128, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 5, 1, 2, 3, 4, 5, // L2Transaction
				2, 0, 0, 0, 89, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 3 - endL2Block
				// endL2Block
				0, 0, 0, 0, 0, 0, 0, 12,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32,
			},
			expectedResult: types.FullL2Block{
				BatchNumber:     101,
				L2BlockNumber:   12,
				Timestamp:       128,
				DeltaTimestamp:  0,
				L1InfoTreeIndex: 0,
				GlobalExitRoot:  [32]byte{10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17},
				Coinbase:        [20]byte{20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24},
				ForkId:          10,
				ChainId:         0,
				L1BlockHash:     common.Hash{0},
				L2Blockhash:     common.BigToHash(big.NewInt(10)),
				StateRoot:       common.BigToHash(big.NewInt(32)),
				L2Txs: []types.L2Transaction{
					{
						EffectiveGasPricePercentage: 128,
						IsValid:                     1,
						EncodedLength:               5,
						Encoded:                     []byte{1, 2, 3, 4, 5},
						StateRoot:                   common.Hash{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10},
					},
				},
				ParentHash: common.Hash{},
			},
			expectedEntriesRead: 4,
			expectedError:       nil,
		},
		{
			name:        "Not matching start block number and end block number",
			inputAmount: 1,
			inputBytes: []byte{
				2, 0, 0, 0, 17, 0, 0, 0, 176, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 176 - bookmark
				2, 0, 0, 0, 95, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 1 - startL2Block
				// startL2Block
				101, 0, 0, 0, 0, 0, 0, 0,
				12, 0, 0, 0, 0, 0, 0, 0,
				128, 0, 0, 0, 0, 0, 0, 0,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24,
				10, 0,
				2, 0, 0, 0, 60, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 2 - l2Transaction
				128, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 5, 1, 2, 3, 4, 5, // L2Transaction
				2, 0, 0, 0, 89, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 3 - endL2Block
				// endL2Block
				0, 0, 0, 0, 0, 0, 0, 10,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32,
			},
			expectedResult:      types.FullL2Block{},
			expectedEntriesRead: 0,
			expectedError:       errors.New("start block block number different than endBlock block number. StartBlock: 12, EndBlock: 10"),
		},
		{
			name:        "Not starting with a startL2Block",
			inputAmount: 1,
			inputBytes: []byte{
				2, 0, 0, 0, 17, 0, 0, 0, 176, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 176 - bookmark
				2, 0, 0, 0, 28, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 1 - startL2Block
				// startL2Block
				128, 1, 5, 0, 0, 0, 1, 2, 3, 4, 5, // L2Transaction
				2, 0, 0, 0, 25, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 3 - endL2Block
				// endL2Block
				10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			},
			expectedResult:      types.FullL2Block{},
			expectedEntriesRead: 0,
			expectedError:       errors.New("expected GerUpdate or Bookmark type, got type: 2"),
		},
		{
			name:        "Unexpected startL1Block in the middle of previous block",
			inputAmount: 1,
			inputBytes: []byte{
				2, 0, 0, 0, 17, 0, 0, 0, 176, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 176 - bookmark
				2, 0, 0, 0, 95, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 1 - startL2Block
				// startL2Block
				101, 0, 0, 0, 0, 0, 0, 0,
				12, 0, 0, 0, 0, 0, 0, 0,
				128, 0, 0, 0, 0, 0, 0, 0,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17,
				10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24,
				10, 0,
				2, 0, 0, 0, 60, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 2 - l2Transaction
				128, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 5, 1, 2, 3, 4, 5, // L2Transaction
				2, 0, 0, 0, 95, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, // fileEntry with entrytype 1 - startL2Block
				// startL2Block
				101, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 10, 0,
			},
			expectedResult:      types.FullL2Block{},
			expectedEntriesRead: 0,
			expectedError:       errors.New("expected EndL2Block or L2Transaction type, got type: 1"),
		},
	}
	for _, testCase := range testCases {
		c := NewClient("", BigEndianVersion)
		c.Header.TotalEntries = 3
		server, conn := net.Pipe()
		defer server.Close()
		defer c.Stop()

		c.conn = conn
		t.Run(testCase.name, func(t *testing.T) {
			go func() {
				server.Write(testCase.inputBytes)
				server.Close()
			}()

			result, _, _, _, entriesRead, err := c.readFullBlock()
			require.Equal(t, testCase.expectedError, err)
			require.Equal(t, testCase.expectedEntriesRead, entriesRead)
			if testCase.expectedError != nil {
				require.Nil(t, result)
			} else {
				assert.DeepEqual(t, testCase.expectedResult, *result)
			}
		})
	}
}
