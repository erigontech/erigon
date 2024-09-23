package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/stretchr/testify/require"
	"gotest.tools/v3/assert"
)

const (
	streamTypeFieldName = "stream type"
)

func TestStreamClientReadHeaderEntry(t *testing.T) {
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
			expectedError:  errors.New("failed to read header bytes reading from server: unexpected EOF"),
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

func TestStreamClientReadResultEntry(t *testing.T) {
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
			expectedError:  errors.New("failed to read main result bytes reading from server: unexpected EOF"),
		},
		{
			name:           "Invalid error length",
			input:          []byte{0, 0, 0, 12, 0, 0, 0, 0, 20, 21},
			expectedResult: nil,
			expectedError:  errors.New("failed to read result errStr bytes reading from server: unexpected EOF"),
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

func TestStreamClientReadFileEntry(t *testing.T) {
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
			expectedError:  errors.New("expected data packet type 2 or 254 and received 5"),
		},
		{
			name:           "Invalid byte array length",
			input:          []byte{2, 21, 22, 23, 24, 20},
			expectedResult: nil,
			expectedError:  errors.New("error reading file bytes: reading from server: unexpected EOF"),
		}, {
			name:           "Invalid data length",
			input:          []byte{2, 0, 0, 0, 31, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			expectedResult: nil,
			expectedError:  errors.New("error reading file data bytes: reading from server: unexpected EOF"),
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

func TestStreamClientReadParsedProto(t *testing.T) {
	c := NewClient(context.Background(), "", 0, 0, 0)
	serverConn, clientConn := net.Pipe()
	c.conn = clientConn
	defer func() {
		serverConn.Close()
		clientConn.Close()
	}()

	l2Block, l2Txs := createL2BlockAndTransactions(t, 3, 1)
	l2BlockProto := &types.L2BlockProto{L2Block: l2Block}
	l2BlockRaw, err := l2BlockProto.Marshal()
	require.NoError(t, err)

	l2Tx := l2Txs[0]
	l2TxProto := &types.TxProto{Transaction: l2Tx}
	l2TxRaw, err := l2TxProto.Marshal()
	require.NoError(t, err)

	l2BlockEnd := &types.L2BlockEndProto{Number: l2Block.GetNumber()}
	l2BlockEndRaw, err := l2BlockEnd.Marshal()
	require.NoError(t, err)

	var (
		errCh = make(chan error)
		wg    sync.WaitGroup
	)
	wg.Add(1)

	go func() {
		defer wg.Done()
		fileEntries := []*types.FileEntry{
			createFileEntry(t, types.EntryTypeL2Block, 1, l2BlockRaw),
			createFileEntry(t, types.EntryTypeL2Tx, 2, l2TxRaw),
			createFileEntry(t, types.EntryTypeL2BlockEnd, 3, l2BlockEndRaw),
		}
		for _, fe := range fileEntries {
			_, writeErr := serverConn.Write(fe.Encode())
			if writeErr != nil {
				errCh <- writeErr
				break
			}
		}
	}()

	go func() {
		wg.Wait()
		close(errCh)
	}()

	parsedEntry, err := ReadParsedProto(c)
	require.NoError(t, err)
	serverErr := <-errCh
	require.NoError(t, serverErr)
	expectedL2Tx := types.ConvertToL2TransactionProto(l2Tx)
	expectedL2Block := types.ConvertToFullL2Block(l2Block)
	expectedL2Block.L2Txs = append(expectedL2Block.L2Txs, *expectedL2Tx)
	require.Equal(t, expectedL2Block, parsedEntry)
}

func TestStreamClientGetLatestL2Block(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() {
		serverConn.Close()
		clientConn.Close()
	}()

	c := NewClient(context.Background(), "", 0, 0, 0)
	c.conn = clientConn

	expectedL2Block, _ := createL2BlockAndTransactions(t, 5, 0)
	l2BlockProto := &types.L2BlockProto{L2Block: expectedL2Block}
	l2BlockRaw, err := l2BlockProto.Marshal()
	require.NoError(t, err)

	var (
		errCh = make(chan error)
		wg    sync.WaitGroup
	)
	wg.Add(1)

	// Prepare the server to send responses in a separate goroutine
	go func() {
		defer wg.Done()

		// Read the Command
		if err := readAndValidateUint(t, serverConn, uint64(CmdHeader), "command"); err != nil {
			errCh <- err
			return
		}

		// Read the StreamType
		if err := readAndValidateUint(t, serverConn, uint64(StSequencer), streamTypeFieldName); err != nil {
			errCh <- err
			return
		}

		// Write ResultEntry
		re := createResultEntry(t)
		_, err = serverConn.Write(re.Encode())
		if err != nil {
			errCh <- fmt.Errorf("failed to write result entry to the connection: %w", err)
		}

		// Write HeaderEntry
		he := &types.HeaderEntry{
			PacketType:   uint8(CmdHeader),
			HeadLength:   types.HeaderSize,
			Version:      2,
			SystemId:     1,
			StreamType:   types.StreamType(StSequencer),
			TotalEntries: 4,
		}
		_, err = serverConn.Write(he.Encode())
		if err != nil {
			errCh <- fmt.Errorf("failed to write header entry to the connection: %w", err)
		}

		// Read the Command
		if err := readAndValidateUint(t, serverConn, uint64(CmdEntry), "command"); err != nil {
			errCh <- err
			return
		}

		// Read the StreamType
		if err := readAndValidateUint(t, serverConn, uint64(StSequencer), streamTypeFieldName); err != nil {
			errCh <- err
			return
		}

		// Read the EntryNumber
		if err := readAndValidateUint(t, serverConn, he.TotalEntries-1, "entry number"); err != nil {
			errCh <- err
			return
		}

		// Write the ResultEntry
		_, err = serverConn.Write(re.Encode())
		if err != nil {
			errCh <- fmt.Errorf("failed to write result entry to the connection: %w", err)
			return
		}

		// Write the FileEntry containing the L2 block information
		fe := createFileEntry(t, types.EntryTypeL2Block, 1, l2BlockRaw)
		_, err = serverConn.Write(fe.Encode())
		if err != nil {
			errCh <- fmt.Errorf("failed to write the l2 block file entry to the connection: %w", err)
			return
		}

		serverConn.Close()
	}()

	go func() {
		wg.Wait()
		close(errCh)
	}()

	// ACT
	l2Block, err := c.GetLatestL2Block()
	require.NoError(t, err)

	// ASSERT
	serverErr := <-errCh
	require.NoError(t, serverErr)

	expectedFullL2Block := types.ConvertToFullL2Block(expectedL2Block)
	require.Equal(t, expectedFullL2Block, l2Block)
}

func TestStreamClientGetL2BlockByNumber(t *testing.T) {
	const blockNum = uint64(5)

	serverConn, clientConn := net.Pipe()
	defer func() {
		serverConn.Close()
		clientConn.Close()
	}()

	c := NewClient(context.Background(), "", 0, 0, 0)
	c.conn = clientConn

	bookmark := types.NewBookmarkProto(blockNum, datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK)
	bookmarkRaw, err := bookmark.Marshal()
	require.NoError(t, err)

	expectedL2Block, l2Txs := createL2BlockAndTransactions(t, blockNum, 3)
	l2BlockProto := &types.L2BlockProto{L2Block: expectedL2Block}
	l2BlockRaw, err := l2BlockProto.Marshal()
	require.NoError(t, err)

	l2TxsRaw := make([][]byte, len(l2Txs))
	for i, l2Tx := range l2Txs {
		l2TxProto := &types.TxProto{Transaction: l2Tx}
		l2TxRaw, err := l2TxProto.Marshal()
		require.NoError(t, err)
		l2TxsRaw[i] = l2TxRaw
	}

	l2BlockEnd := &types.L2BlockEndProto{Number: expectedL2Block.GetNumber()}
	l2BlockEndRaw, err := l2BlockEnd.Marshal()
	require.NoError(t, err)

	errCh := make(chan error)

	createServerResponses := func(t *testing.T, serverConn net.Conn, bookmarkRaw, l2BlockRaw []byte, l2TxsRaw [][]byte, l2BlockEndRaw []byte, errCh chan error) {
		defer func() {
			close(errCh)
			serverConn.Close()
		}()

		// Read the command
		if err := readAndValidateUint(t, serverConn, uint64(CmdStartBookmark), "command"); err != nil {
			errCh <- err
			return
		}

		// Read the stream type
		if err := readAndValidateUint(t, serverConn, uint64(StSequencer), streamTypeFieldName); err != nil {
			errCh <- err
			return
		}

		// Read the bookmark length
		if err := readAndValidateUint(t, serverConn, uint32(len(bookmarkRaw)), "bookmark length"); err != nil {
			errCh <- err
			return
		}

		// Read the actual bookmark
		actualBookmarkRaw, err := readBuffer(serverConn, uint32(len(bookmarkRaw)))
		if err != nil {
			errCh <- err
			return
		}
		if !bytes.Equal(bookmarkRaw, actualBookmarkRaw) {
			errCh <- fmt.Errorf("mismatch between expected %v and actual bookmark %v", bookmarkRaw, actualBookmarkRaw)
			return
		}

		// Write ResultEntry
		re := createResultEntry(t)
		if _, err := serverConn.Write(re.Encode()); err != nil {
			errCh <- err
			return
		}

		// Write File entries (EntryTypeL2Block, EntryTypeL2Tx and EntryTypeL2BlockEnd)
		fileEntries := make([]*types.FileEntry, 0, len(l2TxsRaw)+2)
		fileEntries = append(fileEntries, createFileEntry(t, types.EntryTypeL2Block, 1, l2BlockRaw))
		entryNum := uint64(2)
		for _, l2TxRaw := range l2TxsRaw {
			fileEntries = append(fileEntries, createFileEntry(t, types.EntryTypeL2Tx, entryNum, l2TxRaw))
			entryNum++
		}
		fileEntries = append(fileEntries, createFileEntry(t, types.EntryTypeL2BlockEnd, entryNum, l2BlockEndRaw))

		for _, fe := range fileEntries {
			if _, err := serverConn.Write(fe.Encode()); err != nil {
				errCh <- err
				return
			}
		}

	}

	go createServerResponses(t, serverConn, bookmarkRaw, l2BlockRaw, l2TxsRaw, l2BlockEndRaw, errCh)

	l2Block, errCode, err := c.GetL2BlockByNumber(blockNum)
	require.NoError(t, err)
	require.Equal(t, types.CmdErrOK, errCode)

	serverErr := <-errCh
	require.NoError(t, serverErr)

	l2TxsProto := make([]types.L2TransactionProto, len(l2Txs))
	for i, tx := range l2Txs {
		l2TxProto := types.ConvertToL2TransactionProto(tx)
		l2TxsProto[i] = *l2TxProto
	}
	expectedFullL2Block := types.ConvertToFullL2Block(expectedL2Block)
	expectedFullL2Block.L2Txs = l2TxsProto
	require.Equal(t, expectedFullL2Block, l2Block)
}

// readAndValidateUint reads the uint value and validates it against expected value from the connection in order to unblock future write operations
func readAndValidateUint(t *testing.T, conn net.Conn, expected interface{}, paramName string) error {
	t.Helper()

	var length uint32
	switch expected.(type) {
	case uint64:
		length = 8
	case uint32:
		length = 4
	default:
		return fmt.Errorf("unsupported expected type for %s: %T", paramName, expected)
	}

	valueRaw, err := readBuffer(conn, length)
	if err != nil {
		return fmt.Errorf("failed to read %s parameter: %w", paramName, err)
	}

	switch expectedValue := expected.(type) {
	case uint64:
		value := binary.BigEndian.Uint64(valueRaw)
		if value != expectedValue {
			return fmt.Errorf("%s parameter value mismatch between expected %d and actual %d", paramName, expectedValue, value)
		}
	case uint32:
		value := binary.BigEndian.Uint32(valueRaw)
		if value != expectedValue {
			return fmt.Errorf("%s parameter value mismatch between expected %d and actual %d", paramName, expectedValue, value)
		}
	}

	return nil
}

// createFileEntry is a helper function that creates FileEntry
func createFileEntry(t *testing.T, entryType types.EntryType, num uint64, data []byte) *types.FileEntry {
	t.Helper()
	return &types.FileEntry{
		PacketType: PtData,
		Length:     types.FileEntryMinSize + uint32(len(data)),
		EntryType:  entryType,
		EntryNum:   num,
		Data:       data,
	}
}

func createResultEntry(t *testing.T) *types.ResultEntry {
	t.Helper()
	return &types.ResultEntry{
		PacketType: PtResult,
		ErrorNum:   types.CmdErrOK,
		Length:     types.ResultEntryMinSize,
		ErrorStr:   nil,
	}
}

// createL2BlockAndTransactions creates a single L2 block with the transactions
func createL2BlockAndTransactions(t *testing.T, blockNum uint64, txnCount int) (*datastream.L2Block, []*datastream.Transaction) {
	t.Helper()
	txns := make([]*datastream.Transaction, 0, txnCount)
	l2Block := &datastream.L2Block{
		Number:        blockNum,
		BatchNumber:   1,
		Timestamp:     uint64(time.Now().UnixMilli()),
		Hash:          common.HexToHash("0x123456987654321").Bytes(),
		BlockGasLimit: 1000000000,
	}

	for i := 0; i < txnCount; i++ {
		txns = append(txns,
			&datastream.Transaction{
				L2BlockNumber: l2Block.GetNumber(),
				Index:         uint64(i),
				IsValid:       true,
				Debug:         &datastream.Debug{Message: fmt.Sprintf("Hello %d. transaction!", i+1)},
			})
	}

	return l2Block, txns
}
