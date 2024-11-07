package client

import (
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"gotest.tools/v3/assert"
)

func Test_WriteFullUint64ToConn(t *testing.T) {
	type testCase struct {
		name           string
		input          uint64
		shouldOpenConn bool
		expectedError  error
	}

	testCases := []testCase{
		{
			name:           "happy path",
			input:          10,
			shouldOpenConn: true,
			expectedError:  nil,
		},
		{
			name:           "nil connection",
			input:          10,
			shouldOpenConn: false,
			expectedError:  ErrNilConnection,
		},
	}

	for _, testCase := range testCases {
		server, client := net.Pipe()
		defer server.Close()
		t.Run(testCase.name, func(t *testing.T) {
			go func() {
				buffer := make([]byte, 8)
				io.ReadFull(server, buffer)
			}()

			var err error
			if testCase.shouldOpenConn {
				err = writeFullUint64ToConn(client, testCase.input)
			} else {
				err = writeFullUint64ToConn(nil, testCase.input)
				require.ErrorIs(t, err, ErrSocket)
			}
			require.ErrorIs(t, err, testCase.expectedError)
		})
	}
}

func Test_WriteFullUint32ToConn(t *testing.T) {
	type testCase struct {
		name           string
		input          uint32
		shouldOpenConn bool
		expectedError  error
	}

	testCases := []testCase{
		{
			name:           "happy path",
			input:          10,
			shouldOpenConn: true,
			expectedError:  nil,
		},
		{
			name:           "nil connection",
			input:          10,
			shouldOpenConn: false,
			expectedError:  ErrNilConnection,
		},
	}

	for _, testCase := range testCases {
		server, client := net.Pipe()
		defer server.Close()
		t.Run(testCase.name, func(t *testing.T) {
			go func() {
				buffer := make([]byte, 4)
				io.ReadFull(server, buffer)
			}()

			var err error
			if testCase.shouldOpenConn {
				err = writeFullUint32ToConn(client, testCase.input)
			} else {
				err = writeFullUint32ToConn(nil, testCase.input)
				require.ErrorIs(t, err, ErrSocket)
			}
			require.ErrorIs(t, err, testCase.expectedError)
		})
	}
}

func Test_WriteBytesToConn(t *testing.T) {
	type testCase struct {
		name           string
		input          []byte
		shouldOpenConn bool
		expectedError  error
	}

	testCases := []testCase{
		{
			name:           "happy path",
			input:          []byte{1, 2, 3, 4, 5},
			shouldOpenConn: true,
			expectedError:  nil,
		},
		{
			name:           "nil connection",
			input:          []byte{1, 2, 3, 4, 5},
			shouldOpenConn: false,
			expectedError:  ErrNilConnection,
		},
	}

	for _, testCase := range testCases {
		server, client := net.Pipe()
		defer server.Close()
		t.Run(testCase.name, func(t *testing.T) {
			go func() {
				buffer := make([]byte, len(testCase.input))
				io.ReadFull(server, buffer)
			}()

			var err error
			if testCase.shouldOpenConn {
				err = writeBytesToConn(client, testCase.input)
			} else {
				err = writeBytesToConn(nil, testCase.input)
				require.ErrorIs(t, err, ErrSocket)
			}
			require.ErrorIs(t, err, testCase.expectedError)
		})
	}
}

func Test_ReadBuffer(t *testing.T) {
	type testCase struct {
		name           string
		input          uint32
		expectedResult []byte
		expectedError  error
	}

	testCases := []testCase{
		{
			name:           "happy path",
			input:          5,
			expectedResult: []byte{1, 2, 3, 4, 5},
			expectedError:  nil,
		},
		{
			name:           "happy path 0",
			input:          0,
			expectedResult: []byte{},
			expectedError:  nil,
		},
		{
			name:           "test error",
			input:          6,
			expectedResult: []byte{},
			expectedError:  io.ErrUnexpectedEOF,
		},
	}

	for _, testCase := range testCases {
		server, client := net.Pipe()

		go func() {
			server.Write([]byte{1, 2, 3, 4, 5})
			server.Close()
		}()

		t.Run(testCase.name, func(t *testing.T) {
			result, err := readBuffer(client, testCase.input)
			require.ErrorIs(t, err, testCase.expectedError)
			if testCase.expectedError != nil {
				require.ErrorIs(t, err, ErrSocket)
			}
			assert.DeepEqual(t, testCase.expectedResult, result)
		})
	}
}
