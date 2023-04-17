package types

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockNumberMarshalJSON(t *testing.T) {
	testCases := []struct {
		jsonValue           string
		expectedBlockNumber int64
		expectedError       error
	}{
		{"latest", int64(LatestBlockNumber), nil},
		{"pending", int64(PendingBlockNumber), nil},
		{"earliest", int64(EarliestBlockNumber), nil},
		{"", int64(LatestBlockNumber), nil},
		{"0", int64(0), nil},
		{"10", int64(10), nil},
		{"0x2", int64(2), nil},
		{"0xA", int64(10), nil},
		{"abc", int64(0), &strconv.NumError{Err: strconv.ErrSyntax, Func: "ParseUint", Num: "abc"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.jsonValue, func(t *testing.T) {
			data, err := json.Marshal(testCase.jsonValue)
			require.NoError(t, err)
			bn := BlockNumber(int64(0))
			err = json.Unmarshal(data, &bn)
			assert.Equal(t, testCase.expectedError, err)
			assert.Equal(t, testCase.expectedBlockNumber, int64(bn))
		})
	}
}

func TestGetNumericBlockNumber(t *testing.T) {
	s := mocks.NewStateMock(t)

	type testCase struct {
		name                string
		bn                  *BlockNumber
		expectedBlockNumber uint64
		expectedError       Error
		setupMocks          func(s *mocks.StateMock, d *mocks.DBTxMock, t *testCase)
	}

	testCases := []testCase{
		{
			name:                "BlockNumber nil",
			bn:                  nil,
			expectedBlockNumber: 40,
			expectedError:       nil,
			setupMocks: func(s *mocks.StateMock, d *mocks.DBTxMock, t *testCase) {
				s.
					On("GetLastL2BlockNumber", context.Background(), d).
					Return(uint64(40), nil).
					Once()
			},
		},
		{
			name:                "BlockNumber LatestBlockNumber",
			bn:                  bnPtr(LatestBlockNumber),
			expectedBlockNumber: 50,
			expectedError:       nil,
			setupMocks: func(s *mocks.StateMock, d *mocks.DBTxMock, t *testCase) {
				s.
					On("GetLastL2BlockNumber", context.Background(), d).
					Return(uint64(50), nil).
					Once()
			},
		},
		{
			name:                "BlockNumber PendingBlockNumber",
			bn:                  bnPtr(PendingBlockNumber),
			expectedBlockNumber: 30,
			expectedError:       nil,
			setupMocks: func(s *mocks.StateMock, d *mocks.DBTxMock, t *testCase) {
				s.
					On("GetLastL2BlockNumber", context.Background(), d).
					Return(uint64(30), nil).
					Once()
			},
		},
		{
			name:                "BlockNumber EarliestBlockNumber",
			bn:                  bnPtr(EarliestBlockNumber),
			expectedBlockNumber: 0,
			expectedError:       nil,
			setupMocks:          func(s *mocks.StateMock, d *mocks.DBTxMock, t *testCase) {},
		},
		{
			name:                "BlockNumber Positive Number",
			bn:                  bnPtr(BlockNumber(int64(10))),
			expectedBlockNumber: 10,
			expectedError:       nil,
			setupMocks:          func(s *mocks.StateMock, d *mocks.DBTxMock, t *testCase) {},
		},
		{
			name:                "BlockNumber Negative Number <= -4",
			bn:                  bnPtr(BlockNumber(int64(-4))),
			expectedBlockNumber: 0,
			expectedError:       NewRPCError(InvalidParamsErrorCode, "invalid block number: -4"),
			setupMocks:          func(s *mocks.StateMock, d *mocks.DBTxMock, t *testCase) {},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			tc := testCase
			dbTx := mocks.NewDBTxMock(t)
			testCase.setupMocks(s, dbTx, &tc)
			result, rpcErr := testCase.bn.GetNumericBlockNumber(context.Background(), s, dbTx)
			assert.Equal(t, testCase.expectedBlockNumber, result)
			if rpcErr != nil || testCase.expectedError != nil {
				assert.Equal(t, testCase.expectedError.ErrorCode(), rpcErr.ErrorCode())
				assert.Equal(t, testCase.expectedError.Error(), rpcErr.Error())
			}
		})
	}
}

func TestResponseMarshal(t *testing.T) {
	testCases := []struct {
		Name    string
		JSONRPC string
		ID      interface{}
		Result  interface{}
		Error   Error

		ExpectedJSON string
	}{
		{
			Name:    "Error is nil",
			JSONRPC: "2.0",
			ID:      1,
			Result: struct {
				A string `json:"A"`
			}{"A"},
			Error: nil,

			ExpectedJSON: "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"A\":\"A\"}}",
		},
		{
			Name:    "Result is nil and Error is not nil",
			JSONRPC: "2.0",
			ID:      1,
			Result:  nil,
			Error:   NewRPCError(123, "m"),

			ExpectedJSON: "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":123,\"message\":\"m\"}}",
		},
		{
			Name:    "Result is not nil and Error is not nil",
			JSONRPC: "2.0",
			ID:      1,
			Result: struct {
				A string `json:"A"`
			}{"A"},
			Error: NewRPCError(123, "m"),

			ExpectedJSON: "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":123,\"message\":\"m\"}}",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			req := Request{
				JSONRPC: testCase.JSONRPC,
				ID:      testCase.ID,
			}
			var result []byte
			if testCase.Result != nil {
				r, err := json.Marshal(testCase.Result)
				require.NoError(t, err)
				result = r
			}

			res := NewResponse(req, result, testCase.Error)
			bytes, err := json.Marshal(res)
			require.NoError(t, err)
			assert.Equal(t, string(testCase.ExpectedJSON), string(bytes))
		})
	}
}

func TestIndexUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		input         []byte
		expectedIndex int64
		expectedError error
	}{
		{
			input:         []byte("\"0x86\""),
			expectedIndex: 134,
			expectedError: nil,
		},
		{
			input:         []byte("\"abc\""),
			expectedIndex: 0,
			expectedError: &strconv.NumError{},
		},
	}

	for _, testCase := range testCases {
		var i Index
		err := json.Unmarshal(testCase.input, &i)
		assert.Equal(t, int64(testCase.expectedIndex), int64(i))
		assert.IsType(t, testCase.expectedError, err)
	}
}

func TestBlockNumberStringOrHex(t *testing.T) {
	testCases := []struct {
		bn             *BlockNumber
		expectedResult string
	}{
		{bn: bnPtr(BlockNumber(-3)), expectedResult: "pending"},
		{bn: bnPtr(BlockNumber(-2)), expectedResult: "latest"},
		{bn: bnPtr(BlockNumber(-1)), expectedResult: "earliest"},
		{bn: bnPtr(BlockNumber(0)), expectedResult: "0x0"},
		{bn: bnPtr(BlockNumber(100)), expectedResult: "0x64"},
	}

	for _, testCase := range testCases {
		result := testCase.bn.StringOrHex()
		assert.Equal(t, testCase.expectedResult, result)
	}
}

func bnPtr(bn BlockNumber) *BlockNumber {
	return &bn
}
