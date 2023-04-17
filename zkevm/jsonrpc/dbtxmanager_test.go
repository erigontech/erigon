package jsonrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/mocks"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/types"
	"github.com/stretchr/testify/assert"
)

func TestNewDbTxScope(t *testing.T) {
	type testCase struct {
		Name           string
		Fn             dbTxScopedFn
		ExpectedResult interface{}
		ExpectedError  types.Error
		SetupMocks     func(s *mocks.StateMock, d *mocks.DBTxMock)
	}

	testCases := []testCase{
		{
			Name: "Run scoped func commits DB tx",
			Fn: func(ctx context.Context, dbTx pgx.Tx) (interface{}, types.Error) {
				return 1, nil
			},
			ExpectedResult: 1,
			ExpectedError:  nil,
			SetupMocks: func(s *mocks.StateMock, d *mocks.DBTxMock) {
				d.On("Commit", context.Background()).Return(nil).Once()
				s.On("BeginStateTransaction", context.Background()).Return(d, nil).Once()
			},
		},
		{
			Name: "Run scoped func rollbacks DB tx",
			Fn: func(ctx context.Context, dbTx pgx.Tx) (interface{}, types.Error) {
				return nil, types.NewRPCError(types.DefaultErrorCode, "func returned an error")
			},
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "func returned an error"),
			SetupMocks: func(s *mocks.StateMock, d *mocks.DBTxMock) {
				d.On("Rollback", context.Background()).Return(nil).Once()
				s.On("BeginStateTransaction", context.Background()).Return(d, nil).Once()
			},
		},
		{
			Name: "Run scoped func but fails create a db tx",
			Fn: func(ctx context.Context, dbTx pgx.Tx) (interface{}, types.Error) {
				return nil, nil
			},
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to connect to the state"),
			SetupMocks: func(s *mocks.StateMock, d *mocks.DBTxMock) {
				s.On("BeginStateTransaction", context.Background()).Return(nil, errors.New("failed to create db tx")).Once()
			},
		},
		{
			Name: "Run scoped func but fails to commit DB tx",
			Fn: func(ctx context.Context, dbTx pgx.Tx) (interface{}, types.Error) {
				return 1, nil
			},
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to commit db transaction"),
			SetupMocks: func(s *mocks.StateMock, d *mocks.DBTxMock) {
				d.On("Commit", context.Background()).Return(errors.New("failed to commit db tx")).Once()
				s.On("BeginStateTransaction", context.Background()).Return(d, nil).Once()
			},
		},
		{
			Name: "Run scoped func but fails to rollbacks DB tx",
			Fn: func(ctx context.Context, dbTx pgx.Tx) (interface{}, types.Error) {
				return nil, types.NewRPCError(types.DefaultErrorCode, "func returned an error")
			},
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to rollback db transaction"),
			SetupMocks: func(s *mocks.StateMock, d *mocks.DBTxMock) {
				d.On("Rollback", context.Background()).Return(errors.New("failed to rollback db tx")).Once()
				s.On("BeginStateTransaction", context.Background()).Return(d, nil).Once()
			},
		},
	}

	dbTxManager := dbTxManager{}
	s := mocks.NewStateMock(t)
	d := mocks.NewDBTxMock(t)

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(s, d)

			result, err := dbTxManager.NewDbTxScope(s, tc.Fn)
			assert.Equal(t, tc.ExpectedResult, result)
			assert.Equal(t, tc.ExpectedError, err)
		})
	}
}
