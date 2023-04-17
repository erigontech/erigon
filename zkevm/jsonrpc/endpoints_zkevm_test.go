package jsonrpc

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/0xPolygonHermez/zkevm-node/hex"
	"github.com/0xPolygonHermez/zkevm-node/jsonrpc/types"
	"github.com/0xPolygonHermez/zkevm-node/state"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ledgerwatch/erigon/common"
	ethTypes "github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsolidatedBlockNumber(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		ExpectedResult *uint64
		ExpectedError  types.Error
		SetupMocks     func(m *mocksWrapper)
	}

	testCases := []testCase{
		{
			Name:           "Get consolidated block number successfully",
			ExpectedResult: ptrUint64(10),
			SetupMocks: func(m *mocksWrapper) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastConsolidatedL2BlockNumber", context.Background(), m.DbTx).
					Return(uint64(10), nil).
					Once()
			},
		},
		{
			Name:           "failed to get consolidated block number",
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get last consolidated block number from state"),
			SetupMocks: func(m *mocksWrapper) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastConsolidatedL2BlockNumber", context.Background(), m.DbTx).
					Return(uint64(0), errors.New("failed to get last consolidated block number")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m)

			res, err := s.JSONRPCCall("zkevm_consolidatedBlockNumber")
			require.NoError(t, err)

			if res.Result != nil {
				var result types.ArgUint64
				err = json.Unmarshal(res.Result, &result)
				require.NoError(t, err)
				assert.Equal(t, *tc.ExpectedResult, uint64(result))
			}

			if res.Error != nil || tc.ExpectedError != nil {
				assert.Equal(t, tc.ExpectedError.ErrorCode(), res.Error.Code)
				assert.Equal(t, tc.ExpectedError.Error(), res.Error.Message)
			}
		})
	}
}

func TestIsBlockConsolidated(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		ExpectedResult bool
		ExpectedError  types.Error
		SetupMocks     func(m *mocksWrapper)
	}

	testCases := []testCase{
		{
			Name:           "Query status of block number successfully",
			ExpectedResult: true,
			SetupMocks: func(m *mocksWrapper) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("IsL2BlockConsolidated", context.Background(), uint64(1), m.DbTx).
					Return(true, nil).
					Once()
			},
		},
		{
			Name:           "Failed to query the consolidation status",
			ExpectedResult: false,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to check if the block is consolidated"),
			SetupMocks: func(m *mocksWrapper) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("IsL2BlockConsolidated", context.Background(), uint64(1), m.DbTx).
					Return(false, errors.New("failed to check if the block is consolidated")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m)

			res, err := s.JSONRPCCall("zkevm_isBlockConsolidated", "0x1")
			require.NoError(t, err)

			if res.Result != nil {
				var result bool
				err = json.Unmarshal(res.Result, &result)
				require.NoError(t, err)
				assert.Equal(t, tc.ExpectedResult, result)
			}

			if res.Error != nil || tc.ExpectedError != nil {
				assert.Equal(t, tc.ExpectedError.ErrorCode(), res.Error.Code)
				assert.Equal(t, tc.ExpectedError.Error(), res.Error.Message)
			}
		})
	}
}

func TestIsBlockVirtualized(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		ExpectedResult bool
		ExpectedError  types.Error
		SetupMocks     func(m *mocksWrapper)
	}

	testCases := []testCase{
		{
			Name:           "Query status of block number successfully",
			ExpectedResult: true,
			SetupMocks: func(m *mocksWrapper) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("IsL2BlockVirtualized", context.Background(), uint64(1), m.DbTx).
					Return(true, nil).
					Once()
			},
		},
		{
			Name:           "Failed to query the virtualization status",
			ExpectedResult: false,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to check if the block is virtualized"),
			SetupMocks: func(m *mocksWrapper) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("IsL2BlockVirtualized", context.Background(), uint64(1), m.DbTx).
					Return(false, errors.New("failed to check if the block is virtualized")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m)

			res, err := s.JSONRPCCall("zkevm_isBlockVirtualized", "0x1")
			require.NoError(t, err)

			if res.Result != nil {
				var result bool
				err = json.Unmarshal(res.Result, &result)
				require.NoError(t, err)
				assert.Equal(t, tc.ExpectedResult, result)
			}

			if res.Error != nil || tc.ExpectedError != nil {
				assert.Equal(t, tc.ExpectedError.ErrorCode(), res.Error.Code)
				assert.Equal(t, tc.ExpectedError.Error(), res.Error.Message)
			}
		})
	}
}

func TestBatchNumberByBlockNumber(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()
	blockNumber := uint64(1)
	batchNumber := uint64(1)

	type testCase struct {
		Name           string
		ExpectedResult *uint64
		ExpectedError  types.Error
		SetupMocks     func(m *mocksWrapper)
	}

	testCases := []testCase{
		{
			Name:           "get batch number by block number successfully",
			ExpectedResult: &batchNumber,
			SetupMocks: func(m *mocksWrapper) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("BatchNumberByL2BlockNumber", context.Background(), blockNumber, m.DbTx).
					Return(batchNumber, nil).
					Once()
			},
		},
		{
			Name:           "failed to get batch number",
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get batch number from block number"),
			SetupMocks: func(m *mocksWrapper) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("BatchNumberByL2BlockNumber", context.Background(), blockNumber, m.DbTx).
					Return(uint64(0), errors.New("failed to get batch number of l2 batchNum")).
					Once()
			},
		},
		{
			Name:           "batch number not found",
			ExpectedResult: nil,
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("BatchNumberByL2BlockNumber", context.Background(), blockNumber, m.DbTx).
					Return(uint64(0), state.ErrNotFound).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m)

			res, err := s.JSONRPCCall("zkevm_batchNumberByBlockNumber", hex.EncodeUint64(blockNumber))
			require.NoError(t, err)

			if tc.ExpectedResult != nil {
				var result types.ArgUint64
				err = json.Unmarshal(res.Result, &result)
				require.NoError(t, err)
				assert.Equal(t, *tc.ExpectedResult, uint64(result))
			} else {
				if res.Result == nil {
					assert.Nil(t, res.Result)
				} else {
					var result *uint64
					err = json.Unmarshal(res.Result, &result)
					require.NoError(t, err)
					assert.Nil(t, result)
				}
			}

			if tc.ExpectedError != nil {
				assert.Equal(t, tc.ExpectedError.ErrorCode(), res.Error.Code)
				assert.Equal(t, tc.ExpectedError.Error(), res.Error.Message)
			} else {
				assert.Nil(t, res.Error)
			}
		})
	}
}

func TestBatchNumber(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		ExpectedResult uint64
		ExpectedError  types.Error
		SetupMocks     func(m *mocksWrapper)
	}

	testCases := []testCase{
		{
			Name:           "get batch number successfully",
			ExpectedError:  nil,
			ExpectedResult: 10,
			SetupMocks: func(m *mocksWrapper) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastBatchNumber", context.Background(), m.DbTx).
					Return(uint64(10), nil).
					Once()
			},
		},
		{
			Name:           "failed to get batch number",
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get the last batch number from state"),
			ExpectedResult: 0,
			SetupMocks: func(m *mocksWrapper) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastBatchNumber", context.Background(), m.DbTx).
					Return(uint64(0), errors.New("failed to get last batch number")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m)

			res, err := s.JSONRPCCall("zkevm_batchNumber")
			require.NoError(t, err)

			if res.Result != nil {
				var result types.ArgUint64
				err = json.Unmarshal(res.Result, &result)
				require.NoError(t, err)
				assert.Equal(t, tc.ExpectedResult, uint64(result))
			}

			if res.Error != nil || tc.ExpectedError != nil {
				assert.Equal(t, tc.ExpectedError.ErrorCode(), res.Error.Code)
				assert.Equal(t, tc.ExpectedError.Error(), res.Error.Message)
			}
		})
	}
}

func TestVirtualBatchNumber(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		ExpectedResult uint64
		ExpectedError  types.Error
		SetupMocks     func(m *mocksWrapper)
	}

	testCases := []testCase{
		{
			Name:           "get virtual batch number successfully",
			ExpectedError:  nil,
			ExpectedResult: 10,
			SetupMocks: func(m *mocksWrapper) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastVirtualBatchNum", context.Background(), m.DbTx).
					Return(uint64(10), nil).
					Once()
			},
		},
		{
			Name:           "failed to get virtual batch number",
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get the last virtual batch number from state"),
			ExpectedResult: 0,
			SetupMocks: func(m *mocksWrapper) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastVirtualBatchNum", context.Background(), m.DbTx).
					Return(uint64(0), errors.New("failed to get last batch number")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m)

			res, err := s.JSONRPCCall("zkevm_virtualBatchNumber")
			require.NoError(t, err)

			if res.Result != nil {
				var result types.ArgUint64
				err = json.Unmarshal(res.Result, &result)
				require.NoError(t, err)
				assert.Equal(t, tc.ExpectedResult, uint64(result))
			}

			if res.Error != nil || tc.ExpectedError != nil {
				assert.Equal(t, tc.ExpectedError.ErrorCode(), res.Error.Code)
				assert.Equal(t, tc.ExpectedError.Error(), res.Error.Message)
			}
		})
	}
}

func TestVerifiedBatchNumber(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		ExpectedResult uint64
		ExpectedError  types.Error
		SetupMocks     func(m *mocksWrapper)
	}

	testCases := []testCase{
		{
			Name:           "get verified batch number successfully",
			ExpectedError:  nil,
			ExpectedResult: 10,
			SetupMocks: func(m *mocksWrapper) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastVerifiedBatch", context.Background(), m.DbTx).
					Return(&state.VerifiedBatch{BatchNumber: uint64(10)}, nil).
					Once()
			},
		},
		{
			Name:           "failed to get verified batch number",
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get the last verified batch number from state"),
			ExpectedResult: 0,
			SetupMocks: func(m *mocksWrapper) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastVerifiedBatch", context.Background(), m.DbTx).
					Return(nil, errors.New("failed to get last batch number")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m)

			res, err := s.JSONRPCCall("zkevm_verifiedBatchNumber")
			require.NoError(t, err)

			if res.Result != nil {
				var result types.ArgUint64
				err = json.Unmarshal(res.Result, &result)
				require.NoError(t, err)
				assert.Equal(t, tc.ExpectedResult, uint64(result))
			}

			if res.Error != nil || tc.ExpectedError != nil {
				assert.Equal(t, tc.ExpectedError.ErrorCode(), res.Error.Code)
				assert.Equal(t, tc.ExpectedError.Error(), res.Error.Message)
			}
		})
	}
}

func TestGetBatchByNumber(t *testing.T) {
	type testCase struct {
		Name           string
		Number         string
		WithTxDetail   bool
		ExpectedResult *types.Batch
		ExpectedError  types.Error
		SetupMocks     func(*mockedServer, *mocksWrapper, *testCase)
	}

	testCases := []testCase{
		{
			Name:           "Batch not found",
			Number:         "0x123",
			ExpectedResult: nil,
			ExpectedError:  nil,
			SetupMocks: func(s *mockedServer, m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetBatchByNumber", context.Background(), hex.DecodeBig(tc.Number).Uint64(), m.DbTx).
					Return(nil, state.ErrNotFound)
			},
		},
		{
			Name:         "get specific batch successfully with tx detail",
			Number:       "0x345",
			WithTxDetail: true,
			ExpectedResult: &types.Batch{
				Number:              1,
				Coinbase:            common.HexToAddress("0x1"),
				StateRoot:           common.HexToHash("0x2"),
				AccInputHash:        common.HexToHash("0x3"),
				GlobalExitRoot:      common.HexToHash("0x4"),
				Timestamp:           1,
				SendSequencesTxHash: ptrHash(common.HexToHash("0x10")),
				VerifyBatchTxHash:   ptrHash(common.HexToHash("0x20")),
			},
			ExpectedError: nil,
			SetupMocks: func(s *mockedServer, m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				batch := &state.Batch{
					BatchNumber:    1,
					Coinbase:       common.HexToAddress("0x1"),
					StateRoot:      common.HexToHash("0x2"),
					AccInputHash:   common.HexToHash("0x3"),
					GlobalExitRoot: common.HexToHash("0x4"),
					Timestamp:      time.Unix(1, 0),
				}

				m.State.
					On("GetBatchByNumber", context.Background(), hex.DecodeBig(tc.Number).Uint64(), m.DbTx).
					Return(batch, nil).
					Once()

				virtualBatch := &state.VirtualBatch{
					TxHash: common.HexToHash("0x10"),
				}

				m.State.
					On("GetVirtualBatch", context.Background(), hex.DecodeBig(tc.Number).Uint64(), m.DbTx).
					Return(virtualBatch, nil).
					Once()

				verifiedBatch := &state.VerifiedBatch{
					TxHash: common.HexToHash("0x20"),
				}

				m.State.
					On("GetVerifiedBatch", context.Background(), hex.DecodeBig(tc.Number).Uint64(), m.DbTx).
					Return(verifiedBatch, nil).
					Once()

				ger := state.GlobalExitRoot{
					MainnetExitRoot: common.HexToHash("0x4"),
					RollupExitRoot:  common.HexToHash("0x4"),
					GlobalExitRoot:  common.HexToHash("0x4"),
				}
				m.State.
					On("GetExitRootByGlobalExitRoot", context.Background(), batch.GlobalExitRoot, m.DbTx).
					Return(&ger, nil).
					Once()

				txs := []*ethTypes.Transaction{
					signTx(ethTypes.NewTransaction(1001, common.HexToAddress("0x1000"), big.NewInt(1000), 1001, big.NewInt(1002), []byte("1003")), s.Config.ChainID),
					signTx(ethTypes.NewTransaction(1002, common.HexToAddress("0x1000"), big.NewInt(1000), 1001, big.NewInt(1002), []byte("1003")), s.Config.ChainID),
				}

				batchTxs := make([]ethTypes.Transaction, 0, len(txs))

				tc.ExpectedResult.Transactions = []types.TransactionOrHash{}

				for i, tx := range txs {
					blockNumber := big.NewInt(int64(i))
					blockHash := common.HexToHash(hex.EncodeUint64(uint64(i)))
					receipt := ethTypes.NewReceipt([]byte{}, false, uint64(0))
					receipt.TxHash = tx.Hash()
					receipt.TransactionIndex = uint(i)
					receipt.BlockNumber = blockNumber
					receipt.BlockHash = blockHash
					m.State.
						On("GetTransactionReceipt", context.Background(), tx.Hash(), m.DbTx).
						Return(receipt, nil).
						Once()

					from, _ := state.GetSender(*tx)
					V, R, S := tx.RawSignatureValues()

					tc.ExpectedResult.Transactions = append(tc.ExpectedResult.Transactions,
						types.TransactionOrHash{
							Tx: &types.Transaction{
								Nonce:       types.ArgUint64(tx.Nonce()),
								GasPrice:    types.ArgBig(*tx.GasPrice()),
								Gas:         types.ArgUint64(tx.Gas()),
								To:          tx.To(),
								Value:       types.ArgBig(*tx.Value()),
								Input:       tx.Data(),
								Hash:        tx.Hash(),
								From:        from,
								BlockNumber: ptrArgUint64FromUint64(blockNumber.Uint64()),
								BlockHash:   ptrHash(receipt.BlockHash),
								TxIndex:     ptrArgUint64FromUint(receipt.TransactionIndex),
								ChainID:     types.ArgBig(*tx.ChainId()),
								Type:        types.ArgUint64(tx.Type()),
								V:           types.ArgBig(*V),
								R:           types.ArgBig(*R),
								S:           types.ArgBig(*S),
							},
						},
					)

					batchTxs = append(batchTxs, *tx)
				}
				m.State.
					On("GetTransactionsByBatchNumber", context.Background(), hex.DecodeBig(tc.Number).Uint64(), m.DbTx).
					Return(batchTxs, nil).
					Once()
			},
		},
		{
			Name:         "get specific batch successfully without tx detail",
			Number:       "0x345",
			WithTxDetail: false,
			ExpectedResult: &types.Batch{
				Number:              1,
				Coinbase:            common.HexToAddress("0x1"),
				StateRoot:           common.HexToHash("0x2"),
				AccInputHash:        common.HexToHash("0x3"),
				GlobalExitRoot:      common.HexToHash("0x4"),
				Timestamp:           1,
				SendSequencesTxHash: ptrHash(common.HexToHash("0x10")),
				VerifyBatchTxHash:   ptrHash(common.HexToHash("0x20")),
			},
			ExpectedError: nil,
			SetupMocks: func(s *mockedServer, m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				batch := &state.Batch{
					BatchNumber:    1,
					Coinbase:       common.HexToAddress("0x1"),
					StateRoot:      common.HexToHash("0x2"),
					AccInputHash:   common.HexToHash("0x3"),
					GlobalExitRoot: common.HexToHash("0x4"),
					Timestamp:      time.Unix(1, 0),
				}

				m.State.
					On("GetBatchByNumber", context.Background(), hex.DecodeBig(tc.Number).Uint64(), m.DbTx).
					Return(batch, nil).
					Once()

				virtualBatch := &state.VirtualBatch{
					TxHash: common.HexToHash("0x10"),
				}

				m.State.
					On("GetVirtualBatch", context.Background(), hex.DecodeBig(tc.Number).Uint64(), m.DbTx).
					Return(virtualBatch, nil).
					Once()

				verifiedBatch := &state.VerifiedBatch{
					TxHash: common.HexToHash("0x20"),
				}

				m.State.
					On("GetVerifiedBatch", context.Background(), hex.DecodeBig(tc.Number).Uint64(), m.DbTx).
					Return(verifiedBatch, nil).
					Once()

				ger := state.GlobalExitRoot{
					MainnetExitRoot: common.HexToHash("0x4"),
					RollupExitRoot:  common.HexToHash("0x4"),
					GlobalExitRoot:  common.HexToHash("0x4"),
				}
				m.State.
					On("GetExitRootByGlobalExitRoot", context.Background(), batch.GlobalExitRoot, m.DbTx).
					Return(&ger, nil).
					Once()

				txs := []*ethTypes.Transaction{
					signTx(ethTypes.NewTransaction(1001, common.HexToAddress("0x1000"), big.NewInt(1000), 1001, big.NewInt(1002), []byte("1003")), s.Config.ChainID),
					signTx(ethTypes.NewTransaction(1002, common.HexToAddress("0x1000"), big.NewInt(1000), 1001, big.NewInt(1002), []byte("1003")), s.Config.ChainID),
				}

				batchTxs := make([]ethTypes.Transaction, 0, len(txs))

				tc.ExpectedResult.Transactions = []types.TransactionOrHash{}

				for i, tx := range txs {
					blockNumber := big.NewInt(int64(i))
					blockHash := common.HexToHash(hex.EncodeUint64(uint64(i)))
					receipt := ethTypes.NewReceipt([]byte{}, false, uint64(0))
					receipt.TxHash = tx.Hash()
					receipt.TransactionIndex = uint(i)
					receipt.BlockNumber = blockNumber
					receipt.BlockHash = blockHash
					m.State.
						On("GetTransactionReceipt", context.Background(), tx.Hash(), m.DbTx).
						Return(receipt, nil).
						Once()

					tc.ExpectedResult.Transactions = append(tc.ExpectedResult.Transactions,
						types.TransactionOrHash{
							Hash: state.HashPtr(tx.Hash()),
						},
					)

					batchTxs = append(batchTxs, *tx)
				}
				m.State.
					On("GetTransactionsByBatchNumber", context.Background(), hex.DecodeBig(tc.Number).Uint64(), m.DbTx).
					Return(batchTxs, nil).
					Once()
			},
		},
		{
			Name:         "get latest batch successfully",
			Number:       "latest",
			WithTxDetail: true,
			ExpectedResult: &types.Batch{
				Number:              1,
				Coinbase:            common.HexToAddress("0x1"),
				StateRoot:           common.HexToHash("0x2"),
				AccInputHash:        common.HexToHash("0x3"),
				GlobalExitRoot:      common.HexToHash("0x4"),
				Timestamp:           1,
				SendSequencesTxHash: ptrHash(common.HexToHash("0x10")),
				VerifyBatchTxHash:   ptrHash(common.HexToHash("0x20")),
			},
			ExpectedError: nil,
			SetupMocks: func(s *mockedServer, m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastBatchNumber", context.Background(), m.DbTx).
					Return(uint64(tc.ExpectedResult.Number), nil).
					Once()

				batch := &state.Batch{
					BatchNumber:    1,
					Coinbase:       common.HexToAddress("0x1"),
					StateRoot:      common.HexToHash("0x2"),
					AccInputHash:   common.HexToHash("0x3"),
					GlobalExitRoot: common.HexToHash("0x4"),
					Timestamp:      time.Unix(1, 0),
				}

				m.State.
					On("GetBatchByNumber", context.Background(), uint64(tc.ExpectedResult.Number), m.DbTx).
					Return(batch, nil).
					Once()

				virtualBatch := &state.VirtualBatch{
					TxHash: common.HexToHash("0x10"),
				}

				m.State.
					On("GetVirtualBatch", context.Background(), uint64(tc.ExpectedResult.Number), m.DbTx).
					Return(virtualBatch, nil).
					Once()

				verifiedBatch := &state.VerifiedBatch{
					TxHash: common.HexToHash("0x20"),
				}

				m.State.
					On("GetVerifiedBatch", context.Background(), uint64(tc.ExpectedResult.Number), m.DbTx).
					Return(verifiedBatch, nil).
					Once()

				ger := state.GlobalExitRoot{
					MainnetExitRoot: common.HexToHash("0x4"),
					RollupExitRoot:  common.HexToHash("0x4"),
					GlobalExitRoot:  common.HexToHash("0x4"),
				}
				m.State.
					On("GetExitRootByGlobalExitRoot", context.Background(), batch.GlobalExitRoot, m.DbTx).
					Return(&ger, nil).
					Once()

				txs := []*ethTypes.Transaction{
					signTx(ethTypes.NewTransaction(1001, common.HexToAddress("0x1000"), big.NewInt(1000), 1001, big.NewInt(1002), []byte("1003")), s.Config.ChainID),
					signTx(ethTypes.NewTransaction(1002, common.HexToAddress("0x1000"), big.NewInt(1000), 1001, big.NewInt(1002), []byte("1003")), s.Config.ChainID),
				}

				batchTxs := make([]ethTypes.Transaction, 0, len(txs))

				tc.ExpectedResult.Transactions = []types.TransactionOrHash{}

				for i, tx := range txs {
					blockNumber := big.NewInt(int64(i))
					blockHash := common.HexToHash(hex.EncodeUint64(uint64(i)))
					receipt := ethTypes.NewReceipt([]byte{}, false, uint64(0))
					receipt.TxHash = tx.Hash()
					receipt.TransactionIndex = uint(i)
					receipt.BlockNumber = blockNumber
					receipt.BlockHash = blockHash
					m.State.
						On("GetTransactionReceipt", context.Background(), tx.Hash(), m.DbTx).
						Return(receipt, nil).
						Once()

					from, _ := state.GetSender(*tx)
					V, R, S := tx.RawSignatureValues()

					tc.ExpectedResult.Transactions = append(tc.ExpectedResult.Transactions,
						types.TransactionOrHash{
							Tx: &types.Transaction{
								Nonce:       types.ArgUint64(tx.Nonce()),
								GasPrice:    types.ArgBig(*tx.GasPrice()),
								Gas:         types.ArgUint64(tx.Gas()),
								To:          tx.To(),
								Value:       types.ArgBig(*tx.Value()),
								Input:       tx.Data(),
								Hash:        tx.Hash(),
								From:        from,
								BlockNumber: ptrArgUint64FromUint64(blockNumber.Uint64()),
								BlockHash:   ptrHash(receipt.BlockHash),
								TxIndex:     ptrArgUint64FromUint(receipt.TransactionIndex),
								ChainID:     types.ArgBig(*tx.ChainId()),
								Type:        types.ArgUint64(tx.Type()),
								V:           types.ArgBig(*V),
								R:           types.ArgBig(*R),
								S:           types.ArgBig(*S),
							},
						},
					)

					batchTxs = append(batchTxs, *tx)
				}
				m.State.
					On("GetTransactionsByBatchNumber", context.Background(), uint64(tc.ExpectedResult.Number), m.DbTx).
					Return(batchTxs, nil).
					Once()
			},
		},
		{
			Name:           "get latest batch fails to compute batch number",
			Number:         "latest",
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get the last batch number from state"),
			SetupMocks: func(s *mockedServer, m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastBatchNumber", context.Background(), m.DbTx).
					Return(uint64(0), errors.New("failed to get last batch number")).
					Once()
			},
		},
		{
			Name:           "get latest batch fails to load batch by number",
			Number:         "latest",
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "couldn't load batch from state by number 1"),
			SetupMocks: func(s *mockedServer, m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastBatchNumber", context.Background(), m.DbTx).
					Return(uint64(1), nil).
					Once()

				m.State.
					On("GetBatchByNumber", context.Background(), uint64(1), m.DbTx).
					Return(nil, errors.New("failed to load batch by number")).
					Once()
			},
		},
	}

	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			testCase.SetupMocks(s, m, &tc)

			res, err := s.JSONRPCCall("zkevm_getBatchByNumber", tc.Number, tc.WithTxDetail)
			require.NoError(t, err)
			assert.Equal(t, float64(1), res.ID)
			assert.Equal(t, "2.0", res.JSONRPC)

			if res.Result != nil {
				var result interface{}
				err = json.Unmarshal(res.Result, &result)
				require.NoError(t, err)

				if result != nil || testCase.ExpectedResult != nil {
					var batch map[string]interface{}
					err = json.Unmarshal(res.Result, &batch)
					require.NoError(t, err)
					assert.Equal(t, tc.ExpectedResult.Number.Hex(), batch["number"].(string))
					assert.Equal(t, tc.ExpectedResult.Coinbase.String(), batch["coinbase"].(string))
					assert.Equal(t, tc.ExpectedResult.StateRoot.String(), batch["stateRoot"].(string))
					assert.Equal(t, tc.ExpectedResult.GlobalExitRoot.String(), batch["globalExitRoot"].(string))
					assert.Equal(t, tc.ExpectedResult.LocalExitRoot.String(), batch["localExitRoot"].(string))
					assert.Equal(t, tc.ExpectedResult.AccInputHash.String(), batch["accInputHash"].(string))
					assert.Equal(t, tc.ExpectedResult.Timestamp.Hex(), batch["timestamp"].(string))
					assert.Equal(t, tc.ExpectedResult.SendSequencesTxHash.String(), batch["sendSequencesTxHash"].(string))
					assert.Equal(t, tc.ExpectedResult.VerifyBatchTxHash.String(), batch["verifyBatchTxHash"].(string))
					batchTxs := batch["transactions"].([]interface{})
					for i, txOrHash := range tc.ExpectedResult.Transactions {
						switch batchTxOrHash := batchTxs[i].(type) {
						case string:
							assert.Equal(t, txOrHash.Hash.String(), batchTxOrHash)
						case map[string]interface{}:
							tx := txOrHash.Tx
							assert.Equal(t, tx.Nonce.Hex(), batchTxOrHash["nonce"].(string))
							assert.Equal(t, tx.GasPrice.Hex(), batchTxOrHash["gasPrice"].(string))
							assert.Equal(t, tx.Gas.Hex(), batchTxOrHash["gas"].(string))
							assert.Equal(t, tx.To.String(), batchTxOrHash["to"].(string))
							assert.Equal(t, tx.Value.Hex(), batchTxOrHash["value"].(string))
							assert.Equal(t, tx.Input.Hex(), batchTxOrHash["input"].(string))
							assert.Equal(t, tx.V.Hex(), batchTxOrHash["v"].(string))
							assert.Equal(t, tx.R.Hex(), batchTxOrHash["r"].(string))
							assert.Equal(t, tx.S.Hex(), batchTxOrHash["s"].(string))
							assert.Equal(t, tx.Hash.String(), batchTxOrHash["hash"].(string))
							assert.Equal(t, strings.ToLower(tx.From.String()), strings.ToLower(batchTxOrHash["from"].(string)))
							assert.Equal(t, tx.BlockHash.String(), batchTxOrHash["blockHash"].(string))
							assert.Equal(t, tx.BlockNumber.Hex(), batchTxOrHash["blockNumber"].(string))
							assert.Equal(t, tx.TxIndex.Hex(), batchTxOrHash["transactionIndex"].(string))
							assert.Equal(t, tx.ChainID.Hex(), batchTxOrHash["chainId"].(string))
							assert.Equal(t, tx.Type.Hex(), batchTxOrHash["type"].(string))
						}
					}
				}
			}

			if res.Error != nil || testCase.ExpectedError != nil {
				assert.Equal(t, testCase.ExpectedError.ErrorCode(), res.Error.Code)
				assert.Equal(t, testCase.ExpectedError.Error(), res.Error.Message)
			}
		})
	}
}

func ptrUint64(n uint64) *uint64 {
	return &n
}

func ptrArgUint64FromUint(n uint) *types.ArgUint64 {
	tmp := types.ArgUint64(n)
	return &tmp
}

func ptrArgUint64FromUint64(n uint64) *types.ArgUint64 {
	tmp := types.ArgUint64(n)
	return &tmp
}

func ptrHash(h common.Hash) *common.Hash {
	return &h
}

func signTx(tx *ethTypes.Transaction, chainID uint64) *ethTypes.Transaction {
	privateKey, _ := crypto.GenerateKey()
	auth, _ := bind.NewKeyedTransactorWithChainID(privateKey, big.NewInt(0).SetUint64(chainID))
	signedTx, _ := auth.Signer(auth.From, tx)
	return signedTx
}
