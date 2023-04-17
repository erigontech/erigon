package jsonrpc

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/0xPolygonHermez/zkevm-node/encoding"
	"github.com/0xPolygonHermez/zkevm-node/hex"
	"github.com/0xPolygonHermez/zkevm-node/jsonrpc/types"
	"github.com/0xPolygonHermez/zkevm-node/pool"
	"github.com/0xPolygonHermez/zkevm-node/state"
	"github.com/0xPolygonHermez/zkevm-node/state/runtime"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v4"
	"github.com/ledgerwatch/erigon/common"
	ethTypes "github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	latest            = "latest"
	blockNumOne       = big.NewInt(1)
	blockNumOneUint64 = blockNumOne.Uint64()
	blockNumTen       = big.NewInt(10)
	blockNumTenUint64 = blockNumTen.Uint64()
	addressArg        = common.HexToAddress("0x123")
	keyArg            = common.HexToHash("0x123")
	blockHash         = common.HexToHash("0x82ba516e76a4bfaba6d1d95c8ccde96e353ce3c683231d011021f43dee7b2d95")
	blockRoot         = common.HexToHash("0xce3c683231d011021f43dee7b2d9582ba516e76a4bfaba6d1d95c8ccde96e353")
	nilUint64         *uint64
)

func TestBlockNumber(t *testing.T) {
	s, m, c := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		ExpectedResult uint64
		ExpectedError  interface{}
		SetupMocks     func(m *mocksWrapper)
	}

	testCases := []testCase{
		{
			Name:           "get block number successfully",
			ExpectedError:  nil,
			ExpectedResult: blockNumTen.Uint64(),
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
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(blockNumTen.Uint64(), nil).
					Once()
			},
		},
		{
			Name:           "failed to get block number",
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get the last block number from state"),
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
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(uint64(0), errors.New("failed to get last block number")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m)

			result, err := c.BlockNumber(context.Background())
			assert.Equal(t, testCase.ExpectedResult, result)

			if err != nil || testCase.ExpectedError != nil {
				if expectedErr, ok := testCase.ExpectedError.(*types.RPCError); ok {
					rpcErr := err.(rpc.Error)
					assert.Equal(t, expectedErr.ErrorCode(), rpcErr.ErrorCode())
					assert.Equal(t, expectedErr.Error(), rpcErr.Error())
				} else {
					assert.Equal(t, testCase.ExpectedError, err)
				}
			}
		})
	}
}

func TestCall(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		name           string
		params         []interface{}
		expectedResult []byte
		expectedError  interface{}
		setupMocks     func(Config, *mocksWrapper, *testCase)
	}

	testCases := []*testCase{
		{
			name: "Transaction with all information from a block with EIP-1898",
			params: []interface{}{
				types.TxArgs{
					From:     state.HexToAddressPtr("0x1"),
					To:       state.HexToAddressPtr("0x2"),
					Gas:      types.ArgUint64Ptr(24000),
					GasPrice: types.ArgBytesPtr(big.NewInt(1).Bytes()),
					Value:    types.ArgBytesPtr(big.NewInt(2).Bytes()),
					Data:     types.ArgBytesPtr([]byte("data")),
				},
				map[string]interface{}{
					types.BlockNumberKey: hex.EncodeBig(blockNumOne),
				},
			},
			expectedResult: []byte("hello world"),
			expectedError:  nil,
			setupMocks: func(c Config, m *mocksWrapper, testCase *testCase) {
				nonce := uint64(7)
				m.DbTx.On("Commit", context.Background()).Return(nil).Once()
				m.State.On("BeginStateTransaction", context.Background()).Return(m.DbTx, nil).Once()
				txArgs := testCase.params[0].(types.TxArgs)
				txMatchBy := mock.MatchedBy(func(tx *ethTypes.Transaction) bool {
					gasPrice := big.NewInt(0).SetBytes(*txArgs.GasPrice)
					value := big.NewInt(0).SetBytes(*txArgs.Value)
					match := tx != nil &&
						tx.To().Hex() == txArgs.To.Hex() &&
						tx.Gas() == uint64(*txArgs.Gas) &&
						tx.GasPrice().Uint64() == gasPrice.Uint64() &&
						tx.Value().Uint64() == value.Uint64() &&
						hex.EncodeToHex(tx.Data()) == hex.EncodeToHex(*txArgs.Data) &&
						tx.Nonce() == nonce
					return match
				})
				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumOne, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumOneUint64, m.DbTx).Return(block, nil).Once()
				m.State.On("GetNonce", context.Background(), *txArgs.From, blockRoot).Return(nonce, nil).Once()
				m.State.
					On("ProcessUnsignedTransaction", context.Background(), txMatchBy, *txArgs.From, &blockNumOneUint64, false, m.DbTx).
					Return(&runtime.ExecutionResult{ReturnValue: testCase.expectedResult}, nil).
					Once()
			},
		},
		{
			name: "Transaction with all information from block by hash with EIP-1898",
			params: []interface{}{
				types.TxArgs{
					From:     state.HexToAddressPtr("0x1"),
					To:       state.HexToAddressPtr("0x2"),
					Gas:      types.ArgUint64Ptr(24000),
					GasPrice: types.ArgBytesPtr(big.NewInt(1).Bytes()),
					Value:    types.ArgBytesPtr(big.NewInt(2).Bytes()),
					Data:     types.ArgBytesPtr([]byte("data")),
				},
				map[string]interface{}{
					types.BlockHashKey: blockHash.String(),
				},
			},
			expectedResult: []byte("hello world"),
			expectedError:  nil,
			setupMocks: func(c Config, m *mocksWrapper, testCase *testCase) {
				nonce := uint64(7)
				m.DbTx.On("Commit", context.Background()).Return(nil).Once()
				m.State.On("BeginStateTransaction", context.Background()).Return(m.DbTx, nil).Once()
				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumOne, Root: blockRoot})
				m.State.
					On("GetL2BlockByHash", context.Background(), blockHash, m.DbTx).
					Return(block, nil).Once()
				txArgs := testCase.params[0].(types.TxArgs)
				txMatchBy := mock.MatchedBy(func(tx *ethTypes.Transaction) bool {
					gasPrice := big.NewInt(0).SetBytes(*txArgs.GasPrice)
					value := big.NewInt(0).SetBytes(*txArgs.Value)
					return tx != nil &&
						tx.To().Hex() == txArgs.To.Hex() &&
						tx.Gas() == uint64(*txArgs.Gas) &&
						tx.GasPrice().Uint64() == gasPrice.Uint64() &&
						tx.Value().Uint64() == value.Uint64() &&
						hex.EncodeToHex(tx.Data()) == hex.EncodeToHex(*txArgs.Data) &&
						tx.Nonce() == nonce
				})
				m.State.On("GetNonce", context.Background(), *txArgs.From, blockRoot).Return(nonce, nil).Once()
				m.State.
					On("ProcessUnsignedTransaction", context.Background(), txMatchBy, *txArgs.From, &blockNumOneUint64, false, m.DbTx).
					Return(&runtime.ExecutionResult{ReturnValue: testCase.expectedResult}, nil).
					Once()
			},
		},
		{
			name: "Transaction with all information from latest block",
			params: []interface{}{
				types.TxArgs{
					From:     state.HexToAddressPtr("0x1"),
					To:       state.HexToAddressPtr("0x2"),
					Gas:      types.ArgUint64Ptr(24000),
					GasPrice: types.ArgBytesPtr(big.NewInt(1).Bytes()),
					Value:    types.ArgBytesPtr(big.NewInt(2).Bytes()),
					Data:     types.ArgBytesPtr([]byte("data")),
				},
				latest,
			},
			expectedResult: []byte("hello world"),
			expectedError:  nil,
			setupMocks: func(c Config, m *mocksWrapper, testCase *testCase) {
				nonce := uint64(7)
				m.DbTx.On("Commit", context.Background()).Return(nil).Once()
				m.State.On("BeginStateTransaction", context.Background()).Return(m.DbTx, nil).Once()
				m.State.On("GetLastL2BlockNumber", context.Background(), m.DbTx).Return(blockNumOne.Uint64(), nil).Once()
				txArgs := testCase.params[0].(types.TxArgs)
				txMatchBy := mock.MatchedBy(func(tx *ethTypes.Transaction) bool {
					gasPrice := big.NewInt(0).SetBytes(*txArgs.GasPrice)
					value := big.NewInt(0).SetBytes(*txArgs.Value)
					match := tx != nil &&
						tx.To().Hex() == txArgs.To.Hex() &&
						tx.Gas() == uint64(*txArgs.Gas) &&
						tx.GasPrice().Uint64() == gasPrice.Uint64() &&
						tx.Value().Uint64() == value.Uint64() &&
						hex.EncodeToHex(tx.Data()) == hex.EncodeToHex(*txArgs.Data) &&
						tx.Nonce() == nonce
					return match
				})
				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumOne, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumOneUint64, m.DbTx).Return(block, nil).Once()
				m.State.On("GetNonce", context.Background(), *txArgs.From, blockRoot).Return(nonce, nil).Once()
				m.State.
					On("ProcessUnsignedTransaction", context.Background(), txMatchBy, *txArgs.From, nilUint64, false, m.DbTx).
					Return(&runtime.ExecutionResult{ReturnValue: testCase.expectedResult}, nil).
					Once()
			},
		},
		{
			name: "Transaction with all information from block by hash",
			params: []interface{}{
				types.TxArgs{
					From:     state.HexToAddressPtr("0x1"),
					To:       state.HexToAddressPtr("0x2"),
					Gas:      types.ArgUint64Ptr(24000),
					GasPrice: types.ArgBytesPtr(big.NewInt(1).Bytes()),
					Value:    types.ArgBytesPtr(big.NewInt(2).Bytes()),
					Data:     types.ArgBytesPtr([]byte("data")),
				},
				blockHash.String(),
			},
			expectedResult: []byte("hello world"),
			expectedError:  nil,
			setupMocks: func(c Config, m *mocksWrapper, testCase *testCase) {
				nonce := uint64(7)
				m.DbTx.On("Commit", context.Background()).Return(nil).Once()
				m.State.On("BeginStateTransaction", context.Background()).Return(m.DbTx, nil).Once()
				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumTen, Root: blockRoot})
				m.State.
					On("GetL2BlockByHash", context.Background(), blockHash, m.DbTx).
					Return(block, nil).Once()
				txArgs := testCase.params[0].(types.TxArgs)
				txMatchBy := mock.MatchedBy(func(tx *ethTypes.Transaction) bool {
					gasPrice := big.NewInt(0).SetBytes(*txArgs.GasPrice)
					value := big.NewInt(0).SetBytes(*txArgs.Value)
					return tx != nil &&
						tx.To().Hex() == txArgs.To.Hex() &&
						tx.Gas() == uint64(*txArgs.Gas) &&
						tx.GasPrice().Uint64() == gasPrice.Uint64() &&
						tx.Value().Uint64() == value.Uint64() &&
						hex.EncodeToHex(tx.Data()) == hex.EncodeToHex(*txArgs.Data) &&
						tx.Nonce() == nonce
				})
				m.State.On("GetNonce", context.Background(), *txArgs.From, blockRoot).Return(nonce, nil).Once()
				m.State.
					On("ProcessUnsignedTransaction", context.Background(), txMatchBy, *txArgs.From, &blockNumTenUint64, false, m.DbTx).
					Return(&runtime.ExecutionResult{ReturnValue: testCase.expectedResult}, nil).
					Once()
			},
		},
		{
			name: "Transaction with all information from block by number",
			params: []interface{}{
				types.TxArgs{
					From:     state.HexToAddressPtr("0x1"),
					To:       state.HexToAddressPtr("0x2"),
					Gas:      types.ArgUint64Ptr(24000),
					GasPrice: types.ArgBytesPtr(big.NewInt(1).Bytes()),
					Value:    types.ArgBytesPtr(big.NewInt(2).Bytes()),
					Data:     types.ArgBytesPtr([]byte("data")),
				},
				"0xa",
			},
			expectedResult: []byte("hello world"),
			expectedError:  nil,
			setupMocks: func(c Config, m *mocksWrapper, testCase *testCase) {
				nonce := uint64(7)
				m.DbTx.On("Commit", context.Background()).Return(nil).Once()
				m.State.On("BeginStateTransaction", context.Background()).Return(m.DbTx, nil).Once()
				txArgs := testCase.params[0].(types.TxArgs)
				txMatchBy := mock.MatchedBy(func(tx *ethTypes.Transaction) bool {
					gasPrice := big.NewInt(0).SetBytes(*txArgs.GasPrice)
					value := big.NewInt(0).SetBytes(*txArgs.Value)
					return tx != nil &&
						tx.To().Hex() == txArgs.To.Hex() &&
						tx.Gas() == uint64(*txArgs.Gas) &&
						tx.GasPrice().Uint64() == gasPrice.Uint64() &&
						tx.Value().Uint64() == value.Uint64() &&
						hex.EncodeToHex(tx.Data()) == hex.EncodeToHex(*txArgs.Data) &&
						tx.Nonce() == nonce
				})
				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumTen, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumTenUint64, m.DbTx).Return(block, nil).Once()
				m.State.On("GetNonce", context.Background(), *txArgs.From, blockRoot).Return(nonce, nil).Once()
				m.State.
					On("ProcessUnsignedTransaction", context.Background(), txMatchBy, *txArgs.From, &blockNumTenUint64, false, m.DbTx).
					Return(&runtime.ExecutionResult{ReturnValue: testCase.expectedResult}, nil).
					Once()
			},
		},
		{
			name: "Transaction without from and gas from latest block",
			params: []interface{}{
				types.TxArgs{
					To:       state.HexToAddressPtr("0x2"),
					GasPrice: types.ArgBytesPtr(big.NewInt(0).Bytes()),
					Value:    types.ArgBytesPtr(big.NewInt(2).Bytes()),
					Data:     types.ArgBytesPtr([]byte("data")),
				},
				latest,
			},
			expectedResult: []byte("hello world"),
			expectedError:  nil,
			setupMocks: func(c Config, m *mocksWrapper, testCase *testCase) {
				blockHeader := &ethTypes.Header{GasLimit: s.Config.MaxCumulativeGasUsed}
				m.DbTx.On("Commit", context.Background()).Return(nil).Once()
				m.State.On("BeginStateTransaction", context.Background()).Return(m.DbTx, nil).Once()
				m.State.On("GetLastL2BlockNumber", context.Background(), m.DbTx).Return(blockNumOne.Uint64(), nil).Once()
				m.State.On("GetL2BlockHeaderByNumber", context.Background(), blockNumOne.Uint64(), m.DbTx).Return(blockHeader, nil).Once()
				txArgs := testCase.params[0].(types.TxArgs)
				txMatchBy := mock.MatchedBy(func(tx *ethTypes.Transaction) bool {
					gasPrice := big.NewInt(0).SetBytes(*txArgs.GasPrice)
					value := big.NewInt(0).SetBytes(*txArgs.Value)
					hasTx := tx != nil
					gasMatch := tx.Gas() == blockHeader.GasLimit
					toMatch := tx.To().Hex() == txArgs.To.Hex()
					gasPriceMatch := tx.GasPrice().Uint64() == gasPrice.Uint64()
					valueMatch := tx.Value().Uint64() == value.Uint64()
					dataMatch := hex.EncodeToHex(tx.Data()) == hex.EncodeToHex(*txArgs.Data)
					return hasTx && gasMatch && toMatch && gasPriceMatch && valueMatch && dataMatch
				})
				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumOne, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumOneUint64, m.DbTx).Return(block, nil).Once()
				m.State.
					On("ProcessUnsignedTransaction", context.Background(), txMatchBy, common.HexToAddress(c.DefaultSenderAddress), nilUint64, false, m.DbTx).
					Return(&runtime.ExecutionResult{ReturnValue: testCase.expectedResult}, nil).
					Once()
			},
		},
		{
			name: "Transaction without from and gas from pending block",
			params: []interface{}{
				types.TxArgs{
					To:       state.HexToAddressPtr("0x2"),
					GasPrice: types.ArgBytesPtr(big.NewInt(0).Bytes()),
					Value:    types.ArgBytesPtr(big.NewInt(2).Bytes()),
					Data:     types.ArgBytesPtr([]byte("data")),
				},
				"pending",
			},
			expectedResult: []byte("hello world"),
			expectedError:  nil,
			setupMocks: func(c Config, m *mocksWrapper, testCase *testCase) {
				blockHeader := &ethTypes.Header{GasLimit: s.Config.MaxCumulativeGasUsed}
				m.DbTx.On("Commit", context.Background()).Return(nil).Once()
				m.State.On("BeginStateTransaction", context.Background()).Return(m.DbTx, nil).Once()
				m.State.On("GetLastL2BlockNumber", context.Background(), m.DbTx).Return(blockNumOne.Uint64(), nil).Once()
				m.State.On("GetL2BlockHeaderByNumber", context.Background(), blockNumOne.Uint64(), m.DbTx).Return(blockHeader, nil).Once()
				txArgs := testCase.params[0].(types.TxArgs)
				txMatchBy := mock.MatchedBy(func(tx *ethTypes.Transaction) bool {
					gasPrice := big.NewInt(0).SetBytes(*txArgs.GasPrice)
					value := big.NewInt(0).SetBytes(*txArgs.Value)
					hasTx := tx != nil
					gasMatch := tx.Gas() == blockHeader.GasLimit
					toMatch := tx.To().Hex() == txArgs.To.Hex()
					gasPriceMatch := tx.GasPrice().Uint64() == gasPrice.Uint64()
					valueMatch := tx.Value().Uint64() == value.Uint64()
					dataMatch := hex.EncodeToHex(tx.Data()) == hex.EncodeToHex(*txArgs.Data)
					return hasTx && gasMatch && toMatch && gasPriceMatch && valueMatch && dataMatch
				})
				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumOne, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumOneUint64, m.DbTx).Return(block, nil).Once()
				m.State.
					On("ProcessUnsignedTransaction", context.Background(), txMatchBy, common.HexToAddress(c.DefaultSenderAddress), nilUint64, false, m.DbTx).
					Return(&runtime.ExecutionResult{ReturnValue: testCase.expectedResult}, nil).
					Once()
			},
		},
		{
			name: "Transaction without from and gas and failed to get block header",
			params: []interface{}{
				types.TxArgs{
					To:       state.HexToAddressPtr("0x2"),
					GasPrice: types.ArgBytesPtr(big.NewInt(0).Bytes()),
					Value:    types.ArgBytesPtr(big.NewInt(2).Bytes()),
					Data:     types.ArgBytesPtr([]byte("data")),
				},
				latest,
			},
			expectedResult: nil,
			expectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get block header"),
			setupMocks: func(c Config, m *mocksWrapper, testCase *testCase) {
				m.DbTx.On("Rollback", context.Background()).Return(nil).Once()
				m.State.On("BeginStateTransaction", context.Background()).Return(m.DbTx, nil).Once()
				m.State.On("GetLastL2BlockNumber", context.Background(), m.DbTx).Return(blockNumOne.Uint64(), nil).Once()
				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumOne, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumOneUint64, m.DbTx).Return(block, nil).Once()
				m.State.On("GetL2BlockHeaderByNumber", context.Background(), blockNumOne.Uint64(), m.DbTx).Return(nil, errors.New("failed to get block header")).Once()
			},
		},
		{
			name: "Transaction with all information but failed to process unsigned transaction",
			params: []interface{}{
				types.TxArgs{
					From:     state.HexToAddressPtr("0x1"),
					To:       state.HexToAddressPtr("0x2"),
					Gas:      types.ArgUint64Ptr(24000),
					GasPrice: types.ArgBytesPtr(big.NewInt(1).Bytes()),
					Value:    types.ArgBytesPtr(big.NewInt(2).Bytes()),
					Data:     types.ArgBytesPtr([]byte("data")),
				},
				latest,
			},
			expectedResult: nil,
			expectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to process unsigned transaction"),
			setupMocks: func(c Config, m *mocksWrapper, testCase *testCase) {
				nonce := uint64(7)
				m.DbTx.On("Rollback", context.Background()).Return(nil).Once()
				m.State.On("BeginStateTransaction", context.Background()).Return(m.DbTx, nil).Once()
				m.State.On("GetLastL2BlockNumber", context.Background(), m.DbTx).Return(blockNumOne.Uint64(), nil).Once()
				txArgs := testCase.params[0].(types.TxArgs)
				txMatchBy := mock.MatchedBy(func(tx *ethTypes.Transaction) bool {
					gasPrice := big.NewInt(0).SetBytes(*txArgs.GasPrice)
					value := big.NewInt(0).SetBytes(*txArgs.Value)
					hasTx := tx != nil
					gasMatch := tx.Gas() == uint64(*txArgs.Gas)
					toMatch := tx.To().Hex() == txArgs.To.Hex()
					gasPriceMatch := tx.GasPrice().Uint64() == gasPrice.Uint64()
					valueMatch := tx.Value().Uint64() == value.Uint64()
					dataMatch := hex.EncodeToHex(tx.Data()) == hex.EncodeToHex(*txArgs.Data)
					nonceMatch := tx.Nonce() == nonce
					return hasTx && gasMatch && toMatch && gasPriceMatch && valueMatch && dataMatch && nonceMatch
				})
				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumOne, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumOneUint64, m.DbTx).Return(block, nil).Once()
				m.State.On("GetNonce", context.Background(), *txArgs.From, blockRoot).Return(nonce, nil).Once()
				m.State.
					On("ProcessUnsignedTransaction", context.Background(), txMatchBy, *txArgs.From, nilUint64, false, m.DbTx).
					Return(&runtime.ExecutionResult{Err: errors.New("failed to process unsigned transaction")}, nil).
					Once()
			},
		},
		{
			name: "Transaction with all information but reverted to process unsigned transaction",
			params: []interface{}{
				types.TxArgs{
					From:     state.HexToAddressPtr("0x1"),
					To:       state.HexToAddressPtr("0x2"),
					Gas:      types.ArgUint64Ptr(24000),
					GasPrice: types.ArgBytesPtr(big.NewInt(1).Bytes()),
					Value:    types.ArgBytesPtr(big.NewInt(2).Bytes()),
					Data:     types.ArgBytesPtr([]byte("data")),
				},
				latest,
			},
			expectedResult: nil,
			expectedError:  types.NewRPCError(types.RevertedErrorCode, "execution reverted"),
			setupMocks: func(c Config, m *mocksWrapper, testCase *testCase) {
				nonce := uint64(7)
				m.DbTx.On("Rollback", context.Background()).Return(nil).Once()
				m.State.On("BeginStateTransaction", context.Background()).Return(m.DbTx, nil).Once()
				m.State.On("GetLastL2BlockNumber", context.Background(), m.DbTx).Return(blockNumOne.Uint64(), nil).Once()
				txArgs := testCase.params[0].(types.TxArgs)
				txMatchBy := mock.MatchedBy(func(tx *ethTypes.Transaction) bool {
					gasPrice := big.NewInt(0).SetBytes(*txArgs.GasPrice)
					value := big.NewInt(0).SetBytes(*txArgs.Value)
					hasTx := tx != nil
					gasMatch := tx.Gas() == uint64(*txArgs.Gas)
					toMatch := tx.To().Hex() == txArgs.To.Hex()
					gasPriceMatch := tx.GasPrice().Uint64() == gasPrice.Uint64()
					valueMatch := tx.Value().Uint64() == value.Uint64()
					dataMatch := hex.EncodeToHex(tx.Data()) == hex.EncodeToHex(*txArgs.Data)
					nonceMatch := tx.Nonce() == nonce
					return hasTx && gasMatch && toMatch && gasPriceMatch && valueMatch && dataMatch && nonceMatch
				})
				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumOne, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumOneUint64, m.DbTx).Return(block, nil).Once()
				m.State.On("GetNonce", context.Background(), *txArgs.From, blockRoot).Return(nonce, nil).Once()
				m.State.
					On("ProcessUnsignedTransaction", context.Background(), txMatchBy, *txArgs.From, nilUint64, false, m.DbTx).
					Return(&runtime.ExecutionResult{Err: runtime.ErrExecutionReverted}, nil).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.setupMocks(s.Config, m, testCase)

			res, err := s.JSONRPCCall("eth_call", testCase.params...)
			require.NoError(t, err)

			if testCase.expectedResult != nil {
				require.NotNil(t, res.Result)
				require.Nil(t, res.Error)

				var s string
				err = json.Unmarshal(res.Result, &s)
				require.NoError(t, err)

				result, err := hex.DecodeHex(s)
				require.NoError(t, err)

				assert.Equal(t, testCase.expectedResult, result)
			}

			if testCase.expectedError != nil {
				expectedErr := testCase.expectedError.(*types.RPCError)
				assert.Equal(t, expectedErr.ErrorCode(), res.Error.Code)
				assert.Equal(t, expectedErr.Error(), res.Error.Message)
			}
		})
	}
}

func TestChainID(t *testing.T) {
	s, _, c := newSequencerMockedServer(t)
	defer s.Stop()

	chainID, err := c.ChainID(context.Background())
	require.NoError(t, err)

	assert.Equal(t, s.Config.ChainID, chainID.Uint64())
}

func TestEstimateGas(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		name           string
		params         []interface{}
		expectedResult *uint64
		expectedError  interface{}
		setupMocks     func(Config, *mocksWrapper, *testCase)
	}

	testCases := []testCase{
		{
			name: "Transaction with all information",
			params: []interface{}{
				types.TxArgs{
					From:     state.HexToAddressPtr("0x1"),
					To:       state.HexToAddressPtr("0x2"),
					Gas:      types.ArgUint64Ptr(24000),
					GasPrice: types.ArgBytesPtr(big.NewInt(1).Bytes()),
					Value:    types.ArgBytesPtr(big.NewInt(2).Bytes()),
					Data:     types.ArgBytesPtr([]byte("data")),
				},
			},
			expectedResult: ptrUint64(100),
			setupMocks: func(c Config, m *mocksWrapper, testCase *testCase) {
				nonce := uint64(7)
				txArgs := testCase.params[0].(types.TxArgs)
				txMatchBy := mock.MatchedBy(func(tx *ethTypes.Transaction) bool {
					if tx == nil {
						return false
					}

					gasPrice := big.NewInt(0).SetBytes(*txArgs.GasPrice)
					value := big.NewInt(0).SetBytes(*txArgs.Value)

					matchTo := tx.To().Hex() == txArgs.To.Hex()
					matchGasPrice := tx.GasPrice().Uint64() == gasPrice.Uint64()
					matchValue := tx.Value().Uint64() == value.Uint64()
					matchData := hex.EncodeToHex(tx.Data()) == hex.EncodeToHex(*txArgs.Data)
					matchNonce := tx.Nonce() == nonce
					return matchTo && matchGasPrice && matchValue && matchData && matchNonce
				})

				m.DbTx.On("Commit", context.Background()).Return(nil).Once()
				m.State.On("BeginStateTransaction", context.Background()).Return(m.DbTx, nil).Once()

				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumTen, Root: blockRoot})
				m.State.On("GetLastL2Block", context.Background(), m.DbTx).Return(block, nil).Once()

				m.State.
					On("GetNonce", context.Background(), *txArgs.From, blockRoot).
					Return(nonce, nil).
					Once()
				m.State.
					On("EstimateGas", txMatchBy, *txArgs.From, nilUint64, m.DbTx).
					Return(*testCase.expectedResult, nil).
					Once()
			},
		},
		{
			name: "Transaction without from and gas",
			params: []interface{}{
				types.TxArgs{
					To:       state.HexToAddressPtr("0x2"),
					GasPrice: types.ArgBytesPtr(big.NewInt(0).Bytes()),
					Value:    types.ArgBytesPtr(big.NewInt(2).Bytes()),
					Data:     types.ArgBytesPtr([]byte("data")),
				},
			},
			expectedResult: ptrUint64(100),
			setupMocks: func(c Config, m *mocksWrapper, testCase *testCase) {
				nonce := uint64(0)
				txArgs := testCase.params[0].(types.TxArgs)
				txMatchBy := mock.MatchedBy(func(tx *ethTypes.Transaction) bool {
					if tx == nil {
						return false
					}

					gasPrice := big.NewInt(0).SetBytes(*txArgs.GasPrice)
					value := big.NewInt(0).SetBytes(*txArgs.Value)
					matchTo := tx.To().Hex() == txArgs.To.Hex()
					matchGasPrice := tx.GasPrice().Uint64() == gasPrice.Uint64()
					matchValue := tx.Value().Uint64() == value.Uint64()
					matchData := hex.EncodeToHex(tx.Data()) == hex.EncodeToHex(*txArgs.Data)
					matchNonce := tx.Nonce() == nonce
					return matchTo && matchGasPrice && matchValue && matchData && matchNonce
				})

				m.DbTx.On("Commit", context.Background()).Return(nil).Once()
				m.State.On("BeginStateTransaction", context.Background()).Return(m.DbTx, nil).Once()

				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumTen, Root: blockRoot})
				m.State.On("GetLastL2Block", context.Background(), m.DbTx).Return(block, nil).Once()

				m.State.
					On("EstimateGas", txMatchBy, common.HexToAddress(c.DefaultSenderAddress), nilUint64, m.DbTx).
					Return(*testCase.expectedResult, nil).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			tc := testCase
			tc.setupMocks(s.Config, m, &tc)

			res, err := s.JSONRPCCall("eth_estimateGas", testCase.params...)
			require.NoError(t, err)

			if testCase.expectedResult != nil {
				require.NotNil(t, res.Result)
				require.Nil(t, res.Error)

				var s string
				err = json.Unmarshal(res.Result, &s)
				require.NoError(t, err)

				result := hex.DecodeUint64(s)

				assert.Equal(t, *testCase.expectedResult, result)
			}

			if testCase.expectedError != nil {
				expectedErr := testCase.expectedError.(*types.RPCError)
				assert.Equal(t, expectedErr.ErrorCode(), res.Error.Code)
				assert.Equal(t, expectedErr.Error(), res.Error.Message)
			}
		})
	}
}

func TestGasPrice(t *testing.T) {
	s, m, c := newSequencerMockedServer(t)
	defer s.Stop()

	testCases := []struct {
		name             string
		gasPrice         uint64
		error            error
		expectedGasPrice uint64
	}{
		{"GasPrice nil", 0, nil, 0},
		{"GasPrice with value", 50, nil, 50},
		{"failed to get gas price", 50, errors.New("failed to get gas price"), 0},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			m.Pool.
				On("GetGasPrice", context.Background()).
				Return(testCase.gasPrice, testCase.error).
				Once()

			gasPrice, err := c.SuggestGasPrice(context.Background())
			require.NoError(t, err)
			assert.Equal(t, testCase.expectedGasPrice, gasPrice.Uint64())
		})
	}
}

func TestGetBalance(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		name            string
		balance         *big.Int
		params          []interface{}
		expectedBalance uint64
		expectedError   *types.RPCError
		setupMocks      func(m *mocksWrapper, t *testCase)
	}

	testCases := []testCase{
		{
			name:    "get balance but failed to get latest block number",
			balance: big.NewInt(1000),
			params: []interface{}{
				addressArg.String(),
			},
			expectedBalance: 0,
			expectedError:   types.NewRPCError(types.DefaultErrorCode, "failed to get the last block number from state"),
			setupMocks: func(m *mocksWrapper, t *testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2Block", context.Background(), m.DbTx).
					Return(nil, errors.New("failed to get last block number")).Once()
			},
		},
		{
			name: "get balance for block nil",
			params: []interface{}{
				addressArg.String(),
			},
			balance:         big.NewInt(1000),
			expectedBalance: 1000,
			expectedError:   nil,
			setupMocks: func(m *mocksWrapper, t *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumTen, Root: blockRoot})
				m.State.
					On("GetLastL2Block", context.Background(), m.DbTx).
					Return(block, nil).Once()

				m.State.
					On("GetBalance", context.Background(), addressArg, blockRoot).
					Return(t.balance, nil).
					Once()
			},
		},
		{
			name: "get balance for block by hash with EIP-1898",
			params: []interface{}{
				addressArg.String(),
				map[string]interface{}{
					types.BlockHashKey: blockHash.String(),
				},
			},
			balance:         big.NewInt(1000),
			expectedBalance: 1000,
			expectedError:   nil,
			setupMocks: func(m *mocksWrapper, t *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumTen, Root: blockRoot})
				m.State.
					On("GetL2BlockByHash", context.Background(), blockHash, m.DbTx).
					Return(block, nil).
					Once()

				m.State.
					On("GetBalance", context.Background(), addressArg, blockRoot).
					Return(t.balance, nil).
					Once()
			},
		},
		{
			name: "get balance for not found result",
			params: []interface{}{
				addressArg.String(),
			},
			balance:         big.NewInt(1000),
			expectedBalance: 0,
			expectedError:   nil,
			setupMocks: func(m *mocksWrapper, t *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumTen, Root: blockRoot})
				m.State.On("GetLastL2Block", context.Background(), m.DbTx).Return(block, nil).Once()

				m.State.
					On("GetBalance", context.Background(), addressArg, blockRoot).
					Return(big.NewInt(0), state.ErrNotFound).
					Once()
			},
		},
		{
			name: "get balance with state failure",
			params: []interface{}{
				addressArg.String(),
			},
			balance:         big.NewInt(1000),
			expectedBalance: 0,
			expectedError:   types.NewRPCError(types.DefaultErrorCode, "failed to get balance from state"),
			setupMocks: func(m *mocksWrapper, t *testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumTen, Root: blockRoot})
				m.State.On("GetLastL2Block", context.Background(), m.DbTx).Return(block, nil).Once()

				m.State.
					On("GetBalance", context.Background(), addressArg, blockRoot).
					Return(nil, errors.New("failed to get balance")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			tc := testCase
			testCase.setupMocks(m, &tc)

			res, err := s.JSONRPCCall("eth_getBalance", tc.params...)
			require.NoError(t, err)

			if tc.expectedBalance != 0 {
				require.NotNil(t, res.Result)
				require.Nil(t, res.Error)

				var balanceStr string
				err = json.Unmarshal(res.Result, &balanceStr)
				require.NoError(t, err)

				balance := new(big.Int)
				balance.SetString(balanceStr, 0)
				assert.Equal(t, tc.expectedBalance, balance.Uint64())
			}

			if tc.expectedError != nil {
				assert.Equal(t, tc.expectedError.ErrorCode(), res.Error.Code)
				assert.Equal(t, tc.expectedError.Error(), res.Error.Message)
			}
		})
	}
}

func TestGetL2BlockByHash(t *testing.T) {
	type testCase struct {
		Name           string
		Hash           common.Hash
		ExpectedResult *ethTypes.Block
		ExpectedError  interface{}
		SetupMocks     func(*mocksWrapper, *testCase)
	}

	testCases := []testCase{
		{
			Name:           "Block not found",
			Hash:           common.HexToHash("0x123"),
			ExpectedResult: nil,
			ExpectedError:  ethereum.NotFound,
			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetL2BlockByHash", context.Background(), tc.Hash, m.DbTx).
					Return(nil, state.ErrNotFound)
			},
		},
		{
			Name:           "Failed get block from state",
			Hash:           common.HexToHash("0x234"),
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get block by hash from state"),
			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetL2BlockByHash", context.Background(), tc.Hash, m.DbTx).
					Return(nil, errors.New("failed to get block from state")).
					Once()
			},
		},
		{
			Name: "get block successfully",
			Hash: common.HexToHash("0x345"),
			ExpectedResult: ethTypes.NewBlock(
				&ethTypes.Header{Number: big.NewInt(1), UncleHash: ethTypes.EmptyUncleHash, Root: ethTypes.EmptyRootHash},
				[]*ethTypes.Transaction{ethTypes.NewTransaction(1, common.Address{}, big.NewInt(1), 1, big.NewInt(1), []byte{})},
				nil,
				[]*ethTypes.Receipt{ethTypes.NewReceipt([]byte{}, false, uint64(0))},
				&trie.StackTrie{},
			),
			ExpectedError: nil,
			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				block := ethTypes.NewBlock(ethTypes.CopyHeader(tc.ExpectedResult.Header()), tc.ExpectedResult.Transactions(), tc.ExpectedResult.Uncles(), []*ethTypes.Receipt{ethTypes.NewReceipt([]byte{}, false, uint64(0))}, &trie.StackTrie{})

				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetL2BlockByHash", context.Background(), tc.Hash, m.DbTx).
					Return(block, nil).
					Once()
			},
		},
	}

	s, m, c := newSequencerMockedServer(t)
	defer s.Stop()

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			testCase.SetupMocks(m, &tc)

			result, err := c.BlockByHash(context.Background(), tc.Hash)

			if result != nil || tc.ExpectedResult != nil {
				assert.Equal(t, tc.ExpectedResult.Number().Uint64(), result.Number().Uint64())
				assert.Equal(t, len(tc.ExpectedResult.Transactions()), len(result.Transactions()))
				assert.Equal(t, tc.ExpectedResult.Hash(), result.Hash())
			}

			if err != nil || tc.ExpectedError != nil {
				if expectedErr, ok := tc.ExpectedError.(*types.RPCError); ok {
					rpcErr := err.(rpc.Error)
					assert.Equal(t, expectedErr.ErrorCode(), rpcErr.ErrorCode())
					assert.Equal(t, expectedErr.Error(), rpcErr.Error())
				} else {
					assert.Equal(t, tc.ExpectedError, err)
				}
			}
		})
	}
}

func TestGetL2BlockByNumber(t *testing.T) {
	type testCase struct {
		Name           string
		Number         *big.Int
		ExpectedResult *ethTypes.Block
		ExpectedError  interface{}
		SetupMocks     func(*mocksWrapper, *testCase)
	}

	testCases := []testCase{
		{
			Name:           "Block not found",
			Number:         big.NewInt(123),
			ExpectedResult: nil,
			ExpectedError:  ethereum.NotFound,
			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetL2BlockByNumber", context.Background(), tc.Number.Uint64(), m.DbTx).
					Return(nil, state.ErrNotFound)
			},
		},
		{
			Name:   "get specific block successfully",
			Number: big.NewInt(345),
			ExpectedResult: ethTypes.NewBlock(
				&ethTypes.Header{Number: big.NewInt(1), UncleHash: ethTypes.EmptyUncleHash, Root: ethTypes.EmptyRootHash},
				[]*ethTypes.Transaction{ethTypes.NewTransaction(1, common.Address{}, big.NewInt(1), 1, big.NewInt(1), []byte{})},
				nil,
				[]*ethTypes.Receipt{ethTypes.NewReceipt([]byte{}, false, uint64(0))},
				&trie.StackTrie{},
			),
			ExpectedError: nil,
			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				block := ethTypes.NewBlock(ethTypes.CopyHeader(tc.ExpectedResult.Header()), tc.ExpectedResult.Transactions(),
					tc.ExpectedResult.Uncles(), []*ethTypes.Receipt{ethTypes.NewReceipt([]byte{}, false, uint64(0))}, &trie.StackTrie{})

				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetL2BlockByNumber", context.Background(), tc.Number.Uint64(), m.DbTx).
					Return(block, nil).
					Once()
			},
		},
		{
			Name:   "get latest block successfully",
			Number: nil,
			ExpectedResult: ethTypes.NewBlock(
				&ethTypes.Header{Number: big.NewInt(2), UncleHash: ethTypes.EmptyUncleHash, Root: ethTypes.EmptyRootHash},
				[]*ethTypes.Transaction{ethTypes.NewTransaction(1, common.Address{}, big.NewInt(1), 1, big.NewInt(1), []byte{})},
				nil,
				[]*ethTypes.Receipt{ethTypes.NewReceipt([]byte{}, false, uint64(0))},
				&trie.StackTrie{},
			),
			ExpectedError: nil,
			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(tc.ExpectedResult.Number().Uint64(), nil).
					Once()

				m.State.
					On("GetL2BlockByNumber", context.Background(), tc.ExpectedResult.Number().Uint64(), m.DbTx).
					Return(tc.ExpectedResult, nil).
					Once()
			},
		},
		{
			Name:           "get latest block fails to compute block number",
			Number:         nil,
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get the last block number from state"),
			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(uint64(0), errors.New("failed to get last block number")).
					Once()
			},
		},
		{
			Name:           "get latest block fails to load block by number",
			Number:         nil,
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "couldn't load block from state by number 1"),
			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(uint64(1), nil).
					Once()

				m.State.
					On("GetL2BlockByNumber", context.Background(), uint64(1), m.DbTx).
					Return(nil, errors.New("failed to load block by number")).
					Once()
			},
		},
		{
			Name:           "get pending block successfully",
			Number:         big.NewInt(-1),
			ExpectedResult: ethTypes.NewBlock(&ethTypes.Header{Number: big.NewInt(2)}, nil, nil, nil, &trie.StackTrie{}),
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				lastBlockHeader := ethTypes.CopyHeader(tc.ExpectedResult.Header())
				lastBlockHeader.Number.Sub(lastBlockHeader.Number, big.NewInt(1))
				lastBlock := ethTypes.NewBlock(lastBlockHeader, nil, nil, nil, &trie.StackTrie{})

				expectedResultHeader := ethTypes.CopyHeader(tc.ExpectedResult.Header())
				expectedResultHeader.ParentHash = lastBlock.Hash()
				tc.ExpectedResult = ethTypes.NewBlock(expectedResultHeader, nil, nil, nil, &trie.StackTrie{})

				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2Block", context.Background(), m.DbTx).
					Return(lastBlock, nil).
					Once()
			},
		},
		{
			Name:           "get pending block fails",
			Number:         big.NewInt(-1),
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "couldn't load last block from state to compute the pending block"),
			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2Block", context.Background(), m.DbTx).
					Return(nil, errors.New("failed to load last block")).
					Once()
			},
		},
	}

	s, m, c := newSequencerMockedServer(t)
	defer s.Stop()

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			testCase.SetupMocks(m, &tc)

			result, err := c.BlockByNumber(context.Background(), tc.Number)

			if result != nil || tc.ExpectedResult != nil {
				expectedResultJSON, _ := json.Marshal(tc.ExpectedResult.Header())
				resultJSON, _ := json.Marshal(result.Header())

				expectedResultJSONStr := string(expectedResultJSON)
				resultJSONStr := string(resultJSON)

				assert.JSONEq(t, expectedResultJSONStr, resultJSONStr)
				assert.Equal(t, tc.ExpectedResult.Number().Uint64(), result.Number().Uint64())
				assert.Equal(t, len(tc.ExpectedResult.Transactions()), len(result.Transactions()))
				assert.Equal(t, tc.ExpectedResult.Hash(), result.Hash())
			}

			if err != nil || tc.ExpectedError != nil {
				if expectedErr, ok := tc.ExpectedError.(*types.RPCError); ok {
					rpcErr := err.(rpc.Error)
					assert.Equal(t, expectedErr.ErrorCode(), rpcErr.ErrorCode())
					assert.Equal(t, expectedErr.Error(), rpcErr.Error())
				} else {
					assert.Equal(t, tc.ExpectedError, err)
				}
			}
		})
	}
}

func TestGetUncleByBlockHashAndIndex(t *testing.T) {
	s, _, _ := newSequencerMockedServer(t)
	defer s.Stop()

	res, err := s.JSONRPCCall("eth_getUncleByBlockHashAndIndex", common.HexToHash("0x123").Hex(), "0x1")
	require.NoError(t, err)

	assert.Equal(t, float64(1), res.ID)
	assert.Equal(t, "2.0", res.JSONRPC)
	assert.Nil(t, res.Error)

	var result interface{}
	err = json.Unmarshal(res.Result, &result)
	require.NoError(t, err)

	assert.Nil(t, result)
}

func TestGetUncleByBlockNumberAndIndex(t *testing.T) {
	s, _, _ := newSequencerMockedServer(t)
	defer s.Stop()

	res, err := s.JSONRPCCall("eth_getUncleByBlockNumberAndIndex", "0x123", "0x1")
	require.NoError(t, err)

	assert.Equal(t, float64(1), res.ID)
	assert.Equal(t, "2.0", res.JSONRPC)
	assert.Nil(t, res.Error)

	var result interface{}
	err = json.Unmarshal(res.Result, &result)
	require.NoError(t, err)

	assert.Nil(t, result)
}

func TestGetUncleCountByBlockHash(t *testing.T) {
	s, _, _ := newSequencerMockedServer(t)
	defer s.Stop()

	res, err := s.JSONRPCCall("eth_getUncleCountByBlockHash", common.HexToHash("0x123"))
	require.NoError(t, err)

	assert.Equal(t, float64(1), res.ID)
	assert.Equal(t, "2.0", res.JSONRPC)
	assert.Nil(t, res.Error)

	var result types.ArgUint64
	err = json.Unmarshal(res.Result, &result)
	require.NoError(t, err)

	assert.Equal(t, uint64(0), uint64(result))
}

func TestGetUncleCountByBlockNumber(t *testing.T) {
	s, _, _ := newSequencerMockedServer(t)
	defer s.Stop()

	res, err := s.JSONRPCCall("eth_getUncleCountByBlockNumber", "0x123")
	require.NoError(t, err)

	assert.Equal(t, float64(1), res.ID)
	assert.Equal(t, "2.0", res.JSONRPC)
	assert.Nil(t, res.Error)

	var result types.ArgUint64
	err = json.Unmarshal(res.Result, &result)
	require.NoError(t, err)

	assert.Equal(t, uint64(0), uint64(result))
}

func TestGetCode(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		Params         []interface{}
		ExpectedResult []byte
		ExpectedError  interface{}

		SetupMocks func(m *mocksWrapper, tc *testCase)
	}

	testCases := []testCase{
		{
			Name: "failed to identify the block",
			Params: []interface{}{
				addressArg.String(),
			},
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get the last block number from state"),

			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2Block", context.Background(), m.DbTx).
					Return(nil, errors.New("failed to get last block number")).
					Once()
			},
		},
		{
			Name: "failed to get code",
			Params: []interface{}{
				addressArg.String(),
				map[string]interface{}{types.BlockNumberKey: hex.EncodeBig(blockNumOne)},
			},
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get code"),

			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumOne, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumOne.Uint64(), m.DbTx).Return(block, nil).Once()

				m.State.
					On("GetCode", context.Background(), addressArg, blockRoot).
					Return(nil, errors.New("failed to get code")).
					Once()
			},
		},
		{
			Name: "code not found",
			Params: []interface{}{
				addressArg.String(),
				map[string]interface{}{types.BlockNumberKey: hex.EncodeBig(blockNumOne)},
			},
			ExpectedResult: []byte{},
			ExpectedError:  nil,

			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumOne, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumOne.Uint64(), m.DbTx).Return(block, nil).Once()

				m.State.
					On("GetCode", context.Background(), addressArg, blockRoot).
					Return(nil, state.ErrNotFound).
					Once()
			},
		},
		{
			Name: "get code successfully",
			Params: []interface{}{
				addressArg.String(),
				map[string]interface{}{types.BlockNumberKey: hex.EncodeBig(blockNumOne)},
			},
			ExpectedResult: []byte{1, 2, 3},
			ExpectedError:  nil,

			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumOne, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumOne.Uint64(), m.DbTx).Return(block, nil).Once()

				m.State.
					On("GetCode", context.Background(), addressArg, blockRoot).
					Return(tc.ExpectedResult, nil).
					Once()
			},
		},
		{
			Name: "get code successfully by block hash with EIP-1898",
			Params: []interface{}{
				addressArg.String(),
				map[string]interface{}{types.BlockHashKey: blockHash.String()},
			},
			ExpectedResult: []byte{1, 2, 3},
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumTen, Root: blockRoot})
				m.State.
					On("GetL2BlockByHash", context.Background(), blockHash, m.DbTx).
					Return(block, nil).
					Once()

				m.State.
					On("GetCode", context.Background(), addressArg, blockRoot).
					Return(tc.ExpectedResult, nil).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m, &tc)

			res, err := s.JSONRPCCall("eth_getCode", tc.Params...)
			require.NoError(t, err)

			if tc.ExpectedResult != nil {
				require.NotNil(t, res.Result)
				require.Nil(t, res.Error)

				var codeStr string
				err = json.Unmarshal(res.Result, &codeStr)
				require.NoError(t, err)

				code, err := hex.DecodeString(codeStr[2:])
				require.NoError(t, err)
				assert.Equal(t, tc.ExpectedResult, code)
			}

			if tc.ExpectedError != nil {
				if expectedErr, ok := tc.ExpectedError.(*types.RPCError); ok {
					assert.Equal(t, expectedErr.ErrorCode(), res.Error.Code)
					assert.Equal(t, expectedErr.Error(), res.Error.Message)
				} else {
					assert.Equal(t, tc.ExpectedError, err)
				}
			}
		})
	}
}

func TestGetStorageAt(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		Params         []interface{}
		ExpectedResult []byte
		ExpectedError  *types.RPCError

		SetupMocks func(m *mocksWrapper, tc *testCase)
	}

	testCases := []testCase{
		{
			Name: "failed to identify the block",
			Params: []interface{}{
				addressArg.String(),
				keyArg.String(),
			},
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get the last block number from state"),

			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2Block", context.Background(), m.DbTx).
					Return(nil, errors.New("failed to get last block number")).
					Once()
			},
		},
		{
			Name: "failed to get storage at",
			Params: []interface{}{
				addressArg.String(),
				keyArg.String(),
				map[string]interface{}{
					types.BlockNumberKey: hex.EncodeBig(blockNumOne),
				},
			},
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get storage value from state"),

			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				blockNumber := big.NewInt(1)
				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumber, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumber.Uint64(), m.DbTx).Return(block, nil).Once()

				m.State.
					On("GetStorageAt", context.Background(), addressArg, keyArg.Big(), blockRoot).
					Return(nil, errors.New("failed to get storage at")).
					Once()
			},
		},
		{
			Name: "code not found",
			Params: []interface{}{
				addressArg.String(),
				keyArg.String(),
				map[string]interface{}{
					types.BlockNumberKey: hex.EncodeBig(blockNumOne),
				},
			},
			ExpectedResult: common.Hash{}.Bytes(),
			ExpectedError:  nil,

			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				blockNumber := big.NewInt(1)
				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumber, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumber.Uint64(), m.DbTx).Return(block, nil).Once()

				m.State.
					On("GetStorageAt", context.Background(), addressArg, keyArg.Big(), blockRoot).
					Return(nil, state.ErrNotFound).
					Once()
			},
		},
		{
			Name: "get code successfully",
			Params: []interface{}{
				addressArg.String(),
				keyArg.String(),
				map[string]interface{}{
					types.BlockNumberKey: hex.EncodeBig(blockNumOne),
				},
			},
			ExpectedResult: common.BigToHash(big.NewInt(123)).Bytes(),
			ExpectedError:  nil,

			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				blockNumber := big.NewInt(1)
				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumber, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumber.Uint64(), m.DbTx).Return(block, nil).Once()

				m.State.
					On("GetStorageAt", context.Background(), addressArg, keyArg.Big(), blockRoot).
					Return(big.NewInt(123), nil).
					Once()
			},
		},
		{
			Name: "get code by block hash successfully with EIP-1898",
			Params: []interface{}{
				addressArg.String(),
				keyArg.String(),
				map[string]interface{}{
					types.BlockHashKey: blockHash.String(),
				},
			},
			ExpectedResult: common.BigToHash(big.NewInt(123)).Bytes(),
			ExpectedError:  nil,

			SetupMocks: func(m *mocksWrapper, tc *testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumTen, Root: blockRoot})
				m.State.
					On("GetL2BlockByHash", context.Background(), blockHash, m.DbTx).
					Return(block, nil).
					Once()

				m.State.
					On("GetStorageAt", context.Background(), addressArg, keyArg.Big(), blockRoot).
					Return(big.NewInt(123), nil).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m, &tc)
			res, err := s.JSONRPCCall("eth_getStorageAt", tc.Params...)
			require.NoError(t, err)
			if tc.ExpectedResult != nil {
				require.NotNil(t, res.Result)
				require.Nil(t, res.Error)

				var storage common.Hash
				err = json.Unmarshal(res.Result, &storage)
				require.NoError(t, err)
				assert.Equal(t, tc.ExpectedResult, storage.Bytes())
			}

			if tc.ExpectedError != nil {
				assert.Equal(t, tc.ExpectedError.ErrorCode(), res.Error.Code)
				assert.Equal(t, tc.ExpectedError.Error(), res.Error.Message)
			}
		})
	}
}

func TestGetCompilers(t *testing.T) {
	s, _, _ := newSequencerMockedServer(t)
	defer s.Stop()

	res, err := s.JSONRPCCall("eth_getCompilers")
	require.NoError(t, err)

	assert.Equal(t, float64(1), res.ID)
	assert.Equal(t, "2.0", res.JSONRPC)
	assert.Nil(t, res.Error)

	var result []interface{}
	err = json.Unmarshal(res.Result, &result)
	require.NoError(t, err)

	assert.Equal(t, 0, len(result))
}

func TestSyncing(t *testing.T) {
	s, m, c := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		ExpectedResult *ethereum.SyncProgress
		ExpectedError  types.Error
		SetupMocks     func(m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		{
			Name:           "failed to get last l2 block number",
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get last block number from state"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(uint64(0), errors.New("failed to get last l2 block number from state")).
					Once()
			},
		},
		{
			Name:           "failed to get syncing information",
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get syncing info from state"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(uint64(10), nil).
					Once()

				m.State.
					On("GetSyncingInfo", context.Background(), m.DbTx).
					Return(state.SyncingInfo{}, errors.New("failed to get syncing info from state")).
					Once()
			},
		},
		{
			Name:           "get syncing information successfully while syncing",
			ExpectedResult: &ethereum.SyncProgress{StartingBlock: 1, CurrentBlock: 2, HighestBlock: 3},
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(uint64(10), nil).
					Once()

				m.State.
					On("GetSyncingInfo", context.Background(), m.DbTx).
					Return(state.SyncingInfo{InitialSyncingBlock: 1, CurrentBlockNumber: 2, LastBlockNumberSeen: 3, LastBlockNumberConsolidated: 3}, nil).
					Once()
			},
		},
		{
			Name:           "get syncing information successfully when synced",
			ExpectedResult: nil,
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(uint64(10), nil).
					Once()

				m.State.
					On("GetSyncingInfo", context.Background(), m.DbTx).
					Return(state.SyncingInfo{InitialSyncingBlock: 1, CurrentBlockNumber: 1, LastBlockNumberSeen: 1, LastBlockNumberConsolidated: 1}, nil).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			testCase.SetupMocks(m, testCase)
			result, err := c.SyncProgress(context.Background())

			if result != nil || testCase.ExpectedResult != nil {
				assert.Equal(t, testCase.ExpectedResult.StartingBlock, result.StartingBlock)
				assert.Equal(t, testCase.ExpectedResult.CurrentBlock, result.CurrentBlock)
				assert.Equal(t, testCase.ExpectedResult.HighestBlock, result.HighestBlock)
			}

			if err != nil || testCase.ExpectedError != nil {
				if expectedErr, ok := testCase.ExpectedError.(*types.RPCError); ok {
					rpcErr := err.(rpc.Error)
					assert.Equal(t, expectedErr.ErrorCode(), rpcErr.ErrorCode())
					assert.Equal(t, expectedErr.Error(), rpcErr.Error())
				} else {
					assert.Equal(t, testCase.ExpectedError, err)
				}
			}
		})
	}
}

func TestGetTransactionL2onByBlockHashAndIndex(t *testing.T) {
	s, m, c := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name  string
		Hash  common.Hash
		Index uint

		ExpectedResult *ethTypes.Transaction
		ExpectedError  interface{}
		SetupMocks     func(m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		{
			Name:           "Get Tx Successfully",
			Hash:           common.HexToHash("0x999"),
			Index:          uint(1),
			ExpectedResult: ethTypes.NewTransaction(1, common.HexToAddress("0x111"), big.NewInt(2), 3, big.NewInt(4), []byte{5, 6, 7, 8}),
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				tx := tc.ExpectedResult
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByL2BlockHashAndIndex", context.Background(), tc.Hash, uint64(tc.Index), m.DbTx).
					Return(tx, nil).
					Once()

				receipt := ethTypes.NewReceipt([]byte{}, false, 0)
				receipt.BlockHash = common.Hash{}
				receipt.BlockNumber = big.NewInt(1)
				receipt.TransactionIndex = tc.Index

				m.State.
					On("GetTransactionReceipt", context.Background(), tx.Hash(), m.DbTx).
					Return(receipt, nil).
					Once()
			},
		},
		{
			Name:           "Tx not found",
			Hash:           common.HexToHash("0x999"),
			Index:          uint(1),
			ExpectedResult: nil,
			ExpectedError:  ethereum.NotFound,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByL2BlockHashAndIndex", context.Background(), tc.Hash, uint64(tc.Index), m.DbTx).
					Return(nil, state.ErrNotFound).
					Once()
			},
		},
		{
			Name:           "Get Tx fail to get tx from state",
			Hash:           common.HexToHash("0x999"),
			Index:          uint(1),
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get transaction"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByL2BlockHashAndIndex", context.Background(), tc.Hash, uint64(tc.Index), m.DbTx).
					Return(nil, errors.New("failed to get transaction by block and index from state")).
					Once()
			},
		},
		{
			Name:           "Tx found but receipt not found",
			Hash:           common.HexToHash("0x999"),
			Index:          uint(1),
			ExpectedResult: nil,
			ExpectedError:  ethereum.NotFound,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				tx := ethTypes.NewTransaction(0, common.Address{}, big.NewInt(0), 0, big.NewInt(0), []byte{})
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByL2BlockHashAndIndex", context.Background(), tc.Hash, uint64(tc.Index), m.DbTx).
					Return(tx, nil).
					Once()

				m.State.
					On("GetTransactionReceipt", context.Background(), tx.Hash(), m.DbTx).
					Return(nil, state.ErrNotFound).
					Once()
			},
		},
		{
			Name:           "Get Tx fail to get tx receipt from state",
			Hash:           common.HexToHash("0x999"),
			Index:          uint(1),
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get transaction receipt"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				tx := ethTypes.NewTransaction(0, common.Address{}, big.NewInt(0), 0, big.NewInt(0), []byte{})
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByL2BlockHashAndIndex", context.Background(), tc.Hash, uint64(tc.Index), m.DbTx).
					Return(tx, nil).
					Once()

				m.State.
					On("GetTransactionReceipt", context.Background(), tx.Hash(), m.DbTx).
					Return(nil, errors.New("failed to get transaction receipt from state")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m, tc)

			result, err := c.TransactionInBlock(context.Background(), tc.Hash, tc.Index)

			if result != nil || testCase.ExpectedResult != nil {
				assert.Equal(t, testCase.ExpectedResult.Hash(), result.Hash())
			}

			if err != nil || testCase.ExpectedError != nil {
				if expectedErr, ok := testCase.ExpectedError.(*types.RPCError); ok {
					rpcErr := err.(rpc.Error)
					assert.Equal(t, expectedErr.ErrorCode(), rpcErr.ErrorCode())
					assert.Equal(t, expectedErr.Error(), rpcErr.Error())
				} else {
					assert.Equal(t, testCase.ExpectedError, err)
				}
			}
		})
	}
}

func TestGetTransactionByBlockNumberAndIndex(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name        string
		BlockNumber string
		Index       uint

		ExpectedResult *ethTypes.Transaction
		ExpectedError  types.Error
		SetupMocks     func(m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		{
			Name:           "Get Tx Successfully",
			BlockNumber:    "0x1",
			Index:          uint(0),
			ExpectedResult: ethTypes.NewTransaction(1, common.HexToAddress("0x111"), big.NewInt(2), 3, big.NewInt(4), []byte{5, 6, 7, 8}),
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				tx := tc.ExpectedResult
				blockNumber, _ := encoding.DecodeUint64orHex(&tc.BlockNumber)
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByL2BlockNumberAndIndex", context.Background(), blockNumber, uint64(tc.Index), m.DbTx).
					Return(tx, nil).
					Once()

				receipt := ethTypes.NewReceipt([]byte{}, false, 0)
				receipt.BlockHash = common.Hash{}
				receipt.BlockNumber = big.NewInt(1)
				receipt.TransactionIndex = tc.Index
				m.State.
					On("GetTransactionReceipt", context.Background(), tx.Hash(), m.DbTx).
					Return(receipt, nil).
					Once()
			},
		},
		{
			Name:           "failed to identify block number",
			BlockNumber:    latest,
			Index:          uint(0),
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get the last block number from state"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(uint64(0), errors.New("failed to get last block number")).
					Once()
			},
		},
		{
			Name:           "Tx not found",
			BlockNumber:    "0x1",
			Index:          uint(0),
			ExpectedResult: nil,
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				blockNumber, _ := encoding.DecodeUint64orHex(&tc.BlockNumber)
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByL2BlockNumberAndIndex", context.Background(), blockNumber, uint64(tc.Index), m.DbTx).
					Return(nil, state.ErrNotFound).
					Once()
			},
		},
		{
			Name:           "Get Tx fail to get tx from state",
			BlockNumber:    "0x1",
			Index:          uint(0),
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get transaction"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				blockNumber, _ := encoding.DecodeUint64orHex(&tc.BlockNumber)
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByL2BlockNumberAndIndex", context.Background(), blockNumber, uint64(tc.Index), m.DbTx).
					Return(nil, errors.New("failed to get transaction by block and index from state")).
					Once()
			},
		},
		{
			Name:           "Tx found but receipt not found",
			BlockNumber:    "0x1",
			Index:          uint(0),
			ExpectedResult: nil,
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				tx := ethTypes.NewTransaction(0, common.Address{}, big.NewInt(0), 0, big.NewInt(0), []byte{})

				blockNumber, _ := encoding.DecodeUint64orHex(&tc.BlockNumber)
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByL2BlockNumberAndIndex", context.Background(), blockNumber, uint64(tc.Index), m.DbTx).
					Return(tx, nil).
					Once()

				m.State.
					On("GetTransactionReceipt", context.Background(), tx.Hash(), m.DbTx).
					Return(nil, state.ErrNotFound).
					Once()
			},
		},
		{
			Name:           "Get Tx fail to get tx receipt from state",
			BlockNumber:    "0x1",
			Index:          uint(0),
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get transaction receipt"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				tx := ethTypes.NewTransaction(0, common.Address{}, big.NewInt(0), 0, big.NewInt(0), []byte{})

				blockNumber, _ := encoding.DecodeUint64orHex(&tc.BlockNumber)
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByL2BlockNumberAndIndex", context.Background(), blockNumber, uint64(tc.Index), m.DbTx).
					Return(tx, nil).
					Once()

				m.State.
					On("GetTransactionReceipt", context.Background(), tx.Hash(), m.DbTx).
					Return(nil, errors.New("failed to get transaction receipt from state")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m, tc)

			res, err := s.JSONRPCCall("eth_getTransactionByBlockNumberAndIndex", tc.BlockNumber, tc.Index)
			require.NoError(t, err)
			assert.Equal(t, float64(1), res.ID)
			assert.Equal(t, "2.0", res.JSONRPC)

			if res.Result != nil {
				var result interface{}
				err = json.Unmarshal(res.Result, &result)
				require.NoError(t, err)

				if result != nil || testCase.ExpectedResult != nil {
					var tx ethTypes.Transaction
					err = json.Unmarshal(res.Result, &tx)
					require.NoError(t, err)
					assert.Equal(t, testCase.ExpectedResult.Hash(), tx.Hash())
				}
			}

			if res.Error != nil || testCase.ExpectedError != nil {
				assert.Equal(t, testCase.ExpectedError.ErrorCode(), res.Error.Code)
				assert.Equal(t, testCase.ExpectedError.Error(), res.Error.Message)
			}
		})
	}
}

func TestGetTransactionByHash(t *testing.T) {
	s, m, c := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name            string
		Hash            common.Hash
		ExpectedPending bool
		ExpectedResult  *ethTypes.Transaction
		ExpectedError   interface{}
		SetupMocks      func(m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		{
			Name:            "Get TX Successfully from state",
			Hash:            common.HexToHash("0x123"),
			ExpectedPending: false,
			ExpectedResult:  ethTypes.NewTransaction(1, common.Address{}, big.NewInt(1), 1, big.NewInt(1), []byte{}),
			ExpectedError:   nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByHash", context.Background(), tc.Hash, m.DbTx).
					Return(tc.ExpectedResult, nil).
					Once()

				receipt := ethTypes.NewReceipt([]byte{}, false, 0)
				receipt.BlockHash = common.Hash{}
				receipt.BlockNumber = big.NewInt(1)

				m.State.
					On("GetTransactionReceipt", context.Background(), tc.Hash, m.DbTx).
					Return(receipt, nil).
					Once()
			},
		},
		{
			Name:            "Get TX Successfully from pool",
			Hash:            common.HexToHash("0x123"),
			ExpectedPending: true,
			ExpectedResult:  ethTypes.NewTransaction(1, common.Address{}, big.NewInt(1), 1, big.NewInt(1), []byte{}),
			ExpectedError:   nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByHash", context.Background(), tc.Hash, m.DbTx).
					Return(nil, state.ErrNotFound).
					Once()

				m.Pool.
					On("GetTxByHash", context.Background(), tc.Hash).
					Return(&pool.Transaction{Transaction: *tc.ExpectedResult}, nil).
					Once()
			},
		},
		{
			Name:            "TX Not Found",
			Hash:            common.HexToHash("0x123"),
			ExpectedPending: false,
			ExpectedResult:  nil,
			ExpectedError:   ethereum.NotFound,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByHash", context.Background(), tc.Hash, m.DbTx).
					Return(nil, state.ErrNotFound).
					Once()

				m.Pool.
					On("GetTxByHash", context.Background(), tc.Hash).
					Return(nil, pool.ErrNotFound).
					Once()
			},
		},
		{
			Name:            "TX failed to load from the state",
			Hash:            common.HexToHash("0x123"),
			ExpectedPending: false,
			ExpectedResult:  nil,
			ExpectedError:   types.NewRPCError(types.DefaultErrorCode, "failed to load transaction by hash from state"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByHash", context.Background(), tc.Hash, m.DbTx).
					Return(nil, errors.New("failed to load transaction by hash from state")).
					Once()
			},
		},
		{
			Name:            "TX failed to load from the pool",
			Hash:            common.HexToHash("0x123"),
			ExpectedPending: false,
			ExpectedResult:  nil,
			ExpectedError:   types.NewRPCError(types.DefaultErrorCode, "failed to load transaction by hash from pool"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByHash", context.Background(), tc.Hash, m.DbTx).
					Return(nil, state.ErrNotFound).
					Once()

				m.Pool.
					On("GetTxByHash", context.Background(), tc.Hash).
					Return(nil, errors.New("failed to load transaction by hash from pool")).
					Once()
			},
		},
		{
			Name:            "TX receipt Not Found",
			Hash:            common.HexToHash("0x123"),
			ExpectedPending: false,
			ExpectedResult:  nil,
			ExpectedError:   types.NewRPCError(types.DefaultErrorCode, "transaction receipt not found"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				tx := &ethTypes.Transaction{}
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByHash", context.Background(), tc.Hash, m.DbTx).
					Return(tx, nil).
					Once()

				m.State.
					On("GetTransactionReceipt", context.Background(), tc.Hash, m.DbTx).
					Return(nil, state.ErrNotFound).
					Once()
			},
		},
		{
			Name:            "TX receipt failed to load",
			Hash:            common.HexToHash("0x123"),
			ExpectedPending: false,
			ExpectedResult:  nil,
			ExpectedError:   types.NewRPCError(types.DefaultErrorCode, "failed to load transaction receipt from state"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				tx := &ethTypes.Transaction{}
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByHash", context.Background(), tc.Hash, m.DbTx).
					Return(tx, nil).
					Once()

				m.State.
					On("GetTransactionReceipt", context.Background(), tc.Hash, m.DbTx).
					Return(nil, errors.New("failed to load transaction receipt from state")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m, tc)

			result, pending, err := c.TransactionByHash(context.Background(), testCase.Hash)
			assert.Equal(t, testCase.ExpectedPending, pending)

			if result != nil || testCase.ExpectedResult != nil {
				assert.Equal(t, testCase.ExpectedResult.Hash(), result.Hash())
			}

			if err != nil || testCase.ExpectedError != nil {
				if expectedErr, ok := testCase.ExpectedError.(*types.RPCError); ok {
					rpcErr := err.(rpc.Error)
					assert.Equal(t, expectedErr.ErrorCode(), rpcErr.ErrorCode())
					assert.Equal(t, expectedErr.Error(), rpcErr.Error())
				} else {
					assert.Equal(t, testCase.ExpectedError, err)
				}
			}
		})
	}
}

func TestGetBlockTransactionCountByHash(t *testing.T) {
	s, m, c := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		BlockHash      common.Hash
		ExpectedResult uint
		ExpectedError  interface{}
		SetupMocks     func(m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		{
			Name:           "Count txs successfully",
			BlockHash:      blockHash,
			ExpectedResult: uint(10),
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetL2BlockTransactionCountByHash", context.Background(), tc.BlockHash, m.DbTx).
					Return(uint64(10), nil).
					Once()
			},
		},
		{
			Name:           "Failed to count txs by hash",
			BlockHash:      blockHash,
			ExpectedResult: 0,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to count transactions"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetL2BlockTransactionCountByHash", context.Background(), tc.BlockHash, m.DbTx).
					Return(uint64(0), errors.New("failed to count txs")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m, tc)
			result, err := c.TransactionCount(context.Background(), tc.BlockHash)

			assert.Equal(t, testCase.ExpectedResult, result)

			if err != nil || testCase.ExpectedError != nil {
				if expectedErr, ok := testCase.ExpectedError.(*types.RPCError); ok {
					rpcErr := err.(rpc.Error)
					assert.Equal(t, expectedErr.ErrorCode(), rpcErr.ErrorCode())
					assert.Equal(t, expectedErr.Error(), rpcErr.Error())
				} else {
					assert.Equal(t, testCase.ExpectedError, err)
				}
			}
		})
	}
}

func TestGetBlockTransactionCountByNumber(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		BlockNumber    string
		ExpectedResult uint
		ExpectedError  types.Error
		SetupMocks     func(m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		{
			Name:           "Count txs successfully for latest block",
			BlockNumber:    latest,
			ExpectedResult: uint(10),
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				blockNumber := uint64(10)
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(blockNumber, nil).
					Once()

				m.State.
					On("GetL2BlockTransactionCountByNumber", context.Background(), blockNumber, m.DbTx).
					Return(uint64(10), nil).
					Once()
			},
		},
		{
			Name:           "Count txs successfully for pending block",
			BlockNumber:    "pending",
			ExpectedResult: uint(10),
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.Pool.
					On("CountPendingTransactions", context.Background()).
					Return(uint64(10), nil).
					Once()
			},
		},
		{
			Name:           "failed to get last block number",
			BlockNumber:    latest,
			ExpectedResult: 0,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get the last block number from state"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(uint64(0), errors.New("failed to get last block number")).
					Once()
			},
		},
		{
			Name:           "failed to count tx",
			BlockNumber:    latest,
			ExpectedResult: 0,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to count transactions"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				blockNumber := uint64(10)
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(blockNumber, nil).
					Once()

				m.State.
					On("GetL2BlockTransactionCountByNumber", context.Background(), blockNumber, m.DbTx).
					Return(uint64(0), errors.New("failed to count")).
					Once()
			},
		},
		{
			Name:           "failed to count pending tx",
			BlockNumber:    "pending",
			ExpectedResult: 0,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to count pending transactions"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.Pool.
					On("CountPendingTransactions", context.Background()).
					Return(uint64(0), errors.New("failed to count")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m, tc)
			res, err := s.JSONRPCCall("eth_getBlockTransactionCountByNumber", tc.BlockNumber)

			require.NoError(t, err)
			assert.Equal(t, float64(1), res.ID)
			assert.Equal(t, "2.0", res.JSONRPC)

			if res.Result != nil {
				var result types.ArgUint64
				err = json.Unmarshal(res.Result, &result)
				require.NoError(t, err)
				assert.Equal(t, testCase.ExpectedResult, uint(result))
			}

			if res.Error != nil || testCase.ExpectedError != nil {
				assert.Equal(t, testCase.ExpectedError.ErrorCode(), res.Error.Code)
				assert.Equal(t, testCase.ExpectedError.Error(), res.Error.Message)
			}
		})
	}
}

func TestGetTransactionCount(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		Params         []interface{}
		ExpectedResult uint
		ExpectedError  types.Error
		SetupMocks     func(m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		{
			Name:           "Count txs successfully",
			Params:         []interface{}{addressArg.String()},
			ExpectedResult: uint(10),
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumTen, Root: blockRoot})
				m.State.On("GetLastL2Block", context.Background(), m.DbTx).Return(block, nil).Once()

				m.State.
					On("GetNonce", context.Background(), addressArg, blockRoot).
					Return(uint64(10), nil).
					Once()
			},
		},
		{
			Name: "Count txs successfully by block hash with EIP-1898",
			Params: []interface{}{
				addressArg.String(),
				map[string]interface{}{types.BlockHashKey: blockHash.String()},
			},
			ExpectedResult: uint(10),
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumTen, Root: blockRoot})
				m.State.
					On("GetL2BlockByHash", context.Background(), blockHash, m.DbTx).
					Return(block, nil).
					Once()

				m.State.
					On("GetNonce", context.Background(), addressArg, blockRoot).
					Return(uint64(10), nil).
					Once()
			},
		},
		{
			Name: "Count txs nonce not found",
			Params: []interface{}{
				addressArg.String(),
				latest,
			},
			ExpectedResult: 0,
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(blockNumTen.Uint64(), nil).
					Once()

				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumTen, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumTenUint64, m.DbTx).Return(block, nil).Once()

				m.State.
					On("GetNonce", context.Background(), addressArg, blockRoot).
					Return(uint64(0), state.ErrNotFound).
					Once()
			},
		},
		{
			Name: "failed to get last block number",
			Params: []interface{}{
				addressArg.String(),
				latest,
			},
			ExpectedResult: 0,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get the last block number from state"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(uint64(0), errors.New("failed to get last block number")).
					Once()
			},
		},
		{
			Name: "failed to get nonce",
			Params: []interface{}{
				addressArg.String(),
				latest,
			},
			ExpectedResult: 0,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to count transactions"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(blockNumTen.Uint64(), nil).
					Once()

				block := ethTypes.NewBlockWithHeader(&ethTypes.Header{Number: blockNumTen, Root: blockRoot})
				m.State.On("GetL2BlockByNumber", context.Background(), blockNumTenUint64, m.DbTx).Return(block, nil).Once()

				m.State.
					On("GetNonce", context.Background(), addressArg, blockRoot).
					Return(uint64(0), errors.New("failed to get nonce")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m, tc)
			res, err := s.JSONRPCCall("eth_getTransactionCount", tc.Params...)

			require.NoError(t, err)
			assert.Equal(t, float64(1), res.ID)
			assert.Equal(t, "2.0", res.JSONRPC)

			if res.Result != nil {
				var result types.ArgUint64
				err = json.Unmarshal(res.Result, &result)
				require.NoError(t, err)
				assert.Equal(t, testCase.ExpectedResult, uint(result))
			}

			if res.Error != nil || testCase.ExpectedError != nil {
				assert.Equal(t, testCase.ExpectedError.ErrorCode(), res.Error.Code)
				assert.Equal(t, testCase.ExpectedError.Error(), res.Error.Message)
			}
		})
	}
}

func TestGetTransactionReceipt(t *testing.T) {
	s, m, c := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		Hash           common.Hash
		ExpectedResult *ethTypes.Receipt
		ExpectedError  interface{}
		SetupMocks     func(m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		{
			Name:           "Get TX receipt Successfully",
			Hash:           common.HexToHash("0x123"),
			ExpectedResult: ethTypes.NewReceipt([]byte{}, false, 0),
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				tx := ethTypes.NewTransaction(1, common.Address{}, big.NewInt(1), 1, big.NewInt(1), []byte{})
				privateKey, err := crypto.HexToECDSA(strings.TrimPrefix("0x28b2b0318721be8c8339199172cd7cc8f5e273800a35616ec893083a4b32c02e", "0x"))
				require.NoError(t, err)
				auth, err := bind.NewKeyedTransactorWithChainID(privateKey, big.NewInt(1))
				require.NoError(t, err)

				signedTx, err := auth.Signer(auth.From, tx)
				require.NoError(t, err)

				m.State.
					On("GetTransactionByHash", context.Background(), tc.Hash, m.DbTx).
					Return(signedTx, nil).
					Once()

				m.State.
					On("GetTransactionReceipt", context.Background(), tc.Hash, m.DbTx).
					Return(tc.ExpectedResult, nil).
					Once()
			},
		},
		{
			Name:           "Get TX receipt but tx not found",
			Hash:           common.HexToHash("0x123"),
			ExpectedResult: nil,
			ExpectedError:  ethereum.NotFound,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByHash", context.Background(), tc.Hash, m.DbTx).
					Return(nil, state.ErrNotFound).
					Once()
			},
		},
		{
			Name:           "Get TX receipt but failed to get tx",
			Hash:           common.HexToHash("0x123"),
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get tx from state"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetTransactionByHash", context.Background(), tc.Hash, m.DbTx).
					Return(nil, errors.New("failed to get tx")).
					Once()
			},
		},
		{
			Name:           "TX receipt Not Found",
			Hash:           common.HexToHash("0x123"),
			ExpectedResult: nil,
			ExpectedError:  ethereum.NotFound,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				tx := ethTypes.NewTransaction(1, common.Address{}, big.NewInt(1), 1, big.NewInt(1), []byte{})
				privateKey, err := crypto.HexToECDSA(strings.TrimPrefix("0x28b2b0318721be8c8339199172cd7cc8f5e273800a35616ec893083a4b32c02e", "0x"))
				require.NoError(t, err)
				auth, err := bind.NewKeyedTransactorWithChainID(privateKey, big.NewInt(1))
				require.NoError(t, err)

				signedTx, err := auth.Signer(auth.From, tx)
				require.NoError(t, err)

				m.State.
					On("GetTransactionByHash", context.Background(), tc.Hash, m.DbTx).
					Return(signedTx, nil).
					Once()

				m.State.
					On("GetTransactionReceipt", context.Background(), tc.Hash, m.DbTx).
					Return(nil, state.ErrNotFound).
					Once()
			},
		},
		{
			Name:           "TX receipt failed to load",
			Hash:           common.HexToHash("0x123"),
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to get tx receipt from state"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				tx := ethTypes.NewTransaction(1, common.Address{}, big.NewInt(1), 1, big.NewInt(1), []byte{})
				privateKey, err := crypto.HexToECDSA(strings.TrimPrefix("0x28b2b0318721be8c8339199172cd7cc8f5e273800a35616ec893083a4b32c02e", "0x"))
				require.NoError(t, err)
				auth, err := bind.NewKeyedTransactorWithChainID(privateKey, big.NewInt(1))
				require.NoError(t, err)

				signedTx, err := auth.Signer(auth.From, tx)
				require.NoError(t, err)

				m.State.
					On("GetTransactionByHash", context.Background(), tc.Hash, m.DbTx).
					Return(signedTx, nil).
					Once()

				m.State.
					On("GetTransactionReceipt", context.Background(), tc.Hash, m.DbTx).
					Return(nil, errors.New("failed to get tx receipt from state")).
					Once()
			},
		},
		{
			Name:           "Get TX but failed to build response Successfully",
			Hash:           common.HexToHash("0x123"),
			ExpectedResult: nil,
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to build the receipt response"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				tx := ethTypes.NewTransaction(1, common.Address{}, big.NewInt(1), 1, big.NewInt(1), []byte{})

				m.State.
					On("GetTransactionByHash", context.Background(), tc.Hash, m.DbTx).
					Return(tx, nil).
					Once()

				m.State.
					On("GetTransactionReceipt", context.Background(), tc.Hash, m.DbTx).
					Return(ethTypes.NewReceipt([]byte{}, false, 0), nil).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m, tc)

			result, err := c.TransactionReceipt(context.Background(), testCase.Hash)

			if result != nil || testCase.ExpectedResult != nil {
				assert.Equal(t, testCase.ExpectedResult.TxHash, result.TxHash)
			}

			if err != nil || testCase.ExpectedError != nil {
				if expectedErr, ok := testCase.ExpectedError.(*types.RPCError); ok {
					rpcErr := err.(rpc.Error)
					assert.Equal(t, expectedErr.ErrorCode(), rpcErr.ErrorCode())
					assert.Equal(t, expectedErr.Error(), rpcErr.Error())
				} else {
					assert.Equal(t, testCase.ExpectedError, err)
				}
			}
		})
	}
}

func TestSendRawTransactionViaGeth(t *testing.T) {
	s, m, c := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name          string
		Tx            *ethTypes.Transaction
		ExpectedError interface{}
		SetupMocks    func(t *testing.T, m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		{
			Name:          "Send TX successfully",
			Tx:            ethTypes.NewTransaction(1, common.HexToAddress("0x1"), big.NewInt(1), uint64(1), big.NewInt(1), []byte{}),
			ExpectedError: nil,
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				txMatchByHash := mock.MatchedBy(func(tx ethTypes.Transaction) bool {
					h1 := tx.Hash().Hex()
					h2 := tc.Tx.Hash().Hex()
					return h1 == h2
				})

				m.Pool.
					On("AddTx", context.Background(), txMatchByHash, "").
					Return(nil).
					Once()
			},
		},
		{
			Name:          "Send TX failed to add to the pool",
			Tx:            ethTypes.NewTransaction(1, common.HexToAddress("0x1"), big.NewInt(1), uint64(1), big.NewInt(1), []byte{}),
			ExpectedError: types.NewRPCError(types.DefaultErrorCode, "failed to add TX to the pool"),
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				txMatchByHash := mock.MatchedBy(func(tx ethTypes.Transaction) bool {
					h1 := tx.Hash().Hex()
					h2 := tc.Tx.Hash().Hex()
					return h1 == h2
				})

				m.Pool.
					On("AddTx", context.Background(), txMatchByHash, "").
					Return(errors.New("failed to add TX to the pool")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(t, m, tc)

			err := c.SendTransaction(context.Background(), tc.Tx)

			if err != nil || testCase.ExpectedError != nil {
				if expectedErr, ok := testCase.ExpectedError.(*types.RPCError); ok {
					rpcErr := err.(rpc.Error)
					assert.Equal(t, expectedErr.ErrorCode(), rpcErr.ErrorCode())
					assert.Equal(t, expectedErr.Error(), rpcErr.Error())
				} else {
					assert.Equal(t, testCase.ExpectedError, err)
				}
			}
		})
	}
}

func TestSendRawTransactionJSONRPCCall(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		Input          string
		ExpectedResult *common.Hash
		ExpectedError  types.Error
		Prepare        func(t *testing.T, tc *testCase)
		SetupMocks     func(t *testing.T, m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		{
			Name: "Send TX successfully",
			Prepare: func(t *testing.T, tc *testCase) {
				tx := ethTypes.NewTransaction(1, common.HexToAddress("0x1"), big.NewInt(1), uint64(1), big.NewInt(1), []byte{})

				txBinary, err := tx.MarshalBinary()
				require.NoError(t, err)

				rawTx := hex.EncodeToHex(txBinary)
				require.NoError(t, err)

				tc.Input = rawTx
				tc.ExpectedResult = state.HashPtr(tx.Hash())
				tc.ExpectedError = nil
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				m.Pool.
					On("AddTx", context.Background(), mock.IsType(ethTypes.Transaction{}), "").
					Return(nil).
					Once()
			},
		},
		{
			Name: "Send TX failed to add to the pool",
			Prepare: func(t *testing.T, tc *testCase) {
				tx := ethTypes.NewTransaction(1, common.HexToAddress("0x1"), big.NewInt(1), uint64(1), big.NewInt(1), []byte{})

				txBinary, err := tx.MarshalBinary()
				require.NoError(t, err)

				rawTx := hex.EncodeToHex(txBinary)
				require.NoError(t, err)

				tc.Input = rawTx
				tc.ExpectedResult = nil
				tc.ExpectedError = types.NewRPCError(types.DefaultErrorCode, "failed to add TX to the pool")
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				m.Pool.
					On("AddTx", context.Background(), mock.IsType(ethTypes.Transaction{}), "").
					Return(errors.New("failed to add TX to the pool")).
					Once()
			},
		},
		{
			Name: "Send invalid tx input",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.Input = "0x1234"
				tc.ExpectedResult = nil
				tc.ExpectedError = types.NewRPCError(types.InvalidParamsErrorCode, "invalid tx input")
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.Prepare(t, &tc)
			tc.SetupMocks(t, m, tc)

			res, err := s.JSONRPCCall("eth_sendRawTransaction", tc.Input)
			require.NoError(t, err)

			assert.Equal(t, float64(1), res.ID)
			assert.Equal(t, "2.0", res.JSONRPC)

			if res.Result != nil || tc.ExpectedResult != nil {
				var result common.Hash
				err = json.Unmarshal(res.Result, &result)
				require.NoError(t, err)
				assert.Equal(t, *tc.ExpectedResult, result)
			}
			if res.Error != nil || tc.ExpectedError != nil {
				assert.Equal(t, tc.ExpectedError.ErrorCode(), res.Error.Code)
				assert.Equal(t, tc.ExpectedError.Error(), res.Error.Message)
			}
		})
	}
}

func TestSendRawTransactionViaGethForNonSequencerNode(t *testing.T) {
	sequencerServer, sequencerMocks, _ := newSequencerMockedServer(t)
	defer sequencerServer.Stop()
	nonSequencerServer, _, nonSequencerClient := newNonSequencerMockedServer(t, sequencerServer.ServerURL)
	defer nonSequencerServer.Stop()

	type testCase struct {
		Name          string
		Tx            *ethTypes.Transaction
		ExpectedError interface{}
		SetupMocks    func(t *testing.T, m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		{
			Name:          "Send TX successfully",
			Tx:            ethTypes.NewTransaction(1, common.HexToAddress("0x1"), big.NewInt(1), uint64(1), big.NewInt(1), []byte{}),
			ExpectedError: nil,
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				txMatchByHash := mock.MatchedBy(func(tx ethTypes.Transaction) bool {
					h1 := tx.Hash().Hex()
					h2 := tc.Tx.Hash().Hex()
					return h1 == h2
				})

				m.Pool.
					On("AddTx", context.Background(), txMatchByHash, "").
					Return(nil).
					Once()
			},
		},
		{
			Name:          "Send TX failed to add to the pool",
			Tx:            ethTypes.NewTransaction(1, common.HexToAddress("0x1"), big.NewInt(1), uint64(1), big.NewInt(1), []byte{}),
			ExpectedError: types.NewRPCError(types.DefaultErrorCode, "failed to add TX to the pool"),
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				txMatchByHash := mock.MatchedBy(func(tx ethTypes.Transaction) bool {
					h1 := tx.Hash().Hex()
					h2 := tc.Tx.Hash().Hex()
					return h1 == h2
				})

				m.Pool.
					On("AddTx", context.Background(), txMatchByHash, "").
					Return(errors.New("failed to add TX to the pool")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(t, sequencerMocks, tc)

			err := nonSequencerClient.SendTransaction(context.Background(), tc.Tx)

			if err != nil || testCase.ExpectedError != nil {
				if expectedErr, ok := testCase.ExpectedError.(*types.RPCError); ok {
					rpcErr := err.(rpc.Error)
					assert.Equal(t, expectedErr.ErrorCode(), rpcErr.ErrorCode())
					assert.Equal(t, expectedErr.Error(), rpcErr.Error())
				} else {
					assert.Equal(t, testCase.ExpectedError, err)
				}
			}
		})
	}
}

func TestSendRawTransactionViaGethForNonSequencerNodeFailsToRelayTxToSequencerNode(t *testing.T) {
	nonSequencerServer, _, nonSequencerClient := newNonSequencerMockedServer(t, "http://wrong.url")
	defer nonSequencerServer.Stop()

	type testCase struct {
		Name          string
		Tx            *ethTypes.Transaction
		ExpectedError interface{}
	}

	testCases := []testCase{
		{
			Name:          "Send TX failed to relay tx to the sequencer node",
			Tx:            ethTypes.NewTransaction(1, common.HexToAddress("0x1"), big.NewInt(1), uint64(1), big.NewInt(1), []byte{}),
			ExpectedError: types.NewRPCError(types.DefaultErrorCode, "failed to relay tx to the sequencer node"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase

			err := nonSequencerClient.SendTransaction(context.Background(), tc.Tx)

			if err != nil || testCase.ExpectedError != nil {
				if expectedErr, ok := testCase.ExpectedError.(*types.RPCError); ok {
					rpcErr := err.(rpc.Error)
					assert.Equal(t, expectedErr.ErrorCode(), rpcErr.ErrorCode())
					assert.Equal(t, expectedErr.Error(), rpcErr.Error())
				} else {
					assert.Equal(t, testCase.ExpectedError, err)
				}
			}
		})
	}
}

func TestProtocolVersion(t *testing.T) {
	s, _, _ := newSequencerMockedServer(t)
	defer s.Stop()

	res, err := s.JSONRPCCall("eth_protocolVersion")
	require.NoError(t, err)

	assert.Equal(t, float64(1), res.ID)
	assert.Equal(t, "2.0", res.JSONRPC)
	assert.Nil(t, res.Error)

	var result string
	err = json.Unmarshal(res.Result, &result)
	require.NoError(t, err)

	assert.Equal(t, "0x0", result)
}

func TestNewFilter(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		Request        types.LogFilterRequest
		ExpectedResult string
		ExpectedError  types.Error
		SetupMocks     func(m *mocksWrapper, tc testCase)
	}

	hash := common.HexToHash("0x42")
	blockNumber := "8"
	testCases := []testCase{
		{
			Name: "New filter created successfully",
			Request: types.LogFilterRequest{
				ToBlock: &blockNumber,
			},
			ExpectedResult: "1",
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.Storage.
					On("NewLogFilter", mock.IsType(&websocket.Conn{}), mock.IsType(LogFilter{})).
					Return("1", nil).
					Once()
			},
		},
		{
			Name: "failed to create new filter",
			Request: types.LogFilterRequest{
				BlockHash: &hash,
			},
			ExpectedResult: "",
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to create new log filter"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.Storage.
					On("NewLogFilter", mock.IsType(&websocket.Conn{}), mock.IsType(LogFilter{})).
					Return("", errors.New("failed to add new filter")).
					Once()
			},
		},
		{
			Name: "failed to create new filter because BlockHash and ToBlock are present",
			Request: types.LogFilterRequest{
				BlockHash: &hash,
				ToBlock:   &blockNumber,
			},
			ExpectedResult: "",
			ExpectedError:  types.NewRPCError(types.InvalidParamsErrorCode, "invalid argument 0: cannot specify both BlockHash and FromBlock/ToBlock, choose one or the other"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.Storage.
					On("NewLogFilter", mock.IsType(&websocket.Conn{}), mock.IsType(LogFilter{})).
					Once().
					Return("", ErrFilterInvalidPayload).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m, tc)

			res, err := s.JSONRPCCall("eth_newFilter", tc.Request)
			require.NoError(t, err)

			assert.Equal(t, float64(1), res.ID)
			assert.Equal(t, "2.0", res.JSONRPC)

			if res.Result != nil {
				var result string
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

func TestNewBlockFilter(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		ExpectedResult string
		ExpectedError  types.Error
		SetupMocks     func(m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		{
			Name:           "New block filter created successfully",
			ExpectedResult: "1",
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.Storage.
					On("NewBlockFilter", mock.IsType(&websocket.Conn{})).
					Return("1", nil).
					Once()
			},
		},
		{
			Name:           "failed to create new block filter",
			ExpectedResult: "",
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to create new block filter"),
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.Storage.
					On("NewBlockFilter", mock.IsType(&websocket.Conn{})).
					Return("", errors.New("failed to add new block filter")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m, tc)

			res, err := s.JSONRPCCall("eth_newBlockFilter")
			require.NoError(t, err)

			assert.Equal(t, float64(1), res.ID)
			assert.Equal(t, "2.0", res.JSONRPC)

			if res.Result != nil {
				var result string
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

func TestNewPendingTransactionFilter(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		ExpectedResult string
		ExpectedError  types.Error
		SetupMocks     func(m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		// {
		// 	Name:           "New pending transaction filter created successfully",
		// 	ExpectedResult: "1",
		// 	ExpectedError:  nil,
		// 	SetupMocks: func(m *mocks, tc testCase) {
		// 		m.Storage.
		// 			On("NewPendingTransactionFilter", mock.IsType(&websocket.Conn{})).
		// 			Return("1", nil).
		// 			Once()
		// 	},
		// },
		// {
		// 	Name:           "failed to create new pending transaction filter",
		// 	ExpectedResult: "",
		// 	ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "failed to create new pending transaction filter"),
		// 	SetupMocks: func(m *mocks, tc testCase) {
		// 		m.Storage.
		// 			On("NewPendingTransactionFilter", mock.IsType(&websocket.Conn{})).
		// 			Return("", errors.New("failed to add new pending transaction filter")).
		// 			Once()
		// 	},
		// },
		{
			Name:           "can't create pending tx filter",
			ExpectedResult: "",
			ExpectedError:  types.NewRPCError(types.DefaultErrorCode, "not supported yet"),
			SetupMocks:     func(m *mocksWrapper, tc testCase) {},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m, tc)

			res, err := s.JSONRPCCall("eth_newPendingTransactionFilter")
			require.NoError(t, err)

			assert.Equal(t, float64(1), res.ID)
			assert.Equal(t, "2.0", res.JSONRPC)

			if res.Result != nil {
				var result string
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

func TestUninstallFilter(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		FilterID       string
		ExpectedResult bool
		ExpectedError  types.Error
		SetupMocks     func(m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		{
			Name:           "Uninstalls filter successfully",
			FilterID:       "1",
			ExpectedResult: true,
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.Storage.
					On("UninstallFilter", tc.FilterID).
					Return(nil).
					Once()
			},
		},
		{
			Name:           "filter already uninstalled",
			FilterID:       "1",
			ExpectedResult: false,
			ExpectedError:  nil,
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.Storage.
					On("UninstallFilter", tc.FilterID).
					Return(ErrNotFound).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.SetupMocks(m, tc)

			res, err := s.JSONRPCCall("eth_uninstallFilter", tc.FilterID)
			require.NoError(t, err)

			assert.Equal(t, float64(1), res.ID)
			assert.Equal(t, "2.0", res.JSONRPC)

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

func TestGetLogs(t *testing.T) {
	s, m, c := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		Filter         ethereum.FilterQuery
		ExpectedResult []ethTypes.Log
		ExpectedError  interface{}
		Prepare        func(t *testing.T, tc *testCase)
		SetupMocks     func(m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		{
			Name: "Get logs successfully",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.Filter = ethereum.FilterQuery{
					FromBlock: big.NewInt(1), ToBlock: big.NewInt(2),
					Addresses: []common.Address{common.HexToAddress("0x111")},
					Topics:    [][]common.Hash{{common.HexToHash("0x222")}},
				}
				tc.ExpectedResult = []ethTypes.Log{{
					Address: common.Address{}, Topics: []common.Hash{}, Data: []byte{},
					BlockNumber: uint64(1), TxHash: common.Hash{}, TxIndex: uint(1),
					BlockHash: common.Hash{}, Index: uint(1), Removed: false,
				}}
				tc.ExpectedError = nil
			},
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				var since *time.Time
				logs := make([]*ethTypes.Log, 0, len(tc.ExpectedResult))
				for _, log := range tc.ExpectedResult {
					l := log
					logs = append(logs, &l)
				}

				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLogs", context.Background(), tc.Filter.FromBlock.Uint64(), tc.Filter.ToBlock.Uint64(), tc.Filter.Addresses, tc.Filter.Topics, tc.Filter.BlockHash, since, m.DbTx).
					Return(logs, nil).
					Once()
			},
		},
		{
			Name: "Get logs fails to get logs from state",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.Filter = ethereum.FilterQuery{
					FromBlock: big.NewInt(1), ToBlock: big.NewInt(2),
					Addresses: []common.Address{common.HexToAddress("0x111")},
					Topics:    [][]common.Hash{{common.HexToHash("0x222")}},
				}
				tc.ExpectedResult = nil
				tc.ExpectedError = types.NewRPCError(types.DefaultErrorCode, "failed to get logs from state")
			},
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				var since *time.Time
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLogs", context.Background(), tc.Filter.FromBlock.Uint64(), tc.Filter.ToBlock.Uint64(), tc.Filter.Addresses, tc.Filter.Topics, tc.Filter.BlockHash, since, m.DbTx).
					Return(nil, errors.New("failed to get logs from state")).
					Once()
			},
		},
		{
			Name: "Get logs fails to identify from block",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.Filter = ethereum.FilterQuery{
					FromBlock: big.NewInt(-1), ToBlock: big.NewInt(2),
					Addresses: []common.Address{common.HexToAddress("0x111")},
					Topics:    [][]common.Hash{{common.HexToHash("0x222")}},
				}
				tc.ExpectedResult = nil
				tc.ExpectedError = types.NewRPCError(types.DefaultErrorCode, "failed to get the last block number from state")
			},
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(uint64(0), errors.New("failed to get last block number from state")).
					Once()
			},
		},
		{
			Name: "Get logs fails to identify to block",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.Filter = ethereum.FilterQuery{
					FromBlock: big.NewInt(1), ToBlock: big.NewInt(-1),
					Addresses: []common.Address{common.HexToAddress("0x111")},
					Topics:    [][]common.Hash{{common.HexToHash("0x222")}},
				}
				tc.ExpectedResult = nil
				tc.ExpectedError = types.NewRPCError(types.DefaultErrorCode, "failed to get the last block number from state")
			},
			SetupMocks: func(m *mocksWrapper, tc testCase) {
				m.DbTx.
					On("Rollback", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.State.
					On("GetLastL2BlockNumber", context.Background(), m.DbTx).
					Return(uint64(0), errors.New("failed to get last block number from state")).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.Prepare(t, &tc)
			tc.SetupMocks(m, tc)

			result, err := c.FilterLogs(context.Background(), tc.Filter)

			if result != nil || tc.ExpectedResult != nil {
				assert.ElementsMatch(t, tc.ExpectedResult, result)
			}

			if err != nil || tc.ExpectedError != nil {
				if expectedErr, ok := tc.ExpectedError.(*types.RPCError); ok {
					rpcErr := err.(rpc.Error)
					assert.Equal(t, expectedErr.ErrorCode(), rpcErr.ErrorCode())
					assert.Equal(t, expectedErr.Error(), rpcErr.Error())
				} else {
					assert.Equal(t, tc.ExpectedError, err)
				}
			}
		})
	}
}

func TestGetFilterLogs(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name           string
		FilterID       string
		ExpectedResult []ethTypes.Log
		ExpectedError  types.Error
		Prepare        func(t *testing.T, tc *testCase)
		SetupMocks     func(t *testing.T, m *mocksWrapper, tc testCase)
	}

	testCases := []testCase{
		{
			Name: "Get filter logs successfully",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "1"
				tc.ExpectedResult = []ethTypes.Log{{
					Address: common.Address{}, Topics: []common.Hash{}, Data: []byte{},
					BlockNumber: uint64(1), TxHash: common.Hash{}, TxIndex: uint(1),
					BlockHash: common.Hash{}, Index: uint(1), Removed: false,
				}}
				tc.ExpectedError = nil
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				var since *time.Time
				logs := make([]*ethTypes.Log, 0, len(tc.ExpectedResult))
				for _, log := range tc.ExpectedResult {
					l := log
					logs = append(logs, &l)
				}

				bn1 := types.BlockNumber(1)
				bn2 := types.BlockNumber(2)
				logFilter := LogFilter{
					FromBlock: &bn1,
					ToBlock:   &bn2,
					Addresses: []common.Address{common.HexToAddress("0x111")},
					Topics:    [][]common.Hash{{common.HexToHash("0x222")}},
				}

				filter := &Filter{
					ID:         tc.FilterID,
					Type:       FilterTypeLog,
					LastPoll:   time.Now(),
					Parameters: logFilter,
				}

				m.DbTx.
					On("Commit", context.Background()).
					Return(nil).
					Once()

				m.State.
					On("BeginStateTransaction", context.Background()).
					Return(m.DbTx, nil).
					Once()

				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(filter, nil).
					Once()

				m.State.
					On("GetLogs", context.Background(), uint64(*logFilter.FromBlock), uint64(*logFilter.ToBlock), logFilter.Addresses, logFilter.Topics, logFilter.BlockHash, since, m.DbTx).
					Return(logs, nil).
					Once()
			},
		},
		{
			Name: "Get filter logs filter not found",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "1"
				tc.ExpectedResult = nil
				tc.ExpectedError = nil
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(nil, ErrNotFound).
					Once()
			},
		},
		{
			Name: "Get filter logs failed to get filter",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "1"
				tc.ExpectedResult = nil
				tc.ExpectedError = types.NewRPCError(types.DefaultErrorCode, "failed to get filter from storage")
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(nil, errors.New("failed to get filter")).
					Once()
			},
		},
		{
			Name: "Get filter logs is a valid filter but its not a log filter",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "1"
				tc.ExpectedResult = nil
				tc.ExpectedError = nil
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				filter := &Filter{
					ID:         tc.FilterID,
					Type:       FilterTypeBlock,
					LastPoll:   time.Now(),
					Parameters: "",
				}

				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(filter, nil).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.Prepare(t, &tc)
			tc.SetupMocks(t, m, tc)

			res, err := s.JSONRPCCall("eth_getFilterLogs", tc.FilterID)
			require.NoError(t, err)
			assert.Equal(t, float64(1), res.ID)
			assert.Equal(t, "2.0", res.JSONRPC)

			if res.Result != nil {
				var result interface{}
				err = json.Unmarshal(res.Result, &result)
				require.NoError(t, err)

				if result != nil || tc.ExpectedResult != nil {
					var logs []ethTypes.Log
					err = json.Unmarshal(res.Result, &logs)
					require.NoError(t, err)
					assert.ElementsMatch(t, tc.ExpectedResult, logs)
				}
			}

			if res.Error != nil || tc.ExpectedError != nil {
				assert.Equal(t, tc.ExpectedError.ErrorCode(), res.Error.Code)
				assert.Equal(t, tc.ExpectedError.Error(), res.Error.Message)
			}
		})
	}
}

func TestGetFilterChanges(t *testing.T) {
	s, m, _ := newSequencerMockedServer(t)
	defer s.Stop()

	type testCase struct {
		Name            string
		FilterID        string
		ExpectedResults []interface{}
		ExpectedErrors  []types.Error
		Prepare         func(t *testing.T, tc *testCase)
		SetupMocks      func(t *testing.T, m *mocksWrapper, tc testCase)
	}

	var nilTx pgx.Tx
	testCases := []testCase{
		{
			Name: "Get block filter changes multiple times successfully",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "2"
				// first call
				tc.ExpectedResults = append(tc.ExpectedResults, []common.Hash{
					common.HexToHash("0x111"),
				})
				tc.ExpectedErrors = append(tc.ExpectedErrors, nil)

				// second call
				tc.ExpectedResults = append(tc.ExpectedResults, []common.Hash{
					common.HexToHash("0x222"),
					common.HexToHash("0x333"),
				})
				tc.ExpectedErrors = append(tc.ExpectedErrors, nil)

				// third call
				tc.ExpectedResults = append(tc.ExpectedResults, []common.Hash{})
				tc.ExpectedErrors = append(tc.ExpectedErrors, nil)
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				filter := &Filter{
					ID:         tc.FilterID,
					Type:       FilterTypeBlock,
					LastPoll:   time.Now(),
					Parameters: "{}",
				}

				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(filter, nil).
					Once()

				m.State.
					On("GetL2BlockHashesSince", context.Background(), filter.LastPoll, mock.IsType(nilTx)).
					Return(tc.ExpectedResults[0].([]common.Hash), nil).
					Once()

				m.Storage.
					On("UpdateFilterLastPoll", tc.FilterID).
					Run(func(args mock.Arguments) {
						filter.LastPoll = time.Now()

						m.Storage.
							On("GetFilter", tc.FilterID).
							Return(filter, nil).
							Once()

						m.State.
							On("GetL2BlockHashesSince", context.Background(), filter.LastPoll, mock.IsType(nilTx)).
							Return(tc.ExpectedResults[1].([]common.Hash), nil).
							Once()

						m.Storage.
							On("UpdateFilterLastPoll", tc.FilterID).
							Run(func(args mock.Arguments) {
								filter.LastPoll = time.Now()

								m.Storage.
									On("GetFilter", tc.FilterID).
									Return(filter, nil).
									Once()

								m.State.
									On("GetL2BlockHashesSince", context.Background(), filter.LastPoll, mock.IsType(nilTx)).
									Return(tc.ExpectedResults[2].([]common.Hash), nil).
									Once()

								m.Storage.
									On("UpdateFilterLastPoll", tc.FilterID).
									Return(nil).
									Once()
							}).
							Return(nil).
							Once()
					}).
					Return(nil).
					Once()
			},
		},
		{
			Name: "Get pending transactions filter changes multiple times successfully",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "3"
				// first call
				tc.ExpectedResults = append(tc.ExpectedResults, []common.Hash{
					common.HexToHash("0x444"),
				})
				tc.ExpectedErrors = append(tc.ExpectedErrors, nil)

				// second call
				tc.ExpectedResults = append(tc.ExpectedResults, []common.Hash{
					common.HexToHash("0x555"),
					common.HexToHash("0x666"),
				})
				tc.ExpectedErrors = append(tc.ExpectedErrors, nil)

				// third call
				tc.ExpectedResults = append(tc.ExpectedResults, []common.Hash{})
				tc.ExpectedErrors = append(tc.ExpectedErrors, nil)
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				filter := &Filter{
					ID:         tc.FilterID,
					Type:       FilterTypePendingTx,
					LastPoll:   time.Now(),
					Parameters: "{}",
				}

				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(filter, nil).
					Once()

				m.Pool.
					On("GetPendingTxHashesSince", context.Background(), filter.LastPoll).
					Return(tc.ExpectedResults[0].([]common.Hash), nil).
					Once()

				m.Storage.
					On("UpdateFilterLastPoll", tc.FilterID).
					Run(func(args mock.Arguments) {
						filter.LastPoll = time.Now()

						m.Storage.
							On("GetFilter", tc.FilterID).
							Return(filter, nil).
							Once()

						m.Pool.
							On("GetPendingTxHashesSince", context.Background(), filter.LastPoll).
							Return(tc.ExpectedResults[1].([]common.Hash), nil).
							Once()

						m.Storage.
							On("UpdateFilterLastPoll", tc.FilterID).
							Run(func(args mock.Arguments) {
								filter.LastPoll = time.Now()

								m.Storage.
									On("GetFilter", tc.FilterID).
									Return(filter, nil).
									Once()

								m.Pool.
									On("GetPendingTxHashesSince", context.Background(), filter.LastPoll).
									Return(tc.ExpectedResults[2].([]common.Hash), nil).
									Once()

								m.Storage.
									On("UpdateFilterLastPoll", tc.FilterID).
									Return(nil).
									Once()
							}).
							Return(nil).
							Once()
					}).
					Return(nil).
					Once()
			},
		},
		{
			Name: "Get log filter changes multiple times successfully",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "1"
				// first call
				tc.ExpectedResults = append(tc.ExpectedResults, []ethTypes.Log{{
					Address: common.Address{}, Topics: []common.Hash{}, Data: []byte{},
					BlockNumber: uint64(1), TxHash: common.Hash{}, TxIndex: uint(1),
					BlockHash: common.Hash{}, Index: uint(1), Removed: false,
				}})
				tc.ExpectedErrors = append(tc.ExpectedErrors, nil)

				// second call
				tc.ExpectedResults = append(tc.ExpectedResults, []ethTypes.Log{{
					Address: common.Address{}, Topics: []common.Hash{}, Data: []byte{},
					BlockNumber: uint64(1), TxHash: common.Hash{}, TxIndex: uint(1),
					BlockHash: common.Hash{}, Index: uint(1), Removed: false,
				}, {
					Address: common.Address{}, Topics: []common.Hash{}, Data: []byte{},
					BlockNumber: uint64(1), TxHash: common.Hash{}, TxIndex: uint(1),
					BlockHash: common.Hash{}, Index: uint(1), Removed: false,
				}})
				tc.ExpectedErrors = append(tc.ExpectedErrors, nil)

				// third call
				tc.ExpectedResults = append(tc.ExpectedResults, nil)
				tc.ExpectedErrors = append(tc.ExpectedErrors, nil)
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				bn1 := types.BlockNumber(1)
				bn2 := types.BlockNumber(2)
				logFilter := LogFilter{
					FromBlock: &bn1, ToBlock: &bn2,
					Addresses: []common.Address{common.HexToAddress("0x111")},
					Topics:    [][]common.Hash{{common.HexToHash("0x222")}},
				}

				filter := &Filter{
					ID:         tc.FilterID,
					Type:       FilterTypeLog,
					LastPoll:   time.Now(),
					Parameters: logFilter,
				}

				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(filter, nil).
					Once()

				expectedLogs := tc.ExpectedResults[0].([]ethTypes.Log)
				logs := make([]*ethTypes.Log, 0, len(expectedLogs))
				for _, log := range expectedLogs {
					l := log
					logs = append(logs, &l)
				}

				m.State.
					On("GetLogs", context.Background(), uint64(*logFilter.FromBlock), uint64(*logFilter.ToBlock), logFilter.Addresses, logFilter.Topics, logFilter.BlockHash, &filter.LastPoll, mock.IsType(nilTx)).
					Return(logs, nil).
					Once()

				m.Storage.
					On("UpdateFilterLastPoll", tc.FilterID).
					Run(func(args mock.Arguments) {
						filter.LastPoll = time.Now()

						m.Storage.
							On("GetFilter", tc.FilterID).
							Return(filter, nil).
							Once()

						expectedLogs = tc.ExpectedResults[1].([]ethTypes.Log)
						logs = make([]*ethTypes.Log, 0, len(expectedLogs))
						for _, log := range expectedLogs {
							l := log
							logs = append(logs, &l)
						}

						m.State.
							On("GetLogs", context.Background(), uint64(*logFilter.FromBlock), uint64(*logFilter.ToBlock), logFilter.Addresses, logFilter.Topics, logFilter.BlockHash, &filter.LastPoll, mock.IsType(nilTx)).
							Return(logs, nil).
							Once()

						m.Storage.
							On("UpdateFilterLastPoll", tc.FilterID).
							Run(func(args mock.Arguments) {
								filter.LastPoll = time.Now()

								m.Storage.
									On("GetFilter", tc.FilterID).
									Return(filter, nil).
									Once()

								m.State.
									On("GetLogs", context.Background(), uint64(*logFilter.FromBlock), uint64(*logFilter.ToBlock), logFilter.Addresses, logFilter.Topics, logFilter.BlockHash, &filter.LastPoll, mock.IsType(nilTx)).
									Return([]*ethTypes.Log{}, nil).
									Once()

								m.Storage.
									On("UpdateFilterLastPoll", tc.FilterID).
									Return(nil).
									Once()
							}).
							Return(nil).
							Once()
					}).
					Return(nil).
					Once()
			},
		},
		{
			Name: "Get filter changes when filter is not found",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "1"
				// first call
				tc.ExpectedResults = append(tc.ExpectedResults, nil)
				tc.ExpectedErrors = append(tc.ExpectedErrors, types.NewRPCError(types.DefaultErrorCode, "filter not found"))
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(nil, ErrNotFound).
					Once()
			},
		},
		{
			Name: "Get filter changes fails to get filter",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "1"
				// first call
				tc.ExpectedResults = append(tc.ExpectedResults, nil)
				tc.ExpectedErrors = append(tc.ExpectedErrors, types.NewRPCError(types.DefaultErrorCode, "failed to get filter from storage"))
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(nil, errors.New("failed to get filter")).
					Once()
			},
		},
		{
			Name: "Get block filter changes fails to get block hashes",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "2"
				tc.ExpectedResults = append(tc.ExpectedResults, nil)
				tc.ExpectedErrors = append(tc.ExpectedErrors, types.NewRPCError(types.DefaultErrorCode, "failed to get block hashes"))
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				filter := &Filter{
					ID:         tc.FilterID,
					Type:       FilterTypeBlock,
					LastPoll:   time.Now(),
					Parameters: LogFilter{},
				}

				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(filter, nil).
					Once()

				m.State.
					On("GetL2BlockHashesSince", context.Background(), filter.LastPoll, mock.IsType(nilTx)).
					Return([]common.Hash{}, errors.New("failed to get hashes")).
					Once()
			},
		},
		{
			Name: "Get block filter changes fails to update the last time it was requested",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "2"
				tc.ExpectedResults = append(tc.ExpectedResults, nil)
				tc.ExpectedErrors = append(tc.ExpectedErrors, types.NewRPCError(types.DefaultErrorCode, "failed to update last time the filter changes were requested"))
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				filter := &Filter{
					ID:         tc.FilterID,
					Type:       FilterTypeBlock,
					LastPoll:   time.Now(),
					Parameters: LogFilter{},
				}

				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(filter, nil).
					Once()

				m.State.
					On("GetL2BlockHashesSince", context.Background(), filter.LastPoll, mock.IsType(nilTx)).
					Return([]common.Hash{}, nil).
					Once()

				m.Storage.
					On("UpdateFilterLastPoll", tc.FilterID).
					Return(errors.New("failed to update filter last poll")).
					Once()
			},
		},
		{
			Name: "Get pending transactions filter fails to get the hashes",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "3"
				tc.ExpectedResults = append(tc.ExpectedResults, nil)
				tc.ExpectedErrors = append(tc.ExpectedErrors, types.NewRPCError(types.DefaultErrorCode, "failed to get pending transaction hashes"))
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				filter := &Filter{
					ID:         tc.FilterID,
					Type:       FilterTypePendingTx,
					LastPoll:   time.Now(),
					Parameters: LogFilter{},
				}

				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(filter, nil).
					Once()

				m.Pool.
					On("GetPendingTxHashesSince", context.Background(), filter.LastPoll).
					Return([]common.Hash{}, errors.New("failed to get pending tx hashes")).
					Once()
			},
		},
		{
			Name: "Get pending transactions fails to update the last time it was requested",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "3"
				tc.ExpectedResults = append(tc.ExpectedResults, nil)
				tc.ExpectedErrors = append(tc.ExpectedErrors, types.NewRPCError(types.DefaultErrorCode, "failed to update last time the filter changes were requested"))
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				filter := &Filter{
					ID:         tc.FilterID,
					Type:       FilterTypePendingTx,
					LastPoll:   time.Now(),
					Parameters: LogFilter{},
				}

				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(filter, nil).
					Once()

				m.Pool.
					On("GetPendingTxHashesSince", context.Background(), filter.LastPoll).
					Return([]common.Hash{}, nil).
					Once()

				m.Storage.
					On("UpdateFilterLastPoll", tc.FilterID).
					Return(errors.New("failed to update filter last poll")).
					Once()
			},
		},
		{
			Name: "Get log filter changes fails to get logs",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "1"
				tc.ExpectedResults = append(tc.ExpectedResults, nil)
				tc.ExpectedErrors = append(tc.ExpectedErrors, types.NewRPCError(types.DefaultErrorCode, "failed to get logs from state"))
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				bn1 := types.BlockNumber(1)
				bn2 := types.BlockNumber(2)
				logFilter := LogFilter{

					FromBlock: &bn1, ToBlock: &bn2,
					Addresses: []common.Address{common.HexToAddress("0x111")},
					Topics:    [][]common.Hash{{common.HexToHash("0x222")}},
				}

				filter := &Filter{
					ID:         tc.FilterID,
					Type:       FilterTypeLog,
					LastPoll:   time.Now(),
					Parameters: logFilter,
				}

				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(filter, nil).
					Once()

				m.State.
					On("GetLogs", context.Background(), uint64(*logFilter.FromBlock), uint64(*logFilter.ToBlock), logFilter.Addresses, logFilter.Topics, logFilter.BlockHash, &filter.LastPoll, mock.IsType(nilTx)).
					Return(nil, errors.New("failed to get logs")).
					Once()
			},
		},
		{
			Name: "Get log filter changes fails to update the last time it was requested",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "1"
				tc.ExpectedResults = append(tc.ExpectedResults, nil)
				tc.ExpectedErrors = append(tc.ExpectedErrors, types.NewRPCError(types.DefaultErrorCode, "failed to update last time the filter changes were requested"))
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				bn1 := types.BlockNumber(1)
				bn2 := types.BlockNumber(2)
				logFilter := LogFilter{
					FromBlock: &bn1, ToBlock: &bn2,
					Addresses: []common.Address{common.HexToAddress("0x111")},
					Topics:    [][]common.Hash{{common.HexToHash("0x222")}},
				}

				filter := &Filter{
					ID:         tc.FilterID,
					Type:       FilterTypeLog,
					LastPoll:   time.Now(),
					Parameters: logFilter,
				}

				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(filter, nil).
					Once()

				m.State.
					On("GetLogs", context.Background(), uint64(*logFilter.FromBlock), uint64(*logFilter.ToBlock), logFilter.Addresses, logFilter.Topics, logFilter.BlockHash, &filter.LastPoll, mock.IsType(nilTx)).
					Return([]*ethTypes.Log{}, nil).
					Once()

				m.Storage.
					On("UpdateFilterLastPoll", tc.FilterID).
					Return(errors.New("failed to update filter last poll")).
					Once()
			},
		},
		{
			Name: "Get filter changes for a unknown log type",
			Prepare: func(t *testing.T, tc *testCase) {
				tc.FilterID = "4"
				tc.ExpectedResults = append(tc.ExpectedResults, nil)
				tc.ExpectedErrors = append(tc.ExpectedErrors, nil)
			},
			SetupMocks: func(t *testing.T, m *mocksWrapper, tc testCase) {
				filter := &Filter{
					Type: "unknown type",
				}

				m.Storage.
					On("GetFilter", tc.FilterID).
					Return(filter, nil).
					Once()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tc := testCase
			tc.Prepare(t, &tc)
			tc.SetupMocks(t, m, tc)

			timesToCall := len(tc.ExpectedResults)

			for i := 0; i < timesToCall; i++ {
				res, err := s.JSONRPCCall("eth_getFilterChanges", tc.FilterID)
				require.NoError(t, err)
				assert.Equal(t, float64(1), res.ID)
				assert.Equal(t, "2.0", res.JSONRPC)

				if res.Result != nil {
					var result interface{}
					err = json.Unmarshal(res.Result, &result)
					require.NoError(t, err)

					if result != nil || tc.ExpectedResults[i] != nil {
						if logs, ok := tc.ExpectedResults[i].([]ethTypes.Log); ok {
							err = json.Unmarshal(res.Result, &logs)
							require.NoError(t, err)
							assert.ElementsMatch(t, tc.ExpectedResults[i], logs)
						}
						if hashes, ok := tc.ExpectedResults[i].([]common.Hash); ok {
							err = json.Unmarshal(res.Result, &hashes)
							require.NoError(t, err)
							assert.ElementsMatch(t, tc.ExpectedResults[i], hashes)
						}
					}
				}

				if res.Error != nil || tc.ExpectedErrors[i] != nil {
					assert.Equal(t, tc.ExpectedErrors[i].ErrorCode(), res.Error.Code)
					assert.Equal(t, tc.ExpectedErrors[i].Error(), res.Error.Message)
				}
			}
		})
	}
}
