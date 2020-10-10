package commands

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/eth/filters"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// EthAPI is a collection of functions that are exposed in the
type EthAPI interface {
	ChainId(ctx context.Context) (hexutil.Uint64, error)
	ProtocolVersion(_ context.Context) (hexutil.Uint, error)
	// GasPrice(_ context.Context) (*hexutil.Big, error)
	Coinbase(ctx context.Context) (common.Address, error)
	BlockNumber(ctx context.Context) (hexutil.Uint64, error)
	GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error)
	GetBlockByHash(ctx context.Context, hash common.Hash, fullTx bool) (map[string]interface{}, error)
	GetBalance(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Big, error)
	GetTransactionReceipt(ctx context.Context, hash common.Hash) (map[string]interface{}, error)
	GetLogs(ctx context.Context, crit filters.FilterCriteria) ([]*types.Log, error)
	Call(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, overrides *map[common.Address]ethapi.Account) (hexutil.Bytes, error)
	EstimateGas(ctx context.Context, args ethapi.CallArgs) (hexutil.Uint64, error)
	SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error)
	Syncing(ctx context.Context) (interface{}, error)
	GetBlockTransactionCountByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*hexutil.Uint, error)
	GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (*hexutil.Uint, error)
	GetTransactionByHash(ctx context.Context, hash common.Hash) (*RPCTransaction, error)
	GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, txIndex hexutil.Uint64) (*RPCTransaction, error)
	GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, txIndex hexutil.Uint) (*RPCTransaction, error)
	GetStorageAt(ctx context.Context, address common.Address, index string, blockNrOrHash rpc.BlockNumberOrHash) (string, error)
	GetCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error)
	GetTransactionCount(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Uint64, error)
	GetUncleByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, index hexutil.Uint) (map[string]interface{}, error)
	GetUncleByBlockHashAndIndex(ctx context.Context, hash common.Hash, index hexutil.Uint) (map[string]interface{}, error)
	GetUncleCountByBlockNumber(ctx context.Context, blockNr rpc.BlockNumber) (*hexutil.Uint, error)
	GetUncleCountByBlockHash(ctx context.Context, hash common.Hash) (*hexutil.Uint, error)
	// These three commands worked anyway, but were not in this interface. Temporarily adding them for discussion
	GetHeaderByNumber(_ context.Context, number rpc.BlockNumber) (*types.Header, error)
	GetHeaderByHash(_ context.Context, hash common.Hash) (*types.Header, error)
	GetLogsByHash(ctx context.Context, hash common.Hash) ([][]*types.Log, error)
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type APIImpl struct {
	db           ethdb.KV
	ethBackend   ethdb.Backend
	dbReader     ethdb.Database
	chainContext core.ChainContext
	GasCap       uint64
}

// NewAPI returns APIImpl instance
func NewAPI(db ethdb.KV, dbReader ethdb.Database, eth ethdb.Backend, gascap uint64) *APIImpl {
	return &APIImpl{
		db:         db,
		dbReader:   dbReader,
		ethBackend: eth,
		GasCap:     gascap,
	}
}

// BlockNumber returns the latest block number of the chain
func (api *APIImpl) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	execution, _, err := stages.GetStageProgress(api.dbReader, stages.Finish)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint64(execution), nil
}

// Syncing - we can return the progress of the very first stage as the highest block, and then the progress of the very last stage as the current block
func (api *APIImpl) Syncing(ctx context.Context) (interface{}, error) {
	highestBlock, _, err := stages.GetStageProgress(api.dbReader, stages.Headers)
	if err != nil {
		return false, err
	}

	currentBlock, _, err := stages.GetStageProgress(api.dbReader, stages.Finish)
	if err != nil {
		return false, err
	}

	// Return not syncing if the synchronisation already completed
	if currentBlock >= highestBlock {
		return false, nil
	}
	// Otherwise gather the block sync stats
	return map[string]hexutil.Uint64{
		"currentBlock": hexutil.Uint64(currentBlock),
		"highestBlock": hexutil.Uint64(highestBlock),
	}, nil
}

// GetBlockTransactionCountByNumber returns the number of transactions in the block
func (api *APIImpl) GetBlockTransactionCountByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*hexutil.Uint, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, err := getBlockNumber(blockNr, tx)
	if err != nil {
		return nil, err
	}

	block := rawdb.ReadBlockByNumber(tx, blockNum)
	if block == nil {
		return nil, fmt.Errorf("block not found: %d", blockNum)
	}
	n := hexutil.Uint(len(block.Transactions()))
	return &n, nil
}

// GetBlockTransactionCountByHash returns the number of transactions in the block
func (api *APIImpl) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (*hexutil.Uint, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	block := rawdb.ReadBlockByHash(tx, blockHash)
	if block == nil {
		return nil, fmt.Errorf("block not found: %x", blockHash)
	}
	n := hexutil.Uint(len(block.Transactions()))
	return &n, nil
}

// ChainId returns the chain id from the config
func (api *APIImpl) ChainId(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	chainConfig := getChainConfig(tx)
	return hexutil.Uint64(chainConfig.ChainID.Uint64()), nil
}

// ProtocolVersion returns the chain id from the config
func (api *APIImpl) ProtocolVersion(_ context.Context) (hexutil.Uint, error) {
	return hexutil.Uint(eth.ProtocolVersions[0]), nil
}

/*
// GasPrice returns a suggestion for a gas price.
func (api *APIImpl) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	price, err := eth.SuggestPrice(ctx)
	return (*hexutil.Big)(price), err
}
*/

// RPCTransaction represents a transaction that will serialize to the RPC representation of a transaction
type RPCTransaction struct {
	BlockHash        *common.Hash    `json:"blockHash"`
	BlockNumber      *hexutil.Big    `json:"blockNumber"`
	From             common.Address  `json:"from"`
	Gas              hexutil.Uint64  `json:"gas"`
	GasPrice         *hexutil.Big    `json:"gasPrice"`
	Hash             common.Hash     `json:"hash"`
	Input            hexutil.Bytes   `json:"input"`
	Nonce            hexutil.Uint64  `json:"nonce"`
	R                *hexutil.Big    `json:"r"`
	S                *hexutil.Big    `json:"s"`
	To               *common.Address `json:"to"`
	TransactionIndex *hexutil.Uint64 `json:"transactionIndex"`
	V                *hexutil.Big    `json:"v"`
	Value            *hexutil.Big    `json:"value"`
}

// newRPCTransaction returns a transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func newRPCTransaction(tx *types.Transaction, blockHash common.Hash, blockNumber uint64, index uint64) *RPCTransaction {
	var signer types.Signer = types.FrontierSigner{}
	if tx.Protected() {
		signer = types.NewEIP155Signer(tx.ChainID().ToBig())
	}
	from, _ := types.Sender(signer, tx)
	v, r, s := tx.RawSignatureValues()

	result := &RPCTransaction{
		From:     from,
		Gas:      hexutil.Uint64(tx.Gas()),
		GasPrice: (*hexutil.Big)(tx.GasPrice().ToBig()),
		Hash:     tx.Hash(),
		Input:    hexutil.Bytes(tx.Data()),
		Nonce:    hexutil.Uint64(tx.Nonce()),
		R:        (*hexutil.Big)(r.ToBig()),
		S:        (*hexutil.Big)(s.ToBig()),
		To:       tx.To(),
		V:        (*hexutil.Big)(v.ToBig()),
		Value:    (*hexutil.Big)(tx.Value().ToBig()),
	}
	if blockHash != (common.Hash{}) {
		result.BlockHash = &blockHash
		result.BlockNumber = (*hexutil.Big)(new(big.Int).SetUint64(blockNumber))
		result.TransactionIndex = (*hexutil.Uint64)(&index)
	}
	return result
}

// GetTransactionByHash returns the transaction for the given hash
func (api *APIImpl) GetTransactionByHash(ctx context.Context, hash common.Hash) (*RPCTransaction, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByHash
	txn, blockHash, blockNumber, txIndex := rawdb.ReadTransaction(tx, hash)
	if txn == nil {
		return nil, fmt.Errorf("transaction %#x not found", hash)
	}
	return newRPCTransaction(txn, blockHash, blockNumber, txIndex), nil
}

// GetTransactionByBlockHashAndIndex returns the transaction for the given block hash and index.
func (api *APIImpl) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, txIndex hexutil.Uint64) (*RPCTransaction, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByBlockHashAndIndex
	block := rawdb.ReadBlockByHash(tx, blockHash)
	if block == nil {
		return nil, fmt.Errorf("block %#x not found", blockHash)
	}

	txs := block.Transactions()
	if uint64(txIndex) >= uint64(len(txs)) {
		return nil, fmt.Errorf("txIndex (%d) out of range (nTxs: %d)", uint64(txIndex), uint64(len(txs)))
	}

	return newRPCTransaction(txs[txIndex], block.Hash(), block.NumberU64(), uint64(txIndex)), nil
}

// GetTransactionByBlockNumberAndIndex returns the transaction for the given block number and index.
func (api *APIImpl) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, txIndex hexutil.Uint) (*RPCTransaction, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByBlockNumberAndIndex
	blockNum, err := getBlockNumber(blockNr, tx)
	if err != nil {
		return nil, err
	}

	block := rawdb.ReadBlockByNumber(tx, blockNum)
	if block == nil {
		return nil, fmt.Errorf("block %d not found", blockNum)
	}

	txs := block.Transactions()
	if uint64(txIndex) >= uint64(len(txs)) {
		return nil, fmt.Errorf("txIndex (%d) out of range (nTxs: %d)", uint64(txIndex), uint64(len(txs)))
	}

	return newRPCTransaction(txs[txIndex], block.Hash(), block.NumberU64(), uint64(txIndex)), nil
}

// GetStorageAt returns a 32-byte long, zero-left-padded value at storage location 'index' of address 'address'. Returns '0x' if no value
func (api *APIImpl) GetStorageAt(ctx context.Context, address common.Address, index string, blockNrOrHash rpc.BlockNumberOrHash) (string, error) {
	var empty []byte

	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, api.dbReader)
	if err != nil {
		return hexutil.Encode(common.LeftPadBytes(empty[:], 32)), err
	}

	reader := adapter.NewStateReader(api.db, blockNumber)
	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return hexutil.Encode(common.LeftPadBytes(empty[:], 32)), err
	}

	location := common.HexToHash(index)
	res, err := reader.ReadAccountStorage(address, acc.Incarnation, &location)
	if err != nil {
		res = empty
	}
	return hexutil.Encode(common.LeftPadBytes(res[:], 32)), err
}

// GetCode returns the code stored at the given address in the state for the given block number.
func (api *APIImpl) GetCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, api.dbReader)
	if err != nil {
		return nil, err
	}

	reader := adapter.NewStateReader(api.db, blockNumber)
	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return hexutil.Bytes(""), nil
	}
	res, err := reader.ReadAccountCode(address, acc.CodeHash)
	if res == nil {
		return hexutil.Bytes(""), nil
	}
	return res, nil
}

// GetTransactionCount returns the number of transactions the given address has sent for the given block number
func (api *APIImpl) GetTransactionCount(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Uint64, error) {
	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, api.dbReader)
	if err != nil {
		return nil, err
	}
	nonce := hexutil.Uint64(0)
	reader := adapter.NewStateReader(api.db, blockNumber)
	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return &nonce, err
	}
	return (*hexutil.Uint64)(&acc.Nonce), err
}
