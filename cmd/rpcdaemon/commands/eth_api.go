package commands

import (
	"context"
	"fmt"

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
	GetTransactionByHash(ctx context.Context, hash common.Hash) (map[string]interface{}, error)
	GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, txIndex hexutil.Uint64) (map[string]interface{}, error)
	GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNumber rpc.BlockNumber, txIndex hexutil.Uint64) (map[string]interface{}, error)
	GetStorageAt(ctx context.Context, address common.Address, index string, blockNrOrHash rpc.BlockNumberOrHash) (string, error)
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type APIImpl struct {
	db           ethdb.KV
	ethBackend   ethdb.Backend
	dbReader     ethdb.Getter
	chainContext core.ChainContext
	GasCap       uint64
}

// NewAPI returns APIImpl instance
func NewAPI(db ethdb.KV, dbReader ethdb.Getter, eth ethdb.Backend, gascap uint64) *APIImpl {
	return &APIImpl{
		db:         db,
		dbReader:   dbReader,
		ethBackend: eth,
		GasCap:     gascap,
	}
}

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

func (api *APIImpl) GetBlockTransactionCountByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*hexutil.Uint, error) {
	blockNum, err := getBlockNumber(blockNr, api.dbReader)
	if err != nil {
		return nil, err
	}

	block := rawdb.ReadBlockByNumber(api.dbReader, blockNum)
	if block == nil {
		return nil, fmt.Errorf("block not found: %d", blockNum)
	}
	n := hexutil.Uint(len(block.Transactions()))
	return &n, nil
}

func (api *APIImpl) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (*hexutil.Uint, error) {
	block := rawdb.ReadBlockByHash(api.dbReader, blockHash)
	if block == nil {
		return nil, fmt.Errorf("block not found: %x", blockHash)
	}
	n := hexutil.Uint(len(block.Transactions()))
	return &n, nil
}

func (api *APIImpl) GetTransactionByHash(ctx context.Context, hash common.Hash) (map[string]interface{}, error) {
	tx, blockHash, blockNumber, txIndex := rawdb.ReadTransaction(api.dbReader, hash)

	if tx == nil {
		return nil, fmt.Errorf("transaction %#x not found", hash)
	}

	from := rawdb.ReadSenders(api.dbReader, blockHash, blockNumber)[txIndex]

	v, r, s := tx.RawSignatureValues()

	fields := map[string]interface{}{
		"hash":             tx.Hash(),
		"nonce":            tx.Nonce(),
		"blockHash":        blockHash,
		"blockNumber":      hexutil.EncodeUint64(blockNumber),
		"from":             from,
		"to":               tx.To(),
		"value":            tx.Value(),
		"gasPrice":         tx.GasPrice(),
		"gas":              hexutil.EncodeUint64(tx.Gas()),
		"input":            hexutil.Encode(tx.Data()),
		"v":                v,
		"s":                s,
		"r":                r,
		"transactionIndex": txIndex,
	}
	return fields, nil
}

//TODO(tjayrush): probably using the wrong types here
func (api *APIImpl) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, txIndex hexutil.Uint64) (map[string]interface{}, error) {
	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByBlockHashAndIndex
	block := rawdb.ReadBlockByHash(api.dbReader, blockHash)
	if block == nil {
		return nil, fmt.Errorf("block not found: %x", blockHash)
	}
	nTxs := hexutil.Uint64(len(block.Transactions()))
	if txIndex >= nTxs {
		return nil, fmt.Errorf("index %d greater than number of txs: %d", txIndex, nTxs)
	}
	tx := block.Transactions()[txIndex]

	from := rawdb.ReadSenders(api.dbReader, blockHash, block.NumberU64())[txIndex]
	v, r, s := tx.RawSignatureValues()
	fields := map[string]interface{}{
		"hash":             tx.Hash(),
		"nonce":            tx.Nonce(),
		"blockHash":        blockHash,
		"blockNumber":      hexutil.EncodeUint64(block.NumberU64()),
		"from":             from,
		"to":               tx.To(),
		"value":            tx.Value(),
		"gasPrice":         tx.GasPrice(),
		"gas":              hexutil.EncodeUint64(tx.Gas()),
		"input":            hexutil.Encode(tx.Data()),
		"v":                v,
		"s":                s,
		"r":                r,
		"transactionIndex": txIndex,
	}
	return fields, nil
}

//TODO(tjayrush): probably using the wrong types here
func (api *APIImpl) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, txIndex hexutil.Uint64) (map[string]interface{}, error) {
	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByBlockNumberAndIndex
	blockNum, err := getBlockNumber(blockNr, api.dbReader)
	if err != nil {
		return nil, err
	}
	block := rawdb.ReadBlockByNumber(api.dbReader, blockNum)
	if block == nil {
		return nil, fmt.Errorf("block not found: %d", blockNum)
	}
	nTxs := hexutil.Uint64(len(block.Transactions()))
	if txIndex >= nTxs {
		return nil, fmt.Errorf("index %d greater than number of txs: %d", txIndex, nTxs)
	}
	tx := block.Transactions()[txIndex]
	from := rawdb.ReadSenders(api.dbReader, block.Hash(), block.NumberU64())[txIndex]
	v, r, s := tx.RawSignatureValues()
	fields := map[string]interface{}{
		"hash":             tx.Hash(),
		"nonce":            tx.Nonce(),
		"blockHash":        block.Hash(),
		"blockNumber":      hexutil.EncodeUint64(block.NumberU64()),
		"from":             from,
		"to":               tx.To(),
		"value":            tx.Value(),
		"gasPrice":         tx.GasPrice(),
		"gas":              hexutil.EncodeUint64(tx.Gas()),
		"input":            hexutil.Encode(tx.Data()),
		"v":                v,
		"s":                s,
		"r":                r,
		"transactionIndex": txIndex,
	}
	return fields, nil
}

func (api *APIImpl) GetStorageAt(ctx context.Context, address common.Address, index string, blockNrOrHash rpc.BlockNumberOrHash) (string, error) {
	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, api.dbReader)
	if err != nil {
		return "", err
	}

	reader := adapter.NewStateReader(api.db, blockNumber)
	acc, err := reader.ReadAccountData(address)
	if err != nil {
		return "", err
	}

	if acc == nil {
		return "", fmt.Errorf("account not found")
	}

	location := common.HexToHash(index)

	res, err := reader.ReadAccountStorage(address, acc.Incarnation, &location)
	if err != nil {
		return "", err
	}

	return hexutil.Encode(res), nil
}
