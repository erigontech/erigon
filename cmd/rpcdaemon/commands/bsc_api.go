package commands

import (
	"context"
	"fmt"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
)

// BscAPI is a collection of functions that are exposed in the
type BscAPI interface {
	Etherbase(ctx context.Context) (libcommon.Address, error)
	FillTransaction(ctx context.Context, args map[string]interface{}) (map[string]interface{}, error)
	GetDiffAccounts(ctx context.Context, blockNr rpc.BlockNumber) ([]libcommon.Address, error)
	GetDiffAccountsWithScope(ctx context.Context, blockNr rpc.BlockNumber, accounts []libcommon.Address) (*types.DiffAccountsInBlock, error)
	GetFilterLogs(ctx context.Context, id rpc.ID) ([]*types.Log, error)
	GetHashrate(ctx context.Context) (uint64, error)
	GetHeaderByHash(ctx context.Context, hash libcommon.Hash) (*types.Header, error)
	GetHeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error)
	GetTransactionDataAndReceipt(ctx context.Context, hash libcommon.Hash) (map[string]interface{}, error)
	GetTransactionReceiptsByBlockNumber(ctx context.Context, number rpc.BlockNumber) ([]map[string]interface{}, error)
	Health(ctx context.Context) bool
	Resend(ctx context.Context, sendArgs map[string]interface{}, gasPrice *hexutil.Big, gasLimit *hexutil.Uint64) (libcommon.Hash, error)
	GetTransactionsByBlockNumber(ctx context.Context, blockNr rpc.BlockNumber) ([]*RPCTransaction, error)
	GetVerifyResult(ctx context.Context, blockNr rpc.BlockNumber, blockHash libcommon.Hash, diffHash libcommon.Hash) ([]map[string]interface{}, error)
	PendingTransactions() ([]*RPCTransaction, error)
}

type BscAPIImpl struct {
	ethApi *APIImpl
}

// NewBscAPI returns BscAPIImpl instance.
func NewBscAPI(eth *APIImpl) *BscAPIImpl {
	return &BscAPIImpl{
		ethApi: eth,
	}
}

// Etherbase is the address that mining rewards will be send to
func (api *BscAPIImpl) Etherbase(ctx context.Context) (libcommon.Address, error) {
	return api.ethApi.ethBackend.Etherbase(ctx)
}

// FillTransaction fills the defaults (nonce, gas, gasPrice or 1559 fields)
// on a given unsigned transaction, and returns it to the caller for further
// processing (signing + broadcast).
func (api *BscAPIImpl) FillTransaction(ctx context.Context, args map[string]interface{}) (map[string]interface{}, error) {
	return nil, fmt.Errorf(NotImplemented, "eth_fillTransaction")
}

// GetDiffAccountsWithScope returns detailed changes of some interested accounts in a specific block number.
func (api *BscAPIImpl) GetDiffAccountsWithScope(ctx context.Context, blockNr rpc.BlockNumber, accounts []libcommon.Address) (*types.DiffAccountsInBlock, error) {
	return nil, fmt.Errorf(NotImplemented, "eth_getDiffAccountsWithScope")
}

// GetDiffAccounts returns changed accounts in a specific block number.
func (api *BscAPIImpl) GetDiffAccounts(ctx context.Context, blockNr rpc.BlockNumber) ([]libcommon.Address, error) {
	return nil, fmt.Errorf(NotImplemented, "eth_getDiffAccounts")
}

// GetFilterLogs returns the logs for the filter with the given id.
// If the filter could not be found an empty array of logs is returned.
//
// https://eth.wiki/json-rpc/API#eth_getfilterlogs
func (api *BscAPIImpl) GetFilterLogs(ctx context.Context, id rpc.ID) ([]*types.Log, error) {
	return nil, fmt.Errorf(NotImplemented, "eth_getFilterLogs")
}

// GetHashrate returns the current hashrate for local CPU miner and remote miner.
func (api *BscAPIImpl) GetHashrate(ctx context.Context) (uint64, error) {
	return api.ethApi.Hashrate(ctx)
}

// GetHeaderByHash returns the requested header by hash
func (api *BscAPIImpl) GetHeaderByHash(ctx context.Context, hash libcommon.Hash) (*types.Header, error) {
	tx, beginErr := api.ethApi.db.BeginRo(ctx)
	if beginErr != nil {
		return nil, beginErr
	}
	defer tx.Rollback()
	return api.ethApi._blockReader.HeaderByHash(ctx, tx, hash)
}

// GetHeaderByNumber returns the requested canonical block header.
func (api *BscAPIImpl) GetHeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error) {
	tx, beginErr := api.ethApi.db.BeginRo(ctx)
	if beginErr != nil {
		return nil, beginErr
	}
	defer tx.Rollback()
	return api.ethApi._blockReader.HeaderByNumber(ctx, tx, uint64(number.Int64()))
}

// GetTransactionDataAndReceipt returns the original transaction data and transaction receipt for the given transaction hash.
func (api *BscAPIImpl) GetTransactionDataAndReceipt(ctx context.Context, hash libcommon.Hash) (map[string]interface{}, error) {
	rpcTransaction, err := api.ethApi.GetTransactionByHash(ctx, hash)
	if err != nil {
		return nil, err
	}

	receipt, err := api.ethApi.GetTransactionReceipt(ctx, hash)
	if err != nil {
		return nil, err
	}

	txData := map[string]interface{}{
		"blockHash":        rpcTransaction.BlockHash.String(),
		"blockNumber":      rpcTransaction.BlockNumber.String(),
		"from":             rpcTransaction.From.String(),
		"gas":              rpcTransaction.Gas.String(),
		"gasPrice":         rpcTransaction.GasPrice.String(),
		"hash":             rpcTransaction.Hash.String(),
		"input":            rpcTransaction.Input.String(),
		"nonce":            rpcTransaction.Nonce.String(),
		"to":               rpcTransaction.To.String(),
		"transactionIndex": rpcTransaction.TransactionIndex.String(),
		"value":            rpcTransaction.Value.String(),
		"v":                rpcTransaction.V.String(),
		"r":                rpcTransaction.R.String(),
		"s":                rpcTransaction.S.String(),
	}

	result := map[string]interface{}{
		"txData":  txData,
		"receipt": receipt,
	}
	return result, nil
}

// Health returns true if more than 75% of calls are executed faster than 5 secs
func (api *BscAPIImpl) Health(ctx context.Context) bool {
	return true
}

// Resend accepts an existing transaction and a new gas price and limit. It will remove
// the given transaction from the pool and reinsert it with the new gas price and limit.
func (api *BscAPIImpl) Resend(ctx context.Context, sendArgs map[string]interface{}, gasPrice *hexutil.Big, gasLimit *hexutil.Uint64) (libcommon.Hash, error) {
	return libcommon.Hash{}, fmt.Errorf(NotImplemented, "eth_resend")
}

// GetTransactionsByBlockNumber returns all the transactions for the given block number.
func (api *BscAPIImpl) GetTransactionsByBlockNumber(ctx context.Context, blockNr rpc.BlockNumber) ([]*RPCTransaction, error) {
	tx, beginErr := api.ethApi.db.BeginRo(ctx)
	if beginErr != nil {
		return nil, beginErr
	}
	defer tx.Rollback()
	block, err := api.ethApi.blockByNumber(ctx, blockNr, tx)
	if err != nil {
		return nil, err
	}
	txes := block.Transactions()
	result := make([]*RPCTransaction, 0, len(txes))
	for idx, tx := range txes {
		result = append(result, newRPCTransaction(tx, block.Hash(), block.NumberU64(), uint64(idx), block.BaseFee()))
	}
	return result, nil
}

func (api *BscAPIImpl) GetTransactionReceiptsByBlockNumber(ctx context.Context, blockNr rpc.BlockNumber) ([]map[string]interface{}, error) {
	return api.ethApi.GetBlockReceipts(ctx, blockNr)
}

func (api *BscAPIImpl) GetVerifyResult(ctx context.Context, blockNr rpc.BlockNumber, blockHash libcommon.Hash, diffHash libcommon.Hash) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf(NotImplemented, "eth_getVerifyResult")
}

// PendingTransactions returns the transactions that are in the transaction pool
// and have a from address that is one of the accounts this node manages.
func (s *BscAPIImpl) PendingTransactions() ([]*RPCTransaction, error) {
	return nil, fmt.Errorf(NotImplemented, "eth_pendingTransactions")
}
