package commands

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/common/math"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter/ethapi"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

func (api *APIImpl) CallBundle(ctx context.Context, txHashes []common.Hash, stateBlockNumberOrHash rpc.BlockNumberOrHash, timeoutMilliSecondsPtr *int64) (map[string]interface{}, error) {
	dbtx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		return nil, err
	}

	if len(txHashes) == 0 {
		return nil, nil
	}

	var txs types.Transactions

	for _, txHash := range txHashes {
		txn, _, _, _ := rawdb.ReadTransaction(ethdb.NewRoTxDb(dbtx), txHash)
		if txn == nil {
			return nil, nil // not error, see https://github.com/ledgerwatch/turbo-geth/issues/1645
		}
		txs = append(txs, txn)
	}
	defer func(start time.Time) { log.Debug("Executing EVM call finished", "runtime", time.Since(start)) }(time.Now())

	stateBlockNumber, hash, err := rpchelper.GetBlockNumber(stateBlockNumberOrHash, dbtx, api.pending)
	if err != nil {
		return nil, err
	}

	var stateReader state.StateReader
	if num, ok := stateBlockNumberOrHash.Number(); ok && num == rpc.LatestBlockNumber {
		stateReader = state.NewPlainStateReader(dbtx)
	} else {
		stateReader = state.NewPlainKvState(dbtx, stateBlockNumber)
	}
	st := state.New(stateReader)

	parent := rawdb.ReadHeader(dbtx, hash, stateBlockNumber)
	if parent == nil {
		return nil, fmt.Errorf("block %d(%x) not found", stateBlockNumber, hash)
	}

	blockNumber := stateBlockNumber + 1

	timestamp := parent.Time // Dont care about the timestamp

	coinbase := parent.Coinbase
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     big.NewInt(int64(blockNumber)),
		GasLimit:   parent.GasLimit,
		Time:       timestamp,
		Difficulty: parent.Difficulty,
		Coinbase:   coinbase,
	}

	// Get a new instance of the EVM
	signer := types.MakeSigner(chainConfig, blockNumber)
	firstMsg, err := txs[0].AsMessage(*signer)
	if err != nil {
		return nil, err
	}

	blockCtx, txCtx := transactions.GetEvmContext(firstMsg, header, stateBlockNumberOrHash.RequireCanonical, dbtx)
	evm := vm.NewEVM(blockCtx, txCtx, st, chainConfig, vm.Config{Debug: false})

	timeoutMilliSeconds := int64(5000)
	if timeoutMilliSecondsPtr != nil {
		timeoutMilliSeconds = *timeoutMilliSecondsPtr
	}
	timeout := time.Millisecond * time.Duration(timeoutMilliSeconds)

	// Setup context so it may be cancelled the call has completed
	// or, in case of unmetered gas, setup a context with a timeout.
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	// Make sure the context is cancelled when the call has completed
	// this makes sure resources are cleaned up.
	defer cancel()

	// Wait for the context to be done and cancel the evm. Even if the
	// EVM has finished, cancelling may be done (repeatedly)
	go func() {
		<-ctx.Done()
		evm.Cancel()
	}()

	// Setup the gas pool (also for unmetered requests)
	// and apply the message.
	gp := new(core.GasPool).AddGas(math.MaxUint64)

	results := []map[string]interface{}{}

	bundleHash := sha3.NewLegacyKeccak256()
	for _, tx := range txs {
		msg, err := tx.AsMessage(*signer)
		if err != nil {
			return nil, err
		}
		// Execute the transaction message
		result, err := core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			return nil, err
		}
		// If the timer caused an abort, return an appropriate error message
		if evm.Cancelled() {
			return nil, fmt.Errorf("execution aborted (timeout = %v)", timeout)
		}

		txHash := tx.Hash().String()
		jsonResult := map[string]interface{}{
			"txHash":  txHash,
			"gasUsed": result.UsedGas,
		}
		bundleHash.Write(tx.Hash().Bytes())
		if result.Err != nil {
			jsonResult["error"] = result.Err.Error()
		} else {
			jsonResult["value"] = common.BytesToHash(result.Return())
		}

		results = append(results, jsonResult)
	}

	ret := map[string]interface{}{}
	ret["results"] = results
	ret["bundleHash"] = "0x" + common.Bytes2Hex(bundleHash.Sum(nil))
	return ret, nil
}

// GetBlockByNumber implements eth_getBlockByNumber. Returns information about a block given the block's number.
func (api *APIImpl) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, err := getBlockNumber(number, tx)
	if err != nil {
		return nil, err
	}
	additionalFields := make(map[string]interface{})

	block, err := rawdb.ReadBlockByNumber(ethdb.NewRoTxDb(tx), blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/turbo-geth/issues/1645
	}

	td, err := rawdb.ReadTd(tx, block.Hash(), blockNum)
	if err != nil {
		return nil, err
	}
	additionalFields["totalDifficulty"] = (*hexutil.Big)(td)
	response, err := ethapi.RPCMarshalBlock(block, true, fullTx, additionalFields)

	if err == nil && number == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}
	return response, err
}

// GetBlockByHash implements eth_getBlockByHash. Returns information about a block given the block's hash.
func (api *APIImpl) GetBlockByHash(ctx context.Context, numberOrHash rpc.BlockNumberOrHash, fullTx bool) (map[string]interface{}, error) {
	if numberOrHash.BlockHash == nil {
		// some web3.js based apps (like ethstats client) for some reason call
		// eth_getBlockByHash with a block number as a parameter
		// so no matter how weird that is, we would love to support that.
		if numberOrHash.BlockNumber == nil {
			return nil, nil // not error, see https://github.com/ledgerwatch/turbo-geth/issues/1645
		}
		return api.GetBlockByNumber(ctx, *numberOrHash.BlockNumber, fullTx)
	}

	hash := *numberOrHash.BlockHash
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	additionalFields := make(map[string]interface{})

	block, err := rawdb.ReadBlockByHash(ethdb.NewRoTxDb(tx), hash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/turbo-geth/issues/1645
	}
	number := block.NumberU64()

	td, err := rawdb.ReadTd(tx, hash, number)
	if err != nil {
		return nil, err
	}
	additionalFields["totalDifficulty"] = (*hexutil.Big)(td)
	response, err := ethapi.RPCMarshalBlock(block, true, fullTx, additionalFields)

	if err == nil && int64(number) == rpc.PendingBlockNumber.Int64() {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}
	return response, err
}

// GetBlockTransactionCountByNumber implements eth_getBlockTransactionCountByNumber. Returns the number of transactions in a block given the block's block number.
func (api *APIImpl) GetBlockTransactionCountByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*hexutil.Uint, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, err := getBlockNumber(blockNr, tx)
	if err != nil {
		return nil, err
	}

	block, err := rawdb.ReadBlockByNumber(ethdb.NewRoTxDb(tx), blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/turbo-geth/issues/1645
	}
	n := hexutil.Uint(len(block.Transactions()))
	return &n, nil
}

// GetBlockTransactionCountByHash implements eth_getBlockTransactionCountByHash. Returns the number of transactions in a block given the block's block hash.
func (api *APIImpl) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (*hexutil.Uint, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	block, err := rawdb.ReadBlockByHash(ethdb.NewRoTxDb(tx), blockHash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("block not found: %x", blockHash)
	}
	n := hexutil.Uint(len(block.Transactions()))
	return &n, nil
}
