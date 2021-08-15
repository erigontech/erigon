package commands

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/transactions"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/crypto/sha3"
)

func (api *APIImpl) CallBundle(ctx context.Context, txHashes []common.Hash, stateBlockNumberOrHash rpc.BlockNumberOrHash, timeoutMilliSecondsPtr *int64) (map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	if len(txHashes) == 0 {
		return nil, nil
	}

	var txs types.Transactions

	for _, txHash := range txHashes {
		txn, _, _, _, err := rawdb.ReadTransaction(tx, txHash)
		if err != nil {
			return nil, err
		}
		if txn == nil {
			return nil, nil // not error, see https://github.com/ledgerwatch/turbo-geth/issues/1645
		}
		txs = append(txs, txn)
	}
	defer func(start time.Time) { log.Debug("Executing EVM call finished", "runtime", time.Since(start)) }(time.Now())

	stateBlockNumber, hash, err := rpchelper.GetBlockNumber(stateBlockNumberOrHash, tx, api.filters)
	if err != nil {
		return nil, err
	}

	var stateReader state.StateReader
	if num, ok := stateBlockNumberOrHash.Number(); ok && num == rpc.LatestBlockNumber {
		stateReader = state.NewPlainStateReader(tx)
	} else {
		stateReader = state.NewPlainState(tx, stateBlockNumber)
	}
	st := state.New(stateReader)

	parent := rawdb.ReadHeader(tx, hash, stateBlockNumber)
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
	firstMsg, err := txs[0].AsMessage(*signer, nil)
	if err != nil {
		return nil, err
	}

	blockCtx, txCtx := transactions.GetEvmContext(firstMsg, header, stateBlockNumberOrHash.RequireCanonical, tx, ethdb.GetHasTEVM(tx))
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
	for _, txn := range txs {
		msg, err := txn.AsMessage(*signer, nil)
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

		txHash := txn.Hash().String()
		jsonResult := map[string]interface{}{
			"txHash":  txHash,
			"gasUsed": result.UsedGas,
		}
		bundleHash.Write(txn.Hash().Bytes())
		if result.Err != nil {
			jsonResult["error"] = result.Err.Error()
		} else {
			jsonResult["value"] = common.BytesToHash(result.Return())
		}

		results = append(results, jsonResult)
	}

	ret := map[string]interface{}{}
	ret["results"] = results
	ret["bundleHash"] = hexutil.Encode(bundleHash.Sum(nil))
	return ret, nil
}

// GetBlockByNumber implements eth_getBlockByNumber. Returns information about a block given the block's number.
func (api *APIImpl) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	b, err := api.getBlockByNumber(number, tx)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	additionalFields := make(map[string]interface{})

	td, err := rawdb.ReadTd(tx, b.Hash(), b.NumberU64())
	if err != nil {
		return nil, err
	}
	additionalFields["totalDifficulty"] = (*hexutil.Big)(td)
	response, err := ethapi.RPCMarshalBlock(b, true, fullTx, additionalFields)

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
			return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
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

	block, err := rawdb.ReadBlockByHash(tx, hash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
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
	if blockNr == rpc.PendingBlockNumber {
		b, err := api.getBlockByNumber(blockNr, tx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return nil, nil
		}
		n := hexutil.Uint(len(b.Transactions()))
		return &n, nil
	}
	blockNum, err := getBlockNumber(blockNr, tx)
	if err != nil {
		return nil, err
	}
	body, _, txAmount, err := rawdb.ReadBodyByNumber(tx, blockNum)
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, nil
	}
	n := hexutil.Uint(txAmount)
	return &n, nil
}

// GetBlockTransactionCountByHash implements eth_getBlockTransactionCountByHash. Returns the number of transactions in a block given the block's block hash.
func (api *APIImpl) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (*hexutil.Uint, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	num := rawdb.ReadHeaderNumber(tx, blockHash)
	if num == nil {
		return nil, nil
	}
	body, _, txAmount := rawdb.ReadBody(tx, blockHash, *num)
	if body == nil {
		return nil, nil
	}
	n := hexutil.Uint(txAmount)
	return &n, nil
}
