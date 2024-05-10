package commands

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/hexutility"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto/cryptopool"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/transactions"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
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
	engine := api.engine()

	if len(txHashes) == 0 {
		return nil, nil
	}

	var txs types.Transactions
	for _, txHash := range txHashes {
		blockNum, ok, err := api.txnLookup(ctx, tx, txHash)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		block, err := api.blockByNumberWithSenders(tx, blockNum)
		if err != nil {
			return nil, err
		}
		if block == nil {
			return nil, nil
		}
		var txn types.Transaction
		for _, transaction := range block.Transactions() {
			if transaction.Hash() == txHash {
				txn = transaction
				break
			}
		}
		if txn == nil {
			return nil, nil // not error, see https://github.com/ledgerwatch/turbo-geth/issues/1645
		}
		txs = append(txs, txn)
	}
	defer func(start time.Time) { log.Trace("Executing EVM call finished", "runtime", time.Since(start)) }(time.Now())

	stateBlockNumber, hash, latest, err := rpchelper.GetBlockNumber(stateBlockNumberOrHash, tx, api.filters)
	if err != nil {
		return nil, err
	}
	var stateReader state.StateReader
	if latest {
		cacheView, err := api.stateCache.View(ctx, tx)
		if err != nil {
			return nil, err
		}
		stateReader = state.NewCachedReader2(cacheView, tx)
	} else {
		stateReader, err = rpchelper.CreateHistoryStateReader(tx, stateBlockNumber+1, 0, api.historyV3(tx), chainConfig.ChainName)
		if err != nil {
			return nil, err
		}
	}
	ibs := state.New(stateReader)

	parent := rawdb.ReadHeader(tx, hash, stateBlockNumber)
	if parent == nil {
		return nil, fmt.Errorf("block %d(%x) not found", stateBlockNumber, hash)
	}

	blockNumber := stateBlockNumber + 1

	timestamp := parent.Time + clparams.MainnetBeaconConfig.SecondsPerSlot

	coinbase := parent.Coinbase
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     big.NewInt(int64(blockNumber)),
		GasLimit:   parent.GasLimit,
		Time:       timestamp,
		Difficulty: parent.Difficulty,
		Coinbase:   coinbase,
	}

	signer := types.MakeSigner(chainConfig, blockNumber)
	rules := chainConfig.Rules(blockNumber, timestamp)
	firstMsg, err := txs[0].AsMessage(*signer, nil, rules)
	if err != nil {
		return nil, err
	}

	blockCtx := transactions.NewEVMBlockContext(engine, header, stateBlockNumberOrHash.RequireCanonical, tx, api._blockReader)
	txCtx := core.NewEVMTxContext(firstMsg)
	// Get a new instance of the EVM
	evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{Debug: false})

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

	bundleHash := cryptopool.NewLegacyKeccak256()
	defer cryptopool.ReturnToPoolKeccak256(bundleHash)

	hermezReader := hermez_db.NewHermezDbReader(tx)
	for _, txn := range txs {
		msg, err := txn.AsMessage(*signer, nil, rules)
		msg.SetCheckNonce(false)
		if err != nil {
			return nil, err
		}

		// get the effective gas price percentage and apply it to the message
		effectiveGasPricePercentage, err := hermezReader.GetEffectiveGasPricePercentage(txn.Hash())
		if err != nil {
			return nil, err
		}
		msg.SetEffectiveGasPricePercentage(effectiveGasPricePercentage)

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
	ret["bundleHash"] = hexutility.Encode(bundleHash.Sum(nil))
	return ret, nil
}
