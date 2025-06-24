package stages

import (
	"context"
	"errors"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"

	"io"

	"github.com/erigontech/erigon-lib/log/v3"
	types2 "github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
)

func getLimboTransaction(ctx context.Context, cfg SequenceBlockCfg, txHash *common.Hash, executionAt uint64) ([]types.Transaction, error) {
	var transactions []types.Transaction
	// ensure we don't spin forever looking for transactions, attempt for a while then exit up to the caller
	if err := cfg.txPoolDb.View(ctx, func(poolTx kv.Tx) error {
		slots, err := cfg.txPool.GetLimboTxRplsByHash(poolTx, txHash)
		if err != nil {
			return err
		}

		if slots != nil {
			// ignore the toRemove value here, we know the RLP will be sound as we had to read it from the pool
			// in the first place to get it into limbo
			transactions, _, _, err = extractTransactionsFromSlot(slots, executionAt, cfg)
			if err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return transactions, nil
}

func extractTransactionsFromSlot(slot *types2.TxsRlp, currentHeight uint64, cfg SequenceBlockCfg) ([]types.Transaction, []common.Hash, []common.Hash, error) {
	ids := make([]common.Hash, 0, len(slot.TxIds))
	transactions := make([]types.Transaction, 0, len(slot.Txs))
	toRemove := make([]common.Hash, 0)

	for idx, txBytes := range slot.Txs {
		var err error = nil
		var transaction types.Transaction
		txPtr, found := cfg.decodedTxCache.Get(slot.TxIds[idx])
		if !found {
			transaction, err = types.DecodeTransaction(txBytes)
			if err == io.EOF {
				continue
			}
			if err != nil {
				// we have a transaction that cannot be decoded or a similar issue.  We don't want to handle
				// this tx so just WARN about it and remove it from the pool and continue
				log.Warn("[extractTransaction] Failed to decode transaction from pool, skipping and removing from pool",
					"error", err,
					"id", slot.TxIds[idx])
				toRemove = append(toRemove, slot.TxIds[idx])
				continue
			}
			cfg.decodedTxCache.Add(slot.TxIds[idx], &transaction)
		} else {
			transaction = *txPtr
		}

		// Recover sender later only for those transactions that are included in the block
		transactions = append(transactions, transaction)
		ids = append(ids, slot.TxIds[idx])
	}
	return transactions, ids, toRemove, nil
}

type overflowType uint8

const (
	overflowNone overflowType = iota
	overflowCounters
	overflowGas
)

func attemptAddTransaction(
	cfg SequenceBlockCfg,
	sdb *stageDb,
	ibs *state.IntraBlockState,
	batchCounters *vm.BatchCounterCollector,
	blockContext *evmtypes.BlockContext,
	header *types.Header,
	transaction types.Transaction,
	effectiveGasPrice uint8,
	l1Recovery bool,
	forkId, l1InfoIndex uint64,
	blockDataSizeChecker *BlockDataChecker,
	ethBlockGasPool *core.GasPool,
) (*types.Receipt, *evmtypes.ExecutionResult, *vm.TransactionCounter, overflowType, error) {
	var batchDataOverflow, overflow bool
	var err error

	txCounters := vm.NewTransactionCounter(transaction, sdb.smt.GetDepth(), uint16(forkId), cfg.zk.VirtualCountersSmtReduction, cfg.zk.ShouldCountersBeUnlimited(l1Recovery))
	overflow, err = batchCounters.AddNewTransactionCounters(txCounters)

	// run this only once the first time, do not add it on rerun
	if blockDataSizeChecker != nil {
		txL2Data, err := txCounters.GetL2DataCache()
		if err != nil {
			return nil, nil, txCounters, overflowNone, err
		}
		batchDataOverflow = blockDataSizeChecker.AddTransactionData(txL2Data)
		if batchDataOverflow {
			log.Info("BatchL2Data limit reached. Not adding last transaction", "txHash", transaction.Hash())
		}
	}
	if err != nil {
		return nil, nil, txCounters, overflowNone, err
	}
	anyOverflow := overflow || batchDataOverflow
	if anyOverflow && !l1Recovery {
		log.Debug("Transaction preexecute overflow detected", "txHash", transaction.Hash(), "counters", batchCounters.CombineCollectorsNoChanges().UsedAsString())
		return nil, nil, txCounters, overflowCounters, nil
	}

	// if not normalcy we want to create a gas pool per transaction (zkevm block gas limit is infinite), if normalcy create a pool per block.
	var gasPool *core.GasPool
	if !cfg.chainConfig.IsNormalcy(blockContext.BlockNumber) {
		gasPool = new(core.GasPool).AddGas(transactionGasLimit)
	} else {
		gasPool = ethBlockGasPool
	}

	// set the counter collector on the config so that we can gather info during the execution
	cfg.zkVmConfig.CounterCollector = txCounters.ExecutionCounters()

	// TODO: possibly inject zero tracer here!

	snapshot := ibs.Snapshot()
	ibs.Init(transaction.Hash(), common.Hash{}, 0)

	evm := vm.NewZkEVM(*blockContext, evmtypes.TxContext{}, ibs, cfg.chainConfig, *cfg.zkVmConfig)

	gasUsed := header.GasUsed

	receipt, execResult, err := core.ApplyTransaction_zkevm(
		cfg.chainConfig,
		cfg.engine,
		evm,
		gasPool,
		ibs,
		noop,
		header,
		transaction,
		&gasUsed,
		effectiveGasPrice,
		false,
	)

	if err != nil {
		if errors.Is(err, core.ErrGasLimitReached) {
			log.Debug("Transaction gas limit reached", "txHash", transaction.Hash())
			return nil, nil, txCounters, overflowGas, nil
		}
		return nil, nil, txCounters, overflowNone, err
	}

	if err = txCounters.ProcessTx(ibs, execResult.ReturnData); err != nil {
		return nil, nil, txCounters, overflowNone, err
	}

	batchCounters.UpdateExecutionAndProcessingCountersCache(txCounters)
	// now that we have executed we can check again for an overflow
	if overflow, err = batchCounters.CheckForOverflow(l1InfoIndex != 0); err != nil {
		return nil, nil, txCounters, overflowNone, err
	}

	if overflow {
		counters := batchCounters.CombineCollectorsNoChanges().UsedAsString()
		log.Debug("Transaction overflow detected", "txHash", transaction.Hash(), "counters", counters)
		ibs.RevertToSnapshot(snapshot)
		return nil, nil, txCounters, overflowCounters, nil
	}
	if gasUsed > header.GasLimit {
		log.Debug("Transaction overflows block gas limit", "txHash", transaction.Hash(), "txGas", receipt.GasUsed, "blockGasUsed", header.GasUsed)
		ibs.RevertToSnapshot(snapshot)
		return nil, nil, txCounters, overflowGas, nil
	}

	// add the gas only if not reverted. This should not be moved above the overflow check
	header.GasUsed = gasUsed

	// we need to keep hold of the effective percentage used
	// todo [zkevm] for now we're hard coding to the max value but we need to calc this properly
	if err = sdb.hermezDb.WriteEffectiveGasPricePercentage(transaction.Hash(), effectiveGasPrice); err != nil {
		return nil, nil, txCounters, overflowNone, err
	}

	ibs.FinalizeTx(evm.ChainRules(), noop)

	return receipt, execResult, txCounters, overflowNone, nil
}
