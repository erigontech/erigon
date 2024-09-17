package stages

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"io"

	mapset "github.com/deckarep/golang-set/v2"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/log/v3"
)

func getNextPoolTransactions(ctx context.Context, cfg SequenceBlockCfg, executionAt, forkId uint64, alreadyYielded mapset.Set[[32]byte]) ([]types.Transaction, bool, error) {
	cfg.txPool.LockFlusher()
	defer cfg.txPool.UnlockFlusher()

	var transactions []types.Transaction
	var allConditionsOk bool
	var err error

	gasLimit := utils.GetBlockGasLimitForFork(forkId)

	if err := cfg.txPoolDb.View(ctx, func(poolTx kv.Tx) error {
		slots := types2.TxsRlp{}
		if allConditionsOk, _, err = cfg.txPool.YieldBest(cfg.yieldSize, &slots, poolTx, executionAt, gasLimit, 0, alreadyYielded); err != nil {
			return err
		}
		yieldedTxs, err := extractTransactionsFromSlot(&slots)
		if err != nil {
			return err
		}
		transactions = append(transactions, yieldedTxs...)
		return nil
	}); err != nil {
		return nil, allConditionsOk, err
	}

	return transactions, allConditionsOk, err
}

func getLimboTransaction(ctx context.Context, cfg SequenceBlockCfg, txHash *common.Hash) ([]types.Transaction, error) {
	cfg.txPool.LockFlusher()
	defer cfg.txPool.UnlockFlusher()

	var transactions []types.Transaction
	// ensure we don't spin forever looking for transactions, attempt for a while then exit up to the caller
	if err := cfg.txPoolDb.View(ctx, func(poolTx kv.Tx) error {
		slots, err := cfg.txPool.GetLimboTxRplsByHash(poolTx, txHash)
		if err != nil {
			return err
		}

		if slots != nil {
			transactions, err = extractTransactionsFromSlot(slots)
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

func extractTransactionsFromSlot(slot *types2.TxsRlp) ([]types.Transaction, error) {
	transactions := make([]types.Transaction, 0, len(slot.Txs))
	for idx, txBytes := range slot.Txs {
		transaction, err := types.DecodeTransaction(txBytes)
		if err == io.EOF {
			continue
		}
		if err != nil {
			return nil, err
		}
		var sender common.Address
		copy(sender[:], slot.Senders.At(idx))
		transaction.SetSender(sender)
		transactions = append(transactions, transaction)
	}
	return transactions, nil
}

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
) (*types.Receipt, *core.ExecutionResult, bool, error) {
	var batchDataOverflow, overflow bool
	var err error

	txCounters := vm.NewTransactionCounter(transaction, sdb.smt.GetDepth(), uint16(forkId), cfg.zk.VirtualCountersSmtReduction, cfg.zk.ShouldCountersBeUnlimited(l1Recovery))
	overflow, err = batchCounters.AddNewTransactionCounters(txCounters)

	// run this only once the first time, do not add it on rerun
	if blockDataSizeChecker != nil {
		txL2Data, err := txCounters.GetL2DataCache()
		if err != nil {
			return nil, nil, false, err
		}
		batchDataOverflow = blockDataSizeChecker.AddTransactionData(txL2Data)
		if batchDataOverflow {
			log.Info("BatchL2Data limit reached. Not adding last transaction", "txHash", transaction.Hash())
		}
	}
	if err != nil {
		return nil, nil, false, err
	}
	anyOverflow := overflow || batchDataOverflow
	if anyOverflow && !l1Recovery {
		log.Debug("Transaction preexecute overflow detected", "txHash", transaction.Hash(), "coutners", batchCounters.CombineCollectorsNoChanges().UsedAsString())
		return nil, nil, true, nil
	}

	gasPool := new(core.GasPool).AddGas(transactionGasLimit)

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
		return nil, nil, false, err
	}

	if err = txCounters.ProcessTx(ibs, execResult.ReturnData); err != nil {
		return nil, nil, false, err
	}

	batchCounters.UpdateExecutionAndProcessingCountersCache(txCounters)
	// now that we have executed we can check again for an overflow
	if overflow, err = batchCounters.CheckForOverflow(l1InfoIndex != 0); err != nil {
		return nil, nil, false, err
	}

	counters := batchCounters.CombineCollectorsNoChanges().UsedAsString()
	if overflow {
		log.Debug("Transaction overflow detected", "txHash", transaction.Hash(), "coutners", counters)
		ibs.RevertToSnapshot(snapshot)
		return nil, nil, true, nil
	}
	log.Debug("Transaction added", "txHash", transaction.Hash(), "coutners", counters)

	// add the gas only if not reverted. This should not be moved above the overflow check
	header.GasUsed = gasUsed

	// we need to keep hold of the effective percentage used
	// todo [zkevm] for now we're hard coding to the max value but we need to calc this properly
	if err = sdb.hermezDb.WriteEffectiveGasPricePercentage(transaction.Hash(), effectiveGasPrice); err != nil {
		return nil, nil, false, err
	}

	ibs.FinalizeTx(evm.ChainRules(), noop)

	return receipt, execResult, false, nil
}
