package stages

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"

	"bytes"
	"io"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/gateway-fm/cdk-erigon-lib/common/length"
	types2 "github.com/gateway-fm/cdk-erigon-lib/types"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/log/v3"
)

func getNextPoolTransactions(cfg SequenceBlockCfg, executionAt, forkId uint64, alreadyYielded mapset.Set[[32]byte]) ([]types.Transaction, error) {
	var transactions []types.Transaction
	var err error
	var count int
	killer := time.NewTicker(50 * time.Millisecond)
LOOP:
	for {
		// ensure we don't spin forever looking for transactions, attempt for a while then exit up to the caller
		select {
		case <-killer.C:
			break LOOP
		default:
		}
		if err := cfg.txPoolDb.View(context.Background(), func(poolTx kv.Tx) error {
			slots := types2.TxsRlp{}
			_, count, err = cfg.txPool.YieldBest(yieldSize, &slots, poolTx, executionAt, utils.GetBlockGasLimitForFork(forkId), alreadyYielded)
			if err != nil {
				return err
			}
			if count == 0 {
				time.Sleep(500 * time.Microsecond)
				return nil
			}
			transactions, err = extractTransactionsFromSlot(&slots)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return nil, err
		}

		if len(transactions) > 0 {
			break
		}
	}

	return transactions, err
}

func getLimboTransaction(cfg SequenceBlockCfg, txHash *common.Hash) ([]types.Transaction, error) {
	var transactions []types.Transaction

	for {
		// ensure we don't spin forever looking for transactions, attempt for a while then exit up to the caller
		if err := cfg.txPoolDb.View(context.Background(), func(poolTx kv.Tx) error {
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

		if len(transactions) == 0 {
			time.Sleep(250 * time.Millisecond)
		} else {
			break
		}

	}

	return transactions, nil
}

func getNextL1BatchData(batchNumber uint64, forkId uint64, hermezDb *hermez_db.HermezDb) (*nextBatchL1Data, error) {
	nextData := &nextBatchL1Data{}
	// we expect that the batch we're going to load in next should be in the db already because of the l1 block sync
	// stage, if it is not there we need to panic as we're in a bad state
	batchL2Data, err := hermezDb.GetL1BatchData(batchNumber)
	if err != nil {
		return nextData, err
	}

	if len(batchL2Data) == 0 {
		// end of the line for batch recovery so return empty
		return nextData, nil
	}

	nextData.Coinbase = common.BytesToAddress(batchL2Data[:length.Addr])
	nextData.L1InfoRoot = common.BytesToHash(batchL2Data[length.Addr : length.Addr+length.Hash])
	tsBytes := batchL2Data[length.Addr+length.Hash : length.Addr+length.Hash+8]
	nextData.LimitTimestamp = binary.BigEndian.Uint64(tsBytes)
	batchL2Data = batchL2Data[length.Addr+length.Hash+8:]

	nextData.DecodedData, err = zktx.DecodeBatchL2Blocks(batchL2Data, forkId)
	if err != nil {
		return nextData, err
	}

	// no data means no more work to do - end of the line
	if len(nextData.DecodedData) == 0 {
		return nextData, nil
	}

	nextData.IsWorkRemaining = true
	transactionsInBatch := 0
	for _, batch := range nextData.DecodedData {
		transactionsInBatch += len(batch.Transactions)
	}
	if transactionsInBatch == 0 {
		// we need to check if this batch should simply be empty or not so we need to check against the
		// highest known batch number to see if we have work to do still
		highestKnown, err := hermezDb.GetLastL1BatchData()
		if err != nil {
			return nextData, err
		}
		if batchNumber >= highestKnown {
			nextData.IsWorkRemaining = false
		}
	}

	return nextData, err
}

func extractTransactionsFromSlot(slot *types2.TxsRlp) ([]types.Transaction, error) {
	transactions := make([]types.Transaction, 0, len(slot.Txs))
	reader := bytes.NewReader([]byte{})
	stream := new(rlp.Stream)
	for idx, txBytes := range slot.Txs {
		reader.Reset(txBytes)
		stream.Reset(reader, uint64(len(txBytes)))
		transaction, err := types.DecodeTransaction(stream)
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
	txCounters := vm.NewTransactionCounter(transaction, sdb.smt.GetDepth(), uint16(forkId), cfg.zk.VirtualCountersSmtReduction, cfg.zk.ShouldCountersBeUnlimited(l1Recovery))
	overflow, err := batchCounters.AddNewTransactionCounters(txCounters)

	// run this only once the first time, do not add it on rerun
	var batchDataOverflow bool
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
		return nil, nil, true, nil
	}

	gasPool := new(core.GasPool).AddGas(transactionGasLimit)

	// set the counter collector on the config so that we can gather info during the execution
	cfg.zkVmConfig.CounterCollector = txCounters.ExecutionCounters()

	// TODO: possibly inject zero tracer here!

	ibs.Prepare(transaction.Hash(), common.Hash{}, 0)
	evm := vm.NewZkEVM(*blockContext, evmtypes.TxContext{}, ibs, cfg.chainConfig, *cfg.zkVmConfig)

	receipt, execResult, err := core.ApplyTransaction_zkevm(
		cfg.chainConfig,
		cfg.engine,
		evm,
		gasPool,
		ibs,
		noop,
		header,
		transaction,
		&header.GasUsed,
		effectiveGasPrice,
	)

	if err != nil {
		return nil, nil, false, err
	}

	// we need to keep hold of the effective percentage used
	// todo [zkevm] for now we're hard coding to the max value but we need to calc this properly
	if err = sdb.hermezDb.WriteEffectiveGasPricePercentage(transaction.Hash(), effectiveGasPrice); err != nil {
		return nil, nil, false, err
	}

	err = txCounters.ProcessTx(ibs, execResult.ReturnData)
	if err != nil {
		return nil, nil, false, err
	}

	// now that we have executed we can check again for an overflow
	overflow, err = batchCounters.CheckForOverflow(l1InfoIndex != 0)

	return receipt, execResult, overflow, err
}
