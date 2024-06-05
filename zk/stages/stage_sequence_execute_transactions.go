package stages

import (
	"context"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/length"
	"github.com/gateway-fm/cdk-erigon-lib/kv"

	"bytes"
	"io"

	mapset "github.com/deckarep/golang-set/v2"
	types2 "github.com/gateway-fm/cdk-erigon-lib/types"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"errors"
	"github.com/ledgerwatch/erigon/zk/constants"
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
			_, count, err = cfg.txPool.YieldBest(yieldSize, &slots, poolTx, executionAt, getGasLimit(forkId), alreadyYielded)
			if err != nil {
				return err
			}
			if count == 0 {
				time.Sleep(500 * time.Microsecond)
				return nil
			}
			transactions, err = extractTransactionsFromSlot(slots)
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

func getNextL1BatchData(batchNumber uint64, forkId uint64, hermezDb *hermez_db.HermezDb) ([]zktx.DecodedBatchL2Data, common.Address, common.Hash, bool, error) {
	// we expect that the batch we're going to load in next should be in the db already because of the l1 block sync
	// stage, if it is not there we need to panic as we're in a bad state
	batchL2Data, err := hermezDb.GetL1BatchData(batchNumber)
	if err != nil {
		return nil, common.Address{}, common.Hash{}, true, err
	}

	if len(batchL2Data) == 0 {
		// end of the line for batch recovery so return empty
		return nil, common.Address{}, common.Hash{}, false, nil
	}

	coinbase := common.BytesToAddress(batchL2Data[:length.Addr])
	l1InfoRoot := common.BytesToHash(batchL2Data[length.Addr : length.Addr+length.Hash])
	batchL2Data = batchL2Data[length.Addr+length.Hash:]

	decodedBlockData, err := zktx.DecodeBatchL2Blocks(batchL2Data, forkId)
	if err != nil {
		return nil, common.Address{}, common.Hash{}, true, err
	}

	// no data means no more work to do - end of the line
	if len(decodedBlockData) == 0 {
		return nil, common.Address{}, common.Hash{}, false, nil
	}

	isWorkRemaining := true
	transactionsInBatch := 0
	for _, batch := range decodedBlockData {
		transactionsInBatch += len(batch.Transactions)
	}
	if transactionsInBatch == 0 {
		// we need to check if this batch should simply be empty or not so we need to check against the
		// highest known batch number to see if we have work to do still
		highestKnown, err := hermezDb.GetLastL1BatchData()
		if err != nil {
			return nil, common.Address{}, common.Hash{}, true, err
		}
		if batchNumber >= highestKnown {
			isWorkRemaining = false
		}
	}

	return decodedBlockData, coinbase, l1InfoRoot, isWorkRemaining, err
}

func extractTransactionsFromSlot(slot types2.TxsRlp) ([]types.Transaction, error) {
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
	forkId uint64,
) (*types.Receipt, bool, error) {
	txCounters := vm.NewTransactionCounter(transaction, sdb.smt.GetDepth(), cfg.zk.ShouldCountersBeUnlimited(l1Recovery))
	overflow, err := batchCounters.AddNewTransactionCounters(txCounters)
	if err != nil {
		return nil, false, err
	}
	if overflow && !l1Recovery {
		return nil, true, nil
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
		return nil, false, err
	}

	if forkId <= uint64(constants.ForkID7Etrog) && errors.Is(execResult.Err, vm.ErrUnsupportedPrecompile) {
		receipt.Status = 1
	}

	// we need to keep hold of the effective percentage used
	// todo [zkevm] for now we're hard coding to the max value but we need to calc this properly
	if err = sdb.hermezDb.WriteEffectiveGasPricePercentage(transaction.Hash(), effectiveGasPrice); err != nil {
		return nil, false, err
	}

	err = txCounters.ProcessTx(ibs, execResult.ReturnData)
	if err != nil {
		return nil, false, err
	}

	// now that we have executed we can check again for an overflow
	overflow, err = batchCounters.CheckForOverflow()

	return receipt, overflow, err
}
