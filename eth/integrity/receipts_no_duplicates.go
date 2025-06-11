package integrity

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

func ReceiptsNoDuplicates(ctx context.Context, db kv.TemporalRoDB, blockReader services.FullBlockReader, failFast bool) (err error) {
	defer func() {
		log.Info("[integrity] ReceiptsNoDuplicates: done", "err", err)
	}()

	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	fromBlock := uint64(1)
	stageExecProgress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	toBlock := stageExecProgress

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.TxBlockIndexFromBlockReader(ctx, blockReader))
	fromTxNum, err := txNumsReader.Min(tx, fromBlock)
	if err != nil {
		return err
	}
	if toBlock > 0 {
		toBlock-- // [fromBlock,toBlock)
	}

	ac := state.AggTx(tx)
	toTxNum, err := txNumsReader.Max(tx, toBlock)
	if err != nil {
		return err
	}
	log.Info("[integrity] ReceiptsNoDuplicates starting", "fromTxNum", fromTxNum, "toTxNum", toTxNum)

	{
		receiptProgress := ac.HistoryProgress(kv.ReceiptDomain, tx)
		accProgress := ac.HistoryProgress(kv.AccountsDomain, tx)
		if accProgress != receiptProgress {
			err := fmt.Errorf("[integrity] ReceiptDomain=%d is behind AccountDomain=%d", receiptProgress, accProgress)
			log.Warn(err.Error())
		}
	}

	wg := errgroup.Group{}
	wg.SetLimit(estimate.AlmostAllCPUs())

	batchSize := uint64(50_000)
	for bn := fromBlock; bn < toBlock; bn += batchSize {
		wg.Go(func() error {
			if err := receiptsNoDuplicatesRange(ctx, bn, min(bn+batchSize, toBlock), tx, blockReader, failFast); err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				log.Info("[integrity] ReceiptsNoDuplicates", "progress", fmt.Sprintf("%dk/%dk", bn/1_000, toBlock/1_000))
			default:
			}
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return nil
	}

	return nil
}
func receiptsNoDuplicatesRange(ctx context.Context, fromTxNum, toTxNum uint64, tx kv.TemporalTx, blockReader services.FullBlockReader, failFast bool) (err error) {
	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.TxBlockIndexFromBlockReader(ctx, blockReader))
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	prevCumGasUsed := -1
	prevBN := uint64(1)
	log.Info("[integrity] ReceiptsNoDuplicates starting", "fromTxNum", fromTxNum, "toTxNum", toTxNum)

	var cumGasUsed uint64
	for txNum := fromTxNum; txNum <= toTxNum; txNum++ {
		cumGasUsed, _, _, err = rawtemporaldb.ReceiptAsOf(tx, txNum)
		if err != nil {
			return err
		}
		blockNum := badFoundBlockNum(tx, prevBN-1, txNumsReader, txNum)
		blockChanged := blockNum != prevBN
		_min, _ := txNumsReader.Min(tx, blockNum)
		_max, _ := txNumsReader.Max(tx, blockNum)

		lastSystemTxn := txNum == _max
		empyBlock := _max-_min <= 1
		if lastSystemTxn || empyBlock {
			prevCumGasUsed = int(cumGasUsed)
			prevBN = blockNum
			continue
		}
		_ = blockChanged
		//duplicateInsideBlock := !blockChanged && int(cumGasUsed) == prevCumGasUsed
		duplicateInsideBlock := int(cumGasUsed) <= prevCumGasUsed && blockNum == prevBN
		if duplicateInsideBlock && cumGasUsed != 0 {
			err := fmt.Errorf("assert: duplicate receipt at txnum: %d, block: %d(%d-%d), cumGasUsed=%d, prevCumGasUsed=%d", txNum, blockNum, _min, _max, cumGasUsed, prevCumGasUsed)
			return err
		}

		prevCumGasUsed = int(cumGasUsed)
		prevBN = blockNum

		select {
		case <-ctx.Done():
			return
		case <-logEvery.C:
			log.Info("[integrity] ReceiptsNoDuplicates", "progress", fmt.Sprintf("%dk/%dk", blockNum/1_000, toBlock/1_000))
		default:
		}
	}

	return nil
}

func badFoundBlockNum(tx kv.Tx, fromBlock uint64, txNumsReader rawdbv3.TxNumsReader, curTxNum uint64) uint64 {
	txNumMax, _ := txNumsReader.Max(tx, fromBlock)
	i := uint64(0)
	for txNumMax < curTxNum {
		i++
		txNumMax, _ = txNumsReader.Max(tx, fromBlock+i)
	}
	return fromBlock + i
}
