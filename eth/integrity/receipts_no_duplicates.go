package integrity

import (
	"context"
	"fmt"
	"time"

	"github.com/erigontech/erigon-db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
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

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, blockReader))
	fromTxNum, err := txNumsReader.Min(tx, fromBlock)
	if err != nil {
		return err
	}
	if toBlock > 0 {
		toBlock-- // [fromBlock,toBlock)
	}

	ac := tx.AggTx().(*state.AggregatorRoTx)
	//toTxNum := ac.DbgDomain(kv.ReceiptDomain).DbgMaxTxNumInDB(tx)
	toTxNum, err := txNumsReader.Max(tx, toBlock)
	if err != nil {
		return err
	}
	prevCumGasUsed := -1
	prevBN := uint64(1)
	log.Info("[integrity] ReceiptsNoDuplicates starting", "fromTxNum", fromTxNum, "toTxNum", toTxNum)

	{
		receiptProgress := ac.HistoryProgress(kv.ReceiptDomain, tx)
		accProgress := ac.HistoryProgress(kv.AccountsDomain, tx)
		if accProgress != receiptProgress {
			err := fmt.Errorf("[integrity] ReceiptDomain=%d is behind AccountDomain=%d", receiptProgress, accProgress)
			log.Warn(err.Error())
		}
	}

	var cumGasUsed uint64
	for txNum := fromTxNum; txNum <= toTxNum; txNum++ {
		cumGasUsed, _, _, err = rawtemporaldb.ReceiptAsOf(tx, txNum)
		if err != nil {
			return err
		}
		//_, blockNum, _ := txNumsReader.FindBlockNum(tx, txNum)
		blockNum := badFoundBlockNum(tx, prevBN-1, txNumsReader, txNum)
		//fmt.Printf("[dbg] cumGasUsed=%d, txNum=%d, blockNum=%d, prevCumGasUsed=%d\n", cumGasUsed, txNum, blockNum, prevCumGasUsed)
		if int(cumGasUsed) == prevCumGasUsed && cumGasUsed != 0 && blockNum == prevBN {
			err := fmt.Errorf("bad receipt at txnum: %d, block: %d, cumGasUsed=%d, prevCumGasUsed=%d", txNum, blockNum, cumGasUsed, prevCumGasUsed)
			panic(err)
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
