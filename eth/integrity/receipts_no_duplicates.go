package integrity

import (
	"context"
	"fmt"
	"time"

	"github.com/erigontech/erigon-db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
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

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.TxBlockIndexFromBlockReader(ctx, blockReader))

	receiptDomainProgress := tx.Debug().DomainProgress(kv.ReceiptDomain)

	fromBlock := uint64(1)
	toBlock, _, _ := txNumsReader.FindBlockNum(tx, receiptDomainProgress)

	{
		log.Info("[integrity] ReceiptsNoDuplicates starting", "fromBlock", fromBlock, "toBlock", toBlock)
		receiptProgress := tx.Debug().DomainProgress(kv.ReceiptDomain)
		accProgress := tx.Debug().DomainProgress(kv.AccountsDomain)
		if accProgress != receiptProgress {
			err := fmt.Errorf("[integrity] ReceiptDomain=%d is behind AccountDomain=%d", receiptProgress, accProgress)
			log.Warn(err.Error())
		}
	}

	if err := ReceiptsNoDuplicatesRange(ctx, fromBlock, toBlock, tx, blockReader, failFast); err != nil {
		return err
	}
	return nil
}

func ReceiptsNoDuplicatesRange(ctx context.Context, fromBlock, toBlock uint64, tx kv.TemporalTx, blockReader services.FullBlockReader, failFast bool) (err error) {
	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.TxBlockIndexFromBlockReader(ctx, blockReader))
	fromTxNum, err := txNumsReader.Min(tx, fromBlock)
	if err != nil {
		return err
	}
	if fromTxNum < 2 {
		fromTxNum = 2 //i don't remember why need this
	}

	if toBlock > 0 {
		toBlock-- // [fromBlock,toBlock)
	}

	toTxNum, err := txNumsReader.Max(tx, toBlock)
	if err != nil {
		return err
	}

	prevCumUsedGas := -1
	prevLogIdx := uint32(0)
	prevBN := uint64(1)
	for txNum := fromTxNum; txNum <= toTxNum; txNum++ {
		cumUsedGas, _, logIdx, err := rawtemporaldb.ReceiptAsOf(tx, txNum)
		if err != nil {
			return err
		}
		blockNum := badFoundBlockNum(tx, prevBN-1, txNumsReader, txNum)
		_min, _ := txNumsReader.Min(tx, blockNum)
		blockChanged := txNum == _min
		if blockChanged {
			prevCumUsedGas = 0
			prevLogIdx = 0
		}

		_max, _ := txNumsReader.Max(tx, blockNum)

		strongMonotonicCumGasUsed := int(cumUsedGas) > prevCumUsedGas
		if !strongMonotonicCumGasUsed && cumUsedGas != 0 {
			err := fmt.Errorf("ReceiptsNoDuplicates: non-monotonic cumGasUsed at txnum: %d, block: %d(%d-%d), cumGasUsed=%d, prevCumGasUsed=%d", txNum, blockNum, _min, _max, cumUsedGas, prevCumUsedGas)
			if failFast {
				return err
			}
			log.Error(err.Error())
		}

		monotonicLogIdx := logIdx >= prevLogIdx
		if !monotonicLogIdx {
			err := fmt.Errorf("ReceiptsNoDuplicates: non-monotonic logIndex at txnum: %d, block: %d(%d-%d), logIdx=%d, prevLogIdx=%d", txNum, blockNum, _min, _max, logIdx, prevLogIdx)
			if failFast {
				return err
			}
			log.Error(err.Error())
		}

		prevCumUsedGas = int(cumUsedGas)
		prevLogIdx = logIdx
		prevBN = blockNum

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			log.Info("[integrity] ReceiptsNoDuplicates", "progress", fmt.Sprintf("%.1fm/%.1fm", float64(txNum)/1_000_000, float64(toTxNum)/1_000_000))
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
