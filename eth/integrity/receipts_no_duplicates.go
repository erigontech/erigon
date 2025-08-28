package integrity

import (
	"context"
	"fmt"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/turbo/services"
)

// CheckReceiptsNoDups performs integrity checks on receipts to ensure no duplicates exist.
// This function uses parallel processing for improved performance.
func CheckReceiptsNoDups(ctx context.Context, db kv.TemporalRoDB, blockReader services.FullBlockReader, failFast bool) (err error) {
	defer func() {
		log.Info("[integrity] ReceiptsNoDups: done", "err", err)
	}()

	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	txNumsReader := blockReader.TxnumReader(ctx)

	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	receiptProgress := tx.Debug().DomainProgress(kv.ReceiptDomain)

	fromBlock := uint64(1)
	toBlock, _, _ := txNumsReader.FindBlockNum(tx, receiptProgress)

	{
		log.Info("[integrity] ReceiptsNoDups starting", "fromBlock", fromBlock, "toBlock", toBlock)
		accProgress := tx.Debug().DomainProgress(kv.AccountsDomain)
		if accProgress != receiptProgress {
			err := fmt.Errorf("[integrity] ReceiptDomain=%d is behind AccountDomain=%d", receiptProgress, accProgress)
			log.Warn(err.Error())
		}
	}
	tx.Rollback()

	return parallelChunkCheck(ctx, fromBlock, toBlock, db, blockReader, failFast, ReceiptsNoDupsRange)
}

func ReceiptsNoDupsRange(ctx context.Context, fromBlock, toBlock uint64, tx kv.TemporalTx, blockReader services.FullBlockReader, failFast bool) (err error) {
	txNumsReader := blockReader.TxnumReader(ctx)
	fromTxNum, err := txNumsReader.Min(tx, fromBlock)
	if err != nil {
		return err
	}

	if toBlock > 0 {
		toBlock-- // [fromBlock,toBlock)
	}

	toTxNum, err := txNumsReader.Max(tx, toBlock)
	if err != nil {
		return err
	}

	prevCumUsedGas := -1
	prevLogIdxAfterTx := uint32(0)
	blockNum := fromBlock
	var _min, _max uint64
	_min, _ = txNumsReader.Min(tx, fromBlock)
	_max, _ = txNumsReader.Max(tx, fromBlock)
	for txNum := fromTxNum; txNum <= toTxNum; txNum++ {
		cumUsedGas, _, logIdxAfterTx, err := rawtemporaldb.ReceiptAsOf(tx, txNum+1)
		if err != nil {
			return err
		}

		blockChanged := txNum == _min
		if blockChanged {
			prevCumUsedGas = 0
			prevLogIdxAfterTx = 0
		}

		strongMonotonicCumGasUsed := int(cumUsedGas) > prevCumUsedGas
		if !strongMonotonicCumGasUsed && txNum != _min && txNum != _max { // system tx can be skipped
			err := fmt.Errorf("CheckReceiptsNoDups: non-monotonic cumGasUsed at txnum: %d, block: %d(%d-%d), cumGasUsed=%d, prevCumGasUsed=%d", txNum, blockNum, _min, _max, cumUsedGas, prevCumUsedGas)
			if failFast {
				return err
			}
			log.Error(err.Error())
		}

		monotonicLogIdx := logIdxAfterTx >= prevLogIdxAfterTx
		if !monotonicLogIdx && txNum != _min && txNum != _max {
			err := fmt.Errorf("CheckReceiptsNoDups: non-monotonic logIndex at txnum: %d, block: %d(%d-%d), logIdxAfterTx=%d, prevLogIdxAfterTx=%d", txNum, blockNum, _min, _max, logIdxAfterTx, prevLogIdxAfterTx)
			if failFast {
				return err
			}
			log.Error(err.Error())
		}

		prevCumUsedGas = int(cumUsedGas)
		prevLogIdxAfterTx = logIdxAfterTx

		if txNum == _max {
			blockNum++
			_min = _max + 1
			_max, _ = txNumsReader.Max(tx, blockNum)
		}

		if txNum%1000 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	}
	return nil
}
