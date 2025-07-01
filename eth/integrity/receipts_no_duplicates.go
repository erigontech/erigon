package integrity

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/core/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/turbo/services"
	"golang.org/x/sync/errgroup"
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

	receiptDomainProgress := state.AggTx(tx).HistoryProgress(kv.ReceiptDomain, tx)

	fromBlock := uint64(1)
	toBlock, _, _ := txNumsReader.FindBlockNum(tx, receiptDomainProgress)

	{
		ac := state.AggTx(tx)
		log.Info("[integrity] ReceiptsNoDups starting", "fromBlock", fromBlock, "toBlock", toBlock)
		receiptProgress := ac.HistoryProgress(kv.ReceiptDomain, tx)
		accProgress := ac.HistoryProgress(kv.AccountsDomain, tx)
		if accProgress != receiptProgress {
			err := fmt.Errorf("[integrity] ReceiptDomain=%d is behind AccountDomain=%d", receiptProgress, accProgress)
			log.Warn(err.Error())
		}
	}
	tx.Rollback()

	if err := receiptsNoDupsRangeParallel(ctx, fromBlock, toBlock, db, blockReader, failFast); err != nil {
		return err
	}
	return nil
}

func receiptsNoDupsRangeParallel(ctx context.Context, fromBlock, toBlock uint64, db kv.TemporalRoDB, blockReader services.FullBlockReader, failFast bool) (err error) {
	blockRange := toBlock - fromBlock + 1
	if blockRange == 0 {
		return nil
	}

	numWorkers := runtime.NumCPU()
	chunkSize := uint64(1000)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(numWorkers)
	var completedChunks atomic.Uint64
	var totalChunks uint64 = (blockRange + chunkSize - 1) / chunkSize
	log.Info("[integrity] ReceiptsNoDups using parallel processing", "workers", numWorkers, "chunkSize", chunkSize, "blockRange", blockRange)

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-logEvery.C:
				completed := completedChunks.Load()
				progress := float64(completed) / float64(totalChunks) * 100
				log.Info("[integrity] CheckReceiptsNoDups progress", "progress", fmt.Sprintf("%.1f%%", progress))
			}
		}
	}()

	// Process chunks in parallel
	for start := fromBlock; start <= toBlock; start += chunkSize {
		end := start + chunkSize - 1
		if end > toBlock {
			end = toBlock
		}

		chunkStart := start // Capture loop variable
		chunkEnd := end     // Capture loop variable

		g.Go(func() error {
			tx, err := db.BeginTemporalRo(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback()

			chunkErr := ReceiptsNoDupsRange(ctx, chunkStart, chunkEnd, tx, blockReader, failFast)
			if chunkErr != nil {
				return chunkErr
			}

			completedChunks.Add(1)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func ReceiptsNoDupsRange(ctx context.Context, fromBlock, toBlock uint64, tx kv.TemporalTx, blockReader services.FullBlockReader, failFast bool) (err error) {
	txNumsReader := blockReader.TxnumReader(ctx)
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
	for txNum := fromTxNum; txNum <= toTxNum; txNum++ {
		cumUsedGas, _, logIdx, err := rawtemporaldb.ReceiptAsOf(tx, txNum+1)
		if err != nil {
			return err
		}
		blockNum, ok, err := txNumsReader.FindBlockNum(tx, txNum)
		if err != nil {
			return err
		}
		if !ok {
			err := fmt.Errorf("CheckReceiptsNoDups: didn't find block at txnum: %d", txNum)
			if failFast {
				return err
			}
			log.Error(err.Error())
		}
		_min, _ := txNumsReader.Min(tx, blockNum)
		blockChanged := txNum == _min
		if blockChanged {
			prevCumUsedGas = 0
			prevLogIdx = 0
		}

		_max, _ := txNumsReader.Max(tx, blockNum)

		strongMonotonicCumGasUsed := int(cumUsedGas) > prevCumUsedGas
		if !strongMonotonicCumGasUsed && txNum != _min && txNum != _max { // system tx can be skipped
			err := fmt.Errorf("CheckReceiptsNoDups: non-monotonic cumGasUsed at txnum: %d, block: %d(%d-%d), cumGasUsed=%d, prevCumGasUsed=%d", txNum, blockNum, _min, _max, cumUsedGas, prevCumUsedGas)
			if failFast {
				return err
			}
			log.Error(err.Error())
		}

		monotonicLogIdx := logIdx >= prevLogIdx
		if !monotonicLogIdx {
			err := fmt.Errorf("CheckReceiptsNoDups: non-monotonic logIndex at txnum: %d, block: %d(%d-%d), logIdx=%d, prevLogIdx=%d", txNum, blockNum, _min, _max, logIdx, prevLogIdx)
			if failFast {
				return err
			}
			log.Error(err.Error())
		}

		prevCumUsedGas = int(cumUsedGas)
		prevLogIdx = logIdx

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}
