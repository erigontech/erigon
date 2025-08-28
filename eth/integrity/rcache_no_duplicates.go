package integrity

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/turbo/services"
)

func CheckRCacheNoDups(ctx context.Context, db kv.TemporalRoDB, blockReader services.FullBlockReader, failFast bool) (err error) {
	defer func() {
		log.Info("[integrity] RCacheNoDups: done", "err", err)
	}()

	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	txNumsReader := blockReader.TxnumReader(ctx)

	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rcacheDomainProgress := tx.Debug().DomainProgress(kv.RCacheDomain)
	fromBlock := uint64(1)
	toBlock, _, _ := txNumsReader.FindBlockNum(tx, rcacheDomainProgress)

	{
		log.Info("[integrity] RCacheNoDups starting", "fromBlock", fromBlock, "toBlock", toBlock)
		accProgress := tx.Debug().DomainProgress(kv.AccountsDomain)
		diff := int(rcacheDomainProgress - accProgress)
		if diff != 0 && diff != 1 {
			// if no system tx -- nil is stored in rcache; so it might be atmost 1 ahead of accounts.
			var execProgressBlock, execStartTxNum, execEndTxNum uint64
			if execProgressBlock, err = stages.GetStageProgress(tx, stages.Execution); err != nil {
				return err
			}
			if execStartTxNum, err = txNumsReader.Min(tx, execProgressBlock); err != nil {
				return nil
			}
			if execEndTxNum, err = txNumsReader.Max(tx, execProgressBlock); err != nil {
				return nil
			}
			err := fmt.Errorf("[integrity] RCacheDomain=%d (block=%d) not equal AccountDomain=%d, while execBlockProgress(block=%d, startTxNum=%d, endTxNum=%d)", rcacheDomainProgress, toBlock, accProgress, execProgressBlock, execStartTxNum, execEndTxNum)
			log.Warn(err.Error())
			return err
		}
	}

	tx.Rollback()

	defer db.Debug().EnableReadAhead().DisableReadAhead()
	return parallelChunkCheck(ctx, fromBlock, toBlock, db, blockReader, failFast, RCacheNoDupsRange)
}

func RCacheNoDupsRange(ctx context.Context, fromBlock, toBlock uint64, tx kv.TemporalTx, blockReader services.FullBlockReader, failFast bool) (err error) {
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
	expectedFirstLogIdx := uint32(0)
	blockNum := fromBlock
	var _min, _max uint64
	_min, _ = txNumsReader.Min(tx, fromBlock)
	_max, _ = txNumsReader.Max(tx, fromBlock)
	for txNum := fromTxNum; txNum <= toTxNum; txNum++ {
		r, found, err := rawdb.ReadReceiptCacheV2(tx, rawdb.RCacheV2Query{
			TxNum:         txNum,
			BlockNum:      blockNum,
			BlockHash:     common.Hash{}, // don't care about blockHash/txnHash
			TxnHash:       common.Hash{},
			DontCalcBloom: true, // we don't need bloom for this check
		})
		if err != nil {
			return err
		}
		if !found {
			if txNum == _max {
				blockNum++
				_min = _max + 1
				_max, _ = txNumsReader.Max(tx, blockNum)
				expectedFirstLogIdx = 0
				prevCumUsedGas = -1
				continue // skip system txs
			}
			if txNum == _min {
				continue
			}
			if failFast {
				return fmt.Errorf("[integrity] RCacheNoDups: missing receipt for block %d, txNum %d", blockNum, txNum)
			}
			log.Warn("[integrity] RCacheNoDups: missing receipt", "block", blockNum, "txNum", txNum)
			continue
		}

		logIdx := r.FirstLogIndexWithinBlock
		exactLogIdx := logIdx == expectedFirstLogIdx
		if !exactLogIdx && txNum != _max {
			err := fmt.Errorf("RCacheNoDups: non-monotonic logIndex at txnum: %d, block: %d(%d-%d), logIdx=%d, expectedFirstLogIdx=%d", txNum, blockNum, _min, _max, logIdx, expectedFirstLogIdx)
			if failFast {
				return err
			}
			log.Error(err.Error())
		}
		expectedFirstLogIdx = logIdx + uint32(len(r.Logs))

		cumUsedGas := r.CumulativeGasUsed
		strongMonotonicCumGasUsed := int(cumUsedGas) > prevCumUsedGas
		if !strongMonotonicCumGasUsed && txNum != _max { // system tx can be skipped
			err := fmt.Errorf("RCacheNoDups: non-monotonic cumUsedGas at txnum: %d, block: %d(%d-%d), cumUsedGas=%d, prevCumUsedGas=%d", txNum, blockNum, _min, _max, cumUsedGas, prevCumUsedGas)
			if failFast {
				return err
			}
			log.Error(err.Error())
		}
		prevCumUsedGas = int(cumUsedGas)

		if txNum == _max {
			blockNum++
			_min = _max + 1
			_max, _ = txNumsReader.Max(tx, blockNum)
			expectedFirstLogIdx = 0
			prevCumUsedGas = -1
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

type chunkFn func(ctx context.Context, fromBlock, toBlock uint64, tx kv.TemporalTx, blockReader services.FullBlockReader, failFast bool) error

func parallelChunkCheck(ctx context.Context, fromBlock, toBlock uint64, db kv.TemporalRoDB, blockReader services.FullBlockReader, failFast bool, fn chunkFn) (err error) {
	blockRange := toBlock - fromBlock + 1
	if blockRange == 0 {
		return nil
	}

	numWorkers := runtime.NumCPU() * 5
	chunkSize := uint64(1000)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(numWorkers)
	var completedChunks atomic.Uint64
	var totalChunks uint64 = (blockRange + chunkSize - 1) / chunkSize
	log.Info("[integrity] parallel processing", "workers", numWorkers, "chunkSize", chunkSize, "blockRange", blockRange)

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
				log.Info("[integrity] progress", "progress", fmt.Sprintf("%.1f%%", progress))
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

			// chunkErr := ReceiptsNoDupsRange(ctx, chunkStart, chunkEnd, tx, blockReader, failFast)
			chunkErr := fn(ctx, chunkStart, chunkEnd, tx, blockReader, failFast)
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
