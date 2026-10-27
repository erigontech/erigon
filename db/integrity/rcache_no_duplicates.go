package integrity

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/execution/rlp"
)

func CheckRCacheNoDups(ctx context.Context, sc SamplerCfg, db kv.TemporalRoDB, blockReader dbservices.FullBlockReader, failFast bool) (err error) {
	defer func() {
		log.Info("[integrity] RCacheNoDups: done", "err", err)
	}()

	txNumsReader := blockReader.TxnumReader()

	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rcacheDomainProgress := tx.Debug().DomainProgress(kv.RCacheDomain)
	fromBlock := uint64(1)
	toBlock, ok, err := txNumsReader.FindBlockNum(ctx, tx, rcacheDomainProgress)
	if err != nil {
		return fmt.Errorf("findBlockNum(%d) fails: %w", rcacheDomainProgress, err)
	}
	if !ok {
		return fmt.Errorf("findBlockNum(%d) not found", rcacheDomainProgress)
	}

	if err := ValidateDomainProgress(ctx, db, kv.RCacheDomain, txNumsReader); err != nil {
		return err
	}

	log.Info("[integrity] RCacheNoDups starting", "fromBlock", fromBlock, "toBlock", toBlock)

	return parallelChunkCheck(ctx, sc.NewSampler(), fromBlock, toBlock, db, blockReader, failFast, string(RCacheNoDups), rcacheNoDupsRange)
}

func rcacheNoDupsRange(ctx context.Context, fromBlock, toBlock uint64, db kv.TemporalRoDB, blockReader dbservices.FullBlockReader, failFast bool, probs *problems) (err error) {
	if fromBlock > toBlock {
		panic(fmt.Sprintf("fromBlock(%d) > toBlock(%d)", fromBlock, toBlock))
	}
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	txNumsReader := blockReader.TxnumReader()
	fromTxNum, err := txNumsReader.Min(ctx, tx, fromBlock)
	if err != nil {
		return err
	}
	toTxNum, err := txNumsReader.Max(ctx, tx, toBlock)
	if err != nil {
		return err
	}

	stepSize := tx.Debug().StepSize()
	prevCumUsedGas := -1
	expectedFirstLogIdx := uint32(0)
	blockNum := fromBlock
	var _min, _max uint64
	_min = fromTxNum
	_max, _ = txNumsReader.Max(ctx, tx, fromBlock)

	it, err := tx.Debug().TraceKey(kv.RCacheDomain, rawtemporaldb.ReceiptCacheKey, fromTxNum, toTxNum)
	if err != nil {
		return err
	}
	defer it.Close()

	for it.HasNext() {
		txNum, v, err := it.Next()
		if err != nil {
			return err
		}
		if len(v) == 0 {
			continue
		}
		cumUsedGas, logIdx, logs, err := decodeRCacheCounters(v)
		if err != nil {
			return fmt.Errorf("txNum %d: %w", txNum, err)
		}

		for txNum > _max {
			blockNum++
			_min = _max + 1
			_max, _ = txNumsReader.Max(ctx, tx, blockNum)
			expectedFirstLogIdx = 0
			prevCumUsedGas = -1
		}

		exactLogIdx := logIdx == expectedFirstLogIdx
		if !exactLogIdx && txNum <= _max {
			if err := probs.report(failFast, fmt.Errorf("%w: block %d tx %d (txNum %d, step %d): firstLogIndex=%d, want %d", ErrIntegrity, blockNum, txNum-_min-1, txNum, (txNum+1)/stepSize, logIdx, expectedFirstLogIdx)); err != nil {
				return err
			}
		}
		expectedFirstLogIdx = logIdx + uint32(logs)

		strongMonotonicCumGasUsed := int(cumUsedGas) > prevCumUsedGas
		if !strongMonotonicCumGasUsed && txNum <= _max { // system tx can be skipped
			if err := probs.report(failFast, fmt.Errorf("%w: block %d tx %d (txNum %d, step %d): cumulativeGasUsed=%d, previous tx has %d", ErrIntegrity, blockNum, txNum-_min-1, txNum, (txNum+1)/stepSize, cumUsedGas, prevCumUsedGas)); err != nil {
				return err
			}
		}
		prevCumUsedGas = int(cumUsedGas)

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

// decodeRCacheCounters reads only the block counters from a types.ReceiptForStorage encoding.
func decodeRCacheCounters(v []byte) (cumGasUsed uint64, firstLogIdx uint32, logs int, err error) {
	fields, _, err := rlp.SplitList(v)
	if err != nil {
		return 0, 0, 0, err
	}
	if _, _, fields, err = rlp.Split(fields); err != nil { // Type
		return 0, 0, 0, err
	}
	if _, _, fields, err = rlp.Split(fields); err != nil { // PostState or Status
		return 0, 0, 0, err
	}
	if cumGasUsed, fields, err = rlp.SplitUint64(fields); err != nil {
		return 0, 0, 0, err
	}
	idx, fields, err := rlp.SplitUint64(fields)
	if err != nil {
		return 0, 0, 0, err
	}
	logsList, _, err := rlp.SplitList(fields)
	if err != nil {
		return 0, 0, 0, err
	}
	logs, err = rlp.CountValues(logsList)
	return cumGasUsed, uint32(idx), logs, err
}

type chunkFn func(ctx context.Context, fromBlock, toBlock uint64, db kv.TemporalRoDB, blockReader dbservices.FullBlockReader, failFast bool, p *problems) error

func parallelChunkCheck(ctx context.Context, sampler *Sampler, fromBlock, toBlock uint64, db kv.TemporalRoDB, blockReader dbservices.FullBlockReader, failFast bool, prefix string, fn chunkFn) (err error) {
	blockRange := toBlock - fromBlock + 1
	if blockRange == 0 {
		return nil
	}

	numWorkers := estimate.AlmostAllCPUs()
	chunkSize := uint64(100)

	var probs problems
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(numWorkers)
	var completedChunks atomic.Uint64
	totalChunks := (blockRange + chunkSize - 1) / chunkSize
	log.Info("[integrity] "+prefix, "workers", numWorkers, "chunkSize", chunkSize, "blockRange", blockRange, "seed", sampler.Seed, "sampleRatio", sampler.SampleRatio)

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
				log.Info("[integrity] "+prefix, "progress", fmt.Sprintf("%.1f%%", progress))
			}
		}
	}()

	for start := fromBlock; start <= toBlock; start += chunkSize {
		if sampler.CanSkip() {
			continue
		}
		end := min(start+chunkSize-1, toBlock)
		chunkStart := start
		chunkEnd := end
		g.Go(func() error {
			chunkErr := fn(ctx, chunkStart, chunkEnd, db, blockReader, failFast, &probs)
			if chunkErr != nil {
				return chunkErr
			}
			completedChunks.Add(1)
			return nil
		})
	}

	// A chunk that failed operationally must not erase the problems already reported: the count
	// is what --failFast=false is run for.
	return errors.Join(g.Wait(), probs.verdict(prefix))
}
