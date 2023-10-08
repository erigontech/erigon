package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

// Given a log entry, answer the question: does the ETH tx it belongs to deserves to be indexed?
//
// The generic parameter T represents the analysis result and is implementation-specific. E.g. it
// can contain which addresses this log entry touches in a token transfer indexer.
//
// An instance of this interface is meant to be reused, so it can contain caches to speed up further
// analysis.
type LogAnalyzer[T any] interface {
	// Given a log entry (there may be others in the same tx, here we analyze 1 specific log entry),
	// does it match the criteria the implementation is suposed to analyze?
	//
	// Return nil means it doesn't pass the criteria and it shouldn't be indexed.
	Inspect(tx kv.Tx, l *types.Log) (*T, error)
}

// Handles log indexer lifecycle.
type LogIndexerHandler[T any] interface {
	// Given a tx that must be indexed, handles all logs that caused the matching.
	HandleMatch(match *TxMatchedLogs[T])

	Flush(force bool) error

	Load(ctx context.Context, tx kv.RwTx) error

	// Disposes the allocated resources
	Close()
}

func runConcurrentLogIndexerExecutor[T any](db kv.RoDB, tx kv.RwTx, blockReader services.FullBlockReader, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, ctx context.Context, s *StageState, analyzer LogAnalyzer[T], handler LogIndexerHandler[T]) (uint64, error) {
	if !isShortInterval {
		log.Info(fmt.Sprintf("[%s] Using concurrent executor", s.LogPrefix()))
	}

	// Scans logs bucket
	logs, err := tx.Cursor(kv.Log)
	if err != nil {
		return startBlock, err
	}
	defer logs.Close()

	// Tracks how many txs finished analysis so far
	totalTx := atomic.NewUint64(0)

	// Tracks how many txs finished analysis with a match so far
	totalMatch := atomic.NewUint64(0)

	// Each worker processes all logs from 1 ethTx each time
	workers := estimate.AlmostAllCPUs()
	proberCh := make(chan *TxLogs[T], workers*3)
	matchCh := make(chan *TxMatchedLogs[T], workers*3+workers) // must be >= inCh buffer size + n. of workers, otherwise can deadlock

	g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < workers; i++ {
		createLogAnalyzerWorker(g, gCtx, db, analyzer, proberCh, matchCh, totalMatch, totalTx)
	}

	// Process control
	flushEvery := time.NewTicker(bitmapsFlushEvery)
	defer flushEvery.Stop()

	// Get all blocks in [startBlock, endBlock] that contain at least 1 TRANSFER_TOPIC occurrence
	blocks, err := newBlockBitmapFromTopic(tx, startBlock, endBlock, TRANSFER_TOPIC)
	if err != nil {
		return startBlock, err
	}
	defer bitmapdb.ReturnToPool(blocks)

L:
	// Iterate each block that contains a Transfer() event
	for it := blocks.Iterator(); it.HasNext(); {
		blockNum := uint64(it.Next())

		// Avoid recalculating txid from the block basetxid for each match
		baseTxId, err := blockReader.BaseTxIdForBlock(ctx, tx, blockNum)
		if err != nil {
			return startBlock, err
		}

		// Inspect each tx's logs
		logPrefix := hexutility.EncodeTs(blockNum)
		for k, v, err := logs.Seek(logPrefix); k != nil && bytes.HasPrefix(k, logPrefix); k, v, err = logs.Next() {
			if err != nil {
				return startBlock, err
			}

			txLogs := newTxLogsFromRaw[T](blockNum, baseTxId, k, v)
		LG:
			for {
				select {
				case proberCh <- txLogs:
					break LG
				case match := <-matchCh:
					handler.HandleMatch(match)
				case <-logEvery.C:
					log.Info(fmt.Sprintf("[%s] Scanning logs", s.LogPrefix()), "block", blockNum, "matches", totalMatch, "txCount", totalTx, "inCh", len(proberCh), "outCh", len(matchCh))
				}
			}
		}

		select {
		default:
		case <-ctx.Done():
			break L
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Scanning logs", s.LogPrefix()), "block", blockNum, "matches", totalMatch, "txCount", totalTx, "inCh", len(proberCh), "outCh", len(matchCh))
		case <-flushEvery.C:
			if err := handler.Flush(false); err != nil {
				return startBlock, err
			}
		}
	}

	// Close in channel and wait for workers to finish
	close(proberCh)
	if err := g.Wait(); err != nil {
		return startBlock, err
	}

	// Close out channel and drain remaining data saving them into db
	close(matchCh)
	for output := range matchCh {
		handler.HandleMatch(output)
	}

	// Last (forced) flush and batch load (if applicable)
	if err := handler.Flush(true); err != nil {
		return startBlock, err
	}
	if err := handler.Load(ctx, tx); err != nil {
		return startBlock, err
	}

	// Don't print summary if no contracts were analyzed to avoid polluting logs
	if !isShortInterval && totalTx.Load() > 0 {
		log.Info(fmt.Sprintf("[%s] Totals", s.LogPrefix()), "matches", totalMatch, "txCount", totalTx)
	}

	return endBlock, nil
}

func runIncrementalLogIndexerExecutor[T any](db kv.RoDB, tx kv.RwTx, blockReader services.FullBlockReader, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, ctx context.Context, s *StageState, analyzer LogAnalyzer[T], handler LogIndexerHandler[T]) (uint64, error) {
	if !isShortInterval {
		log.Info(fmt.Sprintf("[%s] Using incremental executor", s.LogPrefix()))
	}

	// Scans logs bucket
	logs, err := tx.Cursor(kv.Log)
	if err != nil {
		return startBlock, err
	}
	defer logs.Close()

	// Tracks how many txs finished analysis so far
	totalTx := atomic.NewUint64(0)

	// Tracks how many txs finished analysis with a match so far
	totalMatch := atomic.NewUint64(0)

	// Process control
	flushEvery := time.NewTicker(bitmapsFlushEvery)
	defer flushEvery.Stop()

	// Get all blocks in [startBlock, endBlock] that contain at least 1 TRANSFER_TOPIC occurrence
	blocks, err := newBlockBitmapFromTopic(tx, startBlock, endBlock, TRANSFER_TOPIC)
	if err != nil {
		return startBlock, err
	}
	defer bitmapdb.ReturnToPool(blocks)

	// Iterate each block that contains a Transfer() event
	for it := blocks.Iterator(); it.HasNext(); {
		blockNum := uint64(it.Next())

		// Avoid recalculating txid from the block basetxid for each match
		baseTxId, err := blockReader.BaseTxIdForBlock(ctx, tx, blockNum)
		if err != nil {
			return startBlock, err
		}

		// Inspect each tx's logs
		logPrefix := hexutility.EncodeTs(blockNum)
		for k, v, err := logs.Seek(logPrefix); k != nil && bytes.HasPrefix(k, logPrefix); k, v, err = logs.Next() {
			if err != nil {
				return startBlock, err
			}

			txLogs := newTxLogsFromRaw[T](blockNum, baseTxId, k, v)
			results, err := AnalyzeLogs(tx, analyzer, txLogs.rawLogs)
			if err != nil {
				return startBlock, err
			}

			totalTx.Inc()
			if len(results) > 0 {
				totalMatch.Inc()
				match := &TxMatchedLogs[T]{txLogs, results}
				handler.HandleMatch(match)
			}
		}

		select {
		default:
		case <-ctx.Done():
			return startBlock, common.ErrStopped
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Scanning logs", s.LogPrefix()), "block", blockNum, "matches", totalMatch, "txCount", totalTx)
		case <-flushEvery.C:
			if err := handler.Flush(false); err != nil {
				return startBlock, err
			}
		}
	}

	// Last (forced) flush and batch load (if applicable)
	if err := handler.Flush(true); err != nil {
		return startBlock, err
	}
	if err := handler.Load(ctx, tx); err != nil {
		return startBlock, err
	}

	// Don't print summary if no contracts were analyzed to avoid polluting logs
	if !isShortInterval && totalTx.Load() > 0 {
		log.Info(fmt.Sprintf("[%s] Totals", s.LogPrefix()), "matches", totalMatch, "txCount", totalTx)
	}

	return endBlock, nil
}

// Gets a bitmap with all blocks [startBlock, endBlock] that contains at least 1 occurrence of
// a topic (== log0).
//
// The returned bitmap MUST be returned to pool after use.
func newBlockBitmapFromTopic(tx kv.Tx, startBlock, endBlock uint64, topic []byte) (*roaring.Bitmap, error) {
	allBlocks := bitmapdb.NewBitmap()
	allBlocks.AddRange(uint64(startBlock), uint64(endBlock)+1)

	chunkedTransfer, err := bitmapdb.Get(tx, kv.LogTopicIndex, topic, uint32(startBlock), uint32(endBlock))
	if err != nil {
		return nil, err
	}
	allBlocks.And(chunkedTransfer)

	return allBlocks, nil
}

// Represents a set of all raw logs of 1 transaction that'll be analyzed.
//
// 0 or more logs can contribute for matching and eventual indexing of this
// tx on 0 or more target indexes.
type TxLogs[T any] struct {
	blockNum uint64
	ethTx    uint64
	// raw logs for 1 tx
	rawLogs []byte
}

// k, v are the raw key/value from kv.Logs bucket.
func newTxLogsFromRaw[T any](blockNum, baseTxId uint64, k, v []byte) *TxLogs[T] {
	// idx inside block
	txIdx := binary.BigEndian.Uint32(k[length.BlockNum:])

	// TODO: extract formula function
	ethTx := baseTxId + 1 + uint64(txIdx)

	raw := make([]byte, len(v))
	copy(raw, v)

	return &TxLogs[T]{blockNum, ethTx, raw}
}

// rawLogs contains N encoded logs for 1 tx
func AnalyzeLogs[T any](tx kv.Tx, analyzer LogAnalyzer[T], rawLogs []byte) ([]*T, error) {
	var logs types.Logs
	if err := cbor.Unmarshal(&logs, bytes.NewReader(rawLogs)); err != nil {
		return nil, err
	}

	// scan log entries for tx
	results := make([]*T, 0)
	for _, l := range logs {
		res, err := analyzer.Inspect(tx, l)
		if err != nil {
			return nil, err
		}
		if res == nil {
			continue
		}
		// TODO: dedup
		results = append(results, res)
	}

	return results, nil
}

type TxMatchedLogs[T any] struct {
	*TxLogs[T]
	matchResults []*T
}

func createLogAnalyzerWorker[T any](g *errgroup.Group, ctx context.Context, db kv.RoDB, analyzer LogAnalyzer[T], proberCh <-chan *TxLogs[T], matchCh chan<- *TxMatchedLogs[T], totalMatch, txCount *atomic.Uint64) {
	g.Go(func() error {
		return db.View(ctx, func(tx kv.Tx) error {
			for {
				// wait for input
				txLogs, ok := <-proberCh
				if !ok {
					break
				}

				results, err := AnalyzeLogs(tx, analyzer, txLogs.rawLogs)
				if err != nil {
					return err
				}

				txCount.Inc()
				if len(results) > 0 {
					totalMatch.Inc()
					matchCh <- &TxMatchedLogs[T]{txLogs, results}
				}
			}
			return nil
		})
	})
}
