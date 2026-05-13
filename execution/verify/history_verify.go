package verify

import (
	"context"
	"fmt"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

// NewHistoryVerifier returns a function that verifies history snapshot files
// by re-executing blocks using HistoryReaderV3 (which reads state via GetAsOf
// from history/domain files). ExecuteBlockEphemerally validates receipts, gas,
// and bloom filters against block headers — if any GetAsOf value is wrong,
// execution diverges and receipt validation fails.
//
// This cross-checks three independent data sources: history files, domain files,
// and block headers.
//
// Workers controls the number of parallel goroutines. Each worker opens its own
// TemporalTx and processes blocks from a shared counter.
func NewHistoryVerifier(
	blockReader services.FullBlockReader,
	chainConfig *chain.Config,
	engine rules.Engine,
	workers int,
	logger log.Logger,
) func(ctx context.Context, db kv.TemporalRoDB, fromTxNum, toTxNum uint64) error {
	return func(ctx context.Context, db kv.TemporalRoDB, fromTxNum, toTxNum uint64) error {
		txNumsReader := blockReader.TxnumReader()

		// Open a temporary tx to convert txNum range → block range.
		ttx, err := db.BeginTemporalRo(ctx)
		if err != nil {
			return fmt.Errorf("history verify: begin temporal ro: %w", err)
		}
		defer ttx.Rollback()

		fromBlock, ok, err := txNumsReader.FindBlockNum(ctx, ttx, fromTxNum)
		if err != nil {
			return fmt.Errorf("history verify: find block for txNum %d: %w", fromTxNum, err)
		}
		if !ok {
			return fmt.Errorf("history verify: no block found for txNum %d", fromTxNum)
		}
		toBlock, ok, err := txNumsReader.FindBlockNum(ctx, ttx, toTxNum)
		if err != nil {
			return fmt.Errorf("history verify: find block for txNum %d: %w", toTxNum, err)
		}
		if !ok {
			toBlock, _, err = txNumsReader.FindBlockNum(ctx, ttx, toTxNum-1)
			if err != nil {
				return fmt.Errorf("history verify: find block for txNum %d: %w", toTxNum-1, err)
			}
		}

		// Skip range if it only contains genesis.
		if fromBlock == 0 && toBlock == 0 {
			return nil
		}

		logger.Info("[verify-history] re-executing blocks",
			"fromBlock", fromBlock, "toBlock", toBlock,
			"fromTxNum", fromTxNum, "toTxNum", toTxNum,
			"workers", workers)

		var completed atomic.Uint64

		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(workers)

		for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
			if blockNum == 0 {
				continue
			}
			blockNum := blockNum // capture

			g.Go(func() error {
				if err := gctx.Err(); err != nil {
					return fmt.Errorf("history verify: cancelled at block %d: %w", blockNum, err)
				}

				workerTx, err := db.BeginTemporalRo(gctx)
				if err != nil {
					return fmt.Errorf("history verify: begin temporal ro for block %d: %w", blockNum, err)
				}
				defer workerTx.Rollback()

				hash, ok, err := blockReader.CanonicalHash(gctx, workerTx, blockNum)
				if err != nil {
					return fmt.Errorf("history verify: canonical hash for block %d: %w", blockNum, err)
				}
				if !ok {
					return fmt.Errorf("history verify: no canonical hash for block %d", blockNum)
				}

				block, _, err := blockReader.BlockWithSenders(gctx, workerTx, hash, blockNum)
				if err != nil {
					return fmt.Errorf("history verify: read block %d: %w", blockNum, err)
				}
				if block == nil {
					return fmt.Errorf("history verify: block %d not found", blockNum)
				}

				blockStartTxNum, err := txNumsReader.Min(gctx, workerTx, blockNum)
				if err != nil {
					return fmt.Errorf("history verify: min txNum for block %d: %w", blockNum, err)
				}

				stateReader := state.NewHistoryReaderV3(workerTx, blockStartTxNum)
				stateWriter := state.NewNoopWriter()
				chainReader := consensuschain.NewReader(chainConfig, workerTx, blockReader, logger)

				blockHashFunc := protocol.GetHashFn(block.Header(), func(hash common.Hash, number uint64) (*types.Header, error) {
					return blockReader.Header(gctx, workerTx, hash, number)
				})

				vmConfig := vm.Config{}
				_, err = protocol.ExecuteBlockEphemerally(
					chainConfig, &vmConfig, blockHashFunc,
					engine, block,
					stateReader, stateWriter,
					chainReader, nil, logger,
				)
				if err != nil {
					return fmt.Errorf("history verify: block %d execution failed: %w", blockNum, err)
				}

				n := completed.Add(1)
				if n%1000 == 0 {
					logger.Info("[verify-history] progress",
						"completed", n, "total", toBlock-fromBlock)
				}

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return err
		}

		logger.Info("[verify-history] verification complete",
			"blocks", toBlock-fromBlock+1, "fromTxNum", fromTxNum, "toTxNum", toTxNum)
		return nil
	}
}
