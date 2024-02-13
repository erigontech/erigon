package stagedsync

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

type BodyIndexerHandler interface {
	ResourceAwareIndexHandler
	HandleMatch(blockNum uint64, body *types.Body) error
}

// TODO: extract common logic from runIncrementalLogIndexerExecutor
func runIncrementalBodyIndexerExecutor(db kv.RoDB, tx kv.RwTx, blockReader services.FullBlockReader, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, ctx context.Context, s *StageState, handler BodyIndexerHandler) (uint64, error) {
	// Tracks how many blocks finished analysis so far
	totalBlocks := uint64(0)

	// Tracks how many blocks finished analysis with a match so far
	totalMatch := uint64(0)

	// Process control
	flushEvery := time.NewTicker(bitmapsFlushEvery)
	defer flushEvery.Stop()

	// Iterate over all blocks [startBlock, endBlock]
	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		hash, err := blockReader.CanonicalHash(ctx, tx, blockNum)
		if err != nil {
			return startBlock, err
		}
		body, _, err := blockReader.Body(ctx, tx, hash, blockNum)
		if err != nil {
			return startBlock, err
		}

		totalBlocks++
		if err := handler.HandleMatch(blockNum, body); err != nil {
			return startBlock, err
		}

		select {
		default:
		case <-ctx.Done():
			return startBlock, common.ErrStopped
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Scanning blocks", s.LogPrefix()), "block", blockNum, "matches", totalMatch, "blocks", totalBlocks)
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
	if !isShortInterval && totalBlocks > 0 {
		log.Info(fmt.Sprintf("[%s] Totals", s.LogPrefix()), "matches", totalMatch, "blocks", totalBlocks)
	}

	return endBlock, nil
}
