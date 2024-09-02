package stagedsync

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

func RunBlocksRewardedBlockUnwind(ctx context.Context, tx kv.RwTx, blockReader services.FullBlockReader, isShortInterval bool, logEvery *time.Ticker, u *UnwindState, unwinder IndexUnwinder) error {
	// The unwind interval is ]u.UnwindPoint, EOF]
	startBlock := u.UnwindPoint + 1

	for blockNum := startBlock; blockNum <= u.CurrentBlockNumber; blockNum++ {
		hash, err := blockReader.CanonicalHash(ctx, tx, blockNum)
		if err != nil {
			return err
		}
		header, err := blockReader.HeaderByHash(ctx, tx, hash)
		if err != nil {
			return err
		}

		if err := unwinder.UnwindAddress(tx, header.Coinbase, header.Number.Uint64()); err != nil {
			return err
		}

		select {
		default:
		case <-ctx.Done():
			return common.ErrStopped
		case <-logEvery.C:
			log.Info("Unwinding blocks rewarded indexer", "blockNum", blockNum)
		}
	}

	return nil
}
