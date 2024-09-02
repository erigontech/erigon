package stagedsync

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

func RunWithdrawalsBlockUnwind(ctx context.Context, tx kv.RwTx, blockReader services.FullBlockReader, isShortInterval bool, logEvery *time.Ticker, u *UnwindState, unwinder IndexUnwinder) error {
	// The unwind interval is ]u.UnwindPoint, EOF]
	startBlock := u.UnwindPoint + 1

	idx2Block, err := tx.RwCursor(kv.OtsWithdrawalIdx2Block)
	if err != nil {
		return err
	}
	defer idx2Block.Close()

	// In order to unwind idx2Block, we need to find the max withdrawal ID from the unwind point
	// block or less
	blockNum := u.UnwindPoint
	found := false
	withdrawalId := uint64(0)
	for blockNum > 0 {
		hash, err := blockReader.CanonicalHash(ctx, tx, blockNum)
		if err != nil {
			return err
		}
		body, _, err := blockReader.Body(ctx, tx, hash, blockNum)
		if err != nil {
			return err
		}

		withdrawalsAmount := len(body.Withdrawals)
		if withdrawalsAmount > 0 {
			found = true
			lastWithdrawal := body.Withdrawals[withdrawalsAmount-1]
			withdrawalId = lastWithdrawal.Index
			break
		}

		blockNum--
	}

	// Unwind idx2Block
	if found {
		unwoundToIndex, err := unwindUint64KeyBasedTable(idx2Block, withdrawalId)
		if err != nil {
			return err
		}

		// withdrawal ID MUST exist in idx2Block, otherwise it is a DB inconsistency
		if unwoundToIndex != withdrawalId {
			return fmt.Errorf("couldn't find bucket=%s k=%v to unwind; probably DB corruption", kv.OtsWithdrawalIdx2Block, withdrawalId)
		}
	}

	for blockNum := startBlock; blockNum <= u.CurrentBlockNumber; blockNum++ {
		hash, err := blockReader.CanonicalHash(ctx, tx, blockNum)
		if err != nil {
			return err
		}
		body, _, err := blockReader.Body(ctx, tx, hash, blockNum)
		if err != nil {
			return err
		}

		if len(body.Withdrawals) == 0 {
			continue
		}
		for _, w := range body.Withdrawals {
			if err := unwinder.UnwindAddress(tx, w.Address, w.Index); err != nil {
				return err
			}
		}

		select {
		default:
		case <-ctx.Done():
			return common.ErrStopped
		case <-logEvery.C:
			log.Info("Unwinding withdrawals indexer", "blockNum", blockNum)
		}
	}

	return nil
}
