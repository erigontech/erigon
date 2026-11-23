package rawtemporaldb

import (
	"context"
	"fmt"
	"math"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// CanUnwindToBlockNum returns the earliest allowed unwind target. An unwind
// keeps the target block's state. The limit accounts for state value files and
// the oldest retained changeset (the latest commitment block if none remain).
func CanUnwindToBlockNum(tx kv.TemporalTx, txNumsReader rawdbv3.TxNumsReader) (uint64, error) {
	oldestChangesetBlock, err := changeset.ReadLowestUnwindableBlock(tx)
	if err != nil {
		return 0, err
	}
	var earliestTarget uint64
	if oldestChangesetBlock == math.MaxUint64 { // no unwindable block found
		earliestTarget, err = commitmentdb.LatestBlockNumWithCommitment(tx)
		log.Warn("no unwindable block found from changesets, falling back to latest with commitment", "block", earliestTarget, "err", err)
		if err != nil {
			return 0, err
		}
	} else if oldestChangesetBlock > 0 {
		// Undoing the block with the oldest changeset restores its parent's state.
		earliestTarget = oldestChangesetBlock - 1
	}
	filesEnd := kv.UnderlyingTx(tx).FreezeInfo().ValueFilesEndTxNum()
	if filesEnd > 0 {
		// The end is exclusive. Keep the block containing the last transaction
		// covered by a value file, even when older changesets remain.
		lastFileBlock, ok, err := txNumsReader.FindBlockNum(context.Background(), tx, filesEnd-1)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("%w: no block for snapshot tx %d", commitmentdb.ErrBehindCommitment, filesEnd-1)
		}
		earliestTarget = max(earliestTarget, lastFileBlock)
	}
	return earliestTarget, nil
}

// CanUnwindBeforeBlockNum returns blockNum and true if it meets the lower bound
// from [CanUnwindToBlockNum]. For an older block it returns that bound and false.
func CanUnwindBeforeBlockNum(blockNum uint64, tx kv.TemporalTx, txNumsReader rawdbv3.TxNumsReader) (unwindableBlockNum uint64, ok bool, err error) {
	earliestTarget, err := CanUnwindToBlockNum(tx, txNumsReader)
	if err != nil {
		return 0, false, err
	}
	if blockNum < earliestTarget {
		return earliestTarget, false, nil
	}
	return blockNum, true, nil
}
