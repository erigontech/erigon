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
	minUnwindable, err := changeset.ReadLowestUnwindableBlock(tx)
	if err != nil {
		return 0, err
	}
	if minUnwindable == math.MaxUint64 { // no unwindable block found
		minUnwindable, err = commitmentdb.LatestBlockNumWithCommitment(tx)
		log.Warn("no unwindable block found from changesets, falling back to latest with commitment", "block", minUnwindable, "err", err)
		if err != nil {
			return 0, err
		}
	} else if minUnwindable > 0 {
		minUnwindable-- // Undoing the block with the oldest changeset restores its parent's state.
	}
	var filesEnd uint64
	for domain := range kv.DomainLen {
		filesEnd = max(filesEnd, tx.Debug().TxNumsInFiles(domain))
	}
	if filesEnd > 0 {
		// Files cover [0, filesEnd). Keep the block containing their last tx,
		// even when changesets for older blocks are still retained.
		block, ok, err := txNumsReader.FindBlockNum(context.Background(), tx, filesEnd-1)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("%w: no block for snapshot tx %d", commitmentdb.ErrBehindCommitment, filesEnd-1)
		}
		minUnwindable = max(minUnwindable, block)
	}
	return minUnwindable, nil
}

// CanUnwindBeforeBlockNum returns blockNum and true if it meets the lower bound
// from [CanUnwindToBlockNum]. For an older block it returns that bound and false.
func CanUnwindBeforeBlockNum(blockNum uint64, tx kv.TemporalTx, txNumsReader rawdbv3.TxNumsReader) (unwindableBlockNum uint64, ok bool, err error) {
	_minUnwindableBlockNum, err := CanUnwindToBlockNum(tx, txNumsReader)
	if err != nil {
		return 0, false, err
	}
	if blockNum < _minUnwindableBlockNum {
		return _minUnwindableBlockNum, false, nil
	}
	return blockNum, true, nil
}
