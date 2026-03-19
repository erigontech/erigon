package rawtemporaldb

import (
	"math"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

func CanUnwindToBlockNum(tx kv.TemporalTx) (uint64, error) {
	minUnwindable, err := changeset.ReadLowestUnwindableBlock(tx)
	if err != nil {
		return 0, err
	}
	if minUnwindable == math.MaxUint64 { // no unwindable block found
		minUnwindable, err = commitmentdb.LatestBlockNumWithCommitment(tx)
		log.Warn("no unwindable block found from changesets, falling back to latest with commitment", "block", minUnwindable, "err", err)
		return minUnwindable, err
	}
	if minUnwindable > 0 {
		minUnwindable-- // UnwindTo is exclusive, i.e. (unwindPoint,tip] get unwound
	}
	return minUnwindable, nil
}

func CanUnwindBeforeBlockNum(blockNum uint64, tx kv.TemporalTx) (unwindableBlockNum uint64, ok bool, err error) {
	_minUnwindableBlockNum, err := CanUnwindToBlockNum(tx)
	if err != nil {
		return 0, false, err
	}
	if blockNum < _minUnwindableBlockNum {
		return _minUnwindableBlockNum, false, nil
	}
	return blockNum, true, nil
}
