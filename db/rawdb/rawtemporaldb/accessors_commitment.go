package rawtemporaldb

import (
	"math"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

func CanUnwindToBlockNum(tx kv.TemporalTx) (uint64, error) {
	minUnwindale, err := changeset.ReadLowestUnwindableBlock(tx)
	if err != nil {
		return 0, err
	}
	if minUnwindale == math.MaxUint64 { // no unwindable block found
		return commitmentdb.LatestBlockNumWithCommitment(tx)
	}
	return minUnwindale, nil
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
