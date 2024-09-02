package stagedsync

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/services"
)

type BlockUnwinderRunner func(ctx context.Context, tx kv.RwTx, blockReader services.FullBlockReader, isShortInterval bool, logEvery *time.Ticker, u *UnwindState, unwinder IndexUnwinder) error

func NewGenericBlockIndexerUnwinder(bucket, counterBucket string, unwinderRunner BlockUnwinderRunner) UnwindExecutor {
	return func(ctx context.Context, tx kv.RwTx, u *UnwindState, blockReader services.FullBlockReader, isShortInterval bool, logEvery *time.Ticker) error {
		unwinder, err := newBlockIndexerUnwinder(tx, bucket, counterBucket)
		if err != nil {
			return err
		}
		defer unwinder.Dispose()

		return unwinderRunner(ctx, tx, blockReader, isShortInterval, logEvery, u, unwinder)
	}
}

type BlockIndexerIndexerUnwinder struct {
	indexBucket   string
	counterBucket string
	target        kv.RwCursor
	targetDel     kv.RwCursor
	counter       kv.RwCursorDupSort
}

func newBlockIndexerUnwinder(tx kv.RwTx, indexBucket, counterBucket string) (*BlockIndexerIndexerUnwinder, error) {
	target, err := tx.RwCursor(indexBucket)
	if err != nil {
		return nil, err
	}

	targetDel, err := tx.RwCursor(indexBucket)
	if err != nil {
		return nil, err
	}

	counter, err := tx.RwCursorDupSort(counterBucket)
	if err != nil {
		return nil, err
	}

	return &BlockIndexerIndexerUnwinder{
		indexBucket,
		counterBucket,
		target,
		targetDel,
		counter,
	}, nil
}

func (u *BlockIndexerIndexerUnwinder) UnwindAddress(tx kv.RwTx, addr common.Address, ethTx uint64) error {
	return unwindAddress(tx, u.target, u.targetDel, u.counter, u.indexBucket, u.counterBucket, addr, ethTx)
}

func (u *BlockIndexerIndexerUnwinder) Dispose() error {
	u.target.Close()
	u.targetDel.Close()
	u.counter.Close()

	return nil
}
