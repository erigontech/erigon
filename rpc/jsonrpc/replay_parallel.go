// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/state"
)

const maxReplayWorkers = 32

var (
	errBlockNotCanonical = errors.New("block is no longer canonical")
	errNoReplayReadTx    = errors.New("no read tx left for a replay worker")
)

var replayCPUs = func() int { return runtime.GOMAXPROCS(0) }

func replayWorkerCount(cpus, txnCount int) int {
	return max(1, min(cpus, txnCount, maxReplayWorkers))
}

// replayTxnsParallel runs, for every txn index of a block, the job newWorker builds. The
// history keeps the exact state before every txNum, so the txns replay independently. Each
// worker owns a read tx, because an MDBX tx must not be used by two goroutines. When the
// read-tx limit leaves no tx for any worker, it returns errNoReplayReadTx and runs nothing.
// A txn starts only when the txn window places before it is written.
func replayTxnsParallel(ctx context.Context, db kv.TemporalRoDB, canonical dbservices.CanonicalReader, blockNum uint64, blockHash common.Hash, baseTxNum uint64, txnCount, workers, window int,
	newWorker func(ctx context.Context, tx kv.TemporalTx) (func(txnIndex int) error, error),
) (*txnReplay, error) {
	txs, err := beginReplayReadTxs(ctx, db, workers)
	if err != nil {
		return nil, err
	}

	jobs := make(chan int, txnCount)
	for i := range txnCount {
		jobs <- i
	}
	close(jobs)

	writtenTxns := make([]chan struct{}, txnCount)
	for i := range writtenTxns {
		writtenTxns[i] = make(chan struct{})
	}
	g, gctx := errgroup.WithContext(ctx)
	for _, tx := range txs {
		g.Go(func() error {
			defer tx.Rollback()
			// A worker's view can start after the caller's, when pruning has moved on.
			startTxNum, err := state.StateHistoryStartTxNum(tx)
			if err != nil {
				return err
			}
			if baseTxNum < startTxNum {
				return fmt.Errorf("%w: requested block %d, history is available from txNum %d", state.ErrPruned, blockNum, startTxNum)
			}
			// After a reorg the block's txNums belong to the new fork.
			isCanonical, err := canonical.IsCanonical(gctx, tx, blockHash, blockNum)
			if err != nil {
				return err
			}
			if !isCanonical {
				return fmt.Errorf("%w: block %d %x", errBlockNotCanonical, blockNum, blockHash)
			}
			job, err := newWorker(gctx, tx)
			if err != nil {
				return err
			}
			for txnIndex := range jobs {
				if txnIndex >= window {
					select {
					case <-writtenTxns[txnIndex-window]:
					case <-gctx.Done():
						return gctx.Err()
					}
				}
				if err := gctx.Err(); err != nil {
					return err
				}
				if err := job(txnIndex); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return &txnReplay{Group: g, ctx: gctx, writtenTxns: writtenTxns}, nil
}

type txnReplay struct {
	*errgroup.Group
	ctx          context.Context
	writtenTxns  []chan struct{}
	writtenCount int
}

// written marks the next txn in block order as written. Only the writing goroutine calls it.
func (r *txnReplay) written() {
	close(r.writtenTxns[r.writtenCount])
	r.writtenCount++
}

// beginReplayReadTxs opens up to n read txs and stops at the read-tx limit.
func beginReplayReadTxs(ctx context.Context, db kv.TemporalRoDB, n int) ([]kv.TemporalTx, error) {
	txs := make([]kv.TemporalTx, 0, n)
	for range n {
		tx, err := db.BeginTemporalRo(ctx) //nolint:gocritic // each tx is rolled back by its worker
		if errors.Is(err, kv.ErrReadTxLimitExceeded) {
			break
		}
		if err != nil {
			for _, tx := range txs {
				tx.Rollback()
			}
			return nil, err
		}
		txs = append(txs, tx)
	}
	if len(txs) == 0 {
		return nil, errNoReplayReadTx
	}
	return txs, nil
}
