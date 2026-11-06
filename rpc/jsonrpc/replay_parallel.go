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

var errBlockNotCanonical = errors.New("block is no longer canonical")

var replayCPUs = func() int { return runtime.GOMAXPROCS(0) }

// replayWorkerCount gives a quarter of the cores to one block: more workers per request
// compete for the same cores and caches, so they cost CPU without lowering latency.
func replayWorkerCount(txnCount int) int {
	return max(1, min(replayCPUs()/4, txnCount, maxReplayWorkers))
}

// replayTxnsParallel runs, for every txn index of a block, the job newWorker builds. The
// history keeps the exact state before every txNum, so the txns replay independently. Each
// worker owns a read tx, because an MDBX tx must not be used by two goroutines.
func replayTxnsParallel(ctx context.Context, db kv.TemporalRoDB, canonical dbservices.CanonicalReader, blockNum uint64, blockHash common.Hash, baseTxNum uint64, txnCount int,
	newWorker func(ctx context.Context, tx kv.TemporalTx) (func(txnIndex int) error, error),
) (*errgroup.Group, context.Context) {
	jobs := make(chan int, txnCount)
	for i := range txnCount {
		jobs <- i
	}
	close(jobs)

	g, gctx := errgroup.WithContext(ctx)
	for range replayWorkerCount(txnCount) {
		g.Go(func() error {
			tx, err := db.BeginTemporalRo(gctx)
			if err != nil {
				return err
			}
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
	return g, gctx
}
