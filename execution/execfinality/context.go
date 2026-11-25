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

package execfinality

import (
	"context"
	"math"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

type FinalityContext struct {
	finalisedBlockNum uint64
	maxReorgDepth     uint64
	retentionBlockNum uint64
	txNumsReader      rawdbv3.TxNumsReader
}

type resolveOptions struct {
	withoutFinalisedBlock bool
}

type ResolveOption func(*resolveOptions)

func WithoutFinalisedBlock() ResolveOption {
	return func(options *resolveOptions) {
		options.withoutFinalisedBlock = true
	}
}

func NewContext(headBlockNum, finalisedBlockNum, maxReorgDepth uint64, initialCycle bool, txNumsReader rawdbv3.TxNumsReader) FinalityContext {
	ctx := FinalityContext{
		finalisedBlockNum: finalisedBlockNum,
		maxReorgDepth:     maxReorgDepth,
		txNumsReader:      txNumsReader,
	}
	if finalisedBlockNum > 0 && !initialCycle {
		ctx.retentionBlockNum = finalisedBlockNum
		return ctx
	}
	if headBlockNum <= maxReorgDepth {
		return ctx
	}
	ctx.retentionBlockNum = headBlockNum - maxReorgDepth
	return ctx
}

func Resolve(tx kv.Tx, maxReorgDepth uint64, initialCycle bool, txNumsReader rawdbv3.TxNumsReader, options ...ResolveOption) (kv.FinalityContext, error) {
	headBlockNum, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return nil, err
	}
	var opts resolveOptions
	for _, option := range options {
		option(&opts)
	}
	finalisedBlockNum := rawdb.ReadForkchoiceFinalizedNum(tx)
	if opts.withoutFinalisedBlock {
		finalisedBlockNum = 0
	}
	return NewContext(headBlockNum, finalisedBlockNum, maxReorgDepth, initialCycle, txNumsReader), nil
}

func (c FinalityContext) PruneToBlockNum() uint64 {
	return c.retentionBlockNum
}

func (c FinalityContext) RetireToBlockNum() uint64 {
	return c.retentionBlockNum
}

func (c FinalityContext) MaxReorgDepth() uint64 {
	return c.maxReorgDepth
}

// collateDeleteTimeout bounds the write tx that deletes a step's changesets, which exec waits for at the tip. A step
// whose changesets do not all go in one tx waits for the next pass.
const collateDeleteTimeout = 100 * time.Millisecond

// ReadyForCollation reports whether every block of the step is below the retention boundary, so no unwind may
// reach it. On a writable db the same tx deletes the changesets of those blocks, so collation never waits for
// the changeset prune and a block in files is never unwindable.
func (c FinalityContext) ReadyForCollation(ctx context.Context, db kv.TemporalRoDB, stepLastTxNum uint64) (finalisedBlockNum, lastBlockInStep, lastBlockInDB, lastTxInDB uint64, ok bool, err error) {
	finalisedBlockNum = c.finalisedBlockNum
	decide := func(tx kv.TemporalTx) error {
		var found bool
		if lastBlockInStep, found, err = c.txNumsReader.FindBlockNum(ctx, tx, stepLastTxNum); err != nil {
			return err
		}
		if !found {
			lastBlockInStep = 0
		}
		if lastBlockInDB, lastTxInDB, err = rawdbv3.TxNums.Last(tx); err != nil {
			return err
		}
		ok = lastBlockInStep < c.retentionBlockNum
		return nil
	}
	rwDB, writable := db.(kv.TemporalRwDB)
	if !writable {
		err = db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
			if err := decide(tx); err != nil || !ok {
				return err
			}
			lowestUnwindableBlock, err := changeset.ReadLowestUnwindableBlock(tx)
			ok = lastBlockInStep < lowestUnwindableBlock
			return err
		})
		return
	}
	err = rwDB.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		if err := decide(tx); err != nil || !ok {
			return err
		}
		if err := rawdb.PruneTable(tx, kv.ChangeSets3, lastBlockInStep+1, ctx, math.MaxInt, collateDeleteTimeout, log.Root(), "collate"); err != nil {
			return err
		}
		lowestUnwindableBlock, err := changeset.ReadLowestUnwindableBlock(tx)
		ok = lastBlockInStep < lowestUnwindableBlock
		return err
	})
	ok = ok && err == nil
	return
}
