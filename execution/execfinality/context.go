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

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

type FinalityContext struct {
	finalisedBlockNum uint64
	maxReorgDepth     uint64
	retentionBlockNum uint64
	collateToBlockNum uint64
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
		ctx.collateToBlockNum = finalisedBlockNum
		return ctx
	}
	if headBlockNum <= maxReorgDepth {
		return ctx
	}
	ctx.retentionBlockNum = headBlockNum - maxReorgDepth
	ctx.collateToBlockNum = ctx.retentionBlockNum - 1
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

func (c FinalityContext) ReadyForCollation(ctx context.Context, db kv.TemporalRoDB, stepLastTxNum uint64) (finalisedBlockNum, lastBlockInStep, lastBlockInDB, lastTxInDB uint64, ok bool, err error) {
	finalisedBlockNum = c.finalisedBlockNum
	err = db.View(ctx, func(tx kv.Tx) error {
		lastBlockInStep, ok, err = c.txNumsReader.FindBlockNum(ctx, tx, stepLastTxNum)
		if err != nil {
			return err
		}
		if !ok {
			lastBlockInStep = 0
		}
		lastBlockInDB, lastTxInDB, err = rawdbv3.TxNums.Last(tx)
		return err
	})
	ok = err == nil && c.retentionBlockNum > 0 && lastBlockInStep <= c.collateToBlockNum
	return
}
