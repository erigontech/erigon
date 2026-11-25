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
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

type resolveOptions struct {
	withoutFinalisedBlock bool
}

type ResolveOption func(*resolveOptions)

func WithoutFinalisedBlock() ResolveOption {
	return func(options *resolveOptions) {
		options.withoutFinalisedBlock = true
	}
}

// IrreversibleBlock is the highest block no unwind may reach, so its changeset can go and it can go into files:
// the block below the finalized one, whose changeset stays so that a fork from that block can still be
// validated, or the block below the reorg window without finality or on the initial cycle.
func IrreversibleBlock(headBlockNum, finalisedBlockNum, maxReorgDepth uint64, initialCycle bool) uint64 {
	if finalisedBlockNum > 0 && !initialCycle {
		return finalisedBlockNum - 1
	}
	if headBlockNum <= maxReorgDepth {
		return 0
	}
	return headBlockNum - maxReorgDepth - 1
}

// Resolve reads the execution head and the finalized block from tx and returns the irreversible block.
func Resolve(tx kv.Tx, maxReorgDepth uint64, initialCycle bool, options ...ResolveOption) (uint64, error) {
	headBlockNum, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return 0, err
	}
	var opts resolveOptions
	for _, option := range options {
		option(&opts)
	}
	finalisedBlockNum := rawdb.ReadForkchoiceFinalizedNum(tx)
	if opts.withoutFinalisedBlock {
		finalisedBlockNum = 0
	}
	return IrreversibleBlock(headBlockNum, finalisedBlockNum, maxReorgDepth, initialCycle), nil
}

// IrreversibleTxNum is the last txnum state files may hold: the end of irreversibleBlock, lowered below a
// block that still has a changeset, because that block can still be unwound.
func IrreversibleTxNum(ctx context.Context, tx kv.Tx, txNumsReader rawdbv3.TxNumsReader, irreversibleBlock uint64) (uint64, error) {
	lowest, err := changeset.ReadLowestUnwindableBlock(tx)
	if err != nil || lowest == 0 {
		return 0, err
	}
	txNum, ok, err := txNumsReader.MaxExact(ctx, tx, nil, min(irreversibleBlock, lowest-1))
	if err != nil || !ok {
		return 0, err
	}
	return txNum, nil
}

// ResolveTxNum is Resolve followed by IrreversibleTxNum.
func ResolveTxNum(ctx context.Context, tx kv.Tx, maxReorgDepth uint64, initialCycle bool, txNumsReader rawdbv3.TxNumsReader) (uint64, error) {
	block, err := Resolve(tx, maxReorgDepth, initialCycle)
	if err != nil {
		return 0, err
	}
	return IrreversibleTxNum(ctx, tx, txNumsReader, block)
}
