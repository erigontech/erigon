// Copyright 2021 The Erigon Authors
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

package txpool

import (
	"context"

	"github.com/holiman/uint256"
	"google.golang.org/protobuf/proto"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
)

// Older pool fixtures provide state values without a database execution view.
// Keep them at the reconciliation boundary; identified views exercise OnNewBlock.
func applyTestBlock(ctx context.Context, p *TxPool, batch *remoteproto.StateChangeBatch, unwind, blobs, mined TxnSlots) error {
	if batch.TargetView != nil {
		return p.OnNewBlock(ctx, batch, unwind, blobs, mined)
	}
	for _, txn := range unwind.Txns {
		p.recoverAuthorizations(txn)
	}
	db, cache := p.chainDB()
	cache.OnNewBlock(batch)
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var poolTx kv.Tx
	if p.poolDB != nil {
		poolTx, err = p.poolDB.BeginRo(ctx)
		if err != nil {
			return err
		}
		defer poolTx.Rollback()
	}
	view, err := cache.View(ctx, tx)
	if err != nil {
		return err
	}
	batch = proto.Clone(batch).(*remoteproto.StateChangeBatch)
	head := batch.ChangeBatch[len(batch.ChangeBatch)-1]
	batch.TargetView = &remoteproto.ExecutionView{BlockHash: head.BlockHash, BlockHeight: head.BlockHeight, BlockTime: head.BlockTime, SlotNumber: head.SlotNumber, StateVersionId: batch.StateVersionId}
	frames, err := p.applyBlockUpdate(batch, unwind, blobs, mined, view, poolTx)
	if err != nil {
		return err
	}
	p.lock.Lock()
	p.appliedView = nil
	p.lock.Unlock()
	return p.revalidateFrames(ctx, frames.parked, frames.generation, frames.blockHash)
}

type fixtureStateChangePool struct{ Pool }

func (p fixtureStateChangePool) OnNewBlock(ctx context.Context, batch *remoteproto.StateChangeBatch, unwind, blobs, mined TxnSlots) error {
	if pool, ok := p.Pool.(*TxPool); ok {
		return applyTestBlock(ctx, pool, batch, unwind, blobs, mined)
	}
	return p.Pool.OnNewBlock(ctx, batch, unwind, blobs, mined)
}

func newTestFetch(ctx context.Context, clients []sentryproto.SentryClient, pool Pool, stateChanges StateChangesClient, db kv.RwDB, chainID uint256.Int, logger log.Logger, opts ...Option) *Fetch {
	return NewFetch(ctx, clients, fixtureStateChangePool{pool}, stateChanges, db, chainID, logger, opts...)
}
