// Copyright 2025 The Erigon Authors
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

package shutter

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/event"
	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/log/v3"
)

type BlockEvent struct {
	LatestBlockNum  uint64
	LatestBlockTime uint64
	BlocksBatchLen  uint64
	Unwind          bool
}

type BlockListener struct {
	logger             log.Logger
	stateChangesClient stateChangesClient
	events             *event.Observers[BlockEvent]
}

func NewBlockListener(logger log.Logger, stateChangesClient stateChangesClient) *BlockListener {
	return &BlockListener{
		logger:             logger,
		stateChangesClient: stateChangesClient,
		events:             event.NewObservers[BlockEvent](),
	}
}

func (bl *BlockListener) RegisterObserver(o event.Observer[BlockEvent]) event.UnregisterFunc {
	return bl.events.Register(o)
}

func (bl *BlockListener) Run(ctx context.Context) error {
	defer bl.logger.Info("block listener stopped")
	bl.logger.Info("running block listener")

	sub, err := bl.stateChangesClient.StateChanges(ctx, &remoteproto.StateChangeRequest{})
	if err != nil {
		return err
	}

	// note the changes stream is ctx-aware so Recv should terminate with err if ctx gets done
	var batch *remoteproto.StateChangeBatch
	for batch, err = sub.Recv(); err == nil; batch, err = sub.Recv() {
		if batch == nil || len(batch.ChangeBatch) == 0 {
			continue
		}

		latestChange := batch.ChangeBatch[len(batch.ChangeBatch)-1]
		blockEvent := BlockEvent{
			LatestBlockNum:  latestChange.BlockHeight,
			LatestBlockTime: latestChange.BlockTime,
			BlocksBatchLen:  uint64(len(batch.ChangeBatch)),
			Unwind:          latestChange.Direction == remoteproto.Direction_UNWIND,
		}

		bl.events.NotifySync(blockEvent)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("block listener sub.Recv: %w", err)
	}
}
