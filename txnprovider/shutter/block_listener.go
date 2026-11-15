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
	"sync"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/event"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/grpcutil"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

type BlockEvent struct {
	LatestBlockNum  uint64
	LatestBlockTime uint64
	StateVersionID  uint64
	Unwind          bool
}

type BlockListener struct {
	logger             log.Logger
	stateChangesClient stateChangesClient
	events             *event.Observers[BlockEvent]
	eventsMu           sync.Mutex
	lastEvent          *BlockEvent
}

func NewBlockListener(logger log.Logger, stateChangesClient stateChangesClient) *BlockListener {
	return &BlockListener{
		logger:             logger,
		stateChangesClient: stateChangesClient,
		events:             event.NewObservers[BlockEvent](),
	}
}

// RegisterObserver replays the last known head, if any, before returning.
// The observer must accept that event without waiting for registration to finish.
func (bl *BlockListener) RegisterObserver(o event.Observer[BlockEvent]) event.UnregisterFunc {
	return bl.registerObserver(o, o)
}

func (bl *BlockListener) registerObserver(o, replay event.Observer[BlockEvent]) event.UnregisterFunc {
	bl.eventsMu.Lock()
	defer bl.eventsMu.Unlock()
	unregister := bl.events.Register(o)
	if bl.lastEvent != nil {
		replay(*bl.lastEvent)
	}
	return unregister
}

func (bl *BlockListener) Run(ctx context.Context) error {
	defer bl.logger.Info("block listener stopped")
	bl.logger.Info("running block listener")

	for ctx.Err() == nil {
		err := bl.listen(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !grpcutil.IsRetryLater(err) {
			bl.logger.Warn("block listener subscription failed", "err", err)
		}
		if err := common.Sleep(ctx, 3*time.Second); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (bl *BlockListener) listen(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sub, err := bl.stateChangesClient.StateChanges(ctx, &remoteproto.StateChangeRequest{})
	if err != nil {
		return err
	}

	for {
		batch, recvErr := sub.Recv()
		if recvErr != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return fmt.Errorf("block listener sub.Recv: %w", recvErr)
			}
		}

		if batch == nil || len(batch.ChangeBatch) == 0 {
			continue
		}

		latestChange := batch.ChangeBatch[len(batch.ChangeBatch)-1]
		blockEvent := BlockEvent{
			LatestBlockNum:  latestChange.BlockHeight,
			LatestBlockTime: latestChange.BlockTime,
			StateVersionID:  batch.StateVersionId,
			Unwind:          latestChange.Direction == remoteproto.Direction_UNWIND,
		}
		bl.eventsMu.Lock()
		bl.lastEvent = &blockEvent
		bl.events.NotifySync(blockEvent)
		bl.eventsMu.Unlock()
	}
}
