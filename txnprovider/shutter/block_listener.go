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

	"github.com/erigontech/erigon/common/event"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

// OverlaySubscriber provides the ability to subscribe to SharedDomains
// overlay lifecycle events. When a non-nil SD is received the overlay is
// active (pre-commit); when nil is received the commit is done.
type OverlaySubscriber interface {
	AddOverlaySubscription() (chan *execctx.SharedDomains, func())
}

type BlockEvent struct {
	LatestBlockNum  uint64
	LatestBlockTime uint64
	Unwind          bool
}

type BlockListener struct {
	logger             log.Logger
	stateChangesClient stateChangesClient
	overlaySubscriber  OverlaySubscriber // nil when overlay-awareness is not needed
	events             *event.Observers[BlockEvent]
}

func NewBlockListener(logger log.Logger, stateChangesClient stateChangesClient, overlaySubscriber OverlaySubscriber) *BlockListener {
	return &BlockListener{
		logger:             logger,
		stateChangesClient: stateChangesClient,
		overlaySubscriber:  overlaySubscriber,
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

	// When overlay-awareness is enabled we subscribe to SharedDomains
	// lifecycle events. Block events that arrive while the overlay is
	// active (SD != nil) are buffered and flushed once the overlay
	// commits (SD == nil). This ensures downstream consumers (e.g. eon
	// tracker) that read via JSON-RPC always see committed data.
	var overlayCh chan *execctx.SharedDomains
	if bl.overlaySubscriber != nil {
		var unsubOverlay func()
		overlayCh, unsubOverlay = bl.overlaySubscriber.AddOverlaySubscription()
		defer unsubOverlay()
	}

	overlayActive := false
	var buffered []BlockEvent

	// batchC is fed by a goroutine that reads from the gRPC stream so
	// we can select on both batch arrivals and overlay events.
	batchC := make(chan *remoteproto.StateChangeBatch)
	errC := make(chan error, 1)
	go func() {
		defer close(batchC)
		for {
			batch, recvErr := sub.Recv()
			if recvErr != nil {
				errC <- recvErr
				return
			}
			select {
			case batchC <- batch:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case recvErr := <-errC:
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return fmt.Errorf("block listener sub.Recv: %w", recvErr)
			}
		case sd, ok := <-overlayCh:
			if !ok {
				// Channel closed — overlay subscriber gone. Disable
				// overlay awareness and flush any buffered events.
				overlayCh = nil
				for _, ev := range buffered {
					bl.events.NotifySync(ev)
				}
				buffered = nil
				overlayActive = false
				continue
			}
			if sd != nil {
				overlayActive = true
			} else {
				// Overlay committed — flush buffered events.
				overlayActive = false
				for _, ev := range buffered {
					bl.events.NotifySync(ev)
				}
				buffered = nil
			}
		case batch, ok := <-batchC:
			if !ok {
				// Stream closed, error already sent on errC.
				continue
			}
			if batch == nil || len(batch.ChangeBatch) == 0 {
				continue
			}

			latestChange := batch.ChangeBatch[len(batch.ChangeBatch)-1]
			blockEvent := BlockEvent{
				LatestBlockNum:  latestChange.BlockHeight,
				LatestBlockTime: latestChange.BlockTime,
				Unwind:          latestChange.Direction == remoteproto.Direction_UNWIND,
			}

			if overlayActive {
				bl.logger.Debug("buffering block event during overlay", "blockNum", blockEvent.LatestBlockNum)
				buffered = append(buffered, blockEvent)
			} else {
				bl.events.NotifySync(blockEvent)
			}
		}
	}
}
