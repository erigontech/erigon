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

package p2p

import (
	"context"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

func NewBlockRangeUpdateHandler(
	logger log.Logger,
	sentryClient sentryproto.SentryClient,
	peerPenalizer *PeerPenalizer,
	messageListener *MessageListener,
) *BlockRangeUpdateHandler {
	h := &BlockRangeUpdateHandler{
		logger:        logger,
		sentryClient:  sentryClient,
		peerPenalizer: peerPenalizer,
		tasks:         make(chan *DecodedInboundMessage[*eth.BlockRangeUpdatePacket], responderTaskQueueSize),
	}
	messageListener.RegisterBlockRangeUpdateObserver(func(message *DecodedInboundMessage[*eth.BlockRangeUpdatePacket]) {
		enqueueResponderTask(logger, blockRangeUpdateLogPrefix, h.tasks, message)
	})
	return h
}

// BlockRangeUpdateHandler keeps the sentries' per-peer block range metadata up to
// date from inbound eth/69 BlockRangeUpdate announcements, kicking peers that
// announce invalid ranges.
type BlockRangeUpdateHandler struct {
	logger        log.Logger
	sentryClient  sentryproto.SentryClient
	peerPenalizer *PeerPenalizer
	tasks         chan *DecodedInboundMessage[*eth.BlockRangeUpdatePacket]
}

func (h *BlockRangeUpdateHandler) Run(ctx context.Context) error {
	h.logger.Info(blockRangeUpdateLogPrefix + " running block range update handler component")
	return processResponderTasks(ctx, h.tasks, h.handleBlockRangeUpdate, h.logger, blockRangeUpdateLogPrefix)
}

func (h *BlockRangeUpdateHandler) handleBlockRangeUpdate(ctx context.Context, message *DecodedInboundMessage[*eth.BlockRangeUpdatePacket]) error {
	err := message.Decoded.Validate()
	if err != nil {
		h.logger.Debug(blockRangeUpdateLogPrefix+" kick peer for invalid BlockRangeUpdate", "peer", message.PeerId, "err", err)
		penalizeErr := h.peerPenalizer.Penalize(ctx, message.PeerId)
		if penalizeErr != nil {
			h.logger.Error(blockRangeUpdateLogPrefix+" could not send penalty", "err", penalizeErr)
		}
		return err
	}
	// fire-and-forget so a slow sentry cannot hold back subsequent updates
	go func() {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := h.sentryClient.SetPeerBlockRange(ctx, &sentryproto.SetPeerBlockRangeRequest{
			PeerId:            message.PeerId.H512(),
			LatestBlockHeight: message.Decoded.Latest,
			MinBlockHeight:    message.Decoded.Earliest,
		})
		if err != nil {
			h.logger.Warn(blockRangeUpdateLogPrefix+" could not send latest block range for peer", "err", err, "peer", message.PeerId)
		}
	}()
	return nil
}

const blockRangeUpdateLogPrefix = "[p2p.block.range.update.handler]"
