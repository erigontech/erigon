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
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

func NewBALResponder(
	logger log.Logger,
	messageListener *MessageListener,
	publisher *BALPublisher,
	db kv.TemporalRoDB,
	blockReader services.FullBlockReader,
) *BALResponder {
	r := &BALResponder{
		logger:      logger,
		publisher:   publisher,
		db:          db,
		blockReader: blockReader,
		tasks:       make(chan *DecodedInboundMessage[*eth.GetBlockAccessListsPacket66], responderTaskQueueSize),
	}
	messageListener.RegisterGetBlockAccessListsObserver(func(message *DecodedInboundMessage[*eth.GetBlockAccessListsPacket66]) {
		enqueueResponderTask(logger, balResponderLogPrefix, r.tasks, message)
	})
	return r
}

// BALResponder answers inbound eth/71 GetBlockAccessLists requests (EIP-8159) by
// looking up stored BALs and replying with a BlockAccessLists response positionally
// aligned to the request.
type BALResponder struct {
	logger      log.Logger
	publisher   *BALPublisher
	db          kv.TemporalRoDB
	blockReader services.FullBlockReader
	tasks       chan *DecodedInboundMessage[*eth.GetBlockAccessListsPacket66]
}

func (r *BALResponder) Run(ctx context.Context) error {
	r.logger.Info(balResponderLogPrefix + " running bal responder component")
	return processResponderTasks(ctx, r.tasks, r.handleGetBlockAccessLists, r.logger, balResponderLogPrefix)
}

func (r *BALResponder) handleGetBlockAccessLists(ctx context.Context, message *DecodedInboundMessage[*eth.GetBlockAccessListsPacket66]) error {
	tx, err := r.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	response := eth.AnswerGetBlockAccessListsQuery(tx, message.Decoded.GetBlockAccessListsPacket, r.blockReader)
	tx.Rollback()
	err = r.publisher.PublishBlockAccessLists(ctx, message.PeerId, eth.BlockAccessListsPacket66{
		RequestId:              message.Decoded.RequestId,
		BlockAccessListsPacket: response,
	})
	if err != nil && !errors.Is(err, ErrPeerNotFound) {
		return fmt.Errorf("send BlockAccessLists response: %w", err)
	}
	return nil
}

const balResponderLogPrefix = "[p2p.bal.responder]"
