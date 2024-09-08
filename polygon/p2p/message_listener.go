// Copyright 2024 The Erigon Authors
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
	"fmt"
	"sync"

	"google.golang.org/grpc"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/p2p/sentry"
	"github.com/erigontech/erigon/eth/protocols/eth"
	"github.com/erigontech/erigon/polygon/polygoncommon"
	"github.com/erigontech/erigon/rlp"
)

type DecodedInboundMessage[TPacket any] struct {
	*sentryproto.InboundMessage
	Decoded TPacket
	PeerId  *PeerId
}

type UnregisterFunc = polygoncommon.UnregisterFunc

type MessageListener interface {
	Run(ctx context.Context) error
	RegisterNewBlockObserver(observer polygoncommon.Observer[*DecodedInboundMessage[*eth.NewBlockPacket]]) UnregisterFunc
	RegisterNewBlockHashesObserver(observer polygoncommon.Observer[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]) UnregisterFunc
	RegisterBlockHeadersObserver(observer polygoncommon.Observer[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]) UnregisterFunc
	RegisterBlockBodiesObserver(observer polygoncommon.Observer[*DecodedInboundMessage[*eth.BlockBodiesPacket66]]) UnregisterFunc
	RegisterPeerEventObserver(observer polygoncommon.Observer[*sentryproto.PeerEvent]) UnregisterFunc
}

func NewMessageListener(
	logger log.Logger,
	sentryClient sentryproto.SentryClient,
	statusDataFactory sentry.StatusDataFactory,
	peerPenalizer PeerPenalizer,
) MessageListener {
	return newMessageListener(logger, sentryClient, statusDataFactory, peerPenalizer)
}

func newMessageListener(
	logger log.Logger,
	sentryClient sentryproto.SentryClient,
	statusDataFactory sentry.StatusDataFactory,
	peerPenalizer PeerPenalizer,
) *messageListener {
	return &messageListener{
		logger:                  logger,
		sentryClient:            sentryClient,
		statusDataFactory:       statusDataFactory,
		peerPenalizer:           peerPenalizer,
		newBlockObservers:       polygoncommon.NewObservers[*DecodedInboundMessage[*eth.NewBlockPacket]](),
		newBlockHashesObservers: polygoncommon.NewObservers[*DecodedInboundMessage[*eth.NewBlockHashesPacket]](),
		blockHeadersObservers:   polygoncommon.NewObservers[*DecodedInboundMessage[*eth.BlockHeadersPacket66]](),
		blockBodiesObservers:    polygoncommon.NewObservers[*DecodedInboundMessage[*eth.BlockBodiesPacket66]](),
		peerEventObservers:      polygoncommon.NewObservers[*sentryproto.PeerEvent](),
	}
}

type messageListener struct {
	logger                  log.Logger
	sentryClient            sentryproto.SentryClient
	statusDataFactory       sentry.StatusDataFactory
	peerPenalizer           PeerPenalizer
	newBlockObservers       *polygoncommon.Observers[*DecodedInboundMessage[*eth.NewBlockPacket]]
	newBlockHashesObservers *polygoncommon.Observers[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]
	blockHeadersObservers   *polygoncommon.Observers[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]
	blockBodiesObservers    *polygoncommon.Observers[*DecodedInboundMessage[*eth.BlockBodiesPacket66]]
	peerEventObservers      *polygoncommon.Observers[*sentryproto.PeerEvent]
	stopWg                  sync.WaitGroup
}

func (ml *messageListener) Run(ctx context.Context) error {
	ml.logger.Debug(messageListenerLogPrefix("running p2p message listener component"))

	backgroundLoops := []func(ctx context.Context){
		ml.listenInboundMessages,
		ml.listenPeerEvents,
	}

	ml.stopWg.Add(len(backgroundLoops))
	for _, loop := range backgroundLoops {
		go loop(ctx)
	}

	<-ctx.Done()
	// once context has been cancelled wait for the background loops to stop
	ml.stopWg.Wait()

	// unregister all observers
	ml.newBlockObservers.Close()
	ml.newBlockHashesObservers.Close()
	ml.blockHeadersObservers.Close()
	ml.blockBodiesObservers.Close()
	ml.peerEventObservers.Close()
	return ctx.Err()
}

func (ml *messageListener) RegisterNewBlockObserver(observer polygoncommon.Observer[*DecodedInboundMessage[*eth.NewBlockPacket]]) UnregisterFunc {
	return ml.newBlockObservers.Register(observer)
}

func (ml *messageListener) RegisterNewBlockHashesObserver(observer polygoncommon.Observer[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]) UnregisterFunc {
	return ml.newBlockHashesObservers.Register(observer)
}

func (ml *messageListener) RegisterBlockHeadersObserver(observer polygoncommon.Observer[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]) UnregisterFunc {
	return ml.blockHeadersObservers.Register(observer)
}

func (ml *messageListener) RegisterBlockBodiesObserver(observer polygoncommon.Observer[*DecodedInboundMessage[*eth.BlockBodiesPacket66]]) UnregisterFunc {
	return ml.blockBodiesObservers.Register(observer)
}

func (ml *messageListener) RegisterPeerEventObserver(observer polygoncommon.Observer[*sentryproto.PeerEvent]) UnregisterFunc {
	return ml.peerEventObservers.Register(observer)
}

func (ml *messageListener) listenInboundMessages(ctx context.Context) {
	streamFactory := func(ctx context.Context, sentryClient sentryproto.SentryClient) (grpc.ClientStream, error) {
		messagesRequest := sentryproto.MessagesRequest{
			Ids: []sentryproto.MessageId{
				sentryproto.MessageId_NEW_BLOCK_66,
				sentryproto.MessageId_NEW_BLOCK_HASHES_66,
				sentryproto.MessageId_BLOCK_HEADERS_66,
				sentryproto.MessageId_BLOCK_BODIES_66,
			},
		}

		return sentryClient.Messages(ctx, &messagesRequest, grpc.WaitForReady(true))
	}

	streamMessages(ctx, ml, "InboundMessages", streamFactory, func(message *sentryproto.InboundMessage) error {
		switch message.Id {
		case sentryproto.MessageId_NEW_BLOCK_66:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.newBlockObservers, message)
		case sentryproto.MessageId_NEW_BLOCK_HASHES_66:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.newBlockHashesObservers, message)
		case sentryproto.MessageId_BLOCK_HEADERS_66:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.blockHeadersObservers, message)
		case sentryproto.MessageId_BLOCK_BODIES_66:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.blockBodiesObservers, message)
		default:
			return nil
		}
	})
}

func (ml *messageListener) listenPeerEvents(ctx context.Context) {
	streamFactory := func(ctx context.Context, sentryClient sentryproto.SentryClient) (grpc.ClientStream, error) {
		return sentryClient.PeerEvents(ctx, &sentryproto.PeerEventsRequest{}, grpc.WaitForReady(true))
	}

	streamMessages(ctx, ml, "PeerEvents", streamFactory, ml.notifyPeerEventObservers)
}

func (ml *messageListener) notifyPeerEventObservers(peerEvent *sentryproto.PeerEvent) error {
	// wait on all observers to finish processing the peer event before notifying them
	// with subsequent events in order to preserve the ordering of the sentry messages
	ml.peerEventObservers.NotifySync(peerEvent)
	return nil
}

func streamMessages[TMessage any](
	ctx context.Context,
	ml *messageListener,
	name string,
	streamFactory sentry.MessageStreamFactory,
	handler func(event *TMessage) error,
) {
	defer ml.stopWg.Done()

	messageHandler := func(_ context.Context, event *TMessage, client sentryproto.SentryClient) error {
		return handler(event)
	}

	sentry.ReconnectAndPumpStreamLoop(
		ctx,
		ml.sentryClient,
		ml.statusDataFactory,
		name,
		streamFactory,
		func() *TMessage { return new(TMessage) },
		messageHandler,
		nil,
		ml.logger,
	)
}

func notifyInboundMessageObservers[TPacket any](
	ctx context.Context,
	logger log.Logger,
	peerPenalizer PeerPenalizer,
	observers *polygoncommon.Observers[*DecodedInboundMessage[TPacket]],
	message *sentryproto.InboundMessage,
) error {
	peerId := PeerIdFromH512(message.PeerId)

	var decodedData TPacket
	if err := rlp.DecodeBytes(message.Data, &decodedData); err != nil {
		if rlp.IsInvalidRLPError(err) {
			logger.Debug(messageListenerLogPrefix("penalizing peer - invalid rlp"), "peerId", peerId, "err", err)

			if penalizeErr := peerPenalizer.Penalize(ctx, peerId); penalizeErr != nil {
				err = fmt.Errorf("%w: %w", penalizeErr, err)
			}
		}

		return err
	}

	decodedMessage := DecodedInboundMessage[TPacket]{
		InboundMessage: message,
		Decoded:        decodedData,
		PeerId:         peerId,
	}
	observers.Notify(&decodedMessage)

	return nil
}

func messageListenerLogPrefix(message string) string {
	return "[p2p.message.listener] " + message
}
