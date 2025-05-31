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

	"github.com/erigontech/erigon-lib/event"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/p2p/sentry"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-p2p/protocols/eth"
)

type DecodedInboundMessage[TPacket any] struct {
	*sentryproto.InboundMessage
	Decoded TPacket
	PeerId  *PeerId
}

type UnregisterFunc = event.UnregisterFunc

func NewMessageListener(
	logger log.Logger,
	sentryClient sentryproto.SentryClient,
	statusDataFactory sentry.StatusDataFactory,
	peerPenalizer *PeerPenalizer,
) *MessageListener {
	return &MessageListener{
		logger:                  logger,
		sentryClient:            sentryClient,
		statusDataFactory:       statusDataFactory,
		peerPenalizer:           peerPenalizer,
		newBlockObservers:       event.NewObservers[*DecodedInboundMessage[*eth.NewBlockPacket]](),
		newBlockHashesObservers: event.NewObservers[*DecodedInboundMessage[*eth.NewBlockHashesPacket]](),
		blockHeadersObservers:   event.NewObservers[*DecodedInboundMessage[*eth.BlockHeadersPacket66]](),
		blockBodiesObservers:    event.NewObservers[*DecodedInboundMessage[*eth.BlockBodiesPacket66]](),
		peerEventObservers:      event.NewObservers[*sentryproto.PeerEvent](),
	}
}

type MessageListener struct {
	logger                  log.Logger
	sentryClient            sentryproto.SentryClient
	statusDataFactory       sentry.StatusDataFactory
	peerPenalizer           *PeerPenalizer
	newBlockObservers       *event.Observers[*DecodedInboundMessage[*eth.NewBlockPacket]]
	newBlockHashesObservers *event.Observers[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]
	blockHeadersObservers   *event.Observers[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]
	blockBodiesObservers    *event.Observers[*DecodedInboundMessage[*eth.BlockBodiesPacket66]]
	peerEventObservers      *event.Observers[*sentryproto.PeerEvent]
	stopWg                  sync.WaitGroup
}

func (ml *MessageListener) Run(ctx context.Context) error {
	ml.logger.Info(messageListenerLogPrefix("running p2p message listener component"))

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

func (ml *MessageListener) RegisterNewBlockObserver(observer event.Observer[*DecodedInboundMessage[*eth.NewBlockPacket]]) UnregisterFunc {
	return ml.newBlockObservers.Register(observer)
}

func (ml *MessageListener) RegisterNewBlockHashesObserver(observer event.Observer[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]) UnregisterFunc {
	return ml.newBlockHashesObservers.Register(observer)
}

func (ml *MessageListener) RegisterBlockHeadersObserver(observer event.Observer[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]) UnregisterFunc {
	return ml.blockHeadersObservers.Register(observer)
}

func (ml *MessageListener) RegisterBlockBodiesObserver(observer event.Observer[*DecodedInboundMessage[*eth.BlockBodiesPacket66]]) UnregisterFunc {
	return ml.blockBodiesObservers.Register(observer)
}

func (ml *MessageListener) RegisterPeerEventObserver(observer event.Observer[*sentryproto.PeerEvent]) UnregisterFunc {
	return ml.peerEventObservers.Register(observer)
}

func (ml *MessageListener) listenInboundMessages(ctx context.Context) {
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

func (ml *MessageListener) listenPeerEvents(ctx context.Context) {
	streamFactory := func(ctx context.Context, sentryClient sentryproto.SentryClient) (grpc.ClientStream, error) {
		return sentryClient.PeerEvents(ctx, &sentryproto.PeerEventsRequest{}, grpc.WaitForReady(true))
	}

	streamMessages(ctx, ml, "PeerEvents", streamFactory, ml.notifyPeerEventObservers)
}

func (ml *MessageListener) notifyPeerEventObservers(peerEvent *sentryproto.PeerEvent) error {
	// wait on all observers to finish processing the peer event before notifying them
	// with subsequent events in order to preserve the ordering of the sentry messages
	ml.peerEventObservers.NotifySync(peerEvent)
	return nil
}

func streamMessages[TMessage any](
	ctx context.Context,
	ml *MessageListener,
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
	peerPenalizer *PeerPenalizer,
	observers *event.Observers[*DecodedInboundMessage[TPacket]],
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
