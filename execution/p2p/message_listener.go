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

	"github.com/google/uuid"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common/event"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/protocols/wit"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

type DecodedInboundMessage[TPacket any] struct {
	*sentryproto.InboundMessage
	Decoded TPacket
	PeerId  *PeerId
}

type UnregisterFunc = event.UnregisterFunc

type RegisterOpt func(*registerOptions)

func WithReplayConnected(ctx context.Context) RegisterOpt {
	return func(opts *registerOptions) {
		opts.replayConnected = true
		opts.replayConnectedCtx = ctx
	}
}

type registerOptions struct {
	replayConnected    bool
	replayConnectedCtx context.Context
}

func applyRegisterOptions(opts []RegisterOpt) *registerOptions {
	defaultOptions := &registerOptions{}
	for _, opt := range opts {
		opt(defaultOptions)
	}
	return defaultOptions
}

func NewMessageListener(
	logger log.Logger,
	sentryClient sentryproto.SentryClient,
	statusDataFactory libsentry.StatusDataFactory,
	peerPenalizer *PeerPenalizer,
) *MessageListener {
	return &MessageListener{
		logger:                       logger,
		sentryClient:                 sentryClient,
		statusDataFactory:            statusDataFactory,
		peerPenalizer:                peerPenalizer,
		newBlockObservers:            event.NewObservers[*DecodedInboundMessage[*eth.NewBlockPacket]](),
		newBlockHashesObservers:      event.NewObservers[*DecodedInboundMessage[*eth.NewBlockHashesPacket]](),
		blockHeadersObservers:        event.NewObservers[*DecodedInboundMessage[*eth.BlockHeadersPacket66]](),
		blockBodiesObservers:         event.NewObservers[*DecodedInboundMessage[*eth.BlockBodiesPacket66]](),
		blockAccessListsObservers:    event.NewObservers[*DecodedInboundMessage[*eth.BlockAccessListsPacket66]](),
		blockRangeUpdateObservers:    event.NewObservers[*DecodedInboundMessage[*eth.BlockRangeUpdatePacket]](),
		newWitnessObservers:          event.NewObservers[*DecodedInboundMessage[*wit.NewWitnessPacket]](),
		witnessObservers:             event.NewObservers[*DecodedInboundMessage[*wit.WitnessPacketRLPPacket]](),
		getBlockHeadersObservers:     event.NewObservers[*DecodedInboundMessage[*eth.GetBlockHeadersPacket66]](),
		getBlockBodiesObservers:      event.NewObservers[*DecodedInboundMessage[*eth.GetBlockBodiesPacket66]](),
		getReceiptsObservers:         event.NewObservers[*DecodedInboundMessage[*eth.GetReceiptsPacket66]](),
		getReceipts70Observers:       event.NewObservers[*DecodedInboundMessage[*eth.GetReceiptsPacket70]](),
		getBlockAccessListsObservers: event.NewObservers[*DecodedInboundMessage[*eth.GetBlockAccessListsPacket66]](),
		getWitnessObservers:          event.NewObservers[*DecodedInboundMessage[*wit.GetWitnessPacket]](),
		peerEventObservers:           event.NewObservers[*sentryproto.PeerEvent](),
	}
}

type MessageListener struct {
	logger                       log.Logger
	sentryClient                 sentryproto.SentryClient
	statusDataFactory            libsentry.StatusDataFactory
	peerPenalizer                *PeerPenalizer
	newBlockObservers            *event.Observers[*DecodedInboundMessage[*eth.NewBlockPacket]]
	newBlockHashesObservers      *event.Observers[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]
	blockHeadersObservers        *event.Observers[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]
	blockBodiesObservers         *event.Observers[*DecodedInboundMessage[*eth.BlockBodiesPacket66]]
	blockAccessListsObservers    *event.Observers[*DecodedInboundMessage[*eth.BlockAccessListsPacket66]]
	blockRangeUpdateObservers    *event.Observers[*DecodedInboundMessage[*eth.BlockRangeUpdatePacket]]
	newWitnessObservers          *event.Observers[*DecodedInboundMessage[*wit.NewWitnessPacket]]
	witnessObservers             *event.Observers[*DecodedInboundMessage[*wit.WitnessPacketRLPPacket]]
	getBlockHeadersObservers     *event.Observers[*DecodedInboundMessage[*eth.GetBlockHeadersPacket66]]
	getBlockBodiesObservers      *event.Observers[*DecodedInboundMessage[*eth.GetBlockBodiesPacket66]]
	getReceiptsObservers         *event.Observers[*DecodedInboundMessage[*eth.GetReceiptsPacket66]]
	getReceipts70Observers       *event.Observers[*DecodedInboundMessage[*eth.GetReceiptsPacket70]]
	getBlockAccessListsObservers *event.Observers[*DecodedInboundMessage[*eth.GetBlockAccessListsPacket66]]
	getWitnessObservers          *event.Observers[*DecodedInboundMessage[*wit.GetWitnessPacket]]
	peerEventObservers           *event.Observers[*sentryproto.PeerEvent]
	stopWg                       sync.WaitGroup
}

func (ml *MessageListener) Run(ctx context.Context) error {
	ml.logger.Info(messageListenerLogPrefix("running p2p message listener component"))

	backgroundLoops := []func(ctx context.Context){
		ml.listenInboundMessages,
		ml.listenInboundGetHeadersMessages,
		ml.listenInboundUploadMessages,
		ml.listenPeerEventsBackground,
	}

	ml.stopWg.Add(len(backgroundLoops))
	for _, loop := range backgroundLoops {
		go func() {
			defer ml.stopWg.Done()
			loop(ctx)
		}()
	}

	<-ctx.Done()
	// once context has been cancelled wait for the background loops to stop
	ml.stopWg.Wait()

	// unregister all observers
	ml.newBlockObservers.Close()
	ml.newBlockHashesObservers.Close()
	ml.blockHeadersObservers.Close()
	ml.blockBodiesObservers.Close()
	ml.blockAccessListsObservers.Close()
	ml.blockRangeUpdateObservers.Close()
	ml.newWitnessObservers.Close()
	ml.witnessObservers.Close()
	ml.getBlockHeadersObservers.Close()
	ml.getBlockBodiesObservers.Close()
	ml.getReceiptsObservers.Close()
	ml.getReceipts70Observers.Close()
	ml.getBlockAccessListsObservers.Close()
	ml.getWitnessObservers.Close()
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

func (ml *MessageListener) RegisterBlockAccessListsObserver(observer event.Observer[*DecodedInboundMessage[*eth.BlockAccessListsPacket66]]) UnregisterFunc {
	return ml.blockAccessListsObservers.Register(observer)
}

func (ml *MessageListener) RegisterBlockRangeUpdateObserver(observer event.Observer[*DecodedInboundMessage[*eth.BlockRangeUpdatePacket]]) UnregisterFunc {
	return ml.blockRangeUpdateObservers.Register(observer)
}

func (ml *MessageListener) RegisterNewWitnessObserver(observer event.Observer[*DecodedInboundMessage[*wit.NewWitnessPacket]]) UnregisterFunc {
	return ml.newWitnessObservers.Register(observer)
}

func (ml *MessageListener) RegisterWitnessObserver(observer event.Observer[*DecodedInboundMessage[*wit.WitnessPacketRLPPacket]]) UnregisterFunc {
	return ml.witnessObservers.Register(observer)
}

func (ml *MessageListener) RegisterGetBlockHeadersObserver(observer event.Observer[*DecodedInboundMessage[*eth.GetBlockHeadersPacket66]]) UnregisterFunc {
	return ml.getBlockHeadersObservers.Register(observer)
}

func (ml *MessageListener) RegisterGetBlockBodiesObserver(observer event.Observer[*DecodedInboundMessage[*eth.GetBlockBodiesPacket66]]) UnregisterFunc {
	return ml.getBlockBodiesObservers.Register(observer)
}

// RegisterGetReceiptsObserver delivers both eth/68 and eth/69 GetReceipts requests;
// observers can tell them apart via the embedded InboundMessage.Id.
func (ml *MessageListener) RegisterGetReceiptsObserver(observer event.Observer[*DecodedInboundMessage[*eth.GetReceiptsPacket66]]) UnregisterFunc {
	return ml.getReceiptsObservers.Register(observer)
}

func (ml *MessageListener) RegisterGetReceipts70Observer(observer event.Observer[*DecodedInboundMessage[*eth.GetReceiptsPacket70]]) UnregisterFunc {
	return ml.getReceipts70Observers.Register(observer)
}

func (ml *MessageListener) RegisterGetBlockAccessListsObserver(observer event.Observer[*DecodedInboundMessage[*eth.GetBlockAccessListsPacket66]]) UnregisterFunc {
	return ml.getBlockAccessListsObservers.Register(observer)
}

func (ml *MessageListener) RegisterGetWitnessObserver(observer event.Observer[*DecodedInboundMessage[*wit.GetWitnessPacket]]) UnregisterFunc {
	return ml.getWitnessObservers.Register(observer)
}

func (ml *MessageListener) RegisterPeerEventObserver(observer event.Observer[*sentryproto.PeerEvent], opts ...RegisterOpt) UnregisterFunc {
	options := applyRegisterOptions(opts)
	if options.replayConnected {
		// we always need to open a new stream to replay connected peers
		ctx, cancel := context.WithCancel(options.replayConnectedCtx)
		go ml.listenPeerEvents(ctx, uuid.New().String(), func(peerEvent *sentryproto.PeerEvent) error {
			observer(peerEvent)
			return nil
		})
		return UnregisterFunc(cancel)
	}
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
				sentryproto.MessageId_BLOCK_ACCESS_LISTS_71,
				sentryproto.MessageId_BLOCK_RANGE_UPDATE_69,
				sentryproto.MessageId_NEW_WITNESS_W0,
				sentryproto.MessageId_BLOCK_WITNESS_W0,
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
		case sentryproto.MessageId_BLOCK_ACCESS_LISTS_71:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.blockAccessListsObservers, message)
		case sentryproto.MessageId_BLOCK_RANGE_UPDATE_69:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.blockRangeUpdateObservers, message)
		case sentryproto.MessageId_NEW_WITNESS_W0:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.newWitnessObservers, message)
		case sentryproto.MessageId_BLOCK_WITNESS_W0:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.witnessObservers, message)
		default:
			return nil
		}
	})
}

// listenInboundGetHeadersMessages pumps GetBlockHeaders requests on a dedicated
// stream because header serving speed is important for network health.
func (ml *MessageListener) listenInboundGetHeadersMessages(ctx context.Context) {
	streamFactory := func(ctx context.Context, sentryClient sentryproto.SentryClient) (grpc.ClientStream, error) {
		messagesRequest := sentryproto.MessagesRequest{
			Ids: []sentryproto.MessageId{
				sentryproto.MessageId_GET_BLOCK_HEADERS_66,
			},
		}

		return sentryClient.Messages(ctx, &messagesRequest, grpc.WaitForReady(true))
	}

	streamMessages(ctx, ml, "InboundGetHeadersMessages", streamFactory, func(message *sentryproto.InboundMessage) error {
		switch message.Id {
		case sentryproto.MessageId_GET_BLOCK_HEADERS_66:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.getBlockHeadersObservers, message)
		default:
			return nil
		}
	})
}

// listenInboundUploadMessages pumps the potentially heavy "get" requests
// (bodies, receipts, BALs, witnesses) on their own stream so that slow serving
// does not hold back the other message streams.
func (ml *MessageListener) listenInboundUploadMessages(ctx context.Context) {
	streamFactory := func(ctx context.Context, sentryClient sentryproto.SentryClient) (grpc.ClientStream, error) {
		messagesRequest := sentryproto.MessagesRequest{
			Ids: []sentryproto.MessageId{
				sentryproto.MessageId_GET_BLOCK_BODIES_66,
				sentryproto.MessageId_GET_RECEIPTS_66,
				sentryproto.MessageId_GET_RECEIPTS_69,
				sentryproto.MessageId_GET_RECEIPTS_70,
				sentryproto.MessageId_GET_BLOCK_ACCESS_LISTS_71,
				sentryproto.MessageId_GET_BLOCK_WITNESS_W0,
			},
		}

		return sentryClient.Messages(ctx, &messagesRequest, grpc.WaitForReady(true))
	}

	streamMessages(ctx, ml, "InboundUploadMessages", streamFactory, func(message *sentryproto.InboundMessage) error {
		switch message.Id {
		case sentryproto.MessageId_GET_BLOCK_BODIES_66:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.getBlockBodiesObservers, message)
		case sentryproto.MessageId_GET_RECEIPTS_66, sentryproto.MessageId_GET_RECEIPTS_69:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.getReceiptsObservers, message)
		case sentryproto.MessageId_GET_RECEIPTS_70:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.getReceipts70Observers, message)
		case sentryproto.MessageId_GET_BLOCK_ACCESS_LISTS_71:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.getBlockAccessListsObservers, message)
		case sentryproto.MessageId_GET_BLOCK_WITNESS_W0:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.getWitnessObservers, message)
		default:
			return nil
		}
	})
}

func (ml *MessageListener) listenPeerEventsBackground(ctx context.Context) {
	ml.listenPeerEvents(ctx, "Background", ml.notifyPeerEventObservers)
}

func (ml *MessageListener) listenPeerEvents(ctx context.Context, suffix string, handler func(*sentryproto.PeerEvent) error) {
	streamFactory := func(ctx context.Context, sentryClient sentryproto.SentryClient) (grpc.ClientStream, error) {
		return sentryClient.PeerEvents(ctx, &sentryproto.PeerEventsRequest{}, grpc.WaitForReady(true))
	}

	streamMessages(ctx, ml, fmt.Sprintf("PeerEvents-%s", suffix), streamFactory, handler)
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
	streamFactory libsentry.MessageStreamFactory,
	handler func(event *TMessage) error,
) {
	messageHandler := func(_ context.Context, event *TMessage, client sentryproto.SentryClient) error {
		return handler(event)
	}

	libsentry.ReconnectAndPumpStreamLoop(
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
