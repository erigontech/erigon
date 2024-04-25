package p2p

import (
	"context"
	"fmt"
	"sync"

	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	sentrymulticlient "github.com/ledgerwatch/erigon/p2p/sentry/sentry_multi_client"
	"github.com/ledgerwatch/erigon/rlp"
)

type DecodedInboundMessage[TPacket any] struct {
	*sentry.InboundMessage
	Decoded TPacket
	PeerId  *PeerId
}

type MessageObserver[TMessage any] func(message TMessage)

type UnregisterFunc func()

type MessageListener interface {
	Run(ctx context.Context)
	RegisterNewBlockObserver(observer MessageObserver[*DecodedInboundMessage[*eth.NewBlockPacket]]) UnregisterFunc
	RegisterNewBlockHashesObserver(observer MessageObserver[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]) UnregisterFunc
	RegisterBlockHeadersObserver(observer MessageObserver[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]) UnregisterFunc
	RegisterBlockBodiesObserver(observer MessageObserver[*DecodedInboundMessage[*eth.BlockBodiesPacket66]]) UnregisterFunc
	RegisterPeerEventObserver(observer MessageObserver[*sentry.PeerEvent]) UnregisterFunc
}

func NewMessageListener(
	logger log.Logger,
	sentryClient direct.SentryClient,
	statusDataFactory sentrymulticlient.StatusDataFactory,
	peerPenalizer PeerPenalizer,
) MessageListener {
	return newMessageListener(logger, sentryClient, statusDataFactory, peerPenalizer)
}

func newMessageListener(
	logger log.Logger,
	sentryClient direct.SentryClient,
	statusDataFactory sentrymulticlient.StatusDataFactory,
	peerPenalizer PeerPenalizer,
) *messageListener {
	return &messageListener{
		logger:                  logger,
		sentryClient:            sentryClient,
		statusDataFactory:       statusDataFactory,
		peerPenalizer:           peerPenalizer,
		newBlockObservers:       map[uint64]MessageObserver[*DecodedInboundMessage[*eth.NewBlockPacket]]{},
		newBlockHashesObservers: map[uint64]MessageObserver[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]{},
		blockHeadersObservers:   map[uint64]MessageObserver[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]{},
		blockBodiesObservers:    map[uint64]MessageObserver[*DecodedInboundMessage[*eth.BlockBodiesPacket66]]{},
		peerEventObservers:      map[uint64]MessageObserver[*sentry.PeerEvent]{},
	}
}

type messageListener struct {
	once                    sync.Once
	observerIdSequence      uint64
	logger                  log.Logger
	sentryClient            direct.SentryClient
	statusDataFactory       sentrymulticlient.StatusDataFactory
	peerPenalizer           PeerPenalizer
	observersMu             sync.Mutex
	newBlockObservers       map[uint64]MessageObserver[*DecodedInboundMessage[*eth.NewBlockPacket]]
	newBlockHashesObservers map[uint64]MessageObserver[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]
	blockHeadersObservers   map[uint64]MessageObserver[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]
	blockBodiesObservers    map[uint64]MessageObserver[*DecodedInboundMessage[*eth.BlockBodiesPacket66]]
	peerEventObservers      map[uint64]MessageObserver[*sentry.PeerEvent]
	stopWg                  sync.WaitGroup
}

func (ml *messageListener) Run(ctx context.Context) {
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
	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()
	ml.newBlockObservers = map[uint64]MessageObserver[*DecodedInboundMessage[*eth.NewBlockPacket]]{}
	ml.newBlockHashesObservers = map[uint64]MessageObserver[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]{}
	ml.blockHeadersObservers = map[uint64]MessageObserver[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]{}
	ml.blockBodiesObservers = map[uint64]MessageObserver[*DecodedInboundMessage[*eth.BlockBodiesPacket66]]{}
	ml.peerEventObservers = map[uint64]MessageObserver[*sentry.PeerEvent]{}
}

func (ml *messageListener) RegisterNewBlockObserver(observer MessageObserver[*DecodedInboundMessage[*eth.NewBlockPacket]]) UnregisterFunc {
	return registerObserver(ml, ml.newBlockObservers, observer)
}

func (ml *messageListener) RegisterNewBlockHashesObserver(observer MessageObserver[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]) UnregisterFunc {
	return registerObserver(ml, ml.newBlockHashesObservers, observer)
}

func (ml *messageListener) RegisterBlockHeadersObserver(observer MessageObserver[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]) UnregisterFunc {
	return registerObserver(ml, ml.blockHeadersObservers, observer)
}

func (ml *messageListener) RegisterBlockBodiesObserver(observer MessageObserver[*DecodedInboundMessage[*eth.BlockBodiesPacket66]]) UnregisterFunc {
	return registerObserver(ml, ml.blockBodiesObservers, observer)
}

func (ml *messageListener) RegisterPeerEventObserver(observer MessageObserver[*sentry.PeerEvent]) UnregisterFunc {
	return registerObserver(ml, ml.peerEventObservers, observer)
}

func (ml *messageListener) listenInboundMessages(ctx context.Context) {
	streamFactory := func(ctx context.Context, sentryClient direct.SentryClient) (sentrymulticlient.SentryMessageStream, error) {
		messagesRequest := sentry.MessagesRequest{
			Ids: []sentry.MessageId{
				sentry.MessageId_NEW_BLOCK_66,
				sentry.MessageId_NEW_BLOCK_HASHES_66,
				sentry.MessageId_BLOCK_HEADERS_66,
				sentry.MessageId_BLOCK_BODIES_66,
			},
		}

		return sentryClient.Messages(ctx, &messagesRequest, grpc.WaitForReady(true))
	}

	streamMessages(ctx, ml, "InboundMessages", streamFactory, func(message *sentry.InboundMessage) error {
		ml.observersMu.Lock()
		defer ml.observersMu.Unlock()

		switch message.Id {
		case sentry.MessageId_NEW_BLOCK_66:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.newBlockObservers, message)
		case sentry.MessageId_NEW_BLOCK_HASHES_66:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.newBlockHashesObservers, message)
		case sentry.MessageId_BLOCK_HEADERS_66:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.blockHeadersObservers, message)
		case sentry.MessageId_BLOCK_BODIES_66:
			return notifyInboundMessageObservers(ctx, ml.logger, ml.peerPenalizer, ml.blockBodiesObservers, message)
		default:
			return nil
		}
	})
}

func (ml *messageListener) listenPeerEvents(ctx context.Context) {
	streamFactory := func(ctx context.Context, sentryClient direct.SentryClient) (sentrymulticlient.SentryMessageStream, error) {
		return sentryClient.PeerEvents(ctx, &sentry.PeerEventsRequest{}, grpc.WaitForReady(true))
	}

	streamMessages(ctx, ml, "PeerEvents", streamFactory, ml.notifyPeerEventObservers)
}

func (ml *messageListener) notifyPeerEventObservers(peerEvent *sentry.PeerEvent) error {
	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	// wait on all observers to finish processing the peer event before notifying them
	// with subsequent events in order to preserve the ordering of the sentry messages
	var wg sync.WaitGroup
	for _, observer := range ml.peerEventObservers {
		wg.Add(1)
		go func(observer MessageObserver[*sentry.PeerEvent]) {
			defer wg.Done()
			observer(peerEvent)
		}(observer)
	}

	wg.Wait()
	return nil
}

func (ml *messageListener) nextObserverId() uint64 {
	id := ml.observerIdSequence
	ml.observerIdSequence++
	return id
}

func registerObserver[TMessage any](
	ml *messageListener,
	observers map[uint64]MessageObserver[*TMessage],
	observer MessageObserver[*TMessage],
) UnregisterFunc {
	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	observerId := ml.nextObserverId()
	observers[observerId] = observer
	return unregisterFunc(&ml.observersMu, observers, observerId)
}

func unregisterFunc[TMessage any](mu *sync.Mutex, observers map[uint64]MessageObserver[TMessage], observerId uint64) UnregisterFunc {
	return func() {
		mu.Lock()
		defer mu.Unlock()

		delete(observers, observerId)
	}
}

func streamMessages[TMessage any](
	ctx context.Context,
	ml *messageListener,
	name string,
	streamFactory sentrymulticlient.SentryMessageStreamFactory,
	handler func(event *TMessage) error,
) {
	defer ml.stopWg.Done()

	messageHandler := func(_ context.Context, event *TMessage, _ direct.SentryClient) error {
		return handler(event)
	}

	sentrymulticlient.SentryReconnectAndPumpStreamLoop(
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
	observers map[uint64]MessageObserver[*DecodedInboundMessage[TPacket]],
	message *sentry.InboundMessage,
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

	notifyObservers(observers, &DecodedInboundMessage[TPacket]{
		InboundMessage: message,
		Decoded:        decodedData,
		PeerId:         peerId,
	})

	return nil
}

func notifyObservers[TMessage any](observers map[uint64]MessageObserver[TMessage], message TMessage) {
	for _, observer := range observers {
		go observer(message)
	}
}

func messageListenerLogPrefix(message string) string {
	return fmt.Sprintf("[p2p.message.listener] %s", message)
}
