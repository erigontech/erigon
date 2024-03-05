package p2p

import (
	"context"
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
	Raw       *sentry.InboundMessage
	Decoded   TPacket
	DecodeErr error
	PeerId    PeerId
}

type MessageObserver[TMessage any] func(message TMessage)

type UnregisterFunc func()

type MessageListener interface {
	Start(ctx context.Context)
	Stop()

	RegisterNewBlockObserver(observer MessageObserver[*DecodedInboundMessage[*eth.NewBlockPacket]]) UnregisterFunc
	RegisterNewBlockHashesObserver(observer MessageObserver[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]) UnregisterFunc
	RegisterBlockHeadersObserver(observer MessageObserver[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]) UnregisterFunc
	RegisterPeerEventObserver(observer MessageObserver[*sentry.PeerEvent]) UnregisterFunc
}

func NewMessageListener(logger log.Logger, sentryClient direct.SentryClient) MessageListener {
	return &messageListener{
		logger:       logger,
		sentryClient: sentryClient,

		newBlockObservers:       map[uint64]MessageObserver[*DecodedInboundMessage[*eth.NewBlockPacket]]{},
		newBlockHashesObservers: map[uint64]MessageObserver[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]{},
		blockHeadersObservers:   map[uint64]MessageObserver[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]{},
		peerEventObservers:      map[uint64]MessageObserver[*sentry.PeerEvent]{},
	}
}

type messageListener struct {
	once               sync.Once
	observerIdSequence uint64
	logger             log.Logger
	sentryClient       direct.SentryClient
	observersMu        sync.Mutex
	stopWg             sync.WaitGroup

	newBlockObservers       map[uint64]MessageObserver[*DecodedInboundMessage[*eth.NewBlockPacket]]
	newBlockHashesObservers map[uint64]MessageObserver[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]
	blockHeadersObservers   map[uint64]MessageObserver[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]
	peerEventObservers      map[uint64]MessageObserver[*sentry.PeerEvent]
}

func (ml *messageListener) Start(ctx context.Context) {
	ml.once.Do(func() {
		backgroundLoops := []func(ctx context.Context){
			ml.listenInboundMessages,
			ml.listenPeerEvents,
		}

		ml.stopWg.Add(len(backgroundLoops))
		for _, loop := range backgroundLoops {
			go loop(ctx)
		}
	})
}

func (ml *messageListener) Stop() {
	ml.stopWg.Wait()

	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	ml.newBlockObservers = map[uint64]MessageObserver[*DecodedInboundMessage[*eth.NewBlockPacket]]{}
	ml.newBlockHashesObservers = map[uint64]MessageObserver[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]{}
	ml.blockHeadersObservers = map[uint64]MessageObserver[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]{}
	ml.peerEventObservers = map[uint64]MessageObserver[*sentry.PeerEvent]{}
}

func (ml *messageListener) RegisterNewBlockObserver(observer MessageObserver[*DecodedInboundMessage[*eth.NewBlockPacket]]) UnregisterFunc {
	return registerEventObserver(ml, ml.newBlockObservers, observer)
}

func (ml *messageListener) RegisterNewBlockHashesObserver(observer MessageObserver[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]) UnregisterFunc {
	return registerEventObserver(ml, ml.newBlockHashesObservers, observer)
}

func (ml *messageListener) RegisterBlockHeadersObserver(observer MessageObserver[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]) UnregisterFunc {
	return registerEventObserver(ml, ml.blockHeadersObservers, observer)
}

func (ml *messageListener) RegisterPeerEventObserver(observer MessageObserver[*sentry.PeerEvent]) UnregisterFunc {
	return registerEventObserver(ml, ml.peerEventObservers, observer)
}

func registerEventObserver[TMessage any](ml *messageListener, observers map[uint64]MessageObserver[*TMessage], observer MessageObserver[*TMessage]) UnregisterFunc {
	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	observerId := ml.nextObserverId()
	observers[observerId] = observer
	return unregisterFunc(&ml.observersMu, observers, observerId)
}

func (ml *messageListener) listenInboundMessages(ctx context.Context) {
	streamFactory := func(ctx context.Context, sentryClient direct.SentryClient) (sentrymulticlient.SentryMessageStream, error) {
		messagesRequest := sentry.MessagesRequest{
			Ids: []sentry.MessageId{
				sentry.MessageId_NEW_BLOCK_66,
				sentry.MessageId_NEW_BLOCK_HASHES_66,
				sentry.MessageId_BLOCK_HEADERS_66,
			},
		}

		return sentryClient.Messages(ctx, &messagesRequest, grpc.WaitForReady(true))
	}

	streamEvents(ctx, ml, "InboundMessages", streamFactory, func(message *sentry.InboundMessage) {
		switch message.Id {
		case sentry.MessageId_NEW_BLOCK_66:
			notifyInboundMessageObservers[eth.NewBlockPacket](ml, ml.newBlockObservers, message)
		case sentry.MessageId_NEW_BLOCK_HASHES_66:
			notifyInboundMessageObservers[eth.NewBlockHashesPacket](ml, ml.newBlockHashesObservers, message)
		case sentry.MessageId_BLOCK_HEADERS_66:
			notifyInboundMessageObservers[eth.BlockHeadersPacket66](ml, ml.blockHeadersObservers, message)
		}
	})
}

func (ml *messageListener) listenPeerEvents(ctx context.Context) {
	streamFactory := func(ctx context.Context, sentryClient direct.SentryClient) (sentrymulticlient.SentryMessageStream, error) {
		return sentryClient.PeerEvents(ctx, &sentry.PeerEventsRequest{}, grpc.WaitForReady(true))
	}

	streamEvents(ctx, ml, "PeerEvents", streamFactory, ml.notifyPeerEventObservers)
}

func streamEvents[TMessage any](
	ctx context.Context,
	ml *messageListener,
	name string,
	streamFactory func(ctx context.Context, sentryClient direct.SentryClient) (sentrymulticlient.SentryMessageStream, error),
	handler func(event *TMessage),
) {
	defer ml.stopWg.Done()

	eventHandler := func(_ context.Context, event *TMessage, _ direct.SentryClient) error {
		handler(event)
		return nil
	}

	sentrymulticlient.SentryReconnectAndPumpStreamLoop(
		ctx,
		ml.sentryClient,
		ml.statusDataFactory(),
		name,
		streamFactory,
		func() *TMessage { return new(TMessage) },
		eventHandler,
		nil,
		ml.logger,
	)
}

func notifyInboundMessageObservers[TPacket any](ml *messageListener, observers map[uint64]MessageObserver[*DecodedInboundMessage[*TPacket]], message *sentry.InboundMessage) {
	var decodedData TPacket
	decodeErr := rlp.DecodeBytes(message.Data, &decodedData)

	notifyObservers(&ml.observersMu, observers, &DecodedInboundMessage[*TPacket]{
		Raw:       message,
		Decoded:   &decodedData,
		DecodeErr: decodeErr,
		PeerId:    PeerIdFromH512(message.PeerId),
	})
}

func (ml *messageListener) notifyPeerEventObservers(peerEvent *sentry.PeerEvent) {
	notifyObservers(&ml.observersMu, ml.peerEventObservers, peerEvent)
}

func (ml *messageListener) statusDataFactory() sentrymulticlient.StatusDataFactory {
	return func() *sentry.StatusData {
		// TODO add a "status data component" that message listener will use as a dependency to fetch status data
		//      "status data component" will be responsible for providing a mechanism to provide up-to-date status data
		return &sentry.StatusData{}
	}
}

func (ml *messageListener) nextObserverId() uint64 {
	id := ml.observerIdSequence
	ml.observerIdSequence++
	return id
}

func notifyObservers[TMessage any](mu *sync.Mutex, observers map[uint64]MessageObserver[TMessage], message TMessage) {
	mu.Lock()
	defer mu.Unlock()

	for _, observer := range observers {
		go observer(message)
	}
}

func unregisterFunc[TMessage any](mu *sync.Mutex, observers map[uint64]MessageObserver[TMessage], observerId uint64) UnregisterFunc {
	return func() {
		mu.Lock()
		defer mu.Unlock()

		delete(observers, observerId)
	}
}
