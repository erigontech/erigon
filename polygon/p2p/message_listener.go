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

type DecodedInboundMessage[T any] struct {
	Raw       *sentry.InboundMessage
	Decoded   T
	DecodeErr error
}

type MessageObserver[T any] func(message T)

type UnregisterFunc func()

type MessageListener interface {
	Start(ctx context.Context)
	Stop()
	RegisterBlockHeadersObserver(observer MessageObserver[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]) UnregisterFunc
	RegisterPeerEventObserver(observer MessageObserver[*sentry.PeerEvent]) UnregisterFunc
}

func NewMessageListener(logger log.Logger, sentryClient direct.SentryClient) MessageListener {
	return &messageListener{
		logger:                logger,
		sentryClient:          sentryClient,
		blockHeadersObservers: map[uint64]MessageObserver[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]{},
		peerEventObservers:    map[uint64]MessageObserver[*sentry.PeerEvent]{},
	}
}

type messageListener struct {
	once                  sync.Once
	observerIdSequence    uint64
	logger                log.Logger
	sentryClient          direct.SentryClient
	observersMu           sync.Mutex
	blockHeadersObservers map[uint64]MessageObserver[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]
	peerEventObservers    map[uint64]MessageObserver[*sentry.PeerEvent]
	stopWg                sync.WaitGroup
}

func (ml *messageListener) Start(ctx context.Context) {
	ml.once.Do(func() {
		backgroundLoops := []func(ctx context.Context){
			ml.listenBlockHeaders66,
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

	ml.blockHeadersObservers = map[uint64]MessageObserver[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]{}
	ml.peerEventObservers = map[uint64]MessageObserver[*sentry.PeerEvent]{}
}

func (ml *messageListener) RegisterBlockHeadersObserver(observer MessageObserver[*DecodedInboundMessage[*eth.BlockHeadersPacket66]]) UnregisterFunc {
	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	observerId := ml.nextObserverId()
	ml.blockHeadersObservers[observerId] = observer
	return unregisterFunc(&ml.observersMu, ml.blockHeadersObservers, observerId)
}

func (ml *messageListener) RegisterPeerEventObserver(observer MessageObserver[*sentry.PeerEvent]) UnregisterFunc {
	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	observerId := ml.nextObserverId()
	ml.peerEventObservers[observerId] = observer
	return unregisterFunc(&ml.observersMu, ml.peerEventObservers, observerId)
}

func (ml *messageListener) listenBlockHeaders66(ctx context.Context) {
	ml.listenInboundMessage(ctx, "BlockHeaders66", sentry.MessageId_BLOCK_HEADERS_66, ml.notifyBlockHeadersMessageObservers)
}

func (ml *messageListener) listenInboundMessage(ctx context.Context, name string, msgId sentry.MessageId, handler func(msg *sentry.InboundMessage)) {
	defer ml.stopWg.Done()

	messageStreamFactory := func(ctx context.Context, sentryClient direct.SentryClient) (sentrymulticlient.SentryMessageStream, error) {
		messagesRequest := sentry.MessagesRequest{
			Ids: []sentry.MessageId{msgId},
		}

		return sentryClient.Messages(ctx, &messagesRequest, grpc.WaitForReady(true))
	}

	inboundMessageFactory := func() *sentry.InboundMessage {
		return new(sentry.InboundMessage)
	}

	inboundMessageHandler := func(_ context.Context, msg *sentry.InboundMessage, _ direct.SentryClient) error {
		handler(msg)
		return nil
	}

	sentrymulticlient.SentryReconnectAndPumpStreamLoop(
		ctx,
		ml.sentryClient,
		ml.statusDataFactory(),
		name,
		messageStreamFactory,
		inboundMessageFactory,
		inboundMessageHandler,
		nil,
		ml.logger,
	)
}

func (ml *messageListener) notifyBlockHeadersMessageObservers(message *sentry.InboundMessage) {
	var decodedData eth.BlockHeadersPacket66
	decodeErr := rlp.DecodeBytes(message.Data, &decodedData)

	notifyObservers(&ml.observersMu, ml.blockHeadersObservers, &DecodedInboundMessage[*eth.BlockHeadersPacket66]{
		Raw:       message,
		Decoded:   &decodedData,
		DecodeErr: decodeErr,
	})
}

func (ml *messageListener) listenPeerEvents(ctx context.Context) {
	defer ml.stopWg.Done()

	peerEventStreamFactory := func(ctx context.Context, sentryClient direct.SentryClient) (sentrymulticlient.SentryMessageStream, error) {
		return sentryClient.PeerEvents(ctx, &sentry.PeerEventsRequest{}, grpc.WaitForReady(true))
	}

	peerEventMessageFactory := func() *sentry.PeerEvent {
		return new(sentry.PeerEvent)
	}

	peerEventMessageHandler := func(_ context.Context, peerEvent *sentry.PeerEvent, _ direct.SentryClient) error {
		ml.notifyPeerEventObservers(peerEvent)
		return nil
	}

	sentrymulticlient.SentryReconnectAndPumpStreamLoop(
		ctx,
		ml.sentryClient,
		ml.statusDataFactory(),
		"PeerEvents",
		peerEventStreamFactory,
		peerEventMessageFactory,
		peerEventMessageHandler,
		nil,
		ml.logger,
	)
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

func notifyObservers[T any](mu *sync.Mutex, observers map[uint64]MessageObserver[T], message T) {
	mu.Lock()
	defer mu.Unlock()

	for _, observer := range observers {
		go observer(message)
	}
}

func unregisterFunc[T any](mu *sync.Mutex, observers map[uint64]MessageObserver[T], observerId uint64) UnregisterFunc {
	return func() {
		mu.Lock()
		defer mu.Unlock()

		delete(observers, observerId)
	}
}
