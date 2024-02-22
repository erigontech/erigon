package p2p

import (
	"context"
	"sync"

	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	sentrymulticlient "github.com/ledgerwatch/erigon/p2p/sentry/sentry_multi_client"
)

type MessageListener interface {
	Start(ctx context.Context)
	Stop()
	RegisterBlockHeadersObserver(observer MessageObserver[*sentry.InboundMessage])
	UnregisterBlockHeadersObserver(observer MessageObserver[*sentry.InboundMessage])
	RegisterPeerEventObserver(observer MessageObserver[*sentry.PeerEvent])
	UnregisterPeerEventObserver(observer MessageObserver[*sentry.PeerEvent])
}

func NewMessageListener(logger log.Logger, sentryClient direct.SentryClient) MessageListener {
	return &messageListener{
		logger:                  logger,
		sentryClient:            sentryClient,
		inboundMessageObservers: map[sentry.MessageId]map[MessageObserver[*sentry.InboundMessage]]struct{}{},
		peerEventObservers:      map[MessageObserver[*sentry.PeerEvent]]struct{}{},
	}
}

type messageListener struct {
	once                    sync.Once
	streamCtx               context.Context
	streamCtxCancel         context.CancelFunc
	logger                  log.Logger
	sentryClient            direct.SentryClient
	observersMu             sync.Mutex
	inboundMessageObservers map[sentry.MessageId]map[MessageObserver[*sentry.InboundMessage]]struct{}
	peerEventObservers      map[MessageObserver[*sentry.PeerEvent]]struct{}
	stopWg                  sync.WaitGroup
}

func (ml *messageListener) Start(ctx context.Context) {
	ml.once.Do(func() {
		ml.streamCtx, ml.streamCtxCancel = context.WithCancel(ctx)

		backgroundLoops := []func(){
			ml.listenBlockHeaders66,
			ml.listenPeerEvents,
		}

		ml.stopWg.Add(len(backgroundLoops))
		for _, loop := range backgroundLoops {
			go loop()
		}
	})
}

func (ml *messageListener) Stop() {
	ml.streamCtxCancel()
	ml.stopWg.Wait()

	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	ml.inboundMessageObservers = map[sentry.MessageId]map[MessageObserver[*sentry.InboundMessage]]struct{}{}
	ml.peerEventObservers = map[MessageObserver[*sentry.PeerEvent]]struct{}{}
}

func (ml *messageListener) RegisterBlockHeadersObserver(observer MessageObserver[*sentry.InboundMessage]) {
	ml.registerInboundMessageObserver(observer, sentry.MessageId_BLOCK_HEADERS_66)
}

func (ml *messageListener) UnregisterBlockHeadersObserver(observer MessageObserver[*sentry.InboundMessage]) {
	ml.unregisterInboundMessageObserver(observer, sentry.MessageId_BLOCK_HEADERS_66)
}

func (ml *messageListener) RegisterPeerEventObserver(observer MessageObserver[*sentry.PeerEvent]) {
	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	ml.peerEventObservers[observer] = struct{}{}
}

func (ml *messageListener) UnregisterPeerEventObserver(observer MessageObserver[*sentry.PeerEvent]) {
	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	delete(ml.peerEventObservers, observer)
}

func (ml *messageListener) registerInboundMessageObserver(observer MessageObserver[*sentry.InboundMessage], messageId sentry.MessageId) {
	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	if observers, ok := ml.inboundMessageObservers[messageId]; ok {
		observers[observer] = struct{}{}
	} else {
		ml.inboundMessageObservers[messageId] = map[MessageObserver[*sentry.InboundMessage]]struct{}{
			observer: {},
		}
	}
}

func (ml *messageListener) unregisterInboundMessageObserver(observer MessageObserver[*sentry.InboundMessage], messageId sentry.MessageId) {
	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	if observers, ok := ml.inboundMessageObservers[messageId]; ok {
		delete(observers, observer)
	}
}

func (ml *messageListener) listenBlockHeaders66() {
	ml.listenInboundMessage("BlockHeaders66", sentry.MessageId_BLOCK_HEADERS_66)
}

func (ml *messageListener) listenInboundMessage(name string, msgId sentry.MessageId) {
	defer ml.stopWg.Done()

	sentrymulticlient.SentryReconnectAndPumpStreamLoop(
		ml.streamCtx,
		ml.sentryClient,
		ml.statusDataFactory(),
		name,
		ml.messageStreamFactory([]sentry.MessageId{msgId}),
		ml.inboundMessageFactory(),
		ml.inboundMessageHandler(),
		nil,
		ml.logger,
	)
}

func (ml *messageListener) statusDataFactory() sentrymulticlient.StatusDataFactory {
	return func() *sentry.StatusData {
		// TODO add a "status data component" that message listener will use as a dependency to fetch status data
		//      "status data component" will be responsible for providing a mechanism to provide up-to-date status data
		return &sentry.StatusData{}
	}
}

func (ml *messageListener) messageStreamFactory(ids []sentry.MessageId) sentrymulticlient.SentryMessageStreamFactory {
	return func(streamCtx context.Context, sentryClient direct.SentryClient) (sentrymulticlient.SentryMessageStream, error) {
		return sentryClient.Messages(streamCtx, &sentry.MessagesRequest{Ids: ids}, grpc.WaitForReady(true))
	}
}

func (ml *messageListener) inboundMessageFactory() sentrymulticlient.MessageFactory[*sentry.InboundMessage] {
	return func() *sentry.InboundMessage {
		return new(sentry.InboundMessage)
	}
}

func (ml *messageListener) inboundMessageHandler() sentrymulticlient.MessageHandler[*sentry.InboundMessage] {
	return func(_ context.Context, msg *sentry.InboundMessage, _ direct.SentryClient) error {
		ml.notifyInboundMessageObservers(msg)
		return nil
	}
}

func (ml *messageListener) notifyInboundMessageObservers(msg *sentry.InboundMessage) {
	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	observers, ok := ml.inboundMessageObservers[msg.Id]
	if !ok {
		return
	}

	for observer := range observers {
		go observer.Notify(msg)
	}
}

func (ml *messageListener) listenPeerEvents() {
	defer ml.stopWg.Done()

	sentrymulticlient.SentryReconnectAndPumpStreamLoop(
		ml.streamCtx,
		ml.sentryClient,
		ml.statusDataFactory(),
		"PeerEvents",
		ml.peerEventStreamFactory(),
		ml.peerEventMessageFactory(),
		ml.peerEventMessageHandler(),
		nil,
		ml.logger,
	)
}

func (ml *messageListener) peerEventStreamFactory() sentrymulticlient.SentryMessageStreamFactory {
	return func(streamCtx context.Context, sentryClient direct.SentryClient) (sentrymulticlient.SentryMessageStream, error) {
		return sentryClient.PeerEvents(streamCtx, &sentry.PeerEventsRequest{}, grpc.WaitForReady(true))
	}
}

func (ml *messageListener) peerEventMessageFactory() sentrymulticlient.MessageFactory[*sentry.PeerEvent] {
	return func() *sentry.PeerEvent {
		return new(sentry.PeerEvent)
	}
}

func (ml *messageListener) peerEventMessageHandler() sentrymulticlient.MessageHandler[*sentry.PeerEvent] {
	return func(_ context.Context, peerEvent *sentry.PeerEvent, _ direct.SentryClient) error {
		ml.notifyPeerEventObservers(peerEvent)
		return nil
	}
}

func (ml *messageListener) notifyPeerEventObservers(peerEvent *sentry.PeerEvent) {
	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	for observer := range ml.peerEventObservers {
		go observer.Notify(peerEvent)
	}
}
