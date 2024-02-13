package p2p

import (
	"context"
	"sync"

	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	protosentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	sentrymulticlient "github.com/ledgerwatch/erigon/p2p/sentry/sentry_multi_client"
)

type MessageListener interface {
	Start(ctx context.Context)
	Stop()
	RegisterBlockHeaders66(observer messageObserver)
	UnregisterBlockHeaders66(observer messageObserver)
}

func NewMessageListener(logger log.Logger, sentryClient direct.SentryClient) MessageListener {
	return &messageListener{
		logger:       logger,
		sentryClient: sentryClient,
		observers:    map[protosentry.MessageId]map[messageObserver]struct{}{},
	}
}

type messageListener struct {
	once            sync.Once
	streamCtx       context.Context
	streamCtxCancel context.CancelFunc
	logger          log.Logger
	sentryClient    direct.SentryClient
	observersMu     sync.Mutex
	observers       map[protosentry.MessageId]map[messageObserver]struct{}
	stopWg          sync.WaitGroup
}

func (ml *messageListener) Start(ctx context.Context) {
	ml.once.Do(func() {
		ml.streamCtx, ml.streamCtxCancel = context.WithCancel(ctx)
		go ml.listenBlockHeaders66()
	})
}

func (ml *messageListener) Stop() {
	ml.streamCtxCancel()
	ml.stopWg.Wait()
}

func (ml *messageListener) RegisterBlockHeaders66(observer messageObserver) {
	ml.register(observer, protosentry.MessageId_BLOCK_HEADERS_66)
}

func (ml *messageListener) UnregisterBlockHeaders66(observer messageObserver) {
	ml.unregister(observer, protosentry.MessageId_BLOCK_HEADERS_66)
}

func (ml *messageListener) register(observer messageObserver, messageId protosentry.MessageId) {
	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	if observers, ok := ml.observers[messageId]; ok {
		observers[observer] = struct{}{}
	} else {
		ml.observers[messageId] = map[messageObserver]struct{}{
			observer: {},
		}
	}
}

func (ml *messageListener) unregister(observer messageObserver, messageId protosentry.MessageId) {
	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	if observers, ok := ml.observers[messageId]; ok {
		delete(observers, observer)
	}
}

func (ml *messageListener) listenBlockHeaders66() {
	ml.listenInboundMessage("BlockHeaders66", protosentry.MessageId_BLOCK_HEADERS_66)
}

func (ml *messageListener) listenInboundMessage(name string, msgId protosentry.MessageId) {
	ml.stopWg.Add(1)
	defer ml.stopWg.Done()

	sentrymulticlient.SentryReconnectAndPumpStreamLoop(
		ml.streamCtx,
		ml.sentryClient,
		ml.statusDataFactory(),
		name,
		ml.messageStreamFactory([]protosentry.MessageId{msgId}),
		ml.inboundMessageFactory(),
		ml.handleInboundMessageHandler(),
		nil,
		ml.logger,
	)
}

func (ml *messageListener) statusDataFactory() sentrymulticlient.StatusDataFactory {
	return func() *sentry.StatusData {
		return &sentry.StatusData{}
	}
}

func (ml *messageListener) messageStreamFactory(ids []protosentry.MessageId) sentrymulticlient.SentryMessageStreamFactory {
	return func(streamCtx context.Context, sentry direct.SentryClient) (sentrymulticlient.SentryMessageStream, error) {
		return sentry.Messages(streamCtx, &protosentry.MessagesRequest{Ids: ids}, grpc.WaitForReady(true))
	}
}

func (ml *messageListener) inboundMessageFactory() sentrymulticlient.MessageFactory[*protosentry.InboundMessage] {
	return func() *protosentry.InboundMessage {
		return new(protosentry.InboundMessage)
	}
}

func (ml *messageListener) handleInboundMessageHandler() sentrymulticlient.InboundMessageHandler[*protosentry.InboundMessage] {
	return func(_ context.Context, msg *protosentry.InboundMessage, _ direct.SentryClient) error {
		ml.notifyObservers(msg)
		return nil
	}
}

func (ml *messageListener) notifyObservers(msg *protosentry.InboundMessage) {
	ml.observersMu.Lock()
	defer ml.observersMu.Unlock()

	observers, ok := ml.observers[msg.Id]
	if !ok {
		return
	}

	for observer := range observers {
		go observer.Notify(msg)
	}
}
