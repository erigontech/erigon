package p2p

import (
	"context"

	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	protosentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	sentrymulticlient "github.com/ledgerwatch/erigon/p2p/sentry/sentry_multi_client"
)

type messageListener struct {
	logger    log.Logger
	sentry    direct.SentryClient
	observers map[protosentry.MessageId]map[messageObserver]struct{}
}

func (ml *messageListener) Listen(ctx context.Context) {
	go ml.listenBlockHeaders66(ctx)
}

func (ml *messageListener) RegisterBlockHeaders66(observer messageObserver) {
	msgId := protosentry.MessageId_BLOCK_HEADERS_66
	if observers, ok := ml.observers[msgId]; ok {
		observers[observer] = struct{}{}
	} else {
		ml.observers[msgId] = map[messageObserver]struct{}{
			observer: {},
		}
	}
}

func (ml *messageListener) UnregisterBlockHeaders66(observer messageObserver) {
	msgId := protosentry.MessageId_BLOCK_HEADERS_66
	if observers, ok := ml.observers[msgId]; ok {
		delete(observers, observer)
	}
}

func (ml *messageListener) listenBlockHeaders66(ctx context.Context) {
	ml.listenInboundMessage(ctx, "BlockHeaders66", protosentry.MessageId_BLOCK_HEADERS_66)
}

func (ml *messageListener) listenInboundMessage(ctx context.Context, name string, msgId protosentry.MessageId) {
	sentrymulticlient.SentryReconnectAndPumpStreamLoop(
		ctx,
		ml.sentry,
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
	observers, ok := ml.observers[msg.Id]
	if !ok {
		return
	}

	for observer := range observers {
		go observer.Notify(msg)
	}
}
