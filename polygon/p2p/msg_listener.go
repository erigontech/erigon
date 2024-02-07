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

type msgListener struct {
	logger    log.Logger
	sentry    direct.SentryClient
	observers map[protosentry.MessageId]map[msgObserver]struct{}
}

func (ml *msgListener) Listen(ctx context.Context) {
	go ml.listenBlockHeaders66(ctx)
}

func (ml *msgListener) RegisterBlockHeaders66(observer msgObserver) {
	msgId := protosentry.MessageId_BLOCK_HEADERS_66
	if observers, ok := ml.observers[msgId]; ok {
		observers[observer] = struct{}{}
	} else {
		ml.observers[msgId] = map[msgObserver]struct{}{
			observer: {},
		}
	}
}

func (ml *msgListener) UnregisterBlockHeaders66(observer msgObserver) {
	msgId := protosentry.MessageId_BLOCK_HEADERS_66
	if observers, ok := ml.observers[msgId]; ok {
		delete(observers, observer)
	}
}

func (ml *msgListener) listenBlockHeaders66(ctx context.Context) {
	ml.listenInboundMsg(ctx, "BlockHeaders66", protosentry.MessageId_BLOCK_HEADERS_66)
}

func (ml *msgListener) listenInboundMsg(ctx context.Context, name string, msgIds ...protosentry.MessageId) {
	sentrymulticlient.SentryReconnectAndPumpStreamLoop(
		ctx,
		ml.sentry,
		ml.statusDataFactory(),
		name,
		ml.streamFactory(msgIds),
		sentrymulticlient.MakeInboundMessage,
		ml.handleInboundMsg,
		nil,
		ml.logger,
	)
}

func (ml *msgListener) handleInboundMsg(_ context.Context, msg *protosentry.InboundMessage, _ direct.SentryClient) error {
	ml.notifyObservers(msg)
	return nil
}

func (ml *msgListener) notifyObservers(msg *protosentry.InboundMessage) {
	observers, ok := ml.observers[msg.Id]
	if !ok {
		return
	}

	for observer := range observers {
		go observer.Notify(msg)
	}
}

func (ml *msgListener) statusDataFactory() sentrymulticlient.StatusDataFactory {
	return func() *sentry.StatusData {
		return &sentry.StatusData{}
	}
}

func (ml *msgListener) streamFactory(ids []protosentry.MessageId) sentrymulticlient.SentryMessageStreamFactory {
	return func(streamCtx context.Context, sentry direct.SentryClient) (sentrymulticlient.SentryMessageStream, error) {
		return sentry.Messages(streamCtx, &protosentry.MessagesRequest{Ids: ids}, grpc.WaitForReady(true))
	}
}
