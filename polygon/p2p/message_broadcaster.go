package p2p

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/direct"
	protosentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
)

type MessageBroadcaster interface {
	GetBlockHeaders66(ctx context.Context, pid PeerId, req eth.GetBlockHeadersPacket66) error
}

func NewMessageBroadcaster(sentryClient direct.SentryClient) MessageBroadcaster {
	return &messageBroadcaster{
		sentryClient: sentryClient,
	}
}

type messageBroadcaster struct {
	sentryClient direct.SentryClient
}

func (mb *messageBroadcaster) GetBlockHeaders66(ctx context.Context, pid PeerId, req eth.GetBlockHeadersPacket66) error {
	data, err := rlp.EncodeToBytes(req)
	if err != nil {
		return err
	}

	_, err = mb.sentryClient.SendMessageById(ctx, &protosentry.SendMessageByIdRequest{
		PeerId: pid.H512(),
		Data: &protosentry.OutboundMessageData{
			Id:   protosentry.MessageId_GET_BLOCK_HEADERS_66,
			Data: data,
		},
	})

	return err
}
