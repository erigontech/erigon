package p2p

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
)

type MessageBroadcaster interface {
	GetBlockHeaders66(ctx context.Context, peerId PeerId, req eth.GetBlockHeadersPacket66) error
}

func NewMessageBroadcaster(sentryClient direct.SentryClient) MessageBroadcaster {
	return &messageBroadcaster{
		sentryClient: sentryClient,
	}
}

type messageBroadcaster struct {
	sentryClient direct.SentryClient
}

func (mb *messageBroadcaster) GetBlockHeaders66(ctx context.Context, peerId PeerId, req eth.GetBlockHeadersPacket66) error {
	data, err := rlp.EncodeToBytes(req)
	if err != nil {
		return err
	}

	_, err = mb.sentryClient.SendMessageById(ctx, &sentry.SendMessageByIdRequest{
		PeerId: peerId.H512(),
		Data: &sentry.OutboundMessageData{
			Id:   sentry.MessageId_GET_BLOCK_HEADERS_66,
			Data: data,
		},
	})

	return err
}
