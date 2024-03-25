package p2p

import (
	"context"
	"errors"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
)

var ErrPeerNotFound = errors.New("peer not found")

type MessageSender interface {
	SendGetBlockHeaders(ctx context.Context, peerId *PeerId, req eth.GetBlockHeadersPacket66) error
	SendGetBlockBodies(ctx context.Context, peerId *PeerId, req eth.GetBlockBodiesPacket66) error
}

func NewMessageSender(sentryClient direct.SentryClient) MessageSender {
	return &messageSender{
		sentryClient: sentryClient,
	}
}

type messageSender struct {
	sentryClient direct.SentryClient
}

func (ms *messageSender) SendGetBlockHeaders(ctx context.Context, peerId *PeerId, req eth.GetBlockHeadersPacket66) error {
	return ms.sendMessage(ctx, sentry.MessageId_GET_BLOCK_HEADERS_66, req, peerId)
}

func (ms *messageSender) SendGetBlockBodies(ctx context.Context, peerId *PeerId, req eth.GetBlockBodiesPacket66) error {
	return ms.sendMessage(ctx, sentry.MessageId_GET_BLOCK_BODIES_66, req, peerId)
}

func (ms *messageSender) sendMessage(ctx context.Context, messageId sentry.MessageId, data any, peerId *PeerId) error {
	rlpData, err := rlp.EncodeToBytes(data)
	if err != nil {
		return err
	}

	sent, err := ms.sentryClient.SendMessageById(ctx, &sentry.SendMessageByIdRequest{
		PeerId: peerId.H512(),
		Data: &sentry.OutboundMessageData{
			Id:   messageId,
			Data: rlpData,
		},
	})
	if err != nil {
		return err
	}
	if len(sent.Peers) == 0 {
		return ErrPeerNotFound
	}

	return nil
}
