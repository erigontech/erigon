package p2p

import (
	"context"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
)

var ErrPeerNotFound = errors.New("peer not found")

type MessageSender interface {
	SendGetBlockHeaders(ctx context.Context, peerId PeerId, req eth.GetBlockHeadersPacket66) error
}

func NewMessageSender(sentryClient direct.SentryClient) MessageSender {
	return &messageSender{
		sentryClient: sentryClient,
	}
}

type messageSender struct {
	sentryClient direct.SentryClient
}

func (ms *messageSender) SendGetBlockHeaders(ctx context.Context, peerId PeerId, req eth.GetBlockHeadersPacket66) error {
	data, err := rlp.EncodeToBytes(req)
	if err != nil {
		return err
	}

	sent, err := ms.sentryClient.SendMessageById(ctx, &sentry.SendMessageByIdRequest{
		PeerId: peerId.H512(),
		Data: &sentry.OutboundMessageData{
			Id:   sentry.MessageId_GET_BLOCK_HEADERS_66,
			Data: data,
		},
	})
	if err != nil {
		return err
	}
	if len(sent.Peers) == 0 {
		return fmt.Errorf("%w", ErrPeerNotFound)
	}

	return nil
}
