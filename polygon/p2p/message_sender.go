// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package p2p

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/p2p/sentry"
	"github.com/erigontech/erigon/eth/protocols/eth"
	"github.com/erigontech/erigon/rlp"
)

var ErrPeerNotFound = errors.New("peer not found")

func NewMessageSender(sentryClient sentryproto.SentryClient) *MessageSender {
	return &MessageSender{
		sentryClient: sentryClient,
	}
}

type MessageSender struct {
	sentryClient sentryproto.SentryClient
}

func (ms *MessageSender) SendGetBlockHeaders(ctx context.Context, peerId *PeerId, req eth.GetBlockHeadersPacket66) error {
	return ms.sendMessageToPeer(ctx, sentryproto.MessageId_GET_BLOCK_HEADERS_66, req, peerId)
}

func (ms *MessageSender) SendGetBlockBodies(ctx context.Context, peerId *PeerId, req eth.GetBlockBodiesPacket66) error {
	return ms.sendMessageToPeer(ctx, sentryproto.MessageId_GET_BLOCK_BODIES_66, req, peerId)
}

func (ms *MessageSender) SendNewBlockHashes(ctx context.Context, peerId *PeerId, req eth.NewBlockHashesPacket) error {
	return ms.sendMessageToPeer(ctx, sentryproto.MessageId_NEW_BLOCK_HASHES_66, req, peerId)
}

func (ms *MessageSender) SendNewBlock(ctx context.Context, peerId *PeerId, req eth.NewBlockPacket) error {
	return ms.sendMessageToPeer(ctx, sentryproto.MessageId_NEW_BLOCK_66, req, peerId)
}

func (ms *MessageSender) sendMessageToPeer(ctx context.Context, messageId sentryproto.MessageId, data any, peerId *PeerId) error {
	rlpData, err := rlp.EncodeToBytes(data)
	if err != nil {
		return err
	}

	sent, err := ms.sentryClient.SendMessageById(ctx, &sentryproto.SendMessageByIdRequest{
		PeerId: peerId.H512(),
		Data: &sentryproto.OutboundMessageData{
			Id:   messageId,
			Data: rlpData,
		},
	})
	if err != nil {
		if sentry.IsPeerNotFoundErr(err) {
			return fmt.Errorf("%w: %s", ErrPeerNotFound, peerId.String())
		}
		return err
	}
	if len(sent.Peers) == 0 {
		return fmt.Errorf("%w: %s", ErrPeerNotFound, peerId.String())
	}

	return nil
}
