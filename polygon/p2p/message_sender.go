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

	sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	libsentry "github.com/erigontech/erigon-lib/p2p/sentry"
	"github.com/erigontech/erigon/eth/protocols/eth"
	"github.com/erigontech/erigon/rlp"
)

var ErrPeerNotFound = errors.New("peer not found")

type MessageSender interface {
	SendGetBlockHeaders(ctx context.Context, peerId *PeerId, req eth.GetBlockHeadersPacket66) error
	SendGetBlockBodies(ctx context.Context, peerId *PeerId, req eth.GetBlockBodiesPacket66) error
}

func NewMessageSender(sentryClient sentry.SentryClient) MessageSender {
	return &messageSender{
		sentryClient: sentryClient,
	}
}

type messageSender struct {
	sentryClient sentry.SentryClient
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
		if libsentry.IsPeerNotFoundErr(err) {
			return fmt.Errorf("%w: %s", ErrPeerNotFound, peerId.String())
		}
		return err
	}
	if len(sent.Peers) == 0 {
		return fmt.Errorf("%w: %s", ErrPeerNotFound, peerId.String())
	}

	return nil
}
