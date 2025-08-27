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

package sentry_multi_client

import (
	"context"
	"errors"
	"math/big"
	"syscall"

	"google.golang.org/grpc"

	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stages/headerdownload"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

func (cs *MultiClient) PropagateNewBlockHashes(ctx context.Context, announces []headerdownload.Announce) {
	typedRequest := make(eth.NewBlockHashesPacket, len(announces))
	for i := range announces {
		typedRequest[i].Hash = announces[i].Hash
		typedRequest[i].Number = announces[i].Number
	}

	data, err := rlp.EncodeToBytes(&typedRequest)
	if err != nil {
		log.Error("propagateNewBlockHashes", "err", err)
		return
	}

	req66 := proto_sentry.OutboundMessageData{
		Id:   proto_sentry.MessageId_NEW_BLOCK_HASHES_66,
		Data: data,
	}

	for _, sentry := range cs.sentries {
		if ready, ok := sentry.(interface{ Ready() bool }); ok && !ready.Ready() {
			continue
		}

		_, err = sentry.SendMessageToAll(ctx, &req66, &grpc.EmptyCallOption{})
		if err != nil {
			log.Error("propagateNewBlockHashes", "err", err)
		}
	}
}

func (cs *MultiClient) BroadcastNewBlock(ctx context.Context, header *types.Header, body *types.RawBody, td *big.Int) {
	block, err := types.RawBlock{Header: header, Body: body}.AsBlock()

	if err != nil {
		log.Error("broadcastNewBlock", "err", err)
	}

	data, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: block,
		TD:    td,
	})

	if err != nil {
		log.Error("broadcastNewBlock", "err", err)
		return
	}

	req66 := proto_sentry.SendMessageToRandomPeersRequest{
		MaxPeers: uint64(cs.maxBlockBroadcastPeers(header)),
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_NEW_BLOCK_66,
			Data: data,
		},
	}

	for _, sentry := range cs.sentries {
		if ready, ok := sentry.(interface{ Ready() bool }); ok && !ready.Ready() {
			continue
		}

		_, err = sentry.SendMessageToRandomPeers(ctx, &req66, &grpc.EmptyCallOption{})
		if err != nil {
			if libsentry.IsPeerNotFoundErr(err) || networkTemporaryErr(err) {
				log.Debug("broadcastNewBlock", "err", err)
				continue
			}
			log.Error("broadcastNewBlock", "err", err)
		}
	}
}

func networkTemporaryErr(err error) bool {
	return errors.Is(err, syscall.EPIPE) || errors.Is(err, p2p.ErrShuttingDown)
}
