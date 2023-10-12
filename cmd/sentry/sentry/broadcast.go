package sentry

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"syscall"

	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

func (cs *MultiClient) PropagateNewBlockHashes(ctx context.Context, announces []headerdownload.Announce) {
	cs.lock.RLock()
	defer cs.lock.RUnlock()

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
		if !sentry.Ready() {
			continue
		}

		_, err = sentry.SendMessageToAll(ctx, &req66, &grpc.EmptyCallOption{})
		if err != nil {
			log.Error("propagateNewBlockHashes", "err", err)
		}
	}
}

func (cs *MultiClient) BroadcastNewBlock(ctx context.Context, header *types.Header, body *types.RawBody, td *big.Int) {
	cs.lock.RLock()
	defer cs.lock.RUnlock()

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
		if !sentry.Ready() {
			continue
		}

		_, err = sentry.SendMessageToRandomPeers(ctx, &req66, &grpc.EmptyCallOption{})
		if err != nil {
			if isPeerNotFoundErr(err) || networkTemporaryErr(err) {
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
func isPeerNotFoundErr(err error) bool {
	return strings.Contains(err.Error(), "peer not found")
}
