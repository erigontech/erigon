package download

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	proto_sentry "github.com/ledgerwatch/turbo-geth/gointerfaces/sentry"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"google.golang.org/grpc"
)

func (cs *ControlServerImpl) PropagateNewBlockHashes(ctx context.Context, hash common.Hash, number uint64) {
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	data, err := rlp.EncodeToBytes(&eth.NewBlockHashesPacket{
		{
			Hash:   hash,
			Number: number,
		},
	})
	if err != nil {
		log.Error("propagateNewBlockHashes", "error", err)
		return
	}
	req := &proto_sentry.OutboundMessageData{
		Id:   proto_sentry.MessageId_NewBlockHashes,
		Data: data,
	}
	_, err = cs.sentryClient.SendMessageToAll(ctx, req, &grpc.EmptyCallOption{})
	if err != nil {
		log.Error("propagateNewBlockHashes", "error", err)
	}
}

func (cs *ControlServerImpl) BroadcastNewBlock(ctx context.Context, block *types.Block, td *big.Int) {
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	data, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: block,
		TD:    td,
	})
	if err != nil {
		log.Error("broadcastNewBlock", "error", err)
	}
	req := proto_sentry.SendMessageToRandomPeersRequest{
		MaxPeers: 1024,
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_NewBlock,
			Data: data,
		},
	}
	if _, err = cs.sentryClient.SendMessageToRandomPeers(ctx, &req, &grpc.EmptyCallOption{}); err != nil {
		log.Error("broadcastNewBlock", "error", err)
	}
}
