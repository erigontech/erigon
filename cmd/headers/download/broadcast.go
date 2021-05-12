package download

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	proto_sentry "github.com/ledgerwatch/turbo-geth/gointerfaces/sentry"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
	"google.golang.org/grpc"
)

// Methods of sentry called by Core

func (cs *ControlServerImpl) PropagateNewBlockHashes(ctx context.Context, announces []headerdownload.Announce) {
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	typedRequest := make(eth.NewBlockHashesPacket, len(announces))
	for i := range announces {
		typedRequest[i].Hash = announces[i].Hash
		typedRequest[i].Number = announces[i].Number
	}
	data, err := rlp.EncodeToBytes(&typedRequest)
	if err != nil {
		log.Error("propagateNewBlockHashes", "error", err)
		return
	}
	req := &proto_sentry.OutboundMessageData{
		Id:   proto_sentry.MessageId_NewBlockHashes,
		Data: data,
	}
	for _, sentry := range cs.sentries {
		_, err = sentry.SendMessageToAll(ctx, req, &grpc.EmptyCallOption{})
		if err != nil {
			log.Error("propagateNewBlockHashes", "error", err)
		}
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
	for _, sentry := range cs.sentries {
		if _, err = sentry.SendMessageToRandomPeers(ctx, &req, &grpc.EmptyCallOption{}); err != nil {
			log.Error("broadcastNewBlock", "error", err)
		}
	}
}
