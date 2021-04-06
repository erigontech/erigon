package download

import (
	"context"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	proto_sentry "github.com/ledgerwatch/turbo-geth/gointerfaces/sentry"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
	"google.golang.org/grpc"
)

// Methods of sentry called by Core

func (cs *ControlServerImpl) updateHead(ctx context.Context, height uint64, hash common.Hash, td *uint256.Int) {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	cs.headHeight = height
	cs.headHash = hash
	cs.headTd = td
	statusMsg := &proto_sentry.StatusData{
		NetworkId:       cs.networkId,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(cs.headTd),
		BestHash:        gointerfaces.ConvertHashToH256(cs.headHash),
		MaxBlock:        cs.headHeight,
		ForkData: &proto_sentry.Forks{
			Genesis: gointerfaces.ConvertHashToH256(cs.genesisHash),
			Forks:   cs.forks,
		},
	}
	for _, sentry := range cs.sentries {
		if _, err := sentry.SetStatus(ctx, statusMsg, &grpc.EmptyCallOption{}); err != nil {
			log.Error("Update status message for the sentry", "error", err)
		}
	}
}

func (cs *ControlServerImpl) sendBodyRequest(ctx context.Context, req *bodydownload.BodyRequest) []byte {
	//log.Info(fmt.Sprintf("Sending body request for %v", req.BlockNums))
	var bytes []byte
	var err error
	bytes, err = rlp.EncodeToBytes(req.Hashes)
	if err != nil {
		log.Error("Could not encode block bodies request", "err", err)
		return nil
	}
	outreq := proto_sentry.SendMessageByMinBlockRequest{
		MinBlock: req.BlockNums[len(req.BlockNums)-1],
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_GetBlockBodies,
			Data: bytes,
		},
	}
	sentPeers, err1 := cs.sentries[0].SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
	if err1 != nil {
		log.Error("Could not send block bodies request", "err", err1)
		return nil
	}
	if sentPeers == nil || len(sentPeers.Peers) == 0 {
		return nil
	}
	return gointerfaces.ConvertH512ToBytes(sentPeers.Peers[0])
}

func (cs *ControlServerImpl) penalise(ctx context.Context, peer []byte) {
	penalizeReq := proto_sentry.PenalizePeerRequest{PeerId: gointerfaces.ConvertBytesToH512(peer), Penalty: proto_sentry.PenaltyKind_Kick}
	for _, sentry := range cs.sentries {
		if _, err := sentry.PenalizePeer(ctx, &penalizeReq, &grpc.EmptyCallOption{}); err != nil {
			log.Error("Could not penalise", "peer", peer, "error", err)
		}
	}
}
