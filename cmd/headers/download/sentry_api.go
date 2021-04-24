package download

import (
	"context"
	"math/rand"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	proto_sentry "github.com/ledgerwatch/turbo-geth/gointerfaces/sentry"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
	"google.golang.org/grpc"
)

// Methods of sentry called by Core

func (cs *ControlServerImpl) updateHead(ctx context.Context, height uint64, hash common.Hash, td *uint256.Int) {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	cs.headHeight = height
	cs.headHash = hash
	cs.headTd = td
	statusMsg := makeStatusData(cs)
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
	reqData := &eth.GetBlockBodiesPacket66{
		RequestId:            rand.Uint64(),
		GetBlockBodiesPacket: req.Hashes,
	}
	bytes, err = rlp.EncodeToBytes(reqData)
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

	// if sentry not found peers to send such message, try next one. stop if found.
	for i, ok, next := cs.randSentryIndex(); ok; i, ok = next() {
		sentPeers, err1 := cs.sentries[i].SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
		if err1 != nil {
			log.Error("Could not send block bodies request", "err", err1)
			return nil
		}
		if sentPeers == nil || len(sentPeers.Peers) == 0 {
			continue
		}
		return gointerfaces.ConvertH512ToBytes(sentPeers.Peers[0])
	}
	return nil
}

func (cs *ControlServerImpl) sendHeaderRequest(ctx context.Context, req *headerdownload.HeaderRequest) []byte {
	//log.Info(fmt.Sprintf("Sending header request {hash: %x, height: %d, length: %d}", req.Hash, req.Number, req.Length))
	reqData := &eth.GetBlockHeadersPacket66{
		RequestId: rand.Uint64(),
		GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
			Amount:  req.Length,
			Reverse: req.Reverse,
			Skip:    req.Skip,
			Origin:  eth.HashOrNumber{Hash: req.Hash},
		},
	}
	if req.Hash == (common.Hash{}) {
		reqData.Origin.Number = req.Number
	}
	bytes, err := rlp.EncodeToBytes(reqData)
	if err != nil {
		log.Error("Could not encode header request", "err", err)
		return nil
	}
	minBlock := req.Number
	if !req.Reverse {
		minBlock = req.Number + req.Length*req.Skip
	}
	outreq := proto_sentry.SendMessageByMinBlockRequest{
		MinBlock: minBlock,
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_GetBlockHeaders,
			Data: bytes,
		},
	}

	// if sentry not found peers to send such message, try next one. stop if found.
	for i, ok, next := cs.randSentryIndex(); ok; i, ok = next() {
		sentPeers, err1 := cs.sentries[i].SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
		if err1 != nil {
			log.Error("Could not send header request", "err", err1)
			return nil
		}
		if sentPeers == nil || len(sentPeers.Peers) == 0 {
			continue
		}
		return gointerfaces.ConvertH512ToBytes(sentPeers.Peers[0])
	}
	return nil
}

func (cs *ControlServerImpl) randSentryIndex() (int, bool, func() (int, bool)) {
	var i int
	if len(cs.sentries) > 1 {
		i = rand.Intn(len(cs.sentries) - 1)
	}
	to := i
	return i, true, func() (int, bool) {
		i = (i + 1) % len(cs.sentries)
		return i, i != to
	}
}

func (cs *ControlServerImpl) penalise(ctx context.Context, peer []byte) {
	penalizeReq := proto_sentry.PenalizePeerRequest{PeerId: gointerfaces.ConvertBytesToH512(peer), Penalty: proto_sentry.PenaltyKind_Kick}
	for _, sentry := range cs.sentries {
		if _, err := sentry.PenalizePeer(ctx, &penalizeReq, &grpc.EmptyCallOption{}); err != nil {
			log.Error("Could not penalise", "peer", peer, "error", err)
		}
	}
}
