package sentry

import (
	"context"
	"math/rand"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
)

// Methods of sentry called by Core

func (cs *MultiClient) UpdateHead(ctx context.Context, height uint64, hash common.Hash, td *uint256.Int) {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	cs.headHeight = height
	cs.headHash = hash
	cs.headTd = td
	statusMsg := cs.makeStatusData()
	for _, sentry := range cs.sentries {
		if !sentry.Ready() {
			continue
		}

		if _, err := sentry.SetStatus(ctx, statusMsg, &grpc.EmptyCallOption{}); err != nil {
			log.Error("Update status message for the sentry", "err", err)
		}
	}
}

func (cs *MultiClient) SendBodyRequest(ctx context.Context, req *bodydownload.BodyRequest) (peerID [64]byte, ok bool) {
	// if sentry not found peers to send such message, try next one. stop if found.
	for i, ok, next := cs.randSentryIndex(); ok; i, ok = next() {
		if !cs.sentries[i].Ready() {
			continue
		}

		switch cs.sentries[i].Protocol() {
		case eth.ETH66, eth.ETH67:
			//log.Info(fmt.Sprintf("Sending body request for %v", req.BlockNums))
			var bytes []byte
			var err error
			bytes, err = rlp.EncodeToBytes(&eth.GetBlockBodiesPacket66{
				RequestId:            rand.Uint64(), // nolint: gosec
				GetBlockBodiesPacket: req.Hashes,
			})
			if err != nil {
				log.Error("Could not encode block bodies request", "err", err)
				return [64]byte{}, false
			}
			outreq := proto_sentry.SendMessageByMinBlockRequest{
				MinBlock: req.BlockNums[len(req.BlockNums)-1],
				Data: &proto_sentry.OutboundMessageData{
					Id:   proto_sentry.MessageId_GET_BLOCK_BODIES_66,
					Data: bytes,
				},
				MaxPeers: 1,
			}

			sentPeers, err1 := cs.sentries[i].SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
			if err1 != nil {
				log.Error("Could not send block bodies request", "err", err1)
				return [64]byte{}, false
			}
			if sentPeers == nil || len(sentPeers.Peers) == 0 {
				continue
			}
			return ConvertH512ToPeerID(sentPeers.Peers[0]), true
		}
	}
	return [64]byte{}, false
}

func (cs *MultiClient) SendHeaderRequest(ctx context.Context, req *headerdownload.HeaderRequest) (peerID [64]byte, ok bool) {
	// if sentry not found peers to send such message, try next one. stop if found.
	for i, ok, next := cs.randSentryIndex(); ok; i, ok = next() {
		if !cs.sentries[i].Ready() {
			continue
		}
		switch cs.sentries[i].Protocol() {
		case eth.ETH66, eth.ETH67:
			//log.Info(fmt.Sprintf("Sending header request {hash: %x, height: %d, length: %d}", req.Hash, req.Number, req.Length))
			reqData := &eth.GetBlockHeadersPacket66{
				RequestId: rand.Uint64(), // nolint: gosec
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
				return [64]byte{}, false
			}
			minBlock := req.Number

			var maxPeers uint64
			if cs.passivePeers {
				maxPeers = 5
			} else {
				maxPeers = 1
			}
			outreq := proto_sentry.SendMessageByMinBlockRequest{
				MinBlock: minBlock,
				Data: &proto_sentry.OutboundMessageData{
					Id:   proto_sentry.MessageId_GET_BLOCK_HEADERS_66,
					Data: bytes,
				},
				MaxPeers: maxPeers,
			}
			sentPeers, err1 := cs.sentries[i].SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
			if err1 != nil {
				log.Error("Could not send header request", "err", err1)
				return [64]byte{}, false
			}
			if sentPeers == nil || len(sentPeers.Peers) == 0 {
				continue
			}
			return ConvertH512ToPeerID(sentPeers.Peers[0]), true
		}
	}
	return [64]byte{}, false
}

func (cs *MultiClient) randSentryIndex() (int, bool, func() (int, bool)) {
	var i int
	if len(cs.sentries) > 1 {
		i = rand.Intn(len(cs.sentries) - 1) // nolint: gosec
	}
	to := i
	return i, true, func() (int, bool) {
		i = (i + 1) % len(cs.sentries)
		return i, i != to
	}
}

// sending list of penalties to all sentries
func (cs *MultiClient) Penalize(ctx context.Context, penalties []headerdownload.PenaltyItem) {
	for i := range penalties {
		outreq := proto_sentry.PenalizePeerRequest{
			PeerId:  gointerfaces.ConvertHashToH512(penalties[i].PeerID),
			Penalty: proto_sentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
		}
		for i, ok, next := cs.randSentryIndex(); ok; i, ok = next() {
			if !cs.sentries[i].Ready() {
				continue
			}

			if _, err1 := cs.sentries[i].PenalizePeer(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
				log.Error("Could not send penalty", "err", err1)
			}
		}
	}
}
