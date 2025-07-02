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
	"encoding/hex"
	"math/rand"

	"google.golang.org/grpc"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/execution/stages/bodydownload"
	"github.com/erigontech/erigon/execution/stages/headerdownload"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/sentry"
)

// Methods of sentry called by Core

func (cs *MultiClient) SetStatus(ctx context.Context) {
	statusMsg, err := cs.statusDataProvider.GetStatusData(ctx)
	if err != nil {
		cs.logger.Error("MultiClient.SetStatus: GetStatusData error", "err", err)
		return
	}

	for _, sentry := range cs.sentries {
		if ready, ok := sentry.(interface{ Ready() bool }); ok && !ready.Ready() {
			continue
		}

		if _, err := sentry.SetStatus(ctx, statusMsg, &grpc.EmptyCallOption{}); err != nil {
			cs.logger.Error("Update status message for the sentry", "err", err)
		}
	}
}

func (cs *MultiClient) SendBodyRequest(ctx context.Context, req *bodydownload.BodyRequest) (peerID [64]byte, ok bool) {
	// if sentry not found peers to send such message, try next one. stop if found.
	for i, ok, next := cs.randSentryIndex(); ok; i, ok = next() {
		if ready, ok := cs.sentries[i].(interface{ Ready() bool }); ok && !ready.Ready() {
			continue
		}

		//log.Info(fmt.Sprintf("Sending body request for %v", req.BlockNums))
		var bytes []byte
		var err error
		bytes, err = rlp.EncodeToBytes(&eth.GetBlockBodiesPacket66{
			RequestId:            rand.Uint64(), // nolint: gosec
			GetBlockBodiesPacket: req.Hashes,
		})
		if err != nil {
			cs.logger.Error("Could not encode block bodies request", "err", err)
			return [64]byte{}, false
		}

		var sentPeers *proto_sentry.SentPeers
		if cs.shadowFork {
			// ######################
			// # TWEAK FOR PERFNET2 #
			// ######################
			outreq := proto_sentry.OutboundMessageData{
				Id:   proto_sentry.MessageId_GET_BLOCK_BODIES_66,
				Data: bytes,
			}

			sentPeers, err = cs.sentries[i].SendMessageToAll(ctx, &outreq, &grpc.EmptyCallOption{})
		} else {
			outreq := proto_sentry.SendMessageByMinBlockRequest{
				MinBlock: req.BlockNums[len(req.BlockNums)-1],
				Data: &proto_sentry.OutboundMessageData{
					Id:   proto_sentry.MessageId_GET_BLOCK_BODIES_66,
					Data: bytes,
				},
				MaxPeers: 1,
			}

			sentPeers, err = cs.sentries[i].SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
		}
		if err != nil {
			cs.logger.Error("Could not send block bodies request", "err", err)
			return [64]byte{}, false
		}
		if sentPeers == nil || len(sentPeers.Peers) == 0 {
			var fromNum, toNum uint64
			if len(req.BlockNums) > 0 {
				fromNum, toNum = req.BlockNums[0], req.BlockNums[len(req.BlockNums)-1]
			}
			var fromHash, toHash common.Hash
			if len(req.Hashes) > 0 {
				fromHash, toHash = req.Hashes[0], req.Hashes[len(req.Hashes)-1]
			}
			cs.logger.Trace(
				"body request not sent to any peers",
				"fromNum", fromNum,
				"fromHash", fromHash,
				"toNum", toNum,
				"toHash", toHash,
			)
			continue
		}
		go func() {
			var fromNum, toNum uint64
			if len(req.BlockNums) > 0 {
				fromNum, toNum = req.BlockNums[0], req.BlockNums[len(req.BlockNums)-1]
			}
			var fromHash, toHash common.Hash
			if len(req.Hashes) > 0 {
				fromHash, toHash = req.Hashes[0], req.Hashes[len(req.Hashes)-1]
			}
			for _, p := range sentPeers.Peers {
				pid := sentry.ConvertH512ToPeerID(p)
				cs.logger.Trace(
					"body request sent to peer",
					"fromNum", fromNum,
					"fromHash", fromHash,
					"toNum", toNum,
					"toHash", toHash,
					"peer", hex.EncodeToString(pid[:]),
				)
			}
		}()
		return sentry.ConvertH512ToPeerID(sentPeers.Peers[0]), true
	}
	return [64]byte{}, false
}

func (cs *MultiClient) SendHeaderRequest(ctx context.Context, req *headerdownload.HeaderRequest) (peerID [64]byte, ok bool) {
	// if sentry not found peers to send such message, try next one. stop if found.
	for i, ok, next := cs.randSentryIndex(); ok; i, ok = next() {
		if ready, ok := cs.sentries[i].(interface{ Ready() bool }); ok && !ready.Ready() {
			continue
		}
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
			cs.logger.Error("Could not encode header request", "err", err)
			return [64]byte{}, false
		}

		var sentPeers *proto_sentry.SentPeers
		if cs.shadowFork {
			// ######################
			// # TWEAK FOR PERFNET2 #
			// ######################
			outreq := proto_sentry.OutboundMessageData{
				Id:   proto_sentry.MessageId_GET_BLOCK_HEADERS_66,
				Data: bytes,
			}

			sentPeers, err = cs.sentries[i].SendMessageToAll(ctx, &outreq, &grpc.EmptyCallOption{})
		} else {
			minBlock := req.Number
			outreq := proto_sentry.SendMessageByMinBlockRequest{
				MinBlock: minBlock,
				Data: &proto_sentry.OutboundMessageData{
					Id:   proto_sentry.MessageId_GET_BLOCK_HEADERS_66,
					Data: bytes,
				},
				MaxPeers: 5,
			}

			sentPeers, err = cs.sentries[i].SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
		}
		if err != nil {
			cs.logger.Error("Could not send header request", "err", err)
			return [64]byte{}, false
		}
		if sentPeers == nil || len(sentPeers.Peers) == 0 {
			cs.logger.Trace(
				"header request not sent to any peers",
				"height", req.Number,
				"hash", req.Hash,
				"length", req.Length,
				"reverse", req.Reverse,
			)
			continue
		}
		go func() {
			for _, p := range sentPeers.Peers {
				pid := sentry.ConvertH512ToPeerID(p)
				cs.logger.Trace(
					"header request sent to peer",
					"height", req.Number,
					"hash", req.Hash,
					"length", req.Length,
					"reverse", req.Reverse,
					"peer", hex.EncodeToString(pid[:]),
				)
			}
		}()
		return sentry.ConvertH512ToPeerID(sentPeers.Peers[0]), true
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
			if ready, ok := cs.sentries[i].(interface{ Ready() bool }); ok && !ready.Ready() {
				continue
			}

			if _, err1 := cs.sentries[i].PenalizePeer(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
				cs.logger.Error("Could not send penalty", "err", err1)
			}
		}
	}
}
