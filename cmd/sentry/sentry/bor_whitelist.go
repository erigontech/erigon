package sentry

import (
	"context"
	"math/rand"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/borfinality/generics"
	"github.com/ledgerwatch/erigon/eth/borfinality/whitelist"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
)

func validatePeer(ss *GrpcServer, peerID [64]byte, ctx context.Context) {
	service := whitelist.GetWhitelistingService()
	if service != nil {
		_, err := service.IsValidPeer(getFetchHeadersByNumber(ss, peerID, ctx))
		if err != nil {
			log.Info("Removing peer due to milestone mismatch", "err", err)
			ss.removePeer(peerID)
		}
	}
}

func getFetchHeadersByNumber(ss *GrpcServer, peerID [64]byte, ctx context.Context) func(number uint64, amount int, skip int, reverse bool) ([]*types.Header, []libcommon.Hash, error) {
	return func(number uint64, amount int, skip int, reverse bool) ([]*types.Header, []libcommon.Hash, error) {
		return fetchHeadersByNumber(ss, peerID, ctx, number, amount, skip, reverse)
	}
}

func fetchHeadersByNumber(ss *GrpcServer, peerID [64]byte, ctx context.Context, number uint64, amount int, skip int, reverse bool) ([]*types.Header, []libcommon.Hash, error) {
	reqID := rand.Uint64()
	b, err := rlp.EncodeToBytes(&eth.GetBlockHeadersPacket66{
		RequestId: reqID, // nolint: gosec
		GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
			Amount:  uint64(amount),
			Reverse: reverse,
			Skip:    uint64(skip),
			Origin:  eth.HashOrNumber{Number: number},
		},
	})

	if err != nil {
		log.Error("Could not encode header request", "err", err)
		return nil, nil, err
	}

	if _, err := ss.SendMessageById(ctx, &proto_sentry.SendMessageByIdRequest{
		PeerId: gointerfaces.ConvertHashToH512(peerID),
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_GET_BLOCK_HEADERS_66,
			Data: b,
		},
	}); err != nil {
		return nil, nil, err
	}

	if generics.BorMilestonePeerVerification == nil {
		generics.BorMilestonePeerVerification = make(map[uint64]chan generics.Response)
	}

	generics.BorMilestonePeerVerification[reqID] = make(chan generics.Response, 1)
	defer delete(generics.BorMilestonePeerVerification, reqID)

	response := <-generics.BorMilestonePeerVerification[reqID]

	return response.Headers, response.Hashes, nil
}
