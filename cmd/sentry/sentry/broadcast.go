package sentry

import (
	"bytes"
	"context"
	"errors"
	"math"
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

// Methods of sentry called by Core

const (
	// This is the target size for the packs of transactions or announcements. A
	// pack can get larger than this if a single transactions exceeds this size.
	maxTxPacketSize = 100 * 1024
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
	var req66 *proto_sentry.OutboundMessageData
	// Send the block to a subset of our peers
	sendToAmount := int(math.Sqrt(float64(len(cs.sentries))))
	for i, sentry := range cs.sentries {
		if !sentry.Ready() {
			continue
		}
		if i > sendToAmount { //TODO: send to random sentries, not just to fi
			break
		}

		if req66 == nil {
			req66 = &proto_sentry.OutboundMessageData{
				Id:   proto_sentry.MessageId_NEW_BLOCK_HASHES_66,
				Data: data,
			}

			_, err = sentry.SendMessageToAll(ctx, req66, &grpc.EmptyCallOption{})
			if err != nil {
				log.Error("propagateNewBlockHashes", "err", err)
			}
		}
	}
}

func (cs *MultiClient) BroadcastNewBlock(ctx context.Context, header *types.Header, body *types.RawBody, td *big.Int) {
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	txs := make([]types.Transaction, len(body.Transactions))
	for i, tx := range body.Transactions {
		var err error
		if txs[i], err = types.DecodeTransaction(rlp.NewStream(bytes.NewReader(tx), 0)); err != nil {
			log.Error("broadcastNewBlock", "err", err)
			return
		}
	}
	data, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: types.NewBlock(header, txs, body.Uncles, nil, body.Withdrawals),
		TD:    td,
	})
	if err != nil {
		log.Error("broadcastNewBlock", "err", err)
	}
	var req66 *proto_sentry.SendMessageToRandomPeersRequest
	// Send the block to a subset of our peers
	sendToAmount := int(math.Sqrt(float64(len(cs.sentries))))
	for i, sentry := range cs.sentries {
		if !sentry.Ready() {
			continue
		}
		if i > sendToAmount { //TODO: send to random sentries, not just to fi
			break
		}

		if req66 == nil {
			req66 = &proto_sentry.SendMessageToRandomPeersRequest{
				MaxPeers: 1024,
				Data: &proto_sentry.OutboundMessageData{
					Id:   proto_sentry.MessageId_NEW_BLOCK_66,
					Data: data,
				},
			}
		}
		if _, err = sentry.SendMessageToRandomPeers(ctx, req66, &grpc.EmptyCallOption{}); err != nil {
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
