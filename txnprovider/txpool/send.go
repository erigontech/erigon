// Copyright 2021 The Erigon Authors
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

package txpool

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync"

	"google.golang.org/grpc"

	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/p2p/sentry"
	"github.com/erigontech/erigon-lib/rlp"
)

// Send - does send concrete P2P messages to Sentry. Same as Fetch but for outbound traffic
// does not initiate any messages by self
type Send struct {
	ctx           context.Context
	wg            *sync.WaitGroup
	sentryClients []sentryproto.SentryClient // sentry clients that will be used for accessing the network
	logger        log.Logger
}

func NewSend(ctx context.Context, sentryClients []sentryproto.SentryClient, logger log.Logger, opts ...Option) *Send {
	options := applyOpts(opts...)
	return &Send{
		ctx:           ctx,
		sentryClients: sentryClients,
		logger:        logger,
		wg:            options.p2pSenderWg,
	}
}

const (
	// This is the target size for the packs of transactions or announcements. A
	// pack can get larger than this if a single transactions exceeds this size.
	p2pTxPacketLimit = 100 * 1024
)

func (f *Send) notifyTests() {
	if f.wg != nil {
		f.wg.Done()
	}
}

// Broadcast given RLPs to random peers
func (f *Send) BroadcastPooledTxns(rlps [][]byte, maxPeers uint64) (txnSentTo []int) {
	defer f.notifyTests()
	if len(rlps) == 0 {
		return
	}
	txnSentTo = make([]int, len(rlps))
	var prev, size int
	for i, l := 0, len(rlps); i < len(rlps); i++ {
		size += len(rlps[i])
		// Wait till the combined size of rlps so far is greater than a threshold and
		// send them all at once. Then wait till end of array or this threshold hits again
		if i == l-1 || size >= p2pTxPacketLimit {
			txnsData := EncodeTransactions(rlps[prev:i+1], nil)
			var txns66 *sentryproto.SendMessageToRandomPeersRequest
			for _, sentryClient := range f.sentryClients {
				if ready, ok := sentryClient.(interface{ Ready() bool }); ok && !ready.Ready() {
					continue
				}
				if txns66 == nil {
					txns66 = &sentryproto.SendMessageToRandomPeersRequest{
						Data: &sentryproto.OutboundMessageData{
							Id:   sentryproto.MessageId_TRANSACTIONS_66,
							Data: txnsData,
						},
						MaxPeers: maxPeers,
					}
				}
				peers, err := sentryClient.SendMessageToRandomPeers(f.ctx, txns66)
				if err != nil {
					f.logger.Debug("[txpool.send] BroadcastPooledTxns", "err", err)
				}
				if peers != nil {
					for j := prev; j <= i; j++ {
						txnSentTo[j] = len(peers.Peers)
					}
				}
			}
			prev = i + 1
			size = 0
		}
	}
	return
}

func (f *Send) AnnouncePooledTxns(types []byte, sizes []uint32, hashes Hashes, maxPeers uint64) (hashSentTo []int) {
	defer f.notifyTests()
	hashSentTo = make([]int, len(types))
	if len(types) == 0 {
		return
	}
	prevI := 0
	prevJ := 0
	for prevI < len(hashes) || prevJ < len(types) {
		// Prepare two versions of the announcement message, one for pre-eth/68 peers, another for post-eth/68 peers
		i := prevI
		for i < len(hashes) && rlp.HashesLen(hashes[prevI:i+32]) < p2pTxPacketLimit {
			i += 32
		}
		j := prevJ
		for j < len(types) && rlp.AnnouncementsLen(types[prevJ:j+1], sizes[prevJ:j+1], hashes[32*prevJ:32*j+32]) < p2pTxPacketLimit {
			j++
		}
		iSize := rlp.HashesLen(hashes[prevI:i])
		jSize := rlp.AnnouncementsLen(types[prevJ:j], sizes[prevJ:j], hashes[32*prevJ:32*j])
		iData := make([]byte, iSize)
		jData := make([]byte, jSize)
		if s := rlp.EncodeHashes(hashes[prevI:i], iData); s != iSize {
			panic(fmt.Sprintf("Serialised hashes encoding len mismatch, expected %d, got %d", iSize, s))
		}
		if s := rlp.EncodeAnnouncements(types[prevJ:j], sizes[prevJ:j], hashes[32*prevJ:32*j], jData); s != jSize {
			panic(fmt.Sprintf("Serialised announcements encoding len mismatch, expected %d, got %d", jSize, s))
		}
		for _, sentryClient := range f.sentryClients {
			if ready, ok := sentryClient.(interface{ Ready() bool }); ok && !ready.Ready() {
				continue
			}

			protocols := sentry.Protocols(sentryClient)

			if len(protocols) == 0 {
				continue
			}

			var protocolIndex int

			if len(protocols) > 1 {
				protocolIndex = rand.Intn(len(protocols) - 1)
			}

			switch protocols[protocolIndex] {
			case 67:
				if i > prevI {
					req := &sentryproto.SendMessageToRandomPeersRequest{
						Data: &sentryproto.OutboundMessageData{
							Id:   sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
							Data: iData,
						},
						MaxPeers: maxPeers,
					}
					peers, err := sentryClient.SendMessageToRandomPeers(f.ctx, req)
					if err != nil {
						f.logger.Debug("[txpool.send] AnnouncePooledTxns", "err", err)
					}
					if peers != nil {
						for k := prevI; k < i; k += 32 {
							hashSentTo[k/32] += len(peers.Peers)
						}
					}
				}
			case 68:
				if j > prevJ {
					req := &sentryproto.SendMessageToRandomPeersRequest{
						Data: &sentryproto.OutboundMessageData{
							Id:   sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
							Data: jData,
						},
						MaxPeers: maxPeers,
					}
					peers, err := sentryClient.SendMessageToRandomPeers(f.ctx, req)
					if err != nil {
						f.logger.Debug("[txpool.send] AnnouncePooledTxns68", "err", err)
					}
					if peers != nil {
						for k := prevJ; k < j; k++ {
							hashSentTo[k] += len(peers.Peers)
						}
					}
				}

			}
		}
		prevI = i
		prevJ = j
	}
	return
}

func (f *Send) PropagatePooledTxnsToPeersList(peers []PeerID, types []byte, sizes []uint32, hashes []byte) {
	defer f.notifyTests()

	if len(types) == 0 {
		return
	}

	prevI := 0
	prevJ := 0
	for prevI < len(hashes) || prevJ < len(types) {
		// Prepare two versions of the annoucement message, one for pre-eth/68 peers, another for post-eth/68 peers
		i := prevI
		for i < len(hashes) && rlp.HashesLen(hashes[prevI:i+32]) < p2pTxPacketLimit {
			i += 32
		}
		j := prevJ
		for j < len(types) && rlp.AnnouncementsLen(types[prevJ:j+1], sizes[prevJ:j+1], hashes[32*prevJ:32*j+32]) < p2pTxPacketLimit {
			j++
		}
		iSize := rlp.HashesLen(hashes[prevI:i])
		jSize := rlp.AnnouncementsLen(types[prevJ:j], sizes[prevJ:j], hashes[32*prevJ:32*j])
		iData := make([]byte, iSize)
		jData := make([]byte, jSize)
		if s := rlp.EncodeHashes(hashes[prevI:i], iData); s != iSize {
			panic(fmt.Sprintf("Serialised hashes encoding len mismatch, expected %d, got %d", iSize, s))
		}
		if s := rlp.EncodeAnnouncements(types[prevJ:j], sizes[prevJ:j], hashes[32*prevJ:32*j], jData); s != jSize {
			panic(fmt.Sprintf("Serialised annoucements encoding len mismatch, expected %d, got %d", jSize, s))
		}

		for _, sentryClient := range f.sentryClients {
			if ready, ok := sentryClient.(interface{ Ready() bool }); ok && !ready.Ready() {
				continue
			}

			for _, peer := range peers {
				protocols := sentry.PeerProtocols(sentryClient, peer)
				if len(protocols) > 0 {
					switch slices.Max(protocols) {
					case 66, 67:
						if i > prevI {
							req := &sentryproto.SendMessageByIdRequest{
								PeerId: peer,
								Data: &sentryproto.OutboundMessageData{
									Id:   sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
									Data: iData,
								},
							}
							if _, err := sentryClient.SendMessageById(f.ctx, req, &grpc.EmptyCallOption{}); err != nil {
								f.logger.Debug("[txpool.send] PropagatePooledTxnsToPeersList", "err", err)
							}
						}
					case 68:

						if j > prevJ {
							req := &sentryproto.SendMessageByIdRequest{
								PeerId: peer,
								Data: &sentryproto.OutboundMessageData{
									Id:   sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
									Data: jData,
								},
							}
							if _, err := sentryClient.SendMessageById(f.ctx, req, &grpc.EmptyCallOption{}); err != nil {
								f.logger.Debug("[txpool.send] PropagatePooledTxnsToPeersList68", "err", err)
							}
						}

					}
				}
			}
		}
		prevI = i
		prevJ = j
	}
}
