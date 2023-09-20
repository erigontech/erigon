/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package txpool

import (
	"context"
	"fmt"
	"sync"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/rlp"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
)

type SentryClient interface {
	sentry.SentryClient
	Protocol() uint
}

// Send - does send concrete P2P messages to Sentry. Same as Fetch but for outbound traffic
// does not initiate any messages by self
type Send struct {
	ctx           context.Context
	pool          Pool
	wg            *sync.WaitGroup
	sentryClients []direct.SentryClient // sentry clients that will be used for accessing the network
	logger        log.Logger
}

func NewSend(ctx context.Context, sentryClients []direct.SentryClient, pool Pool, logger log.Logger) *Send {
	return &Send{
		ctx:           ctx,
		pool:          pool,
		sentryClients: sentryClients,
		logger:        logger,
	}
}

func (f *Send) SetWaitGroup(wg *sync.WaitGroup) {
	f.wg = wg
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
func (f *Send) BroadcastPooledTxs(rlps [][]byte) (txSentTo []int) {
	defer f.notifyTests()
	if len(rlps) == 0 {
		return
	}
	txSentTo = make([]int, len(rlps))
	var prev, size int
	for i, l := 0, len(rlps); i < len(rlps); i++ {
		size += len(rlps[i])
		// Wait till the combined size of rlps so far is greater than a threshold and
		// send them all at once. Then wait till end of array or this threshold hits again
		if i == l-1 || size >= p2pTxPacketLimit {
			txsData := types2.EncodeTransactions(rlps[prev:i+1], nil)
			var txs66 *sentry.SendMessageToRandomPeersRequest
			for _, sentryClient := range f.sentryClients {
				if !sentryClient.Ready() {
					continue
				}
				if txs66 == nil {
					txs66 = &sentry.SendMessageToRandomPeersRequest{
						Data: &sentry.OutboundMessageData{
							Id:   sentry.MessageId_TRANSACTIONS_66,
							Data: txsData,
						},
						MaxPeers: 100,
					}
				}
				peers, err := sentryClient.SendMessageToRandomPeers(f.ctx, txs66)
				if err != nil {
					f.logger.Debug("[txpool.send] BroadcastPooledTxs", "err", err)
				}
				if peers != nil {
					for j := prev; j <= i; j++ {
						txSentTo[j] = len(peers.Peers)
					}
				}
			}
			prev = i + 1
			size = 0
		}
	}
	return
}

func (f *Send) AnnouncePooledTxs(types []byte, sizes []uint32, hashes types2.Hashes) (hashSentTo []int) {
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
			if !sentryClient.Ready() {
				continue
			}
			switch sentryClient.Protocol() {
			case direct.ETH66, direct.ETH67:
				if i > prevI {
					req := &sentry.OutboundMessageData{
						Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
						Data: iData,
					}
					peers, err := sentryClient.SendMessageToAll(f.ctx, req, &grpc.EmptyCallOption{})
					if err != nil {
						f.logger.Debug("[txpool.send] AnnouncePooledTxs", "err", err)
					}
					if peers != nil {
						for k := prevI; k < i; k += 32 {
							hashSentTo[k/32] += len(peers.Peers)
						}
					}
				}
			case direct.ETH68:

				if j > prevJ {
					req := &sentry.OutboundMessageData{
						Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
						Data: jData,
					}
					peers, err := sentryClient.SendMessageToAll(f.ctx, req, &grpc.EmptyCallOption{})
					if err != nil {
						f.logger.Debug("[txpool.send] AnnouncePooledTxs68", "err", err)
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

func (f *Send) PropagatePooledTxsToPeersList(peers []types2.PeerID, types []byte, sizes []uint32, hashes []byte) {
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
			if !sentryClient.Ready() {
				continue
			}

			for _, peer := range peers {
				switch sentryClient.Protocol() {
				case direct.ETH66, direct.ETH67:
					if i > prevI {
						req := &sentry.SendMessageByIdRequest{
							PeerId: peer,
							Data: &sentry.OutboundMessageData{
								Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
								Data: iData,
							},
						}
						if _, err := sentryClient.SendMessageById(f.ctx, req, &grpc.EmptyCallOption{}); err != nil {
							f.logger.Debug("[txpool.send] PropagatePooledTxsToPeersList", "err", err)
						}
					}
				case direct.ETH68:

					if j > prevJ {
						req := &sentry.SendMessageByIdRequest{
							PeerId: peer,
							Data: &sentry.OutboundMessageData{
								Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
								Data: jData,
							},
						}
						if _, err := sentryClient.SendMessageById(f.ctx, req, &grpc.EmptyCallOption{}); err != nil {
							f.logger.Debug("[txpool.send] PropagatePooledTxsToPeersList68", "err", err)
						}
					}

				}
			}
		}
		prevI = i
		prevJ = j
	}
}
